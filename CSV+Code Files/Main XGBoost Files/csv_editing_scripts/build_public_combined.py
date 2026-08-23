"""Build the public counterpart of batting_pitching_combined_with_rpi_2026_eada.csv.

The same matrix, with the FanGraphs player columns replaced by the NCAA-derived
ones in ``CSV+Code Files/ncaa_public/``. Nothing team-level changes, because none
of it ever depended on FanGraphs: all 100 ``_team``/``_eada_team`` columns and the
five draft-label columns key off the FanGraphs *acronym*, and
``ncaa_scraper/mapping/acronym.py`` resolves that for 100% of D1 teams in every
year.

The team block is built as a **team-season table** keyed on (team, year) and then
joined onto player rows. That is sound because all 105 team-level and identity
columns are constant within (team, year) in the private file -- verified, zero
exceptions across 1,783 team-seasons -- so they are team attributes that happen to
be stored per player. Building them once and joining is both cheaper and harder to
get subtly wrong than re-deriving them per row.

``--verify`` then compares the public team-season table against the private file
column by column. That is the check that matters: if the public build reproduces
those 100 columns exactly, the only real difference between the two matrices is the
player block, which is what the whole exercise set out to replace.

Scope: 2021-2026. 2026 was added on 2026-08-22 from a live scrape of
stats.ncaa.org, not from the public mirror -- the mirror stopped updating
mid-season and its 2026 holds ~60% of a season. It covers all 308 D1
team-seasons. See ``ncaa_scraper/config.PUBLIC_YEARS``.

Usage:
    python csv_editing_scripts/build_public_combined.py [--verify]
"""

import argparse
import json
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent                      # Main XGBoost Files
CSV_ROOT = ROOT.parent                  # CSV+Code Files
PUBLIC_DIR = CSV_ROOT / "ncaa_public"
STANDARDIZED = CSV_ROOT / "standardized"

OUTPUT_PATH = ROOT / "batting_pitching_combined_with_rpi_public_v2.csv"
FULL_OUTPUT_PATH = ROOT / "batting_pitching_combined_with_rpi_public_v2_nomin.csv"
PRIVATE_REFERENCE = ROOT / "batting_pitching_combined_with_rpi_2026_eada.csv"
# Frozen column order, so the build never needs the private file. Optional inputs
# used only by --verify / --refit-qualification are the ONLY paywalled touchpoints.
PUBLIC_SCHEMA = HERE / "public_matrix_columns.json"

YEARS = [2021, 2022, 2023, 2024, 2025, 2026]

# The player-season key, against the private build's
# ["name","team","age","nameascii","playerid","mlbamid","year"]:
#   `age`     -> `class`      NCAA publishes no date of birth
#   `mlbamid` -> dropped      no NCAA equivalent
#   `person_id` added         NCAA re-mints `playerid` every season, so it cannot
#                             group a player across years
MERGE_KEYS = ["name", "team", "class", "nameascii", "playerid", "person_id", "year"]

# `class` is the public stand-in for FanGraphs' `age`, and the models need a number.
# Kept identical to ncaa_scraper/config.CLASS_ORDINAL. An unknown class ('---',
# 2-19 rows per year) stays NaN rather than becoming a 0 that would read as
# "younger than a freshman".
CLASS_ORDINAL = {"Fr": 1, "So": 2, "Jr": 3, "Sr": 4, "Gr": 5}

# Qualification thresholds, in stat per team game. Fitted once against the
# FanGraphs qualified-vs-no-minimum exports and frozen here so the build needs no
# paywalled input -- a requirement for anyone reproducing this from public sources
# alone. Measured agreement with FanGraphs' own qualified set at these values:
# batting 99.53%, pitching 99.67%. Re-derive with --refit-qualification.
QUALIFICATION_RATES = {"batting": 2.70, "pitching": 0.80}

# The no-minimum panel that add_tenure.py counts first appearances from. It is the
# *unqualified* population on purpose: a player's first qualified season is not
# their first season, and the difference correlates with how good they were.
PUBLIC_PANEL = [
    (str(PUBLIC_DIR / "batting_combined_all.csv"), None),
    (str(PUBLIC_DIR / "pitching_combined_all.csv"), None),
]

# Metadata that exists on both sides of the batting x pitching merge and should
# collapse to one column rather than becoming _bat/_pitch pairs.
SHARED_METADATA = [
    "Acronym", "Full Team Name", "team_old", "team_new",
    "Round", "Pick", "Drafted By", "Drafted From", "Drafted?",
]


def log(message=""):
    print(message, flush=True)


def clean(text):
    """The normalisation masterDraft.py uses, so school matching behaves alike."""
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return ""
    text = str(text).lower().replace("&", "and")
    text = re.sub(r"\buniv\.?\b", "university", text)
    text = re.sub(r"[^a-z0-9 ]", " ", text)
    return " ".join(text.split())


def ip_to_true(value):
    """NCAA thirds notation (97.2 = 97 2/3) to true innings."""
    if pd.isna(value):
        return np.nan
    whole = int(float(value))
    tenths = round(float(value) - whole, 1)
    if tenths == 0.1:
        return whole + 1.0 / 3.0
    if tenths == 0.2:
        return whole + 2.0 / 3.0
    return float(whole)


# ---------------------------------------------------------------------------
# Player data
# ---------------------------------------------------------------------------

def load_public():
    batting = pd.read_csv(PUBLIC_DIR / "batting_combined_all.csv", low_memory=False)
    pitching = pd.read_csv(PUBLIC_DIR / "pitching_combined_all.csv", low_memory=False)
    for name, frame in (("batting", batting), ("pitching", pitching)):
        years = sorted(int(y) for y in frame["year"].unique())
        if years != YEARS:
            raise SystemExit(
                f"{name}: expected years {YEARS}, found {years}. The public "
                f"dataset covers {YEARS[0]}-{YEARS[-1]}; see "
                f"ncaa_scraper/config.PUBLIC_YEARS.")
    log(f"  {len(batting):,} batting and {len(pitching):,} pitching player-seasons")
    return batting, pitching


def add_qualification(batting, pitching, refit=False):
    """Flag the rows FanGraphs' "qualified" leaderboard would have kept.

    FanGraphs' qualification is not a flat cut: in 2025, 648 unqualified players sit
    above the qualified minimum plate appearances (2023: 518), and the per-year
    minima wander (PA 42/100/107/106/98; IP 11.1/32.2/29.0/32.1/37.1). That is a
    per-team-game rate rule.

    The rates in QUALIFICATION_RATES were fitted once against the FanGraphs
    qualified-vs-no-minimum exports and are now **frozen constants**, so this build
    needs no paywalled input: anyone with the public player files reproduces the
    same population. `--refit-qualification` re-derives them for anyone who does
    have the FanGraphs exports, and prints the agreement.

    The flags are advisory -- the full population is kept either way, since Stage 1
    undersamples and the pre-draft simulation scores everyone. Filtering on
    `qualified_bat` is what reproduces the private matrix's population.
    """
    # Team games from BOTH frames. Batting alone leaves 8 team-seasons with a
    # null team_g -- the 7 legacy-patched 2022 teams carry `_absent={"g"}` by
    # design, and Texas Southern 2021 has no batting grid at all -- and a null
    # makes every qualification comparison False, silently dropping all their
    # players (250 player-seasons, 4 real draftees) from the qualified matrix.
    _g = pd.concat([batting[["team", "year", "g"]],
                    pitching[["team", "year", "g"]]], ignore_index=True)
    team_games = (_g.dropna(subset=["g"])
                    .groupby(["team", "year"])["g"].max().rename("team_g"))
    pitching = pitching.copy()
    pitching["ip_true"] = pitching["ip"].map(ip_to_true)

    def fit(frame, stat_column, folder, filename):
        """Choose the rate that best reproduces FanGraphs' qualified set."""
        qualified = CSV_ROOT / folder[0] / filename
        nomin = CSV_ROOT / folder[1] / filename
        if not (qualified.exists() and nomin.exists()):
            return None, None
        q = pd.read_csv(qualified, low_memory=False)
        n = pd.read_csv(nomin, low_memory=False)
        keys = set(zip(q["playerid"], q["year"]))
        n = n[n["year"].isin(YEARS)].copy()
        n["_qual"] = [(p, y) in keys for p, y in zip(n["playerid"], n["year"])]
        n = n.merge(team_games, left_on=["team", "year"], right_index=True,
                    how="left")
        stat = (n[stat_column] if stat_column in n.columns
                else n["ip"].map(ip_to_true))
        stat = pd.to_numeric(stat, errors="coerce")
        best = (None, -1.0)
        for rate in np.arange(0.30, 3.05, 0.05):
            predicted = stat >= n["team_g"] * rate
            agreement = float((predicted == n["_qual"]).mean())
            if agreement > best[1]:
                best = (round(float(rate), 2), agreement)
        return best

    plans = [
        ("batting", batting, "pa", "qualified_bat", QUALIFICATION_RATES["batting"],
         ("ncaa_battingQualifiedCSV", "ncaa_battingNoMinCSV"),
         "batting_combined_all.csv"),
        ("pitching", pitching, "ip_true", "qualified_pitch", QUALIFICATION_RATES["pitching"],
         ("ncaa_pitchingQualifiedCSV", "ncaa_pitchingNoMinCSV"),
         "pitching_combined_all.csv"),
    ]
    out = {}
    for label, frame, stat_column, flag, default, folder, filename in plans:
        rate, agreement = fit(frame, stat_column, folder, filename) if refit else (None, None)
        if rate is None:
            rate = default
            log(f"  {label}: frozen rate {rate} per team game "
                f"(fitted once against FanGraphs; --refit-qualification re-derives)")
        else:
            log(f"  {label}: REFITTED to {rate} per team game, "
                f"{agreement:.2%} agreement with FanGraphs' qualified set")
        merged = frame.merge(team_games, left_on=["team", "year"],
                             right_index=True, how="left")
        stat = pd.to_numeric(merged[stat_column], errors="coerce")
        frame[flag] = (stat >= merged["team_g"] * rate).fillna(False).values
        log(f"    -> {int(frame[flag].sum()):,} of {len(frame):,} flagged "
            f"({frame[flag].mean():.1%})")
        out[label] = frame
    return out["batting"], out["pitching"]


# ---------------------------------------------------------------------------
# Team identity and the team-season table
# ---------------------------------------------------------------------------

def team_identity():
    """(Acronym) -> Full Team Name, team_old, team_new, id_team, division_team."""
    teams = pd.read_csv(STANDARDIZED / "unique_teams.csv")
    teams.columns = teams.columns.str.strip()
    mapping = pd.read_csv(STANDARDIZED / "team_name_mapping.csv")
    mapping.columns = mapping.columns.str.strip()
    for column in ("team_old", "team_new"):
        mapping[column] = mapping[column].astype(str).str.strip()
    mapping = mapping[mapping["division"] == 1]

    frame = teams.merge(mapping, left_on="Full Name", right_on="team_new",
                        how="left")
    frame = frame.rename(columns={"Full Name": "Full Team Name",
                                  "team_id": "id_team",
                                  "division": "division_team"})
    return frame[["Acronym", "Full Team Name", "team_old", "team_new",
                  "id_team", "division_team"]]


def conference_ranks():
    """(conference, year) -> the four conf_*_team columns.

    `league_team` uses NCAA's short conference names while conference_rankings.csv
    uses long ones; missing_conf.csv is the crosswalk, and it is season-bounded
    because conferences rebrand (CAA -> Coastal Athletic).
    """
    ranks = pd.read_csv(STANDARDIZED / "conference_rankings.csv")
    ranks.columns = ranks.columns.str.strip()
    ranks["conference"] = ranks["conference"].astype(str).str.strip()
    ranks = ranks.rename(columns={
        "conf-rpi": "conf_rpi_team", "conf-rank": "conf_rank_team",
        "conf-national-rec": "conf_national_rec_team",
        "conf-national-wp": "conf_national_wp_team",
    })

    crosswalk = pd.read_csv(STANDARDIZED / "missing_conf.csv")
    crosswalk.columns = crosswalk.columns.str.strip()
    for column in ("league name", "conf name"):
        crosswalk[column] = crosswalk[column].astype(str).str.strip()
    return ranks, crosswalk


def build_team_season(batting, pitching):
    """One row per (team, year) carrying every team-level column.

    The team list is the union of both categories, not batting alone. Texas
    Southern's 2021 batting grid exists in no public source while its pitching
    does, so a batting-only list would silently leave those 15 pitchers with an
    empty team block.
    """
    identity = team_identity()
    pairs = (pd.concat([batting[["team", "year"]], pitching[["team", "year"]]],
                       ignore_index=True).drop_duplicates()
             .rename(columns={"team": "Acronym"}))
    frame = pairs.merge(identity, on="Acronym", how="left")

    unresolved = frame["Full Team Name"].isna()
    if unresolved.any():
        raise SystemExit(
            f"{int(unresolved.sum())} team-season(s) have no Full Team Name; "
            f"unmapped acronyms: {sorted(frame.loc[unresolved, 'Acronym'].unique())}. "
            f"Add them to standardized/unique_teams.csv.")

    # --- ncaabb team season stats, on (team_old, year) -----------------------
    ncaabb = pd.read_csv(CSV_ROOT / "ncaabb_dataset.csv")
    ncaabb.columns = ncaabb.columns.str.strip()
    ncaabb["team"] = ncaabb["team"].astype(str).str.strip()
    value_columns = [c for c in ncaabb.columns if c not in ("team", "year")]
    renames = {c: ("league_team" if c == "league" else f"{c}_team")
               for c in value_columns}
    ncaabb = ncaabb.rename(columns=renames)

    frame = frame.merge(ncaabb, left_on=["team_old", "year"],
                        right_on=["team", "year"], how="left",
                        suffixes=("", "_teamstats"))
    frame = frame.rename(columns={"team": "team_teamstats"})

    missing_stats = frame["W_team"].isna().sum()
    if missing_stats:
        sample = sorted(frame.loc[frame["W_team"].isna(), "Acronym"].unique())[:8]
        log(f"  ! {missing_stats} team-season(s) have no ncaabb row: {sample}")

    # `Full Name_team` duplicates `Full Team Name`; the private file carries both,
    # the first from masterTeam's second pass over unique_teams and the second from
    # masterDraft's first.
    frame["Full Name_team"] = frame["Full Team Name"]

    # --- Pythagorean expectation as a share of actual win rate --------------
    # Derived in the notebook rather than any script, so it is recomputed here.
    # A winless team has no Pythagorean ratio, not an infinite one. errstate
    # silences the warning but keeps the inf, and 57 rows carried one (MVSU
    # 2021, UMES 2024). derive/rates._safe exists precisely to avoid this:
    # "inf propagates silently through a model, while None is visibly missing."
    with np.errstate(divide="ignore", invalid="ignore"):
        frame["PE_pct_team"] = (frame["PE_team"]
                                / frame["WPCT_team"].replace(0, np.nan))
    frame["PE_pct_team"] = frame["PE_pct_team"].replace([np.inf, -np.inf], np.nan)

    # add_team_eada.build_crosswalk keys on `team` (the acronym) and reads
    # `Full Name_team`, so present both under the names it expects.
    frame["team"] = frame["Acronym"]

    log(f"  built {len(frame):,} team-seasons with {frame.shape[1]} columns")
    return frame


def add_conference_ranks(frame):
    """Attach the four conf_*_team columns.

    Runs after `league_team` is final -- including the RPI backfill -- because a
    team with no conference cannot be given a conference ranking.
    """
    ranks, crosswalk = conference_ranks()
    long_name = {}
    for row in crosswalk.itertuples():
        start = getattr(row, "start_year", 0) or 0
        end = getattr(row, "end_year", 9999) or 9999
        for year in YEARS:
            if start <= year <= end:
                long_name[(getattr(row, "_1"), year)] = getattr(row, "_2")
    frame["_conference"] = [
        long_name.get((league, year), league)
        for league, year in zip(frame["league_team"], frame["year"])
    ]
    frame = frame.merge(
        ranks[["conference", "year", "conf_rpi_team", "conf_rank_team",
               "conf_national_rec_team", "conf_national_wp_team"]],
        left_on=["_conference", "year"], right_on=["conference", "year"],
        how="left").drop(columns=["conference", "_conference"])

    missing = frame["conf_rpi_team"].isna().sum()
    if missing:
        sample = sorted(frame.loc[frame["conf_rpi_team"].isna(),
                                  "league_team"].dropna().unique())[:8]
        log(f"  {missing} team-season(s) still have no conference rank: {sample}")
    return frame


def add_rpi_and_eada(team_season):
    """Run the existing RPI and EADA joins over the team-season table.

    Imported rather than reimplemented: add_team_rpi carries 120 hand-verified
    name mappings plus year-specific rebrand overrides, and add_team_eada carries
    the IPEDS crosswalk and the carry-forward rule. Both were made
    path-overridable for this.
    """
    sys.path.insert(0, str(HERE))
    import add_team_eada
    import add_team_rpi

    # add_team_eada rebuilds its crosswalk from whatever team list it is given and
    # writes it to a module-level path. Pointed elsewhere here, because our list
    # has no 2026 and would otherwise silently drop the 2026-only programs (New
    # Haven) from the crosswalk the private pipeline depends on.
    add_team_eada.CROSSWALK_PATH = HERE / "eada_crosswalk_public.csv"

    scratch_in = ROOT / "_public_team_season_tmp.csv"
    scratch_rpi = ROOT / "_public_team_season_rpi_tmp.csv"
    scratch_eada = ROOT / "_public_team_season_eada_tmp.csv"

    team_season.to_csv(scratch_in, index=False)
    log("  RPI:")
    add_team_rpi.main(input_path=scratch_in, output_path=scratch_rpi)

    # league_team must be final before the conference ranks join, and the RPI
    # files are what fill it for teams ncaabb_dataset does not carry.
    staged = backfill_league(pd.read_csv(scratch_rpi, low_memory=False))
    staged = add_conference_ranks(staged)
    staged.to_csv(scratch_rpi, index=False)

    log("  EADA:")
    add_team_eada.main(input_path=scratch_rpi, output_path=scratch_eada)

    out = pd.read_csv(scratch_eada, low_memory=False)
    for path in (scratch_in, scratch_rpi, scratch_eada):
        path.unlink(missing_ok=True)
    return out


def backfill_league(team_season):
    """Fill `league_team` from the RPI files where ncaabb_dataset has no row.

    Eleven team-seasons -- Merrimack, Quinnipiac, Stonehill, Mercer and Texas
    Southern 2021 -- are absent from ncaabb_dataset.csv, so they get no conference
    from it and in turn no conference ranking. The private build handles this the
    same way (`fix_acronym_bugs.py`, "fall back to RPI Conference for league_team
    when ncaabb has no row"), and matching it takes these six columns from 99.32%
    to complete agreement.

    `add_team_rpi.load_rpi_year` drops `Conference` as redundant, so the raw files
    are read here for that one column, reusing its name mapping.
    """
    sys.path.insert(0, str(HERE))
    import add_team_rpi

    missing = team_season["league_team"].isna()
    if not missing.any():
        return team_season

    conferences = {}
    for year in YEARS:
        path = ROOT / "ncaa_rpiYears" / f"ncaa_rpi_{year}.csv"
        if not path.exists():
            continue
        rpi = pd.read_csv(path)
        rpi.columns = rpi.columns.str.strip()
        if "Conference" not in rpi.columns:
            continue
        for team, conference in zip(rpi["Team"].astype(str).str.strip(),
                                    rpi["Conference"]):
            conferences[(team, year)] = conference

    joined = team_season["team_old"].replace(add_team_rpi.TEAM_NAME_MAP)
    for index in team_season.index[missing]:
        key = (joined.at[index], team_season.at[index, "year"])
        value = conferences.get(key)
        if value is not None and not pd.isna(value):
            team_season.at[index, "league_team"] = value

    filled = int(missing.sum() - team_season["league_team"].isna().sum())
    log(f"  league_team backfilled from RPI for {filled} of "
        f"{int(missing.sum())} team-season(s) with no ncaabb row")
    return team_season


# ---------------------------------------------------------------------------
# Draft labels
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Assembly
# ---------------------------------------------------------------------------

def merge_batting_pitching(batting, pitching):
    """Outer join on the player key, then derive `role`.

    Suffixes are applied explicitly rather than left to pandas, which only
    disambiguates columns that appear on *both* sides. `ip` is pitching-only and
    `ab` batting-only, so relying on the merge would emit them bare -- while the
    target schema calls them `ip_pitch` and `ab_bat`. Naming them up front makes
    the output schema independent of which columns happen to overlap.
    """
    def suffixed(frame, suffix):
        renames = {c: f"{c}{suffix}" for c in frame.columns
                   if c not in MERGE_KEYS and c not in SHARED_METADATA}
        return frame.rename(columns=renames)

    _p = suffixed(pitching, "_pitch").assign(_in_pitching=True)
    _b = suffixed(batting, "_bat").assign(_in_batting=True)
    merged = _p.merge(_b, on=MERGE_KEYS, how="outer",
                      suffixes=("_pitch", "_bat"))
    merged["_in_pitching"] = merged["_in_pitching"].fillna(False).astype(bool)
    merged["_in_batting"] = merged["_in_batting"].fillna(False).astype(bool)
    # These two are already category-specific; the suffixing would double it.
    merged = merged.rename(columns={"qualified_pitch_pitch": "qualified_pitch",
                                    "qualified_bat_bat": "qualified_bat"})
    merged = merged.drop(columns=["ip_true_pitch"], errors="ignore")

    # Metadata present on both sides collapses to one column.
    for column in SHARED_METADATA:
        left, right = f"{column}_pitch", f"{column}_bat"
        if left in merged.columns and right in merged.columns:
            merged[column] = merged[left].combine_first(merged[right])
            merged = merged.drop(columns=[left, right])
        elif left in merged.columns:
            merged = merged.rename(columns={left: column})
        elif right in merged.columns:
            merged = merged.rename(columns={right: column})
    # Role comes from which source frame a player appeared in, not from whether
    # a particular stat is non-null. Reading `ip_pitch.notna()` mislabelled the
    # 88 relief appearances whose innings are blank but whose batters-faced are
    # not -- they became "Unknown", which ROLE_MAP has no key for, so the model
    # saw NaN -- and called 18 genuine two-way players "Batter". `ip == 0` and
    # `ip == NaN` are the same fact and used to get two different answers.
    merged["role"] = np.select(
        [merged["_in_pitching"] & merged["_in_batting"],
         merged["_in_pitching"],
         merged["_in_batting"]],
        ["Two-Way", "Pitcher", "Batter"], default="Unknown")
    merged = merged.drop(columns=["_in_pitching", "_in_batting"])
    log(f"  {len(merged):,} player-seasons "
        f"({(merged.role == 'Two-Way').sum():,} two-way, "
        f"{(merged.role == 'Pitcher').sum():,} pitcher, "
        f"{(merged.role == 'Batter').sum():,} batter)")
    return merged


def verify_against_private(team_season):
    """Compare the public team-season table to the private file's, column by column."""
    if not PRIVATE_REFERENCE.exists():
        log("  private reference absent -- skipping verification")
        return
    private = pd.read_csv(PRIVATE_REFERENCE, low_memory=False)
    private = private[private["year"].isin(YEARS)]
    # `Acronym` is excluded from the compared set because it becomes the join key.
    columns = [c for c in private.columns
               if c.endswith("_team") or c in ("Full Team Name", "team_old",
                                               "team_new", "team_teamstats")]
    reference = (private.groupby(["team", "year"])[columns].first().reset_index()
                 .rename(columns={"team": "Acronym"}))

    ours = team_season.copy()
    joined = reference.merge(ours, on=["Acronym", "year"], how="inner",
                            suffixes=("_ref", "_ours"))
    log(f"  {len(joined):,} team-seasons compared "
        f"(reference has {len(reference):,}, ours {len(ours):,})")

    # `Full Team Name` carries the same school under different formatting: the
    # private file holds a lowercased, punctuation-stripped form ("alabama aandm
    # university") left behind by an in-place clean() somewhere upstream, while its
    # own `Full Name_team` keeps the proper name. Ours keeps the proper name in
    # both. Same information, and it is metadata rather than a model feature, so it
    # is reported as a known formatting difference instead of a mismatch.
    formatting_only = {"Full Team Name"}

    exact, close, differing, absent = [], [], [], []
    for column in columns:
        if column in ("Acronym",) or column in formatting_only:
            continue
        left, right = f"{column}_ref", f"{column}_ours"
        if right not in joined.columns:
            absent.append(column)
            continue
        a, b = joined[left], joined[right]
        both_null = a.isna() & b.isna()
        a_num = pd.to_numeric(a, errors="coerce")
        b_num = pd.to_numeric(b, errors="coerce")
        if a_num.notna().any() and b_num.notna().any():
            same = np.isclose(a_num, b_num, rtol=1e-6, atol=1e-9,
                              equal_nan=True) | both_null
        else:
            same = (a.astype(str) == b.astype(str)) | both_null
        rate = float(np.mean(same))
        if rate == 1.0:
            exact.append(column)
        elif rate >= 0.99:
            close.append((column, rate))
        else:
            differing.append((column, rate))

    log(f"  exact match: {len(exact)} column(s)")
    log(f"  formatting-only difference: {sorted(formatting_only)} "
        f"(private stores a lowercased form; ours keeps proper case)")
    if close:
        log(f"  >=99% match: {len(close)} -- " +
            ", ".join(f"{c} {r:.2%}" for c, r in close[:10]))
    if differing:
        log(f"  BELOW 99%: {len(differing)} -- " +
            ", ".join(f"{c} {r:.2%}" for c, r in sorted(differing,
                                                        key=lambda x: x[1])[:12]))
    if absent:
        log(f"  not produced by the public build: {len(absent)} -- {absent[:12]}")


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--verify", action="store_true",
                        help="compare the team block against the private file")
    parser.add_argument("--refit-qualification", action="store_true",
                        help="re-derive the qualification rates from the FanGraphs "
                             "exports instead of using the frozen constants. Needs "
                             "paywalled files, so it is off by default.")
    args = parser.parse_args()

    log("=== 1. public player data ===")
    batting, pitching = load_public()

    log("\n=== 2. qualification flags ===")
    batting, pitching = add_qualification(batting, pitching,
                                         refit=args.refit_qualification)

    log("\n=== 3. team-season table ===")
    team_season = build_team_season(batting, pitching)

    log("\n=== 4. RPI and EADA ===")
    team_season = add_rpi_and_eada(team_season)

    if args.verify:
        log("\n=== 4b. verify the team block against the private file ===")
        verify_against_private(team_season)

    log("\n=== 5. attach team identity to players ===")
    identity = team_season[["Acronym", "year", "Full Team Name",
                            "team_old", "team_new"]]
    for label in ("batting", "pitching"):
        frame = batting if label == "batting" else pitching
        frame = frame.merge(identity, left_on=["team", "year"],
                            right_on=["Acronym", "year"], how="left")
        if label == "batting":
            batting = frame
        else:
            pitching = frame
    log(f"  batting {len(batting):,}, pitching {len(pitching):,}")

    log("\n=== 6. batting x pitching ===")
    merged = merge_batting_pitching(batting, pitching)

    log("\n=== 7. attach the team block ===")
    # `team` is only a helper add_team_eada needed; dropping it here keeps the
    # player frame's own `team` intact instead of colliding into team_x/team_y.
    team_columns = [c for c in team_season.columns
                    if c not in ("Acronym", "year", "Full Team Name",
                                 "team_old", "team_new", "team")]
    block = (team_season[["Acronym", "year"] + team_columns]
             .rename(columns={"Acronym": "team"}))
    final = merged.merge(block, on=["team", "year"], how="left")
    log(f"  {len(final):,} rows, {final.shape[1]} columns")

    # The ordinal encoding of `class`, which is what replaces `age` in the feature
    # set. Added before conform() so it lands with the other identity columns.
    final["class_ord"] = final["class"].map(CLASS_ORDINAL)
    known = final["class_ord"].notna().mean()
    log(f"  class_ord populated for {known:.2%} of rows")

    log("\n=== 7b. tenure features ===")
    final = add_tenure_features(final)

    log("\n=== 8. conform to the private schema ===")
    final = conform(final)

    # Two populations, because they answer different questions.
    #
    # The private matrix is built from FanGraphs' *qualified* leaderboards
    # (16,728 rows for 2021-2025, 9.60% drafted). The public player files are the
    # no-minimum population, roughly three times larger with a correspondingly
    # diluted base rate. Shipping only the larger one would silently change the
    # class balance every model in the repo was tuned against, so the qualified
    # cut is the primary output and the full population sits beside it.
    qualified = final[final["qualified_bat"].fillna(False)
                      | final["qualified_pitch"].fillna(False)].copy()

    final.to_csv(FULL_OUTPUT_PATH, index=False)
    qualified.to_csv(OUTPUT_PATH, index=False)
    log(f"  wrote {OUTPUT_PATH.name} -- {len(qualified):,} rows, "
        f"{qualified.shape[1]} columns (qualified; private has 16,728)")
    log(f"  wrote {FULL_OUTPUT_PATH.name} -- {len(final):,} rows "
        f"(full no-minimum population)")

    log("\n=== 9. draft labels ===")
    label_drafts()
    return 0


def label_drafts():
    """Fill the draft labels with add_2026_draft.py, the authoritative matcher.

    Not reimplemented here, and an earlier hand-rolled version of this step is why:
    it reproduced masterDraft.py's two rules and found 1,472 positives against the
    private file's 1,606. add_2026_draft.py is the labeler the private file's
    current labels actually came from -- it relabelled every year, taking positives
    from 1,439 to 1,953 across 2021-2026 -- and it carries the pieces that account
    for the gap: a nickname table (Bob/Bobby, Matt/Matthew), name-suffix handling,
    school aliases, and three passes of decreasing strictness.

    It reads and writes the same CSV shape it is given, so it runs over both
    outputs. `--force` relaxes its per-year sanity band, which is calibrated to the
    private file's row counts rather than ours.
    """
    sys.path.insert(0, str(HERE))
    import add_2026_draft

    for path, label in ((OUTPUT_PATH, "qualified"),
                        (FULL_OUTPUT_PATH, "no-minimum")):
        log(f"  {label}:")
        width = pd.read_csv(path, nrows=0).shape[1]
        argv = ["--csv", str(path), "--output", str(path),
                "--years", *[str(y) for y in YEARS],
                "--report", str(ROOT / f"draft_match_report_public_{label}.csv"),
                "--force", "--expect-columns", str(width)]
        add_2026_draft.main(argv)
        frame = pd.read_csv(path, low_memory=False)
        flag = frame["Drafted?"]
        drafted = int(flag.astype(str).isin(["1", "1.0", "True", "true"]).sum())
        log(f"    {drafted:,} drafted of {len(frame):,} "
            f"({drafted / len(frame):.2%})")


def add_tenure_features(frame):
    """Attach seasons_elapsed and first_class_ord via add_tenure.py.

    Shared with the private build rather than reimplemented, because the part that
    matters is not the arithmetic but the two ways of getting it wrong: counting
    from the qualified matrix instead of the no-minimum panel, and using any
    season statistic that reads forward from the row being scored. add_tenure.py's
    docstring carries both.

    The key is `person_id`, never `playerid` -- the NCAA mints a new `playerid`
    every season, so consecutive roster years share exactly zero ids and every
    first appearance would resolve to the row's own year.
    """
    sys.path.insert(0, str(HERE))
    import add_tenure

    panel = add_tenure.load_panel(PUBLIC_PANEL, "person_id")
    log(f"  panel: {len(panel):,} player-seasons, "
        f"{panel['person_id'].nunique():,} players, "
        f"years {panel['year'].min()}-{panel['year'].max()}")
    # Ids the linkage merged across two schools in one season are two people,
    # not one career. Qualify those by school before taking a first appearance,
    # or the earlier player's debut is credited to the later one.
    split = add_tenure.colliding_ids(panel, "person_id")
    if split:
        log(f"  {len(split)} person_id(s) collide within a season; "
            f"qualifying those by school")
    table = add_tenure.first_appearance(panel, "person_id", with_class=True,
                                        split=split)
    frame = add_tenure.attach(frame, table, "person_id", split=split)
    add_tenure.report(frame)
    return frame


def conform(frame):
    """Order columns like the private file and report the delta."""
    # add_2026_draft.py fills these in place, so they must exist first. Starting
    # them empty (rather than absent) also means its "carried_over_existing"
    # bookkeeping has nothing stale to preserve.
    for column in ("Round", "Pick", "Drafted By", "Drafted From"):
        if column not in frame.columns:
            frame[column] = np.nan
    if "Drafted?" not in frame.columns:
        frame["Drafted?"] = False

    # The column order comes from a frozen schema file, NOT from the private
    # matrix. Reading the private file here would make the whole build depend on a
    # paywalled artefact for nothing more than an ordering, and a third party
    # without it would silently get a differently-ordered CSV. The schema was
    # derived from that file once; PUBLIC_SCHEMA records it.
    if not PUBLIC_SCHEMA.exists():
        raise SystemExit(
            f"missing {PUBLIC_SCHEMA.name}. It records the public matrix's column "
            f"order and is required for a reproducible build.")
    target = json.loads(PUBLIC_SCHEMA.read_text())["columns"]

    present = [c for c in target if c in frame.columns]
    missing = [c for c in target if c not in frame.columns]
    extra = [c for c in frame.columns if c not in target]

    ordered = frame[present + extra]
    log(f"  {len(present)} of {len(target)} schema columns present")
    if missing:
        log(f"  MISSING from this build ({len(missing)}): {missing}")
    if extra:
        log(f"  not in the frozen schema ({len(extra)}): {extra}")

    # The private-file comparison is a validation aid, not a build input.
    if PRIVATE_REFERENCE.exists():
        private = list(pd.read_csv(PRIVATE_REFERENCE, nrows=0).columns)
        reproduced = [c for c in private if c in ordered.columns]
        log(f"  (vs private file: {len(reproduced)} of {len(private)} reproduced; "
            f"absent by design {[c for c in private if c not in ordered.columns]})")
    return ordered


if __name__ == "__main__":
    sys.exit(main())
