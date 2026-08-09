"""
Build a 2026 copy of batting_pitching_combined_with_rpi.csv.

Reproduces the historical pipeline for the new 2026 FanGraphs data:
  1. merge batting leaderboard 1+2 and pitching leaderboard 1+2 by PlayerId
  2. outer-merge batting<->pitching into one row per player (role = Pitcher/Batter/Two-Way)
  3. resolve team identity: acronym -> Full Name -> team_old/team_new/id/division
  4. attach team stats from 2026.json (PE / Difference are COMPUTED, TP is unavailable)
  5. attach conference rankings (missing_conf -> conference_rankings)
  6. attach NCAA RPI (ncaa_rpi_2026.csv) via the existing TEAM_NAME_MAP normalization
  7. append the 2026 rows to the historical file, preserving the exact 171-column schema

The output column set/order is taken verbatim from the existing combined file, so the
result is schema-identical regardless of the historical _team/_teamstats suffix mechanics.

Run:  python csv_editing_scripts/build_2026_combined.py

Downstream: add_team_eada.py reads this script's output and appends the 12 EADA
program-budget columns (171 -> 183) as batting_pitching_combined_with_rpi_2026_eada.csv,
which is what the V7 notebook loads. Keep that step separate -- the 171-column contract
asserted below is deliberately the pre-EADA schema.
"""

import json
import re
from pathlib import Path

import numpy as np
import pandas as pd

# Reuse the RPI team-name normalization + loader from the historical pipeline.
from add_team_rpi import TEAM_NAME_MAP, TEAM_NAME_MAP_BY_YEAR, load_rpi_year

# --------------------------------------------------------------------------------------
# Paths
# --------------------------------------------------------------------------------------
HERE = Path(__file__).resolve().parent          # .../Main XGBoost Files/csv_editing_scripts
ROOT = HERE.parent                              # .../Main XGBoost Files
CSVROOT = ROOT.parent                           # .../CSV+Code Files
STD = CSVROOT / "standardized"
DATA = ROOT / "2026 data"
YEAR = 2026

EXISTING = ROOT / "batting_pitching_combined_with_rpi.csv"
OUTPUT = ROOT / "batting_pitching_combined_with_rpi_2026.csv"

BAT1 = DATA / "batting_fangraphs-college-leaderboard1.csv"
BAT2 = DATA / "batting_fangraphs-college-leaderboard2.csv"
PIT1 = DATA / "pitching_fangraphs-college-leaderboard1.csv"
PIT2 = DATA / "pitching_fangraphs-college-leaderboard2.csv"
TEAM_JSON = DATA / "2026.json"

PE_EXP = 1.83  # baseball Pythagorean exponent (reverse-engineered from ncaabb_dataset.csv)

# team_old -> exact key used inside 2026.json (only the format mismatches need patching).
# Le Moyne / Mercyhurst are genuinely absent from 2026.json (new D1 programs) -> left blank.
JSON_TEAM_PATCH = {
    "University of Miami": "Miami (FL)",
    "Miami University-Ohio": "Miami (OH)",
    "St. John's": "St. John's (NY)",
    "Saint Mary's": "Saint Mary's (CA)",
    "LMU": "LMU (CA)",
    "Queens": "Queens (NC)",
}

IDENT = ["name", "team", "age", "nameascii", "playerid", "mlbamid", "year"]


# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------
def clean(s):
    """Name/full-name normalizer copied from ncaa_*/masterDraft.py."""
    if pd.isna(s):
        return ""
    s = str(s).lower()
    s = s.replace("&", "and")
    s = re.sub(r"\buniv\.?\b", "university", s)
    s = s.replace("college of", "")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def read_fg(path):
    """FanGraphs exports carry a UTF-8 BOM on the first header."""
    return pd.read_csv(path, encoding="utf-8-sig")


def rename_fg(df, suffix):
    """Rename FanGraphs columns to the combined schema: idents -> bare, stats -> lower+suffix."""
    ident_map = {
        "Season": "year", "Name": "name", "Team": "team", "Age": "age",
        "NameASCII": "nameascii", "PlayerId": "playerid", "MLBAMID": "mlbamid",
    }
    out = {}
    for c in df.columns:
        if c in ident_map:
            out[c] = ident_map[c]
        else:
            out[c] = f"{c.lower()}{suffix}"
    return df.rename(columns=out)


# --------------------------------------------------------------------------------------
# 1-2. Merge batting 1+2 and pitching 1+2, then batting<->pitching
# --------------------------------------------------------------------------------------
def build_players():
    b1 = rename_fg(read_fg(BAT1), "_bat")
    b2 = rename_fg(read_fg(BAT2), "_bat")
    p1 = rename_fg(read_fg(PIT1), "_pitch")
    p2 = rename_fg(read_fg(PIT2), "_pitch")

    # From the second leaderboard take only columns the first one doesn't already have
    # (IP/ERA on pitching and PA/AVG on batting overlap -> keep the first file's copy).
    b2_extra = ["playerid"] + [c for c in b2.columns if c.endswith("_bat") and c not in b1.columns]
    p2_extra = ["playerid"] + [c for c in p2.columns if c.endswith("_pitch") and c not in p1.columns]

    batting = b1.merge(b2[b2_extra], on="playerid", how="outer")
    pitching = p1.merge(p2[p2_extra], on="playerid", how="outer")

    # Outer-merge batting<->pitching; coalesce shared identity columns so batter-only and
    # pitcher-only players keep their identity (two-way players appear once).
    combined = pitching.merge(batting, on="playerid", how="outer", suffixes=("", "_b"))
    for c in ["name", "team", "age", "nameascii", "mlbamid", "year"]:
        dup = f"{c}_b"
        if dup in combined.columns:
            combined[c] = combined[c].combine_first(combined[dup])
            combined = combined.drop(columns=[dup])

    combined["year"] = YEAR
    combined["name"] = combined["name"].apply(clean)

    # role: Two-Way if both pitched and batted, else Pitcher / Batter
    combined["role"] = np.select(
        [
            combined["ip_pitch"].notna() & combined["ab_bat"].notna(),
            combined["ip_pitch"].notna(),
            combined["ab_bat"].notna(),
        ],
        ["Two-Way", "Pitcher", "Batter"],
        default="Unknown",
    )
    return combined


# --------------------------------------------------------------------------------------
# 3. Team identity (acronym -> Full Name -> team_old/team_new/id/division)
# --------------------------------------------------------------------------------------
def add_team_identity(df):
    ut = read_fg(STD / "unique_teams.csv")
    ut.columns = ut.columns.str.strip()
    acr2full = dict(zip(ut["Acronym"], ut["Full Name"]))  # proper-case full name

    tnm = pd.read_csv(STD / "team_name_mapping.csv")
    tnm.columns = tnm.columns.str.strip()
    tnm = tnm[tnm["division"] == 1]
    by_new = {
        r["team_new"]: (r["team_old"], r["team_id"], r["division"])
        for _, r in tnm.iterrows()
    }

    df["Acronym"] = df["team"]
    df["Full Name_team"] = df["team"].map(acr2full)               # proper case
    df["Full Team Name"] = df["Full Name_team"].apply(clean)       # cleaned/lower

    def ident(full):
        return by_new.get(full, (np.nan, np.nan, np.nan))

    trip = df["Full Name_team"].map(lambda f: ident(f) if pd.notna(f) else (np.nan, np.nan, np.nan))
    df["team_old"] = [t[0] for t in trip]
    df["id_team"] = [t[1] for t in trip]
    df["division_team"] = [t[2] for t in trip]
    df["team_new"] = df["Full Name_team"]
    df["team_teamstats"] = df["team_old"]
    return df


# --------------------------------------------------------------------------------------
# 4. Team stats from 2026.json (compute PE / Difference, leave TP blank)
# --------------------------------------------------------------------------------------
def add_team_stats(df, team_stat_cols):
    with open(TEAM_JSON) as f:
        raw = json.load(f)
    stats = {v["team"]: v for v in raw.values() if isinstance(v, dict)}

    def lookup(team_old):
        if pd.isna(team_old):
            return None
        key = JSON_TEAM_PATCH.get(team_old, team_old)
        return stats.get(key)

    rows_stats = df["team_old"].map(lookup)
    df["league_team"] = rows_stats.map(lambda s: s.get("league") if s else np.nan)

    # Every team-stat target column equals "<json key>_team" except the three below.
    computed = {"PE_team", "Difference_team", "TP_team"}
    for col in team_stat_cols:
        if col in computed:
            continue
        key = col[:-len("_team")]
        df[col] = rows_stats.map(lambda s: s.get(key) if s else np.nan)

    rb = pd.to_numeric(df["R (Batting)_team"], errors="coerce")
    rp = pd.to_numeric(df["R (Pitching)_team"], errors="coerce")
    wpct = pd.to_numeric(df["WPCT_team"], errors="coerce")
    pe = rb**PE_EXP / (rb**PE_EXP + rp**PE_EXP)
    df["PE_team"] = pe.round(3)
    df["Difference_team"] = (wpct - pe).round(3)
    df["TP_team"] = np.nan  # triple plays not present in 2026.json
    return df


# --------------------------------------------------------------------------------------
# 5. Conference rankings (missing_conf -> conference_rankings)
# --------------------------------------------------------------------------------------
def add_conference(df):
    mc = pd.read_csv(STD / "missing_conf.csv")
    mc.columns = mc.columns.str.strip()
    for c in ["league name", "conf name"]:
        mc[c] = mc[c].astype(str).str.strip()

    cr = pd.read_csv(STD / "conference_rankings.csv")
    cr.columns = cr.columns.str.strip()
    cr["conference"] = cr["conference"].astype(str).str.strip()
    cr = cr[cr["year"] == YEAR]
    rank = {
        r["conference"]: (r["conf-rpi"], r["conf-rank"], r["conf-national-rec"], r["conf-national-wp"])
        for _, r in cr.iterrows()
    }

    def conf_name(league):
        if pd.isna(league):
            return None
        hit = mc[(mc["league name"] == league) & (mc["start_year"] <= YEAR) & (mc["end_year"] >= YEAR)]
        if not hit.empty:
            return hit.iloc[0]["conf name"]
        return league  # fall back to the league name itself (e.g. "American")

    cn = df["league_team"].map(conf_name)
    vals = cn.map(lambda c: rank.get(c, (np.nan, np.nan, np.nan, np.nan)))
    df["conf_rpi_team"] = [v[0] for v in vals]
    df["conf_rank_team"] = [v[1] for v in vals]
    df["conf_national_rec_team"] = [v[2] for v in vals]
    df["conf_national_wp_team"] = [v[3] for v in vals]
    return df


# --------------------------------------------------------------------------------------
# 6. NCAA RPI (ncaa_rpi_2026.csv) via TEAM_NAME_MAP normalization
# --------------------------------------------------------------------------------------
def add_rpi(df):
    rpi = load_rpi_year(YEAR)  # has Team, year, and all *_team RPI columns
    rpi_by_team = rpi.set_index("Team")

    df["_join_team"] = df["team_old"].replace(TEAM_NAME_MAP)
    yr_over = df.apply(lambda r: TEAM_NAME_MAP_BY_YEAR.get((r["team_old"], r["year"])), axis=1)
    df["_join_team"] = yr_over.combine_first(df["_join_team"])

    rpi_cols = [c for c in rpi.columns if c not in ("Team", "year")]
    joined = df["_join_team"].map(
        lambda t: rpi_by_team.loc[t] if (pd.notna(t) and t in rpi_by_team.index) else None
    )
    for col in rpi_cols:
        df[col] = joined.map(lambda row: row[col] if row is not None else np.nan)
    df = df.drop(columns=["_join_team"])
    return df


# --------------------------------------------------------------------------------------
# Coverage report
# --------------------------------------------------------------------------------------
def report(df):
    n = len(df)
    no_team = df[df["team_old"].isna()]
    no_stats = df[df["W_team"].isna()]
    no_rpi = df[df["rpi_team"].isna()]
    print(f"\n=== 2026 coverage ({n} players) ===")
    print(f"unresolved team identity : {len(no_team):>5}  acronyms: {sorted(no_team['team'].unique())}")
    print(f"no team stats (2026.json): {len(no_stats):>5}  teams: {sorted(no_stats['team_old'].dropna().unique())}")
    print(f"no NCAA RPI match        : {len(no_rpi):>5}  teams: {sorted(no_rpi['team_old'].dropna().unique())}")
    print(f"role breakdown: {df['role'].value_counts().to_dict()}")


# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------
def main():
    target_cols = list(pd.read_csv(EXISTING, nrows=0).columns)
    team_stat_cols = target_cols[target_cols.index("W_team"):target_cols.index("BBPG (Pitching)_team") + 1]

    df = build_players()
    df = add_team_identity(df)
    df = add_team_stats(df, team_stat_cols)
    df = add_conference(df)
    df = add_rpi(df)

    # Draft outcome is unknown for 2026 (draft not held yet) -> blank / not drafted.
    for c in ["Round", "Pick", "Drafted By", "Drafted From"]:
        df[c] = np.nan
    df["Drafted?"] = False

    report(df)

    # Conform to the exact schema and append to the historical file.
    missing = [c for c in target_cols if c not in df.columns]
    assert not missing, f"missing target columns: {missing}"
    df = df[target_cols]

    existing = pd.read_csv(EXISTING, low_memory=False)
    assert list(existing.columns) == target_cols
    out = pd.concat([existing, df], ignore_index=True)
    out.to_csv(OUTPUT, index=False)

    print(f"\nhistorical rows: {len(existing)}")
    print(f"2026 rows      : {len(df)}")
    print(f"total rows     : {len(out)}")
    print(f"wrote {OUTPUT.relative_to(CSVROOT)}")


if __name__ == "__main__":
    main()
