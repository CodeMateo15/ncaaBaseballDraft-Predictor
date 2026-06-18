"""
Retroactively fix `batting_pitching_combined.csv` for acronym → full-name
mismapping bugs in `standardized/unique_teams.csv`. Several acronyms were
pointing at the wrong school, so the corresponding player rows ended up with
the wrong team metadata, the wrong team-level stats (`*_team` columns merged
from ncaabb_dataset.csv), and the wrong conference (`league_team`,
`conf_*_team`).

The set of buggy acronyms (after correcting unique_teams.csv):
    KSU  : Kennesaw State  -> Kansas State
    CAN  : UC Santa Barbara -> Canisius
    SAM  : Sam Houston State -> Samford
    STBK : St. Bonaventure  -> Stony Brook
    CARK : UC Riverside     -> Central Arkansas
    QUC  : Quinnipiac       -> Queens University of Charlotte
    STO  : Stony Brook      -> Stonehill
    MER  : Merrimack        -> Mercer
    MERC : Mercer           -> Mercyhurst
    MRMK : Mount St. Mary's -> Merrimack
    MSM  : Mississippi Val. -> Mount St. Mary's

Rather than hardcoding the corrected identity values, this script DERIVES them
from the same chain that masterTeam.py uses for the rest of the pipeline:
    Acronym -> standardized/unique_teams.csv      -> Full Name
    Full Name -> standardized/team_name_mapping.csv (division=1) -> team_old, team_id
That way the patch stays consistent with whatever the rest of the data
pipeline considers canonical — if those source files change, this script
picks up the change automatically.

Steps:
  1. Re-label affected rows (team_old / team_new / Full Team Name /
     team_teamstats / Full Name_team / id_team / division_team).
  2. Re-merge the ncaabb-derived `*_team` columns for those rows on the
     corrected (team_old, year).
  3. Re-merge the conference-rollup `conf_*_team` columns using the chain
     from add_rpi.py (league_team -> missing_conf -> conference_rankings).
  4. Write back to `batting_pitching_combined.csv`.
"""

import pandas as pd
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent                 # trial_xgBoost/
REPO = ROOT.parent                 # CSV+Code Files/

COMBINED_PATH = ROOT / "batting_pitching_combined.csv"
NCAABB_PATH = REPO / "ncaabb_dataset.csv"
UNIQUE_TEAMS_PATH = REPO / "standardized" / "unique_teams.csv"
TEAM_MAPPING_PATH = REPO / "standardized" / "team_name_mapping.csv"
MISSING_CONF_PATH = REPO / "standardized" / "missing_conf.csv"
CONF_RANK_PATH = REPO / "standardized" / "conference_rankings.csv"

ACRONYMS_TO_FIX = [
    "KSU", "CAN", "SAM", "STBK", "CARK", "QUC", "STO",
    "MER", "MERC", "MRMK", "MSM",
]

CONF_COLS = [
    "conf_rpi_team", "conf_rank_team",
    "conf_national_rec_team", "conf_national_wp_team",
]


def derive_identity(acronyms: list[str]) -> dict[str, dict]:
    """Look up canonical (team_old, team_new, team_id) for each acronym
    via unique_teams.csv -> team_name_mapping.csv (D1 only)."""
    ut = pd.read_csv(UNIQUE_TEAMS_PATH)
    ut.columns = ut.columns.str.strip()
    ut["Full Name"] = ut["Full Name"].astype(str).str.strip()
    tm = pd.read_csv(TEAM_MAPPING_PATH)
    tm = tm[tm["division"] == 1].copy()
    tm["team_new"] = tm["team_new"].astype(str).str.strip()
    tm["team_old"] = tm["team_old"].astype(str).str.strip()

    out: dict[str, dict] = {}
    errors: list[str] = []
    for acr in acronyms:
        ut_row = ut[ut["Acronym"] == acr]
        if ut_row.empty:
            errors.append(f"{acr}: not found in unique_teams.csv")
            continue
        full_name = ut_row.iloc[0]["Full Name"]
        tm_row = tm[tm["team_new"] == full_name]
        if tm_row.empty:
            errors.append(
                f"{acr}: Full Name {full_name!r} has no division=1 entry in team_name_mapping.csv"
            )
            continue
        if len(tm_row) > 1:
            errors.append(
                f"{acr}: Full Name {full_name!r} has multiple division=1 entries in team_name_mapping.csv"
            )
            continue
        r = tm_row.iloc[0]
        out[acr] = {
            "team_old": r["team_old"],
            "team_new": full_name,            # Full Name in unique_teams == team_new in mapping
            "Full Team Name": full_name.lower(),
            "team_teamstats": r["team_old"],
            "Full Name_team": full_name,
            "id_team": float(r["team_id"]),
            "division_team": 1.0,
        }
    if errors:
        raise SystemExit(
            "Failed to derive identity for some acronyms:\n  " + "\n  ".join(errors)
        )
    return out


def main() -> None:
    fixes = derive_identity(ACRONYMS_TO_FIX)

    print("Derived corrections from standardized/unique_teams.csv + team_name_mapping.csv:")
    for acr, f in fixes.items():
        print(f"  {acr:>5} -> team_old={f['team_old']!r}, team_new={f['team_new']!r}, id={f['id_team']:.0f}")
    print()

    bp = pd.read_csv(COMBINED_PATH)
    bp.columns = bp.columns.str.strip()

    ncaabb = pd.read_csv(NCAABB_PATH)
    ncaabb.columns = ncaabb.columns.str.strip()
    ncaabb["team"] = ncaabb["team"].astype(str).str.strip()

    conf_map = pd.read_csv(MISSING_CONF_PATH)
    conf_rank = pd.read_csv(CONF_RANK_PATH)
    for df in (conf_map, conf_rank):
        df.columns = df.columns.str.strip()
    conf_map["league name"] = conf_map["league name"].str.strip()
    conf_map["conf name"] = conf_map["conf name"].str.strip()
    conf_rank["conference"] = conf_rank["conference"].str.strip()

    # ncaabb value cols get suffixed with `_team` (and `league` becomes `league_team`).
    ncaabb_value_cols = [c for c in ncaabb.columns if c not in ("team", "year")]
    rename_to_team = {c: f"{c}_team" if c != "league" else "league_team" for c in ncaabb_value_cols}
    for c in rename_to_team.values():
        if c not in bp.columns:
            raise RuntimeError(f"Expected column {c!r} not found in combined CSV")

    mask = bp["team"].isin(fixes)
    total_affected = int(mask.sum())
    print(f"Affected player rows: {total_affected} across {len(fixes)} acronyms")
    print(bp.loc[mask, "team"].value_counts().to_string())
    print()

    # ---- Step 1: rewrite team identity columns on affected rows ----
    for acr, fields in fixes.items():
        sel = bp["team"] == acr
        for col, value in fields.items():
            if col in bp.columns:
                bp.loc[sel, col] = value

    # ---- Step 2: re-merge ncaabb-derived stats for affected rows ----
    affected = bp.loc[mask, ["team_old", "year"]].drop_duplicates()
    fresh = affected.merge(
        ncaabb, how="left", left_on=["team_old", "year"], right_on=["team", "year"]
    ).rename(columns=rename_to_team).drop(columns=["team"])
    lookup = fresh.set_index(["team_old", "year"])
    keys = list(zip(bp.loc[mask, "team_old"], bp.loc[mask, "year"]))
    for col in rename_to_team.values():
        if col not in lookup.columns:
            continue
        bp.loc[mask, col] = pd.Series(
            [lookup.at[k, col] if k in lookup.index else None for k in keys],
            index=bp.loc[mask].index,
        )

    # ---- Step 2b: fall back to RPI Conference for league_team when ncaabb has no row ----
    # ncaabb_dataset.csv doesn't cover every school (Queens, Stonehill, Mercyhurst,
    # Merrimack 2021-2023). For those rows league_team comes out NaN. Pull it from
    # the RPI files' Conference column, mapped through missing_conf.csv so the format
    # matches ncaabb's short codes (e.g. "Northeast" -> "NEC").
    league_missing = mask & bp["league_team"].isna()
    if league_missing.any():
        from add_team_rpi import TEAM_NAME_MAP, TEAM_NAME_MAP_BY_YEAR
        rpi_conf: dict[tuple[str, int], str] = {}
        for yr in sorted({int(y) for y in bp.loc[league_missing, "year"].dropna().unique()}):
            rpi_path = REPO / "CSV+Code Files" / "trial_xgBoost" / "ncaa_rpiYears" / f"ncaa_rpi_{yr}.csv"
            # ROOT already points at trial_xgBoost; use it directly.
            rpi_path = ROOT / "ncaa_rpiYears" / f"ncaa_rpi_{yr}.csv"
            r = pd.read_csv(rpi_path)
            r.columns = r.columns.str.strip()
            for row in r[["Team", "Conference"]].itertuples(index=False):
                rpi_conf[(str(row.Team).strip(), yr)] = str(row.Conference).strip()

        # Reverse map: RPI conf name -> ncaabb league name (e.g. "Northeast" -> "NEC")
        def rpi_conf_to_league(conf: str, yr: int) -> str | None:
            if not conf:
                return None
            m = conf_map[(conf_map["conf name"] == conf)
                         & (conf_map["start_year"] <= yr)
                         & (conf_map["end_year"] >= yr)]
            if len(m):
                return m.iloc[0]["league name"]
            # Some conferences share the name across both columns (e.g. ASUN, Big 12).
            m = conf_map[(conf_map["league name"] == conf)
                         & (conf_map["start_year"] <= yr)
                         & (conf_map["end_year"] >= yr)]
            if len(m):
                return m.iloc[0]["league name"]
            return conf  # last resort: keep whatever RPI had

        def to_rpi_name(team_old: str, year: int) -> str:
            if (team_old, year) in TEAM_NAME_MAP_BY_YEAR:
                return TEAM_NAME_MAP_BY_YEAR[(team_old, year)]
            return TEAM_NAME_MAP.get(team_old, team_old)

        idx_list = bp.index[league_missing]
        for i in idx_list:
            to = bp.at[i, "team_old"]
            yr = int(bp.at[i, "year"])
            rpi_team = to_rpi_name(to, yr)
            conf = rpi_conf.get((rpi_team, yr))
            league = rpi_conf_to_league(conf, yr) if conf else None
            if league:
                bp.at[i, "league_team"] = league

    # ---- Step 3: recompute conf_*_team via add_rpi.py logic ----
    # conf_map can have multiple rows per league_name (different year ranges); resolve
    # active conf name per (league, year) row-by-row to avoid duplicating rows on merge.
    def lookup_conf_name(league, yr):
        if pd.isna(league) or pd.isna(yr):
            return None
        m = conf_map[
            (conf_map["league name"] == league)
            & (conf_map["start_year"] <= yr)
            & (conf_map["end_year"] >= yr)
        ]
        return m.iloc[0]["conf name"] if len(m) else None

    sub_idx = bp.index[mask]
    leagues = bp.loc[sub_idx, "league_team"].astype(str).str.strip()
    years = bp.loc[sub_idx, "year"]
    conf_names = [lookup_conf_name(l, y) for l, y in zip(leagues, years)]
    sub = pd.DataFrame({"conf name": conf_names, "year": years.values}, index=sub_idx)
    sub = sub.merge(
        conf_rank, how="left", left_on=["conf name", "year"], right_on=["conference", "year"]
    )
    sub.index = sub_idx
    sub = sub.rename(columns={
        "conf-rpi": "conf_rpi_team",
        "conf-rank": "conf_rank_team",
        "conf-national-rec": "conf_national_rec_team",
        "conf-national-wp": "conf_national_wp_team",
    })
    for col in CONF_COLS:
        if col in sub.columns:
            bp.loc[sub_idx, col] = sub[col].values

    bp.to_csv(COMBINED_PATH, index=False)

    print("Per-acronym verification (post-patch, sample):")
    cols = ["team", "team_old", "team_new", "league_team", "W_team", "L_team", "BA_team", "conf_rpi_team"]
    summary = (
        bp.loc[mask, cols]
        .drop_duplicates(subset=["team", "year"] if "year" in cols else ["team"])
        .sort_values("team")
    )
    # Compact view: one row per acronym per year combo
    print(bp.loc[mask, cols + ["year"]].drop_duplicates().sort_values(["team", "year"]).to_string(index=False))


if __name__ == "__main__":
    main()
