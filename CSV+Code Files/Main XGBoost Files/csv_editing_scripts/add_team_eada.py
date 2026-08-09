"""Attach EADA program-budget features to the combined batting/pitching file.

Source: the federal Equity in Athletics Disclosure Act (EADA) survey, one row per
institution per reporting year, in `EADA Data/EADA_All_Data_Combined_*/EADA_<YYYY>.xlsx`.
See `EADA Data/FEATURES.txt` for the full data dictionary and the year-alignment evidence.

Year alignment: `EADA_<YYYY>.xlsx` covers the academic year ending in <YYYY>, i.e. the
spring <YYYY> baseball season, so it maps 1:1 onto the dataset's integer `year` with no lag.
2025-26 is not published yet, so 2025 is carried forward to 2026 (see CARRY_FORWARD).

Follows the same shape as `add_team_rpi.py`: build a normalized join key, left-merge on
(key, year), assert the row count is unchanged, report the match rate, and suffix every new
column so it matches the `_team` convention the notebook relies on.

Run:  python csv_editing_scripts/add_team_eada.py
"""

import re
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd

from eada_names import (
    EADA_NAME_OVERRIDES,
    EADA_UNITID_OVERRIDES,
    MISSING_FROM_EADA,
)

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
EADA_DIR = ROOT / "EADA Data"

INPUT_PATH = ROOT / "batting_pitching_combined_with_rpi_2026.csv"
OUTPUT_PATH = ROOT / "batting_pitching_combined_with_rpi_2026_eada.csv"
CROSSWALK_PATH = HERE / "eada_crosswalk.csv"

# EADA reporting years present on disk.
YEARS = [2021, 2022, 2023, 2024, 2025]

# Seasons with no EADA file of their own -> which reporting year to reuse.
# Institutions submit the 2025-26 survey by Oct 15 2026, so 2026 is not available yet.
# Delete the entry once `EADA_2026.xlsx` lands and add 2026 to YEARS.
CARRY_FORWARD = {2026: 2025}

# Raw EADA columns read out of the workbooks. Everything with a `_Baseball` suffix is
# baseball-specific; the rest are department-wide totals across all men's sports.
SOURCE_COLUMNS = [
    "unitid",
    "institution_name",
    "state_cd",
    "classification_name",
    "PARTIC_MEN_Baseball",
    "OPEXPPERPART_MEN_Baseball",
    "EXP_MEN_Baseball",
    "REV_MEN_Baseball",
    "MEN_TOTAL_HEADCOACH_Baseball",
    "MEN_TOTAL_ASSTCOACH_Baseball",
    "RECRUITEXP_MEN",
    "HDCOACH_SAL_FTE_MEN",
]

# The 12 derived features, in the order they are appended to the output file.
FEATURE_COLUMNS = [
    "budget_pct_eada_team",
    "log_budget_eada_team",
    "opex_per_player_pct_eada_team",
    "log_opex_per_player_eada_team",
    "log_budget_per_player_eada_team",
    "roster_size_eada_team",
    "log_revenue_eada_team",
    "net_revenue_eada_team",
    "coaching_staff_size_eada_team",
    "dept_recruiting_pct_eada_team",
    "log_dept_recruiting_eada_team",
    "log_dept_coach_salary_eada_team",
]

MIN_MATCH_RATE = 0.97


def normalize(name: str) -> str:
    """Fold an institution name to a comparable key.

    Deliberately keeps the word "College": stripping it collapses
    "University of Illinois" onto "Illinois College" and "Columbia University" onto
    "Columbia College". "A & M" / "A & T" fold to `aandm` / `aandt` rather than `am` / `at`
    so that removing the stopword "at" cannot turn "North Carolina A&T State" into
    "North Carolina State".
    """
    name = unicodedata.normalize("NFKD", str(name))
    name = "".join(c for c in name if not unicodedata.combining(c))
    name = name.lower().replace("–", "-").replace("—", "-")
    name = name.replace("&", " and ").replace(".", " ").replace(",", " ")
    name = name.replace("-", " ").replace("'", "")
    name = re.sub(r"\ba\s+and\s+m\b", "aandm", name)
    name = re.sub(r"\ba\s+and\s+t\b", "aandt", name)
    name = re.sub(r"\bmain campus\b", " ", name)
    name = re.sub(r"\bst\b(?!ate)", "saint", name)
    name = re.sub(r"\b(university|of|at|the)\b", " ", name)
    return re.sub(r"\s+", " ", name).strip()


def load_eada() -> pd.DataFrame:
    """Read every EADA workbook, keep institutions that sponsor baseball, apply carry-forward.

    No filter on `classification_name`: Dallas Baptist is listed D-II, Presbyterian is
    "Other", and New Haven is D-II in 2025 while playing D-I baseball in 2026. Filtering on
    division would silently drop them.
    """
    frames = []
    for year in YEARS:
        matches = sorted(EADA_DIR.glob(f"EADA_All_Data_Combined_*/EADA_{year}.xlsx"))
        if not matches:
            raise FileNotFoundError(f"No EADA_{year}.xlsx under {EADA_DIR}")
        book = pd.ExcelFile(matches[0])
        frame = book.parse(book.sheet_names[0], usecols=SOURCE_COLUMNS)
        frame["eada_year"] = year
        frames.append(frame)

    eada = pd.concat(frames, ignore_index=True)
    eada = eada[eada["PARTIC_MEN_Baseball"].fillna(0) > 0].copy()

    for target_year, source_year in CARRY_FORWARD.items():
        carried = eada[eada["eada_year"] == source_year].copy()
        carried["eada_year"] = target_year
        eada = pd.concat([eada, carried], ignore_index=True)

    return eada


def derive_features(eada: pd.DataFrame) -> pd.DataFrame:
    """Turn the raw EADA columns into the 12 model features.

    Percentile ranks are taken *within* `eada_year`. Baseball budgets inflate roughly 5%/yr,
    so raw dollars would make the carried-forward 2026 season look systematically poorer than
    the training years; the rank is stable (year-over-year correlation 0.98) and immune to it.
    """
    out = eada[["unitid", "eada_year"]].copy()

    expense = eada["EXP_MEN_Baseball"]
    revenue = eada["REV_MEN_Baseball"]
    # A handful of institutions file a system-wide row that sums every branch campus, which
    # shows up as an impossible baseball "roster" (Penn State reports 453 participants in
    # 2021 and 496 in 2022). No NCAA baseball roster runs past ~60, so treat anything outside
    # a plausible band as unreported rather than letting it poison the per-player ratios.
    participants = eada["PARTIC_MEN_Baseball"].where(
        eada["PARTIC_MEN_Baseball"].between(15, 75), np.nan
    )
    opex_per_player = eada["OPEXPPERPART_MEN_Baseball"]
    recruiting = eada["RECRUITEXP_MEN"]

    def log1p_nonneg(series: pd.Series) -> pd.Series:
        return np.log1p(series.clip(lower=0))

    def pct_within_year(series: pd.Series) -> pd.Series:
        return series.groupby(eada["eada_year"]).rank(pct=True)

    out["budget_pct_eada_team"] = pct_within_year(expense)
    out["log_budget_eada_team"] = log1p_nonneg(expense)
    out["opex_per_player_pct_eada_team"] = pct_within_year(opex_per_player)
    out["log_opex_per_player_eada_team"] = log1p_nonneg(opex_per_player)
    out["log_budget_per_player_eada_team"] = log1p_nonneg(expense / participants)
    out["roster_size_eada_team"] = participants
    out["log_revenue_eada_team"] = log1p_nonneg(revenue)
    out["net_revenue_eada_team"] = revenue - expense
    out["coaching_staff_size_eada_team"] = eada["MEN_TOTAL_HEADCOACH_Baseball"].fillna(0) + eada[
        "MEN_TOTAL_ASSTCOACH_Baseball"
    ].fillna(0)
    out["dept_recruiting_pct_eada_team"] = pct_within_year(recruiting)
    out["log_dept_recruiting_eada_team"] = log1p_nonneg(recruiting)
    out["log_dept_coach_salary_eada_team"] = log1p_nonneg(eada["HDCOACH_SAL_FTE_MEN"])

    return out[["unitid", "eada_year"] + FEATURE_COLUMNS].drop_duplicates(["unitid", "eada_year"])


def build_crosswalk(combined: pd.DataFrame, eada: pd.DataFrame) -> pd.DataFrame:
    """Resolve every FanGraphs acronym to the IPEDS unitid(s) that represent it.

    Keyed on `team` (the acronym) rather than `team_old` -- see the module docstring of
    `eada_names.py` for why. Returns one row per (acronym, unitid): an acronym can own more
    than one unitid because IPEDS occasionally reissues an id for the same campus (Penn State
    files as 495767 "The Pennsylvania State University" in 2021-22 and 214777 "Pennsylvania
    State University-Main Campus" from 2023). Matching on the normalized name picks up both.
    Renames under a *stable* id (Dixie State -> Utah Tech, Iona College -> Iona University)
    are handled for free, since every row for a claimed unitid is kept regardless of its name.

    Raises if an acronym is unresolved or if two acronyms claim the same unitid.
    """
    institutions = eada.drop_duplicates(["unitid", "institution_name"])[
        ["unitid", "institution_name", "state_cd"]
    ].copy()
    institutions["key"] = institutions["institution_name"].map(normalize)

    key_to_unitids = institutions.groupby("key")["unitid"].apply(lambda s: sorted(set(s))).to_dict()

    teams = combined.drop_duplicates("team")[["team", "team_old", "Full Name_team"]].copy()

    rows = []
    for team, team_old, full_name in teams.itertuples(index=False):
        if team in MISSING_FROM_EADA:
            rows.append((team, team_old, full_name, pd.NA, "absent", MISSING_FROM_EADA[team]))
            continue
        if team in EADA_UNITID_OVERRIDES:
            rows.append(
                (team, team_old, full_name, EADA_UNITID_OVERRIDES[team], "unitid_override", "")
            )
            continue

        override = EADA_NAME_OVERRIDES.get(team)
        key = normalize(override if override else full_name)
        source = "name_override" if override else "auto"
        unitids = key_to_unitids.get(key, [])
        if not unitids:
            rows.append((team, team_old, full_name, pd.NA, source, "unresolved"))
            continue
        for unitid in unitids:
            rows.append((team, team_old, full_name, unitid, source, ""))

    crosswalk = pd.DataFrame(
        rows, columns=["team", "team_old", "full_name", "unitid", "source", "note"]
    )
    crosswalk = crosswalk.merge(
        institutions.drop_duplicates("unitid")[["unitid", "institution_name", "state_cd"]],
        on="unitid",
        how="left",
    )

    unresolved = crosswalk[crosswalk["note"] == "unresolved"]
    if len(unresolved):
        raise ValueError(
            "Unresolved acronyms -- add them to EADA_NAME_OVERRIDES:\n"
            + unresolved[["team", "team_old", "full_name"]].to_string(index=False)
        )

    resolved = crosswalk[crosswalk["unitid"].notna()]
    collisions = resolved[resolved["unitid"].duplicated(keep=False)]
    if len(collisions):
        raise ValueError(
            "Two acronyms claimed the same EADA institution -- disambiguate with "
            "EADA_UNITID_OVERRIDES:\n"
            + collisions[["team", "team_old", "full_name", "institution_name"]].to_string(index=False)
        )

    return crosswalk.sort_values(["team", "unitid"]).reset_index(drop=True)


def main():
    combined = pd.read_csv(INPUT_PATH, low_memory=False)
    combined.columns = combined.columns.str.strip()

    eada = load_eada()
    crosswalk = build_crosswalk(combined, eada)
    crosswalk.to_csv(CROSSWALK_PATH, index=False)

    features = derive_features(eada)

    # Collapse EADA to one row per (acronym, season) *before* touching the player table, so a
    # school with two unitids cannot silently duplicate player rows.
    by_team = features.merge(crosswalk[["team", "unitid"]].dropna(), on="unitid", how="inner")
    duplicated = by_team.duplicated(["team", "eada_year"], keep=False)
    if duplicated.any():
        raise ValueError(
            "Two EADA records for the same team and season:\n"
            + by_team[duplicated][["team", "unitid", "eada_year"]].to_string(index=False)
        )
    by_team = by_team.drop(columns=["unitid"])

    n_before = len(combined)
    merged = combined.merge(
        by_team, how="left", left_on=["team", "year"], right_on=["team", "eada_year"]
    )
    merged = merged.drop(columns=["eada_year"])

    assert len(merged) == n_before, (
        f"Row count changed: {n_before} -> {len(merged)}"
    )

    merged.to_csv(OUTPUT_PATH, index=False)

    absent = crosswalk[crosswalk["source"] == "absent"]["team"].tolist()
    resolved_teams = crosswalk.loc[crosswalk["unitid"].notna(), "team"].nunique()
    print(f"Input rows: {n_before}  ({len(combined.columns)} -> {len(merged.columns)} columns)")
    print(f"Teams resolved: {resolved_teams}/{crosswalk['team'].nunique()} "
          f"(absent from EADA by design: {', '.join(absent)})")
    print(f"Crosswalk written to {CROSSWALK_PATH.relative_to(ROOT)}")
    print("\nMatch rate by season:")
    for year, group in merged.groupby("year"):
        rate = group["budget_pct_eada_team"].notna().mean()
        carried = " (carried forward from %d)" % CARRY_FORWARD[year] if year in CARRY_FORWARD else ""
        print(f"  {year}: {rate:.1%} of {len(group):,} player-rows{carried}")
        assert rate >= MIN_MATCH_RATE, f"{year} match rate {rate:.1%} below {MIN_MATCH_RATE:.0%}"
    print(f"\nWrote {OUTPUT_PATH.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
