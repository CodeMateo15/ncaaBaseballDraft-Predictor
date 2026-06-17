import sys
from pathlib import Path

import pandas as pd

from add_team_rpi import TEAM_NAME_MAP, TEAM_NAME_MAP_BY_YEAR

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
RPI_DIR = ROOT / "ncaa_rpiYears"

INPUT_PATH = ROOT / "batting_pitching_combined.csv"
OUTPUT_PATH = ROOT / "batting_pitching_combined_with_rpi.csv"

YEARS = [2021, 2022, 2023, 2024, 2025]

EXPECTED_NEW_COLS = [
    "rpi_team",
    "Conference_Record_team", "Conference_Record_Wins_team", "Conference_Record_Losses_team",
    "SOS_team",
    "NC_Rec_team", "NC_Rec_Wins_team", "NC_Rec_Losses_team",
    "NC_RPI_team", "NC_SOS_team",
    "Home_team", "Home_Wins_team", "Home_Losses_team",
    "Road_team", "Road_Wins_team", "Road_Losses_team",
    "Neutral_team", "Neutral_Wins_team", "Neutral_Losses_team",
    "Q1_team", "Q1_Wins_team", "Q1_Losses_team",
    "Q2_team", "Q2_Wins_team", "Q2_Losses_team",
    "Q3_team", "Q3_Wins_team", "Q3_Losses_team",
    "Q4_team", "Q4_Wins_team", "Q4_Losses_team",
]

# Columns the user explicitly asked to keep OUT of the output.
FORBIDDEN_COLS = [
    "Rank", "Team", "Conference",
    "Overall_Record", "Overall_Record_Wins", "Overall_Record_Losses",
]

# Identity columns that must be byte-identical between input and output.
IDENTITY_COLS = ["name", "team", "year", "playerid"]

RENAME_HRN = {
    "H": "Home", "H_Wins": "Home_Wins", "H_Losses": "Home_Losses",
    "R": "Road", "R_Wins": "Road_Wins", "R_Losses": "Road_Losses",
    "N": "Neutral", "N_Wins": "Neutral_Wins", "N_Losses": "Neutral_Losses",
}


def load_rpi_year(year: int) -> pd.DataFrame:
    df = pd.read_csv(RPI_DIR / f"ncaa_rpi_{year}.csv")
    df.columns = df.columns.str.strip()
    if "RPI" in df.columns and "Rank" in df.columns:
        df = df.drop(columns=["RPI"])
    df = df.rename(columns={"Rank": "rpi", **RENAME_HRN})
    df = df.drop(columns=[c for c in ("Conference", "Overall_Record",
                                       "Overall_Record_Wins", "Overall_Record_Losses")
                          if c in df.columns])
    df["year"] = year
    df["Team"] = df["Team"].astype(str).str.strip()
    # Mirror the merge script: append `_team` to all RPI value columns.
    keep_as_is = {"Team", "year"}
    df = df.rename(columns={c: f"{c}_team" for c in df.columns if c not in keep_as_is})
    return df


def section(title: str) -> None:
    print()
    print(f"=== {title} ===")


def main() -> int:
    failures: list[str] = []

    batting = pd.read_csv(INPUT_PATH)
    batting.columns = batting.columns.str.strip()
    batting["team_old"] = batting["team_old"].astype(str).str.strip()
    # Mirror the merge script: build a normalized join column.
    batting["_join_team"] = batting["team_old"].replace(TEAM_NAME_MAP)
    if TEAM_NAME_MAP_BY_YEAR:
        yr_overrides = batting.apply(
            lambda r: TEAM_NAME_MAP_BY_YEAR.get((r["team_old"], r["year"])), axis=1
        )
        batting["_join_team"] = yr_overrides.combine_first(batting["_join_team"])

    merged = pd.read_csv(OUTPUT_PATH)
    merged.columns = merged.columns.str.strip()

    rpi_all = pd.concat([load_rpi_year(y) for y in YEARS], ignore_index=True)

    # ---- Check 1: row count + order preserved ----
    section("Check 1: row count preserved")
    if len(merged) == len(batting):
        print(f"PASS: {len(merged)} rows in both input and output.")
    else:
        msg = f"FAIL: input has {len(batting)} rows, output has {len(merged)}."
        print(msg)
        failures.append(msg)

    # ---- Check 2: player identity preserved ----
    section("Check 2: player identity preserved")
    id_ok = True
    for col in IDENTITY_COLS:
        if col not in batting.columns or col not in merged.columns:
            msg = f"FAIL: identity column '{col}' missing from input or output."
            print(msg)
            failures.append(msg)
            id_ok = False
            continue
        # Compare with NaN-aware equality.
        a, b = batting[col].reset_index(drop=True), merged[col].reset_index(drop=True)
        diff = (a != b) & ~(a.isna() & b.isna())
        if diff.any():
            n_diff = int(diff.sum())
            msg = f"FAIL: column '{col}' differs in {n_diff} rows between input and output."
            print(msg)
            failures.append(msg)
            id_ok = False
    if id_ok:
        print(f"PASS: {IDENTITY_COLS} are identical between input and output.")

    # ---- Check 3: expected new columns present, forbidden columns absent ----
    section("Check 3: column schema")
    missing = [c for c in EXPECTED_NEW_COLS if c not in merged.columns]
    if missing:
        msg = f"FAIL: missing expected new columns: {missing}"
        print(msg)
        failures.append(msg)
    else:
        print(f"PASS: all {len(EXPECTED_NEW_COLS)} expected new columns present.")

    forbidden_present = [c for c in FORBIDDEN_COLS if c in merged.columns]
    # `team` (lowercase) and `year` are pre-existing in batting and are fine; we only
    # forbid the capitalized RPI versions and Overall_Record* (which were never in batting).
    if forbidden_present:
        msg = f"FAIL: forbidden columns present in output: {forbidden_present}"
        print(msg)
        failures.append(msg)
    else:
        print(f"PASS: none of {FORBIDDEN_COLS} appear in the output.")

    # ---- Check 4: value spot-check on joined rows ----
    section("Check 4: joined values match the RPI source row-for-row")
    rpi_lookup = rpi_all.set_index(["Team", "year"])
    spot_cols = ["rpi_team", "SOS_team", "Home_Wins_team", "Q1_Wins_team", "NC_RPI_team"]
    mismatches: list[str] = []
    checked = 0
    join_lookup = batting.set_index(batting.index)["_join_team"].to_dict()
    for idx, row in merged.iterrows():
        key = (join_lookup.get(idx), row.get("year"))
        if key not in rpi_lookup.index:
            continue
        src = rpi_lookup.loc[key]
        if isinstance(src, pd.DataFrame):
            # Duplicate Team+year in RPI; merge would have exploded rows, so this
            # shouldn't happen, but be defensive.
            mismatches.append(f"row {idx}: duplicate RPI source for {key}")
            continue
        for col in spot_cols:
            if col not in merged.columns or col not in src.index:
                continue
            mv, sv = row[col], src[col]
            if pd.isna(mv) and pd.isna(sv):
                continue
            if mv != sv:
                mismatches.append(f"row {idx} {key} col '{col}': output={mv!r}, source={sv!r}")
        checked += 1
        if len(mismatches) >= 10:
            break
    if mismatches:
        msg = f"FAIL: value mismatches on joined rows ({len(mismatches)} shown):"
        print(msg)
        for m in mismatches:
            print(f"  - {m}")
        failures.append(msg)
    else:
        print(f"PASS: spot-checked {checked} joined rows on {spot_cols}; all values match.")

    # ---- Check 5: unmatched teams report (informational) ----
    section("Check 5: unmatched teams report")
    # Use the normalized join key so the report reflects post-mapping reality.
    batting_pairs = set(
        zip(batting["_join_team"].astype(str), batting["year"].astype("Int64"))
    )
    rpi_pairs = set(
        zip(rpi_all["Team"].astype(str), rpi_all["year"].astype("Int64"))
    )

    in_input_not_rpi = sorted(
        {(t, y) for (t, y) in batting_pairs if (t, y) not in rpi_pairs and pd.notna(y)},
        key=lambda x: (x[1], x[0]),
    )
    in_rpi_not_input = sorted(
        {(t, y) for (t, y) in rpi_pairs if (t, y) not in batting_pairs},
        key=lambda x: (x[1], x[0]),
    )

    # For input-only, also surface the pre-map name to make follow-up fixes easier.
    inv_map: dict[str, str] = {}
    for src, dst in TEAM_NAME_MAP.items():
        inv_map.setdefault(dst, src)
    raw_by_norm = (
        batting[["team_old", "_join_team", "year"]]
        .drop_duplicates()
        .set_index(["_join_team", "year"])["team_old"]
        .to_dict()
    )

    print(f"Distinct (team, year) pairs from input still unmatched after mapping: {len(in_input_not_rpi)}")
    for t, y in in_input_not_rpi[:50]:
        raw = raw_by_norm.get((t, y), t)
        if raw != t:
            print(f"  input only:  {y}  '{t}'  (raw team_old: '{raw}')")
        else:
            print(f"  input only:  {y}  '{t}'")
    if len(in_input_not_rpi) > 50:
        print(f"  ... and {len(in_input_not_rpi) - 50} more")

    print()
    print(f"Distinct (Team, year) pairs in RPI not referenced by input: {len(in_rpi_not_input)}")
    for t, y in in_rpi_not_input[:50]:
        print(f"  RPI only:    {y}  '{t}'")
    if len(in_rpi_not_input) > 50:
        print(f"  ... and {len(in_rpi_not_input) - 50} more")

    # ---- Check 6: per-year match counts ----
    section("Check 6: per-year match counts")
    print(f"{'year':>6} {'rows':>8} {'matched':>10} {'unmatched':>12} {'pct':>8}")
    for y in YEARS:
        sub = merged[merged["year"] == y]
        n = len(sub)
        m = int(sub["rpi_team"].notna().sum()) if "rpi_team" in sub.columns else 0
        pct = (m / n * 100) if n else 0.0
        print(f"{y:>6} {n:>8} {m:>10} {n - m:>12} {pct:>7.1f}%")

    # ---- Final ----
    section("Summary")
    if failures:
        print(f"FAILED with {len(failures)} hard-check failure(s).")
        return 1
    print("All hard checks PASSED. See unmatched teams report above for any name-format gaps.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
