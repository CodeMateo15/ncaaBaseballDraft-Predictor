"""One-shot repair for three bad rows in the team-identity reference tables.

`standardized/team_name_mapping.csv` maps an institution name (`team_new`) to the NCAA
short name (`team_old`), and `team_old` is what the team-stats JSON and the NCAA RPI files
are keyed on. Three entries pointed at the wrong school:

    team_new                      team_old was    should be
    Coppin State University       "App State"     "Coppin St."
    Appalachian State University  "Coppin St."    "App State"
    Indiana State University      "Indiana"       "Indiana St."
    Indiana University            "Indiana St."   "Indiana"

so Indiana carried Indiana State's conference, record, RPI and every other team-level
column (and vice versa), and the same for Appalachian State and Coppin State -- across
every season. Verified against reality: Indiana is Big Ten and Indiana State is MVC, but
the file had IU tagged MVC with Indiana State's 45-17 2023; Appalachian State is Sun Belt
and Coppin State is NEC/MEAC, and those were reversed too.

Separately, `standardized/unique_teams.csv` mapped acronym PORT to "Portland State
University". Portland State has not sponsored baseball since 1993 -- the program in this
dataset (team_old "Portland", WCC) is the University of Portland. Only the display name
was wrong there; `team_old` was already correct, so the team stats are fine.

The reference tables are fixed at the source. This script repairs the already-materialized
combined CSV, which would otherwise need the full historical pipeline to regenerate. The
two swapped pairs are an exact exchange, so swapping the team-level block back between the
two acronyms within each season restores the correct values.

Idempotent: re-running is a no-op once the data is already correct.

Run:  python csv_editing_scripts/fix_swapped_team_identities.py
"""

from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent

TARGETS = [
    ROOT / "batting_pitching_combined_with_rpi_2026.csv",
    # The redistributable copy carries the same team-level columns, so it carries the same
    # error. (It is also stale for unrelated reasons -- 2021-2025 only, pre-relabel -- but
    # that is a separate regeneration.)
    ROOT.parent / "archive" / "data" / "batting_pitching_combined_with_rpi_public.csv",
]

# (acronym_a, acronym_b) whose team-level columns are exchanged, plus the conference(s) each
# acronym should end up in over 2021-2026 -- used both to detect whether the fix is still
# needed and to verify it afterwards. Coppin State moved from the MEAC to the NEC in 2023,
# so its side is legitimately two values.
SWAPPED_PAIRS = [
    ("IU", "INST", {"Big Ten"}, {"MVC"}),
    ("APP", "COPP", {"Sun Belt"}, {"MEAC", "NEC"}),
]

# Acronym -> corrected institution name, for `Full Name_team` / `team_new`.
RENAMED = {"PORT": "University of Portland"}

# Columns that describe the *institution* and are already correct -- never swap these.
IDENTITY_COLUMNS = ["Acronym", "Full Team Name", "Full Name_team", "id_team", "division_team", "team_new"]

# Everything from `team_old` onward is team-level data keyed on `team_old`: the NCAA short
# name itself, the team-stats block from the JSON, the conference block, and the RPI block.
FIRST_TEAM_DATA_COLUMN = "team_old"


def team_data_columns(columns: list[str]) -> list[str]:
    start = columns.index(FIRST_TEAM_DATA_COLUMN)
    return [c for c in columns[start:] if c not in IDENTITY_COLUMNS]


def repair(target: Path):
    if not target.exists():
        print(f"{target.name}: not present, skipping")
        return

    df = pd.read_csv(target, low_memory=False)
    columns = list(df.columns)
    data_cols = team_data_columns(columns)
    print(f"{target.name}: {len(df):,} rows, {len(data_cols)} team-level columns")

    changed = False

    for acr_a, acr_b, conf_a, conf_b in SWAPPED_PAIRS:
        mask_a = df["team"] == acr_a
        mask_b = df["team"] == acr_b
        if not mask_a.any() or not mask_b.any():
            print(f"  {acr_a}/{acr_b}: one side absent, skipping")
            continue

        observed = set(df.loc[mask_a, "league_team"].dropna().unique())
        if observed <= conf_a:
            print(f"  {acr_a}/{acr_b}: already correct ({acr_a} in {sorted(conf_a)}), skipping")
            continue

        # Both sides carry a single team-level row per season, so build season -> values
        # lookups from each side and write them across.
        rows_a = df.loc[mask_a].drop_duplicates("year").set_index("year")[data_cols]
        rows_b = df.loc[mask_b].drop_duplicates("year").set_index("year")[data_cols]

        for mask, source in ((mask_a, rows_b), (mask_b, rows_a)):
            years = df.loc[mask, "year"]
            replacement = source.reindex(years.values)
            replacement.index = years.index
            df.loc[mask, data_cols] = replacement

        changed = True
        print(f"  {acr_a}/{acr_b}: swapped "
              f"({int(mask_a.sum()):,} + {int(mask_b.sum()):,} player-rows)")

    for acronym, correct_name in RENAMED.items():
        mask = df["team"] == acronym
        if not mask.any() or (df.loc[mask, "Full Name_team"] == correct_name).all():
            print(f"  {acronym}: name already correct, skipping")
            continue
        for column in ("Full Name_team", "team_new"):
            df.loc[mask, column] = correct_name
        changed = True
        print(f"  {acronym}: renamed to {correct_name!r} ({int(mask.sum()):,} player-rows)")

    if not changed:
        print("  nothing to do -- already correct")
        return

    # Verify against the known-correct conferences before writing anything back.
    for acr_a, acr_b, conf_a, conf_b in SWAPPED_PAIRS:
        for acronym, expected in ((acr_a, conf_a), (acr_b, conf_b)):
            got = set(df.loc[df["team"] == acronym, "league_team"].dropna().unique())
            assert got <= expected, f"{acronym} should be in {sorted(expected)}, got {sorted(got)}"

    df.to_csv(target, index=False)
    print(f"  wrote {target.name}")


def main():
    for target in TARGETS:
        repair(target)
        print()
    print("Now re-run:  python csv_editing_scripts/add_team_eada.py")


if __name__ == "__main__":
    main()
