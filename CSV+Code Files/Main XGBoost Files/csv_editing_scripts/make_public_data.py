"""Generate public, FanGraphs-stripped copies of the combined input matrices.

The original CSVs contain player-level batting and pitching columns scraped
from FanGraphs, which we cannot redistribute. This script reads the private
originals and writes `*_public.csv` copies with all FG-sourced player-level
columns removed.

Run from this directory:
    python make_public_data.py
"""

from pathlib import Path
import pandas as pd

HERE = Path(__file__).resolve().parent
DATA_DIR = HERE.parent  # CSV+Code Files/Main XGBoost Files/

# Pairs of (private_filename, public_filename). The private originals stay
# untouched on disk and are gitignored; the public copies are what gets
# committed and shared.
FILES = [
    ("batting_pitching_combined_with_rpi.csv",
     "batting_pitching_combined_with_rpi_public.csv"),
    ("batting_pitching_combined.csv",
     "batting_pitching_combined_public.csv"),
]

# Any column ending in `_bat` or `_pitch` is a player-level statistic sourced
# from the FanGraphs scrape. Edit COLUMN_KEEP_OVERRIDES below if you want to
# retain a specific column (e.g., basic counting stats also available from
# NCAA stats).
def is_fg_column(col: str) -> bool:
    return col.endswith("_bat") or col.endswith("_pitch")

COLUMN_KEEP_OVERRIDES: set[str] = set()  # e.g. {"ab_bat", "ip_pitch"}


def strip(df: pd.DataFrame) -> pd.DataFrame:
    drop_cols = [c for c in df.columns
                 if is_fg_column(c) and c not in COLUMN_KEEP_OVERRIDES]
    return df.drop(columns=drop_cols), drop_cols


def main() -> None:
    for private, public in FILES:
        src = DATA_DIR / private
        if not src.exists():
            print(f"skip: {private} (not present)")
            continue
        df = pd.read_csv(src)
        stripped, dropped = strip(df)
        dst = DATA_DIR / public
        stripped.to_csv(dst, index=False)
        print(f"wrote {public}: {len(stripped.columns)} cols kept, "
              f"{len(dropped)} FG cols stripped")


if __name__ == "__main__":
    main()
