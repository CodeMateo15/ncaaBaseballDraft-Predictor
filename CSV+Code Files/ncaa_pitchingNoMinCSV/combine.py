import os
import re
import pandas as pd

FOLDER = os.path.dirname(os.path.abspath(__file__))

# Columns to appear at the front if present
INFO_COLS_ORDER = ["name", "team", "age", "nameascii", "playerid", "mlbamid", "year"]

# Regex for filenames like: batting_standard_2023.csv
FNAME_RE = re.compile(r"^(batting|pitching)_(standard|advanced)_(\d{4})\.csv$", re.IGNORECASE)

def load_csv(path: str, year: int) -> pd.DataFrame:
    """Read a CSV, normalize column names, add year column."""
    df = pd.read_csv(path)
    df.columns = [c.strip().lower() for c in df.columns]
    df["year"] = int(year)
    if "playerid" in df.columns:
        df["playerid"] = df["playerid"].astype(str).str.strip()
    return df

def main():
    files = [f for f in os.listdir(FOLDER) if f.lower().endswith(".csv")]

    # Group by (player_type, year)
    groups = {}
    for f in files:
        m = FNAME_RE.match(f)
        if not m:
            continue
        player_type, data_type, year = m.group(1).lower(), m.group(2).lower(), m.group(3)
        groups.setdefault((player_type, year), {})[data_type] = f

    if not groups:
        print("No matching CSVs found.")
        return

    combined_results = {"batting": [], "pitching": []}

    for (player_type, year), files_dict in sorted(groups.items()):
        if "standard" not in files_dict or "advanced" not in files_dict:
            print(f"Skipping {player_type}_{year}: need both standard & advanced.")
            continue

        std_path = os.path.join(FOLDER, files_dict["standard"])
        adv_path = os.path.join(FOLDER, files_dict["advanced"])
        print(f"Merging {player_type}_{year}...")

        df_std = load_csv(std_path, int(year))
        df_adv = load_csv(adv_path, int(year))

        if "playerid" not in df_std.columns or "playerid" not in df_adv.columns:
            print(f"  !! Missing 'PlayerId' in one of the files for {player_type}_{year}. Skipping.")
            continue

        # Drop duplicate cols from advanced
        drop_cols = [c for c in df_adv.columns if c in df_std.columns and c not in {"playerid"}]
        df_adv = df_adv.drop(columns=drop_cols)

        # Merge on playerid
        merged = pd.merge(df_std, df_adv, on="playerid", how="outer")

        combined_results[player_type].append(merged)

    # Concatenate all years for each player_type
    for player_type, dfs in combined_results.items():
        if dfs:
            all_years = pd.concat(dfs, ignore_index=True)

            # Reorder columns
            info_present = [c for c in INFO_COLS_ORDER if c in all_years.columns]
            others = [c for c in all_years.columns if c not in info_present]
            all_years = all_years[info_present + others]

            out_name = f"{player_type}_combined_all.csv"
            all_years.to_csv(os.path.join(FOLDER, out_name), index=False)
            print(f" -> Saved {out_name} (rows: {len(all_years)})")

if __name__ == "__main__":
    main()
