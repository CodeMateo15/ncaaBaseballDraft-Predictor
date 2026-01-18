import pandas as pd
import os

base_dir = os.path.dirname(__file__)
parent_dir = os.path.dirname(base_dir)
teams_path = os.path.join(parent_dir, "standardized", "unique_teams.csv")
mapping_path = os.path.join(parent_dir, "standardized", "team_name_mapping.csv")
batting_path = os.path.join(base_dir, "battingDraftTable.csv")
all_path = os.path.join(parent_dir, "ncaabb_dataset.csv")


# === Load Data ===
batting = pd.read_csv(batting_path)
unique_teams = pd.read_csv(teams_path)
team_mapping = pd.read_csv(mapping_path)
ncaabb = pd.read_csv(all_path)

for df in [team_mapping]:
    df['team_old'] = df['team_old'].astype(str).str.strip()
    df['team_new'] = df['team_new'].astype(str).str.strip()

for df in [ncaabb]:
    df['team'] = df['team'].astype(str).str.strip()

# --- Step 0: Filter Division I teams only ---
team_mapping = team_mapping[team_mapping["division"] == 1]

# --- Step 1: Merge BattingDraftTable with unique_teams on Acronym ---
batting_merged = batting.merge(unique_teams, on="Acronym", how="left")

# --- Step 2: Merge with team_name_mapping on Full Name ↔ team_new ---
batting_merged = batting_merged.merge(team_mapping, left_on="Full Name", right_on="team_new", how="left")

# --- Step 3: Merge with ncaabb_dataset on team_old ↔ team and year ---
final = batting_merged.merge(
    ncaabb,
    left_on=["team_old", "year"],
    right_on=["team", "year"],
    how="left",
    suffixes=("", "_teamstats")
)

# --- Optional: Drop redundant columns ---
#final.drop(columns=["team_old", "team_new", "Full Name", "team"], inplace=True, errors="ignore")

# --- Save result ---
output_path = os.path.join(base_dir, "BattingDraftTable_with_teamstats.csv")
final.to_csv(output_path, index=False)
print("✅ Merged file saved as BattingDraftTable_with_teamstats.csv")
