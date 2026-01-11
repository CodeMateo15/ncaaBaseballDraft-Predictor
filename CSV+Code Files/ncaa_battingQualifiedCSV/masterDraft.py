import pandas as pd
import json
import os
from rapidfuzz import process, fuzz
from tqdm import tqdm
import re

# Paths
base_dir = os.path.dirname(__file__)
parent_dir = os.path.dirname(base_dir)
teams_path = os.path.join(parent_dir, "standardized", "unique_teams.csv")
mapping_path = os.path.join(parent_dir, "standardized", "team_name_mapping.csv")
batting_path = os.path.join(base_dir, "batting_combined_all.csv")
drafts_path = os.path.join(parent_dir, "all_drafts.json")

# === Load Data ===
batting_df = pd.read_csv(batting_path)
teams_df = pd.read_csv(teams_path)
mapping_df = pd.read_csv(mapping_path)

with open(drafts_path, "r") as f:
    drafts_df = pd.DataFrame(json.load(f))

# === Normalize strings ===
def clean(s):
    if pd.isna(s):
        return ""
    s = str(s).lower()
    s = s.replace("&", "and")
    s = re.sub(r"\buniv\.?\b", "university", s)  # only replace whole word "univ" or "univ."
    s = s.replace("college of", "")
    s = re.sub(r"\s+", " ", s)  # remove extra spaces
    return s.strip()


batting_df["name"] = batting_df["name"].apply(clean)
batting_df["team"] = batting_df["team"]
drafts_df["Player Name"] = drafts_df["Player Name"].apply(clean)
drafts_df["Drafted From"] = drafts_df["Drafted From"].apply(clean)

teams_df["Acronym"] = teams_df["Acronym"]
teams_df["Full Name"] = teams_df["Full Name"].apply(clean)
mapping_df["team_old"] = mapping_df["team_old"].apply(clean)
mapping_df["team_new"] = mapping_df["team_new"].apply(clean)

missing_path = os.path.join(parent_dir, "standardized", "missing_schools.csv")
missing_df = pd.read_csv(missing_path)
missing_df["Old name"] = missing_df["Old name"].apply(clean)
missing_df["New name"] = missing_df["New name"].apply(clean)

# Create a dictionary for fast lookup
missing_dict = dict(zip(missing_df["Old name"], missing_df["New name"]))

# === Merge team acronyms into full names ===
batting_df = batting_df.merge(
    teams_df,
    left_on="team",
    right_on="Acronym",
    how="left"
)
batting_df.rename(columns={"Full Name": "Full Team Name"}, inplace=True)

# If still missing, use the mapping CSV
for idx, row in batting_df[batting_df["Full Team Name"].isna()].iterrows():
    old_team = row["team"]
    mapping_match = mapping_df[mapping_df["team_old"] == old_team]
    if not mapping_match.empty:
        new_name = mapping_match.iloc[0]["team_new"]
        matched_team = teams_df[teams_df["Full Name"].str.contains(new_name)]
        if not matched_team.empty:
            batting_df.at[idx, "Full Team Name"] = matched_team.iloc[0]["Full Name"]

# === Helper to get draft match ===
def find_draft_match(player_name, school_name, year):
    drafts_year = drafts_df[drafts_df["Year"] == year]
    if drafts_year.empty:
        return None

    best_row = None
    best_score = 0

    for _, d_row in drafts_year.iterrows():
        d_name, d_school = d_row["Player Name"], d_row["Drafted From"]

        # Apply missing_schools correction if needed
        d_school_fixed = missing_dict.get(clean(d_school), clean(d_school))

        name_score = fuzz.token_set_ratio(player_name, d_name)
        school_score = fuzz.token_set_ratio(school_name, d_school_fixed)

        # Condition 1: fuzzy name ≥90 + exact school
        if name_score >= 90 and clean(school_name) == d_school_fixed:
            return d_row

        # Condition 2: exact name + fuzzy school ≥90
        if clean(player_name) == clean(d_name) and school_score >= 90:
            return d_row

        # Track best for debugging (optional)
        if name_score > best_score:
            best_score = name_score
            best_row = d_row

    return None  # no valid match


# === Match and build master table ===
rounds, picks, drafted_by, drafted_from, drafted_flags = [], [], [], [], []

for _, row in tqdm(batting_df.iterrows(), total=len(batting_df), desc="Matching players"):
    match = find_draft_match(row["name"], row["Full Team Name"], row.get("year"))
    if match is not None:
        rounds.append(match["Round"])
        picks.append(match["Pick"])
        drafted_by.append(match["Drafted By"])
        drafted_from.append(match["Drafted From"])
        drafted_flags.append(True)
    else:
        rounds.append(None)
        picks.append(None)
        drafted_by.append(None)
        drafted_from.append(None)
        drafted_flags.append(False)

batting_df["Round"] = rounds
batting_df["Pick"] = picks
batting_df["Drafted By"] = drafted_by
batting_df["Drafted From"] = drafted_from
batting_df["Drafted?"] = drafted_flags

# === Output ===
output_path = os.path.join(base_dir, "battingDraftTablev2.csv")
batting_df.to_csv(output_path, index=False)
print(f"✅ Master CSV created: {output_path}")