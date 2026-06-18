import pandas as pd

# Load data
batting = pd.read_csv("batting_pitching_combined.csv")
conf_map = pd.read_csv("missing_conf.csv")
conf_rank = pd.read_csv("conference_rankings.csv")

# Clean column names
for df in [batting, conf_map, conf_rank]:
    df.columns = df.columns.str.strip()

# Strip whitespace from keys
batting["league_team"] = batting["league_team"].str.strip()
conf_map["league name"] = conf_map["league name"].str.strip()
conf_map["conf name"] = conf_map["conf name"].str.strip()
conf_rank["conference"] = conf_rank["conference"].str.strip()

# ------------------------------------------------
# Merge with year-aware conference mapping
# ------------------------------------------------

# First merge on league name
merged = batting.merge(
    conf_map,
    how="left",
    left_on="league_team",
    right_on="league name"
)

# If start_year / end_year exist, filter by valid year
if {"start_year", "end_year"}.issubset(conf_map.columns):
    merged = merged[
        (merged["year"] >= merged["start_year"]) &
        (merged["year"] <= merged["end_year"])
    ]

# ------------------------------------------------
# Merge conference rankings
# ------------------------------------------------

merged = merged.merge(
    conf_rank,
    how="left",
    left_on=["conf name", "year"],
    right_on=["conference", "year"]
)

# Rename columns
rename_dict = {
    "conf-rpi": "conf_rpi_team",
    "conf-rank": "conf_rank_team",
    "conf-national-rec": "conf_national_rec_team",
    "conf-national-wp": "conf_national_wp_team"
}

merged = merged.rename(columns=rename_dict)

# Insert after league_team
cols = list(merged.columns)
insert_index = cols.index("league_team") + 1

new_cols = [
    "conf_rpi_team",
    "conf_rank_team",
    "conf_national_rec_team",
    "conf_national_wp_team"
]

for col in new_cols:
    cols.remove(col)

for i, col in enumerate(new_cols):
    cols.insert(insert_index + i, col)

merged = merged[cols]

# Drop helper columns
drop_cols = ["league name", "conf name", "conference", "start_year", "end_year"]
merged = merged.drop(columns=[c for c in drop_cols if c in merged.columns])

# Save
merged.to_csv("batting_pitching_with_conf_stats.csv", index=False)

print("Done.")