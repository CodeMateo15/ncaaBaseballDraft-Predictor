import pandas as pd
import numpy as np

# -----------------------------
# Load CSVs
# -----------------------------
batting = pd.read_csv("BattingDraftTable_with_teamstats.csv")
pitching = pd.read_csv("PitchingDraftTable_with_teamstats.csv")

# -----------------------------
# Player-season keys
# -----------------------------
keys = ["name", "team", "age", "nameascii", "playerid", "mlbamid", "year"]

# -----------------------------
# Merge
# -----------------------------
merged = pd.merge(
    pitching,
    batting,
    on=keys,
    how="outer",
    suffixes=("_pitch", "_bat")
)

# -----------------------------
# Add role column
# -----------------------------
merged["role"] = np.select(
    [
        merged["ip"].notna() & merged["ab"].notna(),
        merged["ip"].notna(),
        merged["ab"].notna()
    ],
    ["Two-Way", "Pitcher", "Batter"],
    default="Unknown"
)

# -----------------------------
# Metadata columns to collapse
# -----------------------------
shared_columns = [
    "Acronym","Full Team Name","Round","Pick","Drafted By",
    "Drafted From","Drafted?","Full Name","team_id","division",
    "team_old","team_new","team_teamstats","league",
    "W","L","T","G","WPCT","PE","Difference",
    "BB (Batting)","AB","H","BA","DP","DPPG","2B","2BPG","IP",
    "R (Pitching)","ER","ERA","PO","A","E","FPCT","HB","HBP",
    "HA","HAPG","HR","HRPG","SF","SH","OBP","SB","SBPG","CS",
    "R (Batting)","RPG","SHO","TB","SLG","SO","BB (Pitching)",
    "K/BB","K/9","TP","3B","3BPG","WHIP","BBPG (Pitching)"
]

# -----------------------------
# Collapse shared metadata columns
# -----------------------------
for col in shared_columns:
    pitch_col = f"{col}_pitch"
    bat_col = f"{col}_bat"

    if pitch_col in merged.columns or bat_col in merged.columns:
        merged[col] = merged[pitch_col].combine_first(
            merged[bat_col] if bat_col in merged.columns else None
        )

        if pitch_col in merged.columns:
            merged.drop(columns=pitch_col, inplace=True)
        if bat_col in merged.columns:
            merged.drop(columns=bat_col, inplace=True)

# -----------------------------
# SAVE WITHOUT FILTERING STATS
# -----------------------------
merged.to_csv("batting_pitching_combined2.csv", index=False)
