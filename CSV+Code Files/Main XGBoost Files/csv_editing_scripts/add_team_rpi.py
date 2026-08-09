import pandas as pd
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
RPI_DIR = ROOT / "ncaa_rpiYears"

INPUT_PATH = ROOT / "batting_pitching_combined.csv"
OUTPUT_PATH = ROOT / "batting_pitching_combined_with_rpi.csv"

YEARS = [2021, 2022, 2023, 2024, 2025]

RENAME_HRN = {
    "H": "Home", "H_Wins": "Home_Wins", "H_Losses": "Home_Losses",
    "R": "Road", "R_Wins": "Road_Wins", "R_Losses": "Road_Losses",
    "N": "Neutral", "N_Wins": "Neutral_Wins", "N_Losses": "Neutral_Losses",
}

DROP_REDUNDANT = [
    "Conference",
    "Overall_Record", "Overall_Record_Wins", "Overall_Record_Losses",
]

# Maps input `team_old` values to the matching `Team` value in the RPI files.
# Built by diffing unmatched names between the two sources. Cases:
#   - "St." abbreviation -> "State"
#   - Acronyms expanded ("NIU" -> "Northern Illinois", "SFA" -> "Stephen F. Austin")
#   - "U" / "University" prefix/suffix differences ("UConn" -> "Connecticut", "Lamar University" -> "Lamar")
#   - Rebrand/legacy names ("Southern California" -> "USC", "Col. of Charleston" -> "Charleston")
TEAM_NAME_MAP = {
    # St. -> State
    "Alabama St.": "Alabama State",
    "Arizona St.": "Arizona State",
    "Arkansas St.": "Arkansas State",
    "Ball St.": "Ball State",
    "Coppin St.": "Coppin State",
    "Delaware St.": "Delaware State",
    "Florida St.": "Florida State",
    "Fresno St.": "Fresno State",
    "Georgia St.": "Georgia State",
    "Illinois St.": "Illinois State",
    "Indiana St.": "Indiana State",
    "Jackson St.": "Jackson State",
    "Jacksonville St.": "Jacksonville State",
    "Kansas St.": "Kansas State",
    "Kennesaw St.": "Kennesaw State",
    "Kent St.": "Kent State",
    "Long Beach St.": "Long Beach State",
    "Michigan St.": "Michigan State",
    "Mississippi St.": "Mississippi State",
    "Missouri St.": "Missouri State",
    "Morehead St.": "Morehead State",
    "Murray St.": "Murray State",
    "New Mexico St.": "New Mexico State",
    "Norfolk St.": "Norfolk State",
    "North Dakota St.": "North Dakota State",
    "Ohio St.": "Ohio State",
    "Oklahoma St.": "Oklahoma State",
    "Oregon St.": "Oregon State",
    "Penn St.": "Penn State",
    "Sacramento St.": "Sacramento State",
    "San Diego St.": "San Diego State",
    "San Jose St.": "San Jose State",
    "South Dakota St.": "South Dakota State",
    "Tarleton St.": "Tarleton State",
    "Texas St.": "Texas State",
    "Washington St.": "Washington State",
    "Western Ill.": "Western Illinois",
    "Wichita St.": "Wichita State",
    "Wright St.": "Wright State",
    "Youngstown St.": "Youngstown State",
    # Abbreviations / shorter forms
    "A&M-Corpus Christi": "Texas A&M-Corpus Christi",
    "Alcorn": "Alcorn State",
    "App State": "Appalachian State",
    "Ark.-Pine Bluff": "Arkansas-Pine Bluff",
    "Army West Point": "Army",
    "CSU Bakersfield": "Cal State Bakersfield",
    "CSUN": "Cal State Northridge",
    "Cal St. Fullerton": "Cal State Fullerton",
    "Central Ark.": "Central Arkansas",
    "Central Conn. St.": "Central Connecticut",
    "Central Mich.": "Central Michigan",
    "Charleston So.": "Charleston Southern",
    "Col. of Charleston": "Charleston",
    "DBU": "Dallas Baptist",
    "ETSU": "East Tennessee State",
    "Eastern Ill.": "Eastern Illinois",
    "Eastern Ky.": "Eastern Kentucky",
    "Eastern Mich.": "Eastern Michigan",
    "FDU": "Fairleigh Dickinson",
    "Fla. Atlantic": "FAU",
    "Ga. Southern": "Georgia Southern",
    "Grambling": "Grambling State",
    "LIU": "Long Island",
    "LMU": "Loyola-Marymount",
    "Lamar University": "Lamar",
    "Massachusetts": "UMass",
    "Miami University-Ohio": "Miami (OH)",
    "Middle Tenn.": "Middle Tennessee",
    "Mississippi Val.": "Mississippi Valley State",
    "Mount St. Mary's": "Mount Saint Mary's",
    "N.C. A&T": "North Carolina A&T",
    "N.C. Central": "North Carolina Central",
    "NC State": "North Carolina State",
    "NIU": "Northern Illinois",
    "North Ala.": "North Alabama",
    "Northern Colo.": "Northern Colorado",
    "Northern Ky.": "Northern Kentucky",
    "Northwestern St.": "Northwestern State",
    "Prairie View": "Prairie View A&M",
    "Presbyterian": "Presbyterian College",
    "SFA": "Stephen F. Austin",
    "Saint Mary's": "Saint Mary's College",
    "Sam Houston": "Sam Houston State",
    "Seattle U": "Seattle University",
    "South Fla.": "South Florida",
    "Southern Ind.": "Southern Indiana",
    "St. Thomas (MN)": "Saint Thomas",
    "Southeast Mo. St.": "Southeast Missouri",
    "Southeastern La.": "Southeastern Louisiana",
    "Southern California": "USC",
    "Southern Ill.": "Southern Illinois",
    "Southern Miss.": "Southern Miss",
    "Southern U.": "Southern",
    "St. Bonaventure": "Saint Bonaventure",
    "St. John's": "Saint John's",
    "UAlbany": "Albany",
    "UConn": "Connecticut",
    "UIW": "Incarnate Word",
    "UMES": "Maryland Eastern Shore",
    "UMass Lowell": "UMass-Lowell",
    "UNC Greensboro": "UNCG",
    "USC Upstate": "South Carolina Upstate",
    "UT Arlington": "UTA",
    "UT Martin": "Tennessee-Martin",
    "University of Miami": "Miami (FL)",
    "Western Caro.": "Western Carolina",
    "Western Ky.": "Western Kentucky",
    "Western Mich.": "Western Michigan",
}

# Year-specific overrides for schools that were renamed mid-period.
# Applied AFTER TEAM_NAME_MAP. Key: (team_old, year), value: RPI Team for that year.
# Houston Baptist -> Houston Christian rebrand: NCAA RPI files used the old name
# through 2022 and the new name from 2023 onward; the input always says "Houston Christian".
TEAM_NAME_MAP_BY_YEAR = {
    ("Houston Christian", 2021): "Houston Baptist",
    ("Houston Christian", 2022): "Houston Baptist",
}


def load_rpi_year(year: int) -> pd.DataFrame:
    df = pd.read_csv(RPI_DIR / f"ncaa_rpi_{year}.csv")
    df.columns = df.columns.str.strip()

    # 2021 has an extra standalone "RPI" column; use "Rank" as the source.
    if "RPI" in df.columns and "Rank" in df.columns:
        df = df.drop(columns=["RPI"])

    df = df.rename(columns={"Rank": "rpi", **RENAME_HRN})
    df = df.drop(columns=[c for c in DROP_REDUNDANT if c in df.columns])
    df["year"] = year
    df["Team"] = df["Team"].astype(str).str.strip()

    # Append `_team` suffix to every new RPI-derived column so they match the
    # naming convention of the other team-level columns in the combined file.
    keep_as_is = {"Team", "year"}
    df = df.rename(columns={c: f"{c}_team" for c in df.columns if c not in keep_as_is})
    return df


def main():
    batting = pd.read_csv(INPUT_PATH)
    batting.columns = batting.columns.str.strip()
    batting["team_old"] = batting["team_old"].astype(str).str.strip()

    # Normalized join key — leaves `team_old` itself untouched in the output.
    batting["_join_team"] = batting["team_old"].replace(TEAM_NAME_MAP)
    if TEAM_NAME_MAP_BY_YEAR:
        yr_overrides = batting.apply(
            lambda r: TEAM_NAME_MAP_BY_YEAR.get((r["team_old"], r["year"])), axis=1
        )
        batting["_join_team"] = yr_overrides.combine_first(batting["_join_team"])

    rpi_all = pd.concat([load_rpi_year(y) for y in YEARS], ignore_index=True)

    n_before = len(batting)
    merged = batting.merge(
        rpi_all,
        how="left",
        left_on=["_join_team", "year"],
        right_on=["Team", "year"],
    )
    merged = merged.drop(columns=["Team", "_join_team"])

    assert len(merged) == n_before, (
        f"Row count changed: {n_before} -> {len(merged)} (possible duplicate Team+year in RPI data)"
    )

    merged.to_csv(OUTPUT_PATH, index=False)

    matched = merged["rpi_team"].notna().sum()
    unmatched = n_before - matched
    print(f"Input rows: {n_before}")
    print(f"Matched (got RPI data): {matched} ({matched / n_before:.1%})")
    print(f"Unmatched (NaN RPI):    {unmatched} ({unmatched / n_before:.1%})")
    print(f"Wrote {OUTPUT_PATH.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
