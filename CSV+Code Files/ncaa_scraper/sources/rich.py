"""Adapter for the modern mirror's 2022-2024 "rich" generation.

43 batting and 51 pitching columns, carrying school, division, conference and
class inline, so this is the closest thing to a drop-in replacement for the live
scrape. Measured against the local FanGraphs 2022 export after reading blanks as
zeros: G 97.9% exact, AB 97.6%, H 98.2%, 2B 99.2%, 3B 99.8%, HR 99.7%, R 98.9%,
RBI 98.8%, BB 99.1%, SO 98.7%, HBP 99.6%, SF 99.6%, SH 99.7%, SB 99.3%, CS 99.6%,
GDP 91.4% -- with every mean difference at or under 0.03.

Two things that look like bugs and are not:

* **Pitching rows are far fewer than batting rows** from 2024 on (5,330 vs 10,429
  for D1). The 2024+ pitching grid lists only pitchers while the batting grid
  lists every rostered player. FanGraphs 2024 has 5,318 pitching rows, so the
  mirror is at 100.2% -- complete, not truncated. 2022 and 2023 have identical
  batting and pitching row counts because those grids were keyed on games played.
* **`pitching_APP`, not `pitching_G`, is the appearances count** -- the same
  choice `ncaa/schema.py::PITCHING_MAP` already makes for the live grid.
"""

import pandas as pd

import config
from sources import _shape, rosters

YEARS = (2022, 2023, 2024)

# NCAA grid column -> the key derive/rates.py reads.
BATTING_MAP = {
    "batting_GP": "g",
    "batting_AB": "ab",
    "batting_H": "h",
    "batting_2B": "2b",
    "batting_3B": "3b",
    "batting_HR": "hr",
    "batting_R": "r",
    "batting_RBI": "rbi",
    "batting_BB": "bb",
    "batting_SO": "so",
    "batting_HBP": "hbp",
    "batting_SF": "sf",
    "batting_SH": "sh",
    "batting_GDP": "gdp",
    "batting_SB": "sb",
    "batting_CS": "cs",
}
# Kept for cross-checks rather than emitted directly.
BATTING_EXTRA = {
    "batting_TB": "tb",
    "batting_AVG": "ncaa_ba",
    "OPP DP": "opp_dp",
}

PITCHING_MAP = {
    "pitching_APP": "g",
    "pitching_GS": "gs",
    "pitching_W": "w",
    "pitching_L": "l",
    "pitching_SV": "sv",
    "pitching_CG": "cg",
    "pitching_SHO": "sho",
    "pitching_IP": "ip",
    "pitching_BF": "tbf",
    "pitching_H": "h",
    "pitching_R": "r",
    "pitching_ER": "er",
    "pitching_BB": "bb",
    "pitching_SO": "so",
    "pitching_HR": "hr",
    "pitching_HBP": "hbp",
    "pitching_WP": "wp",
    "pitching_BK": "bk",
}
PITCHING_EXTRA = {
    "pitching_AB": "p_oab",
    "pitching_ERA": "ncaa_era",
}

IDENTITY_COLUMNS = ("player_id", "player_full_name", "player_class",
                    "school_id", "school_name", "team_conference_name",
                    "player_position")

# The rich files report innings the way the NCAA site does.
IP_FORMAT = _shape.NCAA_NOTATION


def _rows(frame, year, division, category, acronyms):
    column_map = BATTING_MAP if category == "batting" else PITCHING_MAP
    extra_map = BATTING_EXTRA if category == "batting" else PITCHING_EXTRA

    _shape.require_columns(frame, column_map, where=f"rich {year} {category}")

    if category == "pitching":
        _shape.assert_ip_format(frame["pitching_IP"], IP_FORMAT,
                                where=f"rich {year} pitching")

    present_extra = {src: dst for src, dst in extra_map.items()
                     if src in frame.columns}
    rows = []
    for record in frame.to_dict("records"):
        school_id = record.get("school_id")
        acronym = acronyms.get(int(school_id)) if pd.notna(school_id) else None
        if acronym is None:
            continue
        row = {
            "playerid": int(record["player_id"]),
            "name": record.get("player_full_name"),
            "class": rosters._norm_class(record.get("player_class")),
            "team": acronym,
            "conference": record.get("team_conference_name"),
            "year": year,
            "division": division,
            "ncaa_team_id": int(school_id),
            "pos": record.get("player_position"),
        }
        for src, dst in column_map.items():
            row[dst] = None if _shape.absent(year, dst) else record.get(src)
        for src, dst in present_extra.items():
            row[dst] = record.get(src)
        rows.append(row)
    return rows


def collect(year: int, division: int, *, manifest, shas, offline=False,
            refresh=False, roster=None):
    """Return (batting_rows, pitching_rows, teams) for one rich year."""
    from sources import bulk

    if roster is None:
        roster = rosters.load(year, division, manifest=manifest, shas=shas,
                              offline=offline, refresh=refresh)

    frames = {}
    for category in ("batting", "pitching"):
        path = bulk.season_path("modern", year, category)
        frame = bulk.load_csv("modern", path, manifest=manifest,
                              sha=shas["modern"], offline=offline,
                              refresh=refresh)
        frame = frame[frame["ncaa_division"] == division].copy()
        frame["player_id"] = pd.to_numeric(frame["player_id"], errors="coerce")
        frames[category] = frame.dropna(subset=["player_id"])

    # Team identity comes from the stat file itself here, not the roster, so the
    # acronym mapping sees exactly the schools that have stats.
    teams = _team_dicts(frames["batting"], frames["pitching"])
    return frames, teams


def _team_dicts(batting, pitching):
    combined = pd.concat(
        [batting[["school_id", "school_name", "team_conference_name"]],
         pitching[["school_id", "school_name", "team_conference_name"]]],
        ignore_index=True)
    combined = (combined.dropna(subset=["school_id", "school_name"])
                .drop_duplicates(subset=["school_id"]))
    return [
        {"team_id": int(row.school_id), "ncaa_name": row.school_name,
         "conference": row.team_conference_name}
        for row in combined.itertuples()
    ]


def shape(frames, year, division, acronyms):
    return (_rows(frames["batting"], year, division, "batting", acronyms),
            _rows(frames["pitching"], year, division, "pitching", acronyms))
