"""Adapter for the modern mirror's 2025-2026 "lean" generation.

This generation is the awkward one. It carries 28 batting and 30 pitching columns
against the rich generation's 43 and 51, and three things about it will produce
wrong numbers if taken literally:

1. **Rows are season *segments*, not season totals.** 2025 has 83,771 rows for
   33,611 distinct ``player_id``. Taking the largest row per player -- the obvious
   reading -- understates at-bats by an average of 22.7. They have to be summed.
   Verified against the repo's own complete 2025 scrape, joined on NCAA
   ``player_id``: summing gives at-bats within 0.52 on average, the max-row
   reading within 22.68.

2. **Innings are true innings, not NCAA thirds notation**, and they are
   pre-summed floats carrying accumulated error (.332, .666 and .668 all appear).
   So innings are converted to *outs* per segment, summed as integers, and
   re-encoded once at the end. Summing the decimals directly would drift.

3. **No identity columns at all** beyond the player's name -- no school, no
   division, no class. Those come from ``{year}_rosters.csv`` joined on
   ``player_id``, which also supplies the D1 filter. Coverage is 99.94%.

Absent entirely: ``GDP``, ``W/L/SV/CG/SHO``, ``ERA`` and pitching ``AB``. ERA is
recomputed by ``derive/rates.py`` from earned runs and innings so it is not a
loss; the rest are declared in ``config`` as absent years so they arrive as null
rather than zero.

Aggregating the per-game box scores instead was tested and reproduces these
totals identically to three decimals -- the season files simply *are* the game
logs re-aggregated -- so there is no better route, and the ~1% of games the mirror
is missing is inherited either way.
"""

import os

import pandas as pd

import config
from sources import _shape, rosters

YEARS = (2025, 2026)

BATTING_MAP = {
    "batting_G": "g", "batting_AB": "ab", "batting_H": "h", "batting_2B": "2b",
    "batting_3B": "3b", "batting_HR": "hr", "batting_R": "r",
    "batting_RBI": "rbi", "batting_BB": "bb", "batting_SO": "so",
    "batting_HBP": "hbp", "batting_SF": "sf", "batting_SH": "sh",
    "batting_SB": "sb", "batting_CS": "cs",
}
BATTING_EXTRA = {"batting_TB": "tb", "batting_OPP_DP": "opp_dp"}

PITCHING_MAP = {
    "pitching_GP": "g", "pitching_GS": "gs", "pitching_BF": "tbf",
    "pitching_H": "h", "pitching_R": "r", "pitching_ER": "er",
    "pitching_BB": "bb", "pitching_SO": "so", "pitching_HR": "hr",
    "pitching_HBP": "hbp", "pitching_WP": "wp", "pitching_BK": "bk",
}
PITCHING_EXTRA = {}

# Columns this generation simply does not have. Declared so `require_columns`
# does not look for them and `_shape.absent` can null them per year.
ABSENT_BATTING = ("gdp",)
ABSENT_PITCHING = ("w", "l", "sv", "cg", "sho", "p_oab")

IP_FORMAT = _shape.TRUE_INNINGS

# Physical sanity bounds. A collapse bug that double-counts shows up here
# immediately, which is cheaper than noticing it in a fitted model.
MAX_GAMES = 110
MAX_AB = 400
MAX_OUTS = 600


def _collapse(frame, category, year):
    """Sum a player's season segments into one row per (player_id, team_id)."""
    stat_columns = [c for c in frame.columns
                    if c.startswith(("batting_", "pitching_"))]
    numeric = frame.copy()
    for column in stat_columns:
        numeric[column] = pd.to_numeric(numeric[column], errors="coerce")

    if category == "pitching":
        _shape.assert_ip_format(numeric["pitching_IP"], IP_FORMAT,
                                where=f"lean {year} pitching (pre-collapse)")
        # Integer outs, so the sum cannot drift the way the source floats do.
        numeric["_outs"] = numeric["pitching_IP"].map(_shape.outs_from_true_innings)
        stat_columns = stat_columns + ["_outs"]

    # Collapse on player_id ALONE, not (player_id, team_id).
    #
    # `team_id` is not reliably the player's team in this generation. In 2025 it
    # happens to be 1:1 with the player, but in 2026 a player's segments carry
    # *different* team_ids -- N. Yeary (9974828) has twelve rows, each one game,
    # under twelve distinct team_ids, and 914 players look like that. Whatever
    # those ids are, they are not the player's program, so including them in the
    # key would split one season across a dozen rows.
    #
    # The school therefore comes from the roster file, joined on player_id, which
    # is the only identity claim this generation supports. Grouping on player_id
    # alone gives the identical result for 2025, where the pair was already 1:1.
    grouped = (numeric.groupby("player_id", as_index=False)[stat_columns]
               .sum(min_count=1))
    _sanity(grouped, category, year)
    return grouped


def _sanity(grouped, category, year):
    if category == "batting":
        checks = (("batting_G", MAX_GAMES), ("batting_AB", MAX_AB))
    else:
        checks = (("pitching_GP", MAX_GAMES), ("_outs", MAX_OUTS))
    for column, limit in checks:
        if column not in grouped.columns:
            continue
        worst = grouped[column].max()
        if pd.notna(worst) and worst > limit:
            offenders = int((grouped[column] > limit).sum())
            raise AssertionError(
                f"lean {year} {category}: {offenders} row(s) have {column} above "
                f"{limit} (max {worst}). Segments were probably counted twice.")


def _report_unrostered(collapsed, missing_ids, year, category):
    """Record players the roster file has no row for, since they get dropped."""
    if not missing_ids:
        return
    rows = collapsed[collapsed["player_id"].isin(missing_ids)].copy()
    rows.insert(0, "year", year)
    rows.insert(1, "category", category)
    os.makedirs(config.REPORT_DIR, exist_ok=True)
    path = os.path.join(config.REPORT_DIR,
                        f"lean_unrostered_{year}_{category}.csv")
    rows.to_csv(path, index=False)
    print(f"  ! lean {year} {category}: {len(rows):,} player(s) have no roster "
          f"row and are dropped; listed in {os.path.basename(path)}", flush=True)


def collect(year, division, *, manifest, shas, offline=False, refresh=False,
            roster=None):
    from sources import bulk

    if roster is None:
        roster = rosters.load(year, division, manifest=manifest, shas=shas,
                              offline=offline, refresh=refresh)
    identity = rosters.identity_frame(roster)
    all_divisions = rosters.load(year, None, manifest=manifest, shas=shas,
                                 offline=offline, refresh=refresh)
    all_division_ids = set(all_divisions["player_id"])

    frames = {}
    for category in ("batting", "pitching"):
        path = bulk.season_path("modern", year, category)
        frame = bulk.load_csv("modern", path, manifest=manifest,
                              sha=shas["modern"], offline=offline,
                              refresh=refresh)
        frame["player_id"] = pd.to_numeric(frame["player_id"], errors="coerce")
        frame = frame.dropna(subset=["player_id"])
        frame["player_id"] = frame["player_id"].astype("int64")

        collapsed = _collapse(frame, category, year)

        # Identity coverage is measured against ALL divisions, because the stat
        # file mixes D1/D2/D3 and the roster join is also what selects D1. Judging
        # coverage on the D1 subset would read as a 70% failure when it is really
        # the other two divisions being correctly excluded.
        known = set(all_division_ids)
        seen = set(collapsed["player_id"])
        resolved = len(seen & known)
        # Measured: 2025 resolves 99.88%, 2026 only 93.67%. The 2026 roster
        # snapshot simply omits ~2,231 players who do appear in the stat file, and
        # with no roster row there is no division, school or class for them, so
        # they cannot be placed. They are overwhelmingly marginal (1,426 have any
        # at-bats at all, averaging 3.7), but they are dropped, so the list is
        # written out rather than left implicit.
        if seen and resolved / len(seen) < 0.90:
            raise AssertionError(
                f"lean {year} {category}: only {resolved}/{len(seen)} players "
                f"({resolved / len(seen):.1%}) appear in the roster file at all. "
                f"Identity has drifted, so the D1 filter cannot be trusted.")
        _report_unrostered(collapsed, seen - known, year, category)

        merged = collapsed.merge(identity, on="player_id", how="inner")
        print(f"  lean {year} {category}: {len(frame):,} segment rows -> "
              f"{len(collapsed):,} player-seasons "
              f"({resolved / len(seen):.1%} in a roster), {len(merged):,} in D1",
              flush=True)
        frames[category] = merged

    teams = rosters.team_dicts(roster)
    return frames, teams


def _rows(frame, year, division, category, acronyms):
    column_map = BATTING_MAP if category == "batting" else PITCHING_MAP
    extra_map = BATTING_EXTRA if category == "batting" else PITCHING_EXTRA
    absent = ABSENT_BATTING if category == "batting" else ABSENT_PITCHING
    _shape.require_columns(frame, column_map, where=f"lean {year} {category}")

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
            "name": record.get("name"),
            "class": record.get("class"),
            "team": acronym,
            "conference": record.get("conference"),
            "year": year,
            "division": division,
            "ncaa_team_id": int(school_id),
            "pos": record.get("pos"),
            # Columns this generation never published. Marked per row so
            # `to_int`'s zero default cannot invent an observation, and so the
            # acceptance gate's all-or-nothing null check has something to see.
            "_absent": set(absent),
        }
        for src, dst in column_map.items():
            row[dst] = record.get(src)
        for src, dst in present_extra.items():
            row[dst] = record.get(src)
        if category == "pitching":
            row["ip"] = _shape.ncaa_from_outs(record.get("_outs"))
        rows.append(row)
    return rows


def shape(frames, year, division, acronyms):
    return (_rows(frames["batting"], year, division, "batting", acronyms),
            _rows(frames["pitching"], year, division, "pitching", acronyms))
