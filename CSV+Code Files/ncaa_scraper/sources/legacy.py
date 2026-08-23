"""Adapter for the legacy mirror (2013-2024): the only source for 2021.

This mirror is a different vintage with a different schema, and three of its
quirks will silently corrupt the output if taken at face value. Each is asserted
or worked around rather than trusted:

1. **Batting ``G`` is TEAM games, not player games.** Constant within school for
   288 of 290 D1 schools in 2021 (row 0 reads ``G=57, AB=0``). Mapping it would
   give every player their team's schedule length. Per-player games come from the
   modern mirror's ``{year}_rosters.csv`` ``player_G`` instead, and are left null
   where that bridge fails. Pitching ``App`` *is* per-player (28 distinct values
   within a single school) and is used directly.

2. **``IP`` is true innings, not NCAA thirds notation.** Fractional parts are
   {.0, .333, .667}, so it is re-encoded to thirds before it reaches
   ``derive/rates.py``. Skipping this does not error -- it silently halves every
   pitching rate.

3. **``stats_player_seq`` is a third, disjoint id space.** Range 1.42M-2.67M
   against the modern mirror's 6.90M-11.38M, with zero overlap. So ids cannot be
   carried across mirrors; reaching a modern ``player_id`` needs a name bridge
   (school + last + first), and rows that fail it keep a namespaced ``L<seq>``
   id rather than being dropped.

Also: names are stored "Last, First" and must be reversed, ``K`` is strikeouts,
and 2021's ``GDP`` column exists but is 0% nonzero -- hence
``config.GDP_ABSENT_YEARS``. The 2022 file's ``GDP`` *is* real (30.6% nonzero),
which is why the absence is per-year rather than blanket.
"""

import pandas as pd

import config
from mapping import acronym
from sources import _shape, rosters

BATTING_MAP = {
    "AB": "ab", "H": "h", "2B": "2b", "3B": "3b", "HR": "hr", "R": "r",
    "RBI": "rbi", "BB": "bb", "K": "so", "HBP": "hbp", "SF": "sf", "SH": "sh",
    "GDP": "gdp", "SB": "sb", "CS": "cs",
}
BATTING_EXTRA = {"TB": "tb", "BA": "ncaa_ba", "OPP DP": "opp_dp"}

PITCHING_MAP = {
    "App": "g", "GS": "gs", "W": "w", "L": "l", "SV": "sv", "CG": "cg",
    "SHO": "sho", "IP": "ip", "BF": "tbf", "H": "h", "R": "r", "ER": "er",
    "BB": "bb", "SO": "so", "HR": "hr", "HBP": "hbp", "WP": "wp", "Bk": "bk",
}
PITCHING_EXTRA = {"P-OAB": "p_oab", "ERA": "ncaa_era"}

# This mirror stores real innings, unlike the modern "rich" generation.
IP_FORMAT = _shape.TRUE_INNINGS


def _reverse_name(value):
    """'Abel, Ryan' -> 'Ryan Abel'. Anything without a comma passes through."""
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if "," not in text:
        return text
    last, _, first = text.partition(",")
    first, last = first.strip(), last.strip()
    return f"{first} {last}".strip() if first else last


def _split_name(value):
    if value is None or pd.isna(value):
        return "", ""
    text = str(value).strip()
    if "," in text:
        last, _, first = text.partition(",")
        return rosters.fold(first), rosters.fold(last)
    parts = rosters.fold(text).split()
    if len(parts) >= 2:
        return parts[0], parts[-1]
    return "", rosters.fold(text)


def _load_legacy(year, division, category, *, manifest, shas, offline, refresh):
    from sources import bulk
    path = bulk.season_path("legacy", year, category)
    frame = bulk.load_csv("legacy", path, manifest=manifest,
                          sha=shas["legacy"], offline=offline, refresh=refresh)
    frame = frame[pd.to_numeric(frame["division"], errors="coerce") == division].copy()
    frame["stats_player_seq"] = pd.to_numeric(frame["stats_player_seq"],
                                              errors="coerce")
    return frame.dropna(subset=["stats_player_seq"])


def _load_legacy_roster(year, division, *, manifest, shas, offline, refresh):
    from sources import bulk
    path = bulk.roster_path("legacy", year)
    frame = bulk.load_csv("legacy", path, manifest=manifest,
                          sha=shas["legacy"], offline=offline, refresh=refresh)
    frame = frame[pd.to_numeric(frame["division"], errors="coerce") == division].copy()
    frame["player_id"] = pd.to_numeric(frame["player_id"], errors="coerce")
    frame = frame.dropna(subset=["player_id"]).drop_duplicates("player_id")
    # The name lives in a different column depending on the year: 2021 fills
    # `name` and leaves `player_name` empty, 2022 does the exact reverse. Reading
    # only one of them silently produces a whole year of nameless players, which
    # then fail every downstream draft match and count as confident negatives.
    # Both are "Last, First".
    frame["name_raw"] = frame["name"]
    if "player_name" in frame.columns:
        frame["name_raw"] = frame["name_raw"].fillna(frame["player_name"])
    unnamed = frame["name_raw"].isna().mean()
    if unnamed > 0.01:
        raise AssertionError(
            f"legacy {year} roster: {unnamed:.1%} of players have no name in "
            f"either `name` or `player_name`. A nameless row cannot receive a "
            f"draft label, so it would enter the model as a false negative.")
    frame["name_display"] = frame["name_raw"].map(_reverse_name)
    # `class` is a Python keyword, so itertuples() cannot expose it.
    frame["class_norm"] = frame["class"].map(rosters._norm_class)
    return frame


def _bridge_to_modern(legacy_roster, modern_roster):
    """Map legacy player ids onto modern ones via school + last + first.

    A name join, deliberately and narrowly: it bridges two *keys* inside a single
    season, using files that both carry the school, and the emitted identity stays
    a vendor id on either side. It is never used to attach stats to a player.
    """
    if modern_roster is None or modern_roster.empty:
        return {}, {}

    right = modern_roster.copy()
    right["bridge_first"] = right["player_first_name"].map(rosters.fold)
    right["bridge_last"] = right["player_last_name"].map(rosters.fold)
    right["bridge_school"] = right["school_name"].map(rosters.fold)
    right = right.drop_duplicates(subset=["bridge_school", "bridge_last", "bridge_first"], keep=False)
    lookup = {
        (row.bridge_school, row.bridge_last, row.bridge_first): (int(row.player_id), row.roster_g
                                               if hasattr(row, "roster_g") else None,
                                               row.school_id, row.school_name)
        for row in right.itertuples()
    }

    ids, games = {}, {}
    for row in legacy_roster.itertuples():
        first, last = _split_name(row.name_raw)
        key = (rosters.fold(row.team_name), last, first)
        hit = lookup.get(key)
        if hit is None:
            continue
        ids[int(row.player_id)] = hit[0]
        if hit[1] is not None and not pd.isna(hit[1]):
            games[int(row.player_id)] = hit[1]
    return ids, games


def _team_dicts(roster_slice):
    grouped = roster_slice.drop_duplicates(subset=["stats_school_id"])
    return [
        {"team_id": int(row.stats_school_id), "ncaa_name": row.team_name,
         "conference": row.conference}
        for row in grouped.itertuples()
    ]


def _attach_school(stats, legacy_roster):
    """Give every legacy roster row the school_id its stats rows carry."""
    seq_to_school = dict(zip(stats["stats_player_seq"].astype("int64"),
                             stats["school_id"]))
    roster = legacy_roster.copy()
    roster["stats_school_id"] = roster["player_id"].astype("int64").map(seq_to_school)
    return roster.dropna(subset=["stats_school_id"])


def _rows(frame, year, division, category, acronyms, identity, games):
    column_map = BATTING_MAP if category == "batting" else PITCHING_MAP
    extra_map = BATTING_EXTRA if category == "batting" else PITCHING_EXTRA
    _shape.require_columns(frame, column_map, where=f"legacy {year} {category}")

    if category == "pitching":
        _shape.assert_ip_format(frame["IP"], IP_FORMAT,
                                where=f"legacy {year} pitching")

    present_extra = {src: dst for src, dst in extra_map.items()
                     if src in frame.columns}
    rows = []
    for record in frame.to_dict("records"):
        seq = int(record["stats_player_seq"])
        info = identity.get(seq)
        if info is None:
            continue
        acronym_value = acronyms.get(int(info["school_id"]))
        if acronym_value is None:
            continue

        row = {
            "playerid": info["playerid"],
            "name": info["name"],
            "class": info["class"],
            "team": acronym_value,
            "conference": info["conference"],
            "year": year,
            "division": division,
            "ncaa_team_id": int(info["school_id"]),
            "pos": info["pos"],
        }
        for src, dst in column_map.items():
            if dst == "ip":
                row["ip"] = _shape.true_innings_to_ncaa(record.get("IP"))
            elif _shape.absent(year, dst):
                row[dst] = None
            else:
                row[dst] = record.get(src)
        for src, dst in present_extra.items():
            row[dst] = record.get(src)

        if category == "batting":
            # Never the legacy `G` column: it is the team's schedule length,
            # constant within school for every school in both 2021 and 2022.
            row["g"] = games.get(seq)
            if row["g"] is None:
                # No roster games-played for this player, and no other source for
                # it. Mark it absent so `to_int`'s zero default cannot turn a
                # missing value into a claim that he played no games.
                row["_absent"] = {"g"}
        rows.append(row)
    return rows


def _build(year, division, *, manifest, shas, offline, refresh, roster,
           restrict_to=None):
    stats = {c: _load_legacy(year, division, c, manifest=manifest, shas=shas,
                             offline=offline, refresh=refresh)
             for c in ("batting", "pitching")}
    legacy_roster = _load_legacy_roster(year, division, manifest=manifest,
                                       shas=shas, offline=offline, refresh=refresh)
    combined = pd.concat([stats["batting"][["stats_player_seq", "school_id"]],
                          stats["pitching"][["stats_player_seq", "school_id"]]],
                         ignore_index=True).drop_duplicates("stats_player_seq")
    legacy_roster = _attach_school(combined, legacy_roster)

    modern_ident = rosters.identity_frame(roster) if roster is not None else None
    if modern_ident is not None and "school_name" in modern_ident.columns:
        modern_ident = modern_ident.merge(
            roster[["player_id", "player_first_name", "player_last_name"]],
            on="player_id", how="left")
    ids, games = _bridge_to_modern(legacy_roster, modern_ident)

    identity = {}
    for row in legacy_roster.itertuples():
        seq = int(row.player_id)
        identity[seq] = {
            # A failed bridge keeps a namespaced legacy id rather than dropping
            # the player: 2021 losing ~3% of its population silently would be a
            # worse error than an id that cannot join to other seasons.
            "playerid": ids.get(seq, f"L{seq}"),
            "name": row.name_display,
            "class": row.class_norm,
            "conference": row.conference,
            "pos": row.position,
            "school_id": int(row.stats_school_id),
        }

    teams = _team_dicts(legacy_roster)
    if restrict_to is not None:
        mapping, _f, _r = acronym.map_teams(teams, year, division)
        teams = [t for t in teams
                 if mapping.get(t["team_id"]) in restrict_to]
    return stats, teams, identity, games


def collect(year, division, *, manifest, shas, offline=False, refresh=False,
            roster=None):
    stats, teams, identity, games = _build(
        year, division, manifest=manifest, shas=shas, offline=offline,
        refresh=refresh, roster=roster)
    bridged = sum(1 for v in identity.values()
                  if not str(v["playerid"]).startswith("L"))
    print(f"  legacy {year}: {len(identity)} roster players, {bridged} bridged to "
          f"modern ids ({bridged / max(len(identity), 1):.1%}), "
          f"{len(games)} with roster games-played", flush=True)
    return {"frames": stats, "identity": identity, "games": games}, teams


def shape(payload, year, division, acronyms):
    frames = payload["frames"]
    return (_rows(frames["batting"], year, division, "batting", acronyms,
                  payload["identity"], payload["games"]),
            _rows(frames["pitching"], year, division, "pitching", acronyms,
                  payload["identity"], payload["games"]))


def collect_patch(year, division, *, manifest, shas, offline=False,
                  refresh=False, have=(), roster=None, wanted=None):
    """Contribute only the teams the primary source is missing.

    Two callers, same mechanism:

    * **2022 (`legacy_patch`)** -- the modern file covers 293 D1 schools against
      FanGraphs' 301. Seven of the eight absent programs, the D2->D1 transition
      cohort, are in this mirror; Stonehill is in neither, so 2022 tops out at
      300. Those seven are absent from the modern *roster* too, so their players
      get no ``player_G`` (batting `g` stays null) and no ``person_id``.
    * **2021 (`legacy_fill`)** -- the local cache holds only 175 of 293
      team-seasons, so every team it lacks comes from here.

    An incomplete row for a real player beats a missing team: a missing team
    removes a whole conference's worth of players from the population, which
    biases everything fitted on it.
    """
    wanted = set(config.BULK_2022_PATCH_TEAMS) if wanted is None else set(wanted)
    wanted -= set(have)
    if not wanted:
        return [], [], []

    stats, teams, identity, games = _build(
        year, division, manifest=manifest, shas=shas, offline=offline,
        refresh=refresh, roster=roster, restrict_to=wanted)
    mapping, failures, _relaxed = acronym.map_teams(teams, year, division)
    if failures:
        raise SystemExit(
            f"legacy patch for {year}: could not map {len(failures)} team(s): "
            f"{[f['ncaa_name'] for f in failures]}")

    bat = _rows(stats["batting"], year, division, "batting", mapping, identity, games)
    pit = _rows(stats["pitching"], year, division, "pitching", mapping, identity, games)
    got = sorted({row["team"] for row in bat} | {row["team"] for row in pit})
    missing = sorted(wanted - set(got)) if len(wanted) < 30 else []
    print(f"  legacy patch {year}: added {len(got)} team(s) {got}, "
          f"{len(bat)} batting / {len(pit)} pitching rows", flush=True)
    if missing:
        print(f"  ! legacy patch {year}: still missing {missing}", flush=True)
    return bat, pit, [{**t, "acronym": mapping[t["team_id"]]} for t in teams]
