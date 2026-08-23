"""Parse a team-season's ``season_to_date_stats`` grid.

Two requests per team-season:

1. ``/teams/{id}/season_to_date_stats`` with no params -> the **batting** grid,
   and the page's tab links expose all three ``year_stat_category_id`` values.
2. ``...?year_stat_category_id={ids[1]}`` -> the **pitching** grid.

The ids are contiguous per team-season with batting lowest (2025 team 596471 ->
15687/15688/15689; 2021 team 508948 -> 14840/14841/14842). They change every
season, so they are parsed off the page every time rather than tabulated.

**Row filter.** ``table#stat_grid`` carries far more ``<tr>`` than players -- 323
rows for 39 players in one 2025 case. The surplus is per-player situational split
labels (``Hits-AB with 2 outs``, ``Hits-AB vs Left Handed Pitchers``, ...) plus
``Totals`` and ``Opponent Totals``, all with blank stat cells. Keeping only rows
whose ``Player`` cell contains an ``<a href="/players/{id}">`` selects exactly the
real players; verified across 2021-2026, where rows-with-link always equals the
number of distinct player ids on the page.
"""

import re

from bs4 import BeautifulSoup

import config
from ncaa import cache, schema

_RE_YSC = re.compile(r"year_stat_category_id=(\d+)")
_RE_PLAYER_ID = re.compile(r"/players/(\d+)")


class StatIdAnomaly(RuntimeError):
    """A team-season page did not expose the expected three category ids."""


class SkipLedger:
    """Records every discarded row, so nothing vanishes silently.

    This replaces the upstream ``except Exception: continue`` pattern, whose
    comment ("About 5 rows are skipped each time due to the format of the NCAA
    page") is precisely the kind of unexamined loss this scraper must not repeat.
    """

    # Expected and uninteresting: the situational-split and Totals rows.
    EXPECTED = {"no_player_link"}

    def __init__(self):
        self.entries = []

    def add(self, *, year, division, team_id, ncaa_name, category, row_index,
            player_id, reason, raw_cells):
        self.entries.append(
            {
                "year": year,
                "division": division,
                "team_id": team_id,
                "ncaa_name": ncaa_name,
                "category": category,
                "row_index": row_index,
                "player_id": player_id,
                "reason": reason,
                "raw_cells": "|".join("" if c is None else str(c) for c in raw_cells),
            }
        )

    @property
    def unexpected(self):
        return [e for e in self.entries if e["reason"] not in self.EXPECTED]

    def count(self, reason=None):
        if reason is None:
            return len(self.entries)
        return sum(1 for e in self.entries if e["reason"] == reason)


def batting_url(team_id) -> str:
    return f"{config.BASE}/teams/{team_id}/season_to_date_stats"


def category_url(team_id, ysc_id) -> str:
    return f"{config.BASE}/teams/{team_id}/season_to_date_stats?year_stat_category_id={ysc_id}"


def extract_ysc_ids(html: str):
    """Return the three category ids on a team page, ascending.

    Raises:
        StatIdAnomaly: not exactly three ids, or they are not contiguous. Either
            means the page is not the shape we verified, and guessing which id is
            pitching would silently produce the wrong table.
    """
    ids = sorted({int(x) for x in _RE_YSC.findall(html)})
    if len(ids) != 3:
        raise StatIdAnomaly(f"expected 3 year_stat_category_id values, found {ids}")
    if ids[1] != ids[0] + 1 or ids[2] != ids[1] + 1:
        raise StatIdAnomaly(f"category ids are not contiguous: {ids}")
    return ids


def _cell_text(cell):
    text = cell.get_text(strip=True)
    return text if text not in ("", "-") else None


def parse_grid(html, *, year, division, team_id, ncaa_name, category, ledger):
    """Parse one grid into ``(rows, unknown_headers)``.

    Each row is a dict of target/extra column name -> raw string (or None).
    Type coercion happens in ``derive/``, so a surprising value shows up as a
    parse-time skip with its raw cells rather than as a silent NaN.
    """
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", {"id": "stat_grid"})
    if table is None:
        raise ValueError(
            f"{year} team {team_id} {category}: no table#stat_grid "
            "(challenge page, or the page shape changed)"
        )

    headers = [th.get_text(strip=True) for th in table.select("thead th")]
    index, unknown = schema.build_index(headers, category)

    absent = schema.missing_required(index, category, year)
    if absent:
        raise ValueError(
            f"{year} team {team_id} {category}: required columns absent from the "
            f"page: {absent}. Headers were: {headers}"
        )

    body = table.find("tbody")
    all_rows = body.find_all("tr", recursive=False) if body else []

    rows = []
    seen_ids = set()

    for row_index, tr in enumerate(all_rows):
        cells = tr.find_all(["td", "th"])
        raw = [_cell_text(c) for c in cells]

        link = tr.find("a", href=_RE_PLAYER_ID)
        if link is None:
            ledger.add(
                year=year, division=division, team_id=team_id, ncaa_name=ncaa_name,
                category=category, row_index=row_index, player_id=None,
                reason="no_player_link", raw_cells=raw[:4],
            )
            continue

        player_id = _RE_PLAYER_ID.search(link["href"]).group(1)

        if player_id in seen_ids:
            ledger.add(
                year=year, division=division, team_id=team_id, ncaa_name=ncaa_name,
                category=category, row_index=row_index, player_id=player_id,
                reason="duplicate_player_id", raw_cells=raw,
            )
            continue

        if max(index.values()) >= len(raw):
            ledger.add(
                year=year, division=division, team_id=team_id, ncaa_name=ncaa_name,
                category=category, row_index=row_index, player_id=player_id,
                reason="short_row", raw_cells=raw,
            )
            continue

        record = {name: raw[position] for name, position in index.items()}
        record["playerid"] = player_id
        record["year"] = year
        record["division"] = division
        record["ncaa_team_id"] = team_id
        record["ncaa_name"] = ncaa_name

        seen_ids.add(player_id)
        rows.append(record)

    return rows, unknown


def is_cached(year, division, team_id) -> bool:
    """True if this team-season needs no network.

    Checks the parsed-rows layer, which is what ``fetch_team_season`` prefers.
    Used up front to size the job and estimate time remaining.
    """
    import os

    return all(
        os.path.exists(cache.rows_path(year, division, team_id, category))
        for category in schema.CATEGORIES
    )


def fetch_team_season(session, *, year, division, team_id, ncaa_name, ledger,
                      refresh=False):
    """Fetch and parse both grids for one team-season.

    Returns:
        dict with ``batting``, ``pitching``, ``ysc_ids``, ``unknown_headers``,
        and ``header_fingerprint`` keys.

    Uses the parsed-rows cache when available, so a warm run never re-parses.
    """
    rows_cache = {
        category: cache.rows_path(year, division, team_id, category)
        for category in schema.CATEGORIES
    }

    if not refresh:
        cached = {c: cache.read_json_gz(p) for c, p in rows_cache.items()}
        if all(v is not None for v in cached.values()):
            return {
                "batting": cached["batting"]["rows"],
                "pitching": cached["pitching"]["rows"],
                "ysc_ids": cached["batting"].get("ysc_ids"),
                "unknown_headers": (
                    cached["batting"].get("unknown", [])
                    + cached["pitching"].get("unknown", [])
                ),
                "header_fingerprint": {
                    c: cached[c].get("header_fingerprint") for c in schema.CATEGORIES
                },
                "from_cache": True,
            }

    batting_html = cache.fetch_or_cached(
        session, batting_url(team_id),
        cache.html_path(year, division, team_id, "batting"), refresh=refresh,
    )
    ysc_ids = extract_ysc_ids(batting_html)

    pitching_html = cache.fetch_or_cached(
        session, category_url(team_id, ysc_ids[1]),
        cache.html_path(year, division, team_id, "pitching"), refresh=refresh,
    )

    result = {"ysc_ids": ysc_ids, "unknown_headers": [], "header_fingerprint": {},
              "from_cache": False}

    for category, html in (("batting", batting_html), ("pitching", pitching_html)):
        rows, unknown = parse_grid(
            html, year=year, division=division, team_id=team_id,
            ncaa_name=ncaa_name, category=category, ledger=ledger,
        )
        fingerprint = ",".join(
            th.get_text(strip=True)
            for th in BeautifulSoup(html, "html.parser")
            .find("table", {"id": "stat_grid"})
            .select("thead th")
        )
        result[category] = rows
        result["unknown_headers"].extend(f"{category}:{h}" for h in unknown)
        result["header_fingerprint"][category] = fingerprint

        cache.write_json_gz(
            rows_cache[category],
            {"rows": rows, "unknown": unknown, "ysc_ids": ysc_ids,
             "header_fingerprint": fingerprint},
        )

    return result
