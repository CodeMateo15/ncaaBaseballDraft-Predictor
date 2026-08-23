"""Adapter reading the local pre-block scrape out of ``cache/``.

Before stats.ncaa.org blocked this IP, the live scraper banked 307 of 307 D1
team-seasons for 2025 and 175 of 293 for 2021 as parsed rows under
``cache/d1/{year}/``. That data came straight off the NCAA pages, so where it
exists it is the most authoritative source available -- strictly better than
either mirror:

* **2025** is complete, and the lean mirror is measurably not. Joined on NCAA
  ``player_id`` against this cache, the mirror's summed segment totals run short
  by 0.71 games and 0.52 at-bats per player, with 13% of players off by more than
  two at-bats. So 2025 reads from here, and ``--source bulk`` exists to force the
  mirror instead when you want that A/B.
* **2021** is only 60% covered, but the legacy mirror's 2021 batting file is
  missing ~347 players who had real plate appearances (Georgia Tech lists 43
  roster players and only 38 stat rows), so the cached teams fill a real hole.
  See ``config.BULK_YEAR_SOURCES``: 2021 reads the cache first and falls back to
  the legacy mirror for the teams the cache lacks.

Everything here goes through ``ncaa.team_page.fetch_team_season`` with
``session=None``, which serves parsed rows from disk and raises for anything
uncached. No network, and the validated parser is reused rather than
reimplemented.
"""

import config
from ncaa import discovery, schema, team_page
from ncaa.team_page import SkipLedger


def cached_team_ids(year: int, division: int) -> set:
    """Team ids whose parsed rows are on disk for both categories."""
    teams = discovery.discover(None, year, division)
    return {team["team_id"] for team in teams
            if team_page.is_cached(year, division, team["team_id"])}


def collect(year, division, *, manifest=None, shas=None, offline=False,
            refresh=False, roster=None, only=None):
    """Read every cached team-season for the year.

    ``only`` restricts to a set of team ids, which is how the 2021 hybrid keeps
    the cache and the mirror from both contributing the same team.
    """
    teams = discovery.discover(None, year, division)
    ledger = SkipLedger()

    usable, rows = [], {"batting": [], "pitching": []}
    for team in teams:
        team_id = team["team_id"]
        if only is not None and team_id not in only:
            continue
        if not team_page.is_cached(year, division, team_id):
            continue
        result = team_page.fetch_team_season(
            None, year=year, division=division, team_id=team_id,
            ncaa_name=team["ncaa_name"], ledger=ledger, refresh=False)
        usable.append(team)
        for category in schema.CATEGORIES:
            for row in result[category]:
                row["conference"] = team.get("conference")
                rows[category].append(row)

    print(f"  cache {year}: {len(usable)} of {len(teams)} team-seasons on disk, "
          f"{len(rows['batting'])} batting / {len(rows['pitching'])} pitching rows",
          flush=True)
    return {"rows": rows, "teams": usable}, usable


def shape(payload, year, division, acronyms):
    """The cached rows are already in the live path's shape; just attach acronyms.

    ``team_page`` emits ``ncaa_team_id`` and the mapped stat keys, and the live
    ``collect()`` is what normally sets ``team``. Doing the same here is why
    ``build_batting_frame`` needs no branch for this source.
    """
    out = []
    for category in ("batting", "pitching"):
        shaped = []
        for row in payload["rows"][category]:
            acronym = acronyms.get(row["ncaa_team_id"])
            if acronym is None:
                continue
            row["team"] = acronym
            shaped.append(row)
        out.append(shaped)
    return out[0], out[1]
