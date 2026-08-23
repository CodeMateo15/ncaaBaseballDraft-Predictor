"""Enumerate the teams in a season.

NCAA team ids are **per-season** -- Texas A&M-Corpus Christi is 508948 in 2021
and 596471 in 2025 -- so discovery runs once per year and there is no such thing
as a stable team id to cache globally.

``/rankings/institution_trends`` is used rather than the registry because it *is*
the NCAA's own answer to "which programs played D1 this season", and its counts
match the FanGraphs data exactly (293 teams in 2021, 307 in 2025 -- the same as
the distinct ``team`` values in the FanGraphs CSVs). The vendored registry's
strict-D1 set is 7-8 smaller, since it excludes programs mid-transition.
"""

import re

from bs4 import BeautifulSoup

import config
from ncaa import cache

_RE_TEAM_ID = re.compile(r"/teams/(\d+)")


def discovery_url(year: int, division: int) -> str:
    return (
        f"{config.BASE}/rankings/institution_trends"
        f"?academic_year={float(year)}"
        f"&division={float(division)}"
        f"&ranking_period={config.ranking_period(year, division)}"
        f"&sport_code={config.SPORT_CODE}"
        f"&stat_seq={config.DISCOVERY_STAT_SEQ}"
    )


def parse_teams(html: str):
    """Return ``[{team_id, ncaa_name, conference}, ...]`` from a discovery page."""
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", {"id": "stat_grid"})
    if table is None:
        raise ValueError(
            "no table#stat_grid on the discovery page -- either the page shape "
            "changed or this is still a challenge page"
        )

    body = table.find("tbody")
    rows = body.find_all("tr", recursive=False) if body else []

    teams = []
    for row in rows:
        match = _RE_TEAM_ID.search(str(row))
        if match is None:
            continue  # Totals and other non-team rows carry no team link.
        cells = [td.get_text(strip=True) for td in row.find_all("td")]
        if len(cells) < 2:
            continue
        teams.append(
            {
                "team_id": int(match.group(1)),
                "ncaa_name": cells[0],
                # Conference. The per-conference league constants need this, and
                # taking it here means never fetching a separate page for it.
                "conference": cells[1],
            }
        )
    return teams


def discover(session, year: int, division: int, refresh: bool = False):
    """Return the team list for a season, caching it as ``_teams.json``.

    Raises:
        ValueError: the team count is implausible, or ids are not unique. Both
            mean a truncated page, and continuing would silently drop teams.
    """
    path = cache.teams_path(year, division)
    if not refresh:
        cached = cache.read_json(path)
        if cached is not None:
            return cached

    html = session.get(discovery_url(year, division))
    teams = parse_teams(html)

    ids = [team["team_id"] for team in teams]
    if len(ids) != len(set(ids)):
        raise ValueError(f"{year} D{division}: duplicate team ids in discovery")

    low, high = config.TEAM_COUNT_BAND
    if not low <= len(teams) <= high:
        raise ValueError(
            f"{year} D{division}: discovery returned {len(teams)} teams, outside "
            f"the plausible band {low}-{high}. Refusing to continue -- this is "
            f"what a truncated page looks like."
        )

    cache.write_json(path, teams)
    return teams
