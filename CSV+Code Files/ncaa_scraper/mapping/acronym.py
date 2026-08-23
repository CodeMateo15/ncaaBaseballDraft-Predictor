"""NCAA school name -> FanGraphs acronym.

The emitted ``team`` column must stay a FanGraphs-style acronym (``WMU``,
``TA&M``, ``M-OH``) so these CSVs line up with the rest of the pipeline:
``build_2026_combined.py`` joins ``team`` through
``standardized/unique_teams.csv`` to get a full name, then to a team id.

The vendored registry already solves this completely. Verified: the
``fg_acronym`` alias namespace holds 311 distinct acronyms over 311 team ids,
**set-identical** to the 311 acronyms in ``unique_teams.csv`` -- no gaps in
either direction -- and every D1 program in 2021-2026 carries both an
``ncaa_short`` and an ``fg_acronym`` alias.

``resolve_team`` has no fuzzy fallback, by design: a silent near-match is how two
programs end up swapped without anyone noticing. So an unresolvable name is an
error here, never a guess and never a dropped row.
"""

from mapping.team_registry import resolve_team, team_aliases


class UnmappedTeamsError(RuntimeError):
    """One or more NCAA teams could not be mapped to a FanGraphs acronym."""


def to_fg_acronym(ncaa_name: str, season: int, division: int = 1):
    """Resolve one NCAA school name.

    Returns:
        (acronym, how) where ``how`` records which lookup succeeded, or
        ``(None, reason)`` on failure. ``how`` matters: a name that only resolves
        without the division filter is worth logging, because it means the
        program was mid-transition that season.
    """
    attempts = [
        ("ncaa_short", dict(namespace="ncaa_short", division=division)),
        ("ncaa_label", dict(namespace="ncaa_label", division=division)),
        ("any-namespace", dict(division=division)),
        # Relaxed: drop the division filter. Discovery returns 293-308 D1 teams
        # while the registry's strict-D1 set is 7-8 smaller, so transitional
        # programs land here.
        ("relaxed-no-division", dict()),
    ]

    for how, kwargs in attempts:
        try:
            team_id = resolve_team(ncaa_name, season=season, **kwargs)
        except Exception:  # noqa: BLE001 -- a lookup error is just a miss
            team_id = None
        if team_id is None:
            continue

        acronyms = team_aliases(team_id, namespace="fg_acronym")
        if len(acronyms) == 1:
            return acronyms[0], how
        if len(acronyms) > 1:
            # Ambiguity is a failure, not a coin flip.
            return None, f"ambiguous fg_acronym for {team_id}: {sorted(acronyms)}"
        return None, f"resolved to {team_id} but it has no fg_acronym alias"

    return None, "no registry match"


def map_teams(teams, season: int, division: int = 1):
    """Map a season's whole team list.

    Args:
        teams: dicts from ``ncaa.discovery.discover``.

    Returns:
        (mapping, failures, relaxed) -- ``mapping`` is ``{team_id: acronym}``,
        ``failures`` is a list of dicts for the report, ``relaxed`` lists the
        teams that needed the no-division fallback.
    """
    mapping = {}
    failures = []
    relaxed = []

    for team in teams:
        acronym, how = to_fg_acronym(team["ncaa_name"], season, division)
        if acronym is None:
            failures.append(
                {
                    "year": season,
                    "division": division,
                    "ncaa_team_id": team["team_id"],
                    "ncaa_name": team["ncaa_name"],
                    "conference": team.get("conference"),
                    "reason": how,
                }
            )
            continue
        mapping[team["team_id"]] = acronym
        if how == "relaxed-no-division":
            relaxed.append({**team, "acronym": acronym, "year": season})

    return mapping, failures, relaxed


def raise_for_failures(failures, report_path: str):
    """Abort with an actionable message. Never emit CSVs with teams missing."""
    if not failures:
        return

    lines = [
        f"{len(failures)} NCAA team-seasons have no FanGraphs acronym:",
        "",
    ]
    for failure in sorted(failures, key=lambda f: (f["year"], f["ncaa_name"])):
        lines.append(
            f"  {failure['year']}  {failure['ncaa_name']:<32} "
            f"({failure['conference']})  -- {failure['reason']}"
        )
    lines += [
        "",
        f"Full list written to {report_path}",
        "",
        "Fix by adding rows to mapping/data/registry/team_aliases.csv with",
        "namespace=fg_acronym. The acronym must ALSO exist in",
        "standardized/unique_teams.csv, or build_2026_combined.py will drop",
        "those players downstream.",
    ]
    raise UnmappedTeamsError("\n".join(lines))
