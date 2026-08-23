"""Pick a source per year, run its adapter, and return exactly what ``collect()`` does.

``run.py::pipeline`` calls either the live ``collect()`` or this module's
``collect()`` and cannot tell the difference afterwards: same two row lists, same
``meta`` keys. That is what keeps the diff in ``pipeline()`` to a dozen lines and
leaves ``derive/``, ``validate/`` and ``emit()`` untouched.

``quota_hit`` and ``progress`` are always None here. There is no quota to exhaust
and nothing to pace -- which is the point.
"""

import os
import time

import pandas as pd

import config
from mapping import acronym
from ncaa import discovery
from sources import bulk, identity, legacy, lean, local, rich, rosters

# Adapter module per source name in config.BULK_YEAR_SOURCES.
_ADAPTERS = {"rich": rich, "lean": lean, "legacy": legacy, "cache": local}


class SkipLedgerShim:
    """``collect()`` returns a SkipLedger; the bulk path has no skips to record.

    The mirrors are already-parsed tables, so there are no unlinked rows or
    malformed cells to skip -- the whole category of failure the ledger exists
    for happens upstream, before the CSV we read. An empty ledger keeps
    ``pipeline()``'s report code working without pretending otherwise.
    """

    EXPECTED = frozenset({"no_player_link"})

    def __init__(self):
        self.entries = []

    @property
    def unexpected(self):
        return [e for e in self.entries if e["reason"] not in self.EXPECTED]

    def count(self, reason=None):
        if reason is None:
            return len(self.entries)
        return sum(1 for e in self.entries if e["reason"] == reason)

    def to_frame(self):
        return pd.DataFrame(self.entries)


def resolve(years, division, args):
    """Build the per-year source plan, honouring --source and --source-for."""
    forced = getattr(args, "source", "auto")
    overrides = _parse_overrides(getattr(args, "source_for", None))

    plan = {}
    for year in years:
        if year in overrides:
            plan[year] = overrides[year]
        elif forced in ("auto", "bulk"):
            chosen = config.BULK_YEAR_SOURCES.get(year)
            if not chosen:
                raise SystemExit(
                    f"no bulk source is configured for {year}; add it to "
                    f"config.BULK_YEAR_SOURCES or pass --source-for {year}=...")
            # `--source bulk` deliberately overrides the local-cache preference
            # for 2025, so the mirror-vs-cache A/B stays reachable.
            if forced == "bulk":
                chosen = tuple("lean" if s == "cache" else s for s in chosen)
            plan[year] = chosen
        elif forced == "live":
            plan[year] = ("live",)
        else:
            plan[year] = (forced,)
    return plan


def _parse_overrides(specs):
    overrides = {}
    for spec in specs or []:
        if "=" not in spec:
            raise SystemExit(f"--source-for wants YEAR=SPEC, got {spec!r}")
        year, chain = spec.split("=", 1)
        overrides[int(year)] = tuple(part for part in chain.split("+") if part)
    return overrides


def describe(plan) -> str:
    lines = ["resolved source plan:"]
    for year in sorted(plan):
        lines.append(f"  {year}: {' + '.join(plan[year])}")
    return "\n".join(lines)


#: Sources that read from disk or the site itself rather than a bulk mirror.
#: A year built only from these is unaffected by mirror staleness.
_NON_MIRROR_SOURCES = frozenset({"cache", "live", "local"})


def collect(plan, *, division, offline=False, refresh=False, pins=None,
            bulk_refresh=False):
    years = sorted(plan)
    needs_bulk = any(source != "live" for sources in plan.values()
                     for source in sources)

    manifest, shas = ({}, {})
    if needs_bulk:
        print("bulk sources:", flush=True)
        manifest, shas = bulk.prefetch(years, refresh=bulk_refresh,
                                       offline=offline, pins=pins)
        print(flush=True)

        # A season file written before its season ended holds a partial season,
        # and the data does not say so anywhere. Refuse rather than emit it.
        #
        # Only for years actually read from a mirror, though. 2026 is scraped
        # live and served from `cache`, so the mirror's stale 2026 files are
        # never touched -- checking them anyway made the guard fire on a year it
        # does not govern, and the only way past it was to declare the partial
        # season acceptable via BULK_PARTIAL_SEASON_OK, which would have been a
        # false statement that then applied to every later run.
        mirror_years = [y for y in years
                        if any(src not in _NON_MIRROR_SOURCES for src in plan[y])]
        stale = bulk.stale_seasons(manifest, mirror_years)
        blocking = {y: v for y, v in stale.items()
                    if y not in config.BULK_PARTIAL_SEASON_OK}
        if blocking:
            detail = "; ".join(
                f"{y}: {path} last written {stamp[:10]}"
                for y, (path, stamp) in sorted(blocking.items()))
            raise SystemExit(
                "refusing to build a partial season from a stale mirror.\n"
                f"  {detail}\n"
                "  College baseball ends in late June, so a file written before "
                "1 July of its season year is a mid-season snapshot. Measured for "
                "2026: at-bats average 47.8 below the full-season figure and "
                "batters-faced 56.7 below.\n"
                "  Either scrape the year live once the IP is unblocked "
                f"(--source-for {sorted(blocking)[0]}=live), or acknowledge the "
                "partial season explicitly via config.BULK_PARTIAL_SEASON_OK.")

    # Person keys are minted from every roster year the mirror has, not just the
    # years being built: a key whose value depended on which years you happened to
    # run would be useless for joining across builds.
    all_rosters = identity.load_all(division, manifest=manifest, shas=shas,
                                    offline=offline, refresh=refresh)
    person_ids, person_links = identity.mint(
        all_rosters,
        report_path=os.path.join(config.REPORT_DIR, "person_links.csv"))
    stats = identity.summarise(person_ids, all_rosters)
    print(f"person keys: {stats['persons']:,} people across "
          f"{stats['player_seasons']:,} player-seasons "
          f"({stats['multi_season_persons']:,} multi-season, "
          f"max {stats['max_seasons']}), {len(person_links):,} links\n", flush=True)

    batting_rows, pitching_rows, coverage = [], [], []
    failures, relaxed = [], []

    for year in years:
        sources = plan[year]
        primary = sources[0]
        adapter = _ADAPTERS.get(primary)
        if adapter is None:
            raise SystemExit(f"unknown source {primary!r} for {year}")

        roster = all_rosters.get(year)
        if roster is None:
            roster = rosters.load(year, division, manifest=manifest, shas=shas,
                                  offline=offline, refresh=refresh)

        frames, teams = adapter.collect(year, division, manifest=manifest,
                                        shas=shas, offline=offline,
                                        refresh=refresh, roster=roster)

        mapping, year_failures, year_relaxed = acronym.map_teams(
            teams, year, division)
        failures.extend(year_failures)
        relaxed.extend(year_relaxed)

        bat, pit = adapter.shape(frames, year, division, mapping)

        # A patch source contributes only the teams the primary source lacks. The
        # full D1 team list comes from the cached discovery pages, which is the
        # authoritative answer to "who was D1 this season" for every year.
        all_teams = None
        if any(e == "legacy_fill" for e in sources[1:]):
            full, _f, _r = acronym.map_teams(
                discovery.discover(None, year, division), year, division)
            all_teams = set(full.values())

        # Which source actually produced each team, so coverage.csv attributes
        # rows honestly instead of stamping every one with the primary source.
        team_source = {team["team_id"]: primary for team in teams}

        for extra in sources[1:]:
            patch_bat, patch_pit, patch_teams = _run_patch(
                extra, year, division, manifest=manifest, shas=shas,
                offline=offline, refresh=refresh, have=set(mapping.values()),
                roster=roster, all_teams=all_teams)
            bat.extend(patch_bat)
            pit.extend(patch_pit)
            teams = teams + patch_teams
            for team in patch_teams:
                mapping.setdefault(team["team_id"], team.get("acronym"))
                team_source[team["team_id"]] = extra

        # `playerid` is NCAA's per-season key; `person_id` is ours, spanning
        # seasons. A row whose player is absent from the roster file gets no
        # person key rather than a fabricated one -- downstream can then see
        # exactly which rows cannot participate in a multi-season count.
        # The cached rows carry playerid as a string straight out of the HTML,
        # while the roster files parse it as an integer. Look up both forms rather
        # than silently failing to key two thirds of the year.
        unkeyed = 0
        for row in bat + pit:
            raw = row["playerid"]
            person = person_ids.get((year, raw))
            if person is None:
                try:
                    person = person_ids.get((year, int(raw)))
                except (TypeError, ValueError):
                    person = None
            row["person_id"] = person
            unkeyed += person is None
        if unkeyed:
            print(f"  ! {year}: {unkeyed} row(s) have no roster entry, so no "
                  f"person_id", flush=True)

        batting_rows.extend(bat)
        pitching_rows.extend(pit)
        coverage.extend(_coverage(year, division, teams, mapping, bat, pit,
                                 team_source, shas))

        print(f"[{year}] {' + '.join(sources)}: {len(teams)} teams, "
              f"{len(bat)} batting rows, {len(pit)} pitching rows", flush=True)

    meta = {
        "coverage": pd.DataFrame(coverage),
        "ledger": SkipLedgerShim(),
        "failures": failures,
        "relaxed": relaxed,
        "unknown_headers": [],
        "quota_hit": None,
        "progress": None,
        "manifest": manifest,
    }
    return batting_rows, pitching_rows, meta


def _run_patch(name, year, division, *, manifest, shas, offline, refresh, have,
               roster=None, all_teams=None):
    """Add the teams a primary source is missing, from a secondary mirror.

    ``legacy_patch`` targets a known, named team list (2022's transition cohort).
    ``legacy_fill`` targets everything the primary lacks, which is what 2021 needs
    because the local cache only banked 175 of its 293 team-seasons.
    """
    if name == "legacy_patch":
        wanted = None
    elif name == "legacy_fill":
        wanted = all_teams
    else:
        raise SystemExit(f"unknown patch source {name!r} for {year}")
    return legacy.collect_patch(year, division, manifest=manifest, shas=shas,
                                offline=offline, refresh=refresh, have=have,
                                roster=roster, wanted=wanted)


def _coverage(year, division, teams, mapping, bat, pit, team_source, shas):
    """One row per team-season per category, matching the live path's columns."""
    stamp = time.strftime("%Y-%m-%dT%H:%M:%S")
    bat_counts, pit_counts = {}, {}
    for row in bat:
        bat_counts[row["ncaa_team_id"]] = bat_counts.get(row["ncaa_team_id"], 0) + 1
    for row in pit:
        pit_counts[row["ncaa_team_id"]] = pit_counts.get(row["ncaa_team_id"], 0) + 1

    rows = []
    for team in teams:
        team_id = team["team_id"]
        source = team_source.get(team_id, "unknown")
        alias = "legacy" if source.startswith("legacy") else "modern"
        base = {
            "year": year,
            "division": division,
            "team_id": team_id,
            "ncaa_name": team["ncaa_name"],
            "conference": team.get("conference"),
            # There is no ranking period to honour when reading a static file.
            "ranking_period": None,
            "fetched_at": stamp,
            "mapped_acronym": mapping.get(team_id),
            "source": source,
            "source_sha": (shas.get(alias) or "")[:12],
        }
        for category, counts in (("batting", bat_counts), ("pitching", pit_counts)):
            count = counts.get(team_id, 0)
            rows.append({
                **base,
                "category": category,
                "status": "ok" if count else "parse_empty",
                "parsed_rows": count,
                "filtered_rows": 0,
                "skipped_rows": 0,
            })
    return rows
