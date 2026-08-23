#!/usr/bin/env python3
"""Scrape NCAA player statistics and emit the two combined CSVs.

    python run.py --selftest                    # 5 s: is the site still parseable?
    python run.py --probe-rows                  # verify the row filter across years
    python run.py --probe-playerid-stability    # do NCAA player ids persist?
    python run.py --probe-gdp-2021              # is 2021 `OPP DP` == FanGraphs `gdp`?
    python run.py --year 2025                   # one season, ~34 min
    python run.py                               # all of config.YEARS
    python run.py --validate                    # acceptance gates on existing output
    python run.py --validate-against-fangraphs  # compare to the FanGraphs export

Run the probes before the full scrape. They are cheap and each one settles a
question that would otherwise be discovered hours in.

**WARNING: this site blocks the IP.** Measured: ~603 requests over 43 minutes at
1 req/s, then every URL 403s -- including robots.txt -- and it was still blocked
2 h 52 min later. The run stops with exit code 2 and writes no CSVs. Cached pages
cost no requests so a later re-run resumes, but read the "IP gets blocked" section
of README.md before running this again; there are better options than retrying.
"""

import argparse
import os
import re
import sys
import time

import pandas as pd

import config
import sources.registry
from derive import advanced, constants as constants_mod, rates
from mapping import acronym
from ncaa import cache, discovery, schema, team_page
from ncaa.budget import BudgetExhausted, RequestBudget
from ncaa.session import ChallengeError, FetchError, NcaaSession, QuotaExhausted
from ncaa.team_page import SkipLedger, StatIdAnomaly


def _parse_pins(specs):
    """Turn --bulk-pin repo=sha into {alias: sha}, accepting alias or repo name."""
    pins = {}
    by_repo = {spec["repo"]: alias for alias, spec in config.BULK_REPOS.items()}
    for spec in specs or []:
        if "=" not in spec:
            raise SystemExit(f"--bulk-pin wants REPO=SHA, got {spec!r}")
        name, sha = spec.split("=", 1)
        alias = name if name in config.BULK_REPOS else by_repo.get(name)
        if alias is None:
            raise SystemExit(
                f"--bulk-pin: unknown repository {name!r}; expected one of "
                f"{sorted(config.BULK_REPOS)} or {sorted(by_repo)}")
        pins[alias] = sha
    return pins


# ---------------------------------------------------------------------------
# Collection
# ---------------------------------------------------------------------------

class Progress:
    """Progress and time-remaining reporting for a multi-year scrape.

    Two estimates, because they answer different questions:

    * **this run** -- bounded by the per-run request budget, so it is what you
      will actually wait for before the process stops on its own.
    * **all remaining** -- the whole job across however many runs that takes.

    Both are driven by the measured seconds-per-fetch of the current run rather
    than a hardcoded constant, so they adapt if the network or the budget's
    pacing differs from expectation. Cached team-seasons are excluded from the
    estimate: they cost no requests and resolve in milliseconds, so counting them
    would make the ETA wildly pessimistic on a resumed run.
    """

    def __init__(self, total, cached, budget=None, report_every=10,
                 report_seconds=45.0):
        self.total = total
        self.cached_at_start = cached
        self.to_fetch = total - cached
        self.budget = budget
        self.report_every = report_every
        self.report_seconds = report_seconds

        # Starts at zero: every team-season, cached or not, is counted through
        # record(). Seeding this with `cached` double-counts them.
        self.done = 0
        self.fetched = 0            # team-seasons that cost requests
        self.started = time.time()
        self._last_report = 0.0
        self._since_report = 0

    @property
    def seconds_per_fetch(self):
        """Measured cost of one fetched team-season, or an estimate before any."""
        if self.fetched >= 3:
            return (time.time() - self.started) / self.fetched
        if self.budget is not None:
            return 2 * max(self.budget.min_interval, self.budget.min_sleep) + 0.7
        return 6.7  # measured at 1 req/s, before the budget existed

    def _fmt(self, seconds):
        if seconds < 90:
            return f"{seconds:.0f}s"
        if seconds < 5400:
            return f"{seconds / 60:.0f}m"
        return f"{seconds / 3600:.1f}h"

    def record(self, was_cached):
        self.done += 1
        if not was_cached:
            self.fetched += 1
        self._since_report += 1

    def due(self):
        return (self._since_report >= self.report_every
                or time.time() - self._last_report >= self.report_seconds)

    def line(self):
        self._last_report = time.time()
        self._since_report = 0

        rate = self.seconds_per_fetch
        pct = 100.0 * self.done / self.total if self.total else 100.0

        # Only uncached team-seasons take time; cached ones resolve in
        # milliseconds. Estimating from total remaining makes a resumed run look
        # hours away when it is seconds away.
        left_to_fetch = max(0, self.to_fetch - self.fetched)

        parts = [f"{self.done}/{self.total} team-seasons ({pct:.0f}%)"]
        if self.fetched:
            parts.append(f"{self.fetched} fetched @ {rate:.0f}s")

        # How much of that this run can actually reach before the budget stops it.
        this_run = left_to_fetch
        if self.budget is not None and self.budget.session_max:
            budget_left = max(0, self.budget.session_max - self.budget.session_count)
            this_run = min(left_to_fetch, budget_left // 2)

        if this_run:
            parts.append(f"~{self._fmt(this_run * rate)} left this run")
        if left_to_fetch > this_run:
            parts.append(f"~{self._fmt(left_to_fetch * rate)} of scraping total")
        if not left_to_fetch:
            parts.append("all cached")

        if self.budget is not None:
            parts.append(f"budget {self.budget.session_count}"
                         f"/{self.budget.session_max or '-'} this run, "
                         f"{self.budget.in_window()}/{self.budget.max_per_hour} this hour")

        return "  " + " | ".join(parts)

    def summary(self):
        elapsed = time.time() - self.started
        # Team-seasons never reached (budget stop or block) plus any still
        # uncached among those we did reach.
        left_to_fetch = max(0, self.to_fetch - self.fetched)
        lines = [
            f"  resolved {self.done}/{self.total} team-seasons "
            f"({self.fetched} fetched, {self.done - self.fetched} from cache)",
            f"  elapsed {self._fmt(elapsed)}",
        ]
        if left_to_fetch:
            detail = f"~{self._fmt(left_to_fetch * self.seconds_per_fetch)} of scraping"
            # Only claim a run count when a budget is actually enforcing one.
            if self.budget is not None and self.budget.session_max:
                per_run = max(1, self.budget.session_max // 2)
                runs = max(1, -(-left_to_fetch // per_run))  # ceil
                detail += f", ~{runs} more run{'s' if runs != 1 else ''} at this budget"
            lines.append(f"  {left_to_fetch} team-seasons still to fetch ({detail})")
        else:
            lines.append("  nothing left to fetch")
        return "\n".join(lines)


def collect(years, division, *, session, refresh=False, limit_teams=None,
            wait_on_quota=0):
    """Fetch and parse every team-season. Returns (batting, pitching, meta)."""
    batting_rows = []
    pitching_rows = []
    coverage = []
    ledger = SkipLedger()
    all_failures = []
    all_relaxed = []
    unknown_headers = set()
    quota_hit = None

    # Discover every year first, so the totals and the time estimate are known
    # before any fetching starts. Discovery pages are cached, so on a resumed run
    # this costs nothing.
    per_year = {}
    for year in years:
        try:
            per_year[year] = discovery.discover(session, year, division, refresh=refresh)
        except QuotaExhausted as error:
            quota_hit = (year, "discovery", error)
            break

    total = sum(len(t) if not limit_teams else min(len(t), limit_teams)
                for t in per_year.values())
    cached_now = sum(
        1 for year, teams in per_year.items()
        for team in (teams[:limit_teams] if limit_teams else teams)
        if team_page.is_cached(year, division, team["team_id"])
    )
    progress = Progress(total, cached_now, budget=getattr(session, "budget", None))

    if total:
        print(f"scope: {len(per_year)} year(s), {total} team-seasons, "
              f"{cached_now} already cached, {total - cached_now} to fetch")
        if total - cached_now:
            print(f"       ~{progress._fmt((total - cached_now) * progress.seconds_per_fetch)}"
                  f" of scraping remaining")
        print(flush=True)

    for year, teams in per_year.items():
        if quota_hit:
            break
        mapping, failures, relaxed = acronym.map_teams(teams, year, division)
        all_failures.extend(failures)
        all_relaxed.extend(relaxed)

        if limit_teams:
            teams = teams[:limit_teams]

        period = config.ranking_period(year, division)
        remaining_here = sum(
            1 for t in teams if not team_page.is_cached(year, division, t["team_id"]))
        print(f"[{year}] {len(teams)} teams (ranking_period={period:g}), "
              f"{remaining_here} to fetch", flush=True)

        for i, team in enumerate(teams, 1):
            team_id = team["team_id"]
            name = team["ncaa_name"]
            conference = team.get("conference")
            base = {
                "year": year,
                "division": division,
                "team_id": team_id,
                "ncaa_name": name,
                "conference": conference,
                "ranking_period": period,
                "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "mapped_acronym": mapping.get(team_id),
            }

            if team_id not in mapping:
                # Never silently drop: acronym.raise_for_failures aborts later,
                # but the coverage row records it either way.
                for category in schema.CATEGORIES:
                    coverage.append({**base, "category": category, "status": "unmapped_team",
                                     "parsed_rows": 0, "filtered_rows": 0, "skipped_rows": 0})
                continue

            try:
                result = team_page.fetch_team_season(
                    session, year=year, division=division, team_id=team_id,
                    ncaa_name=name, ledger=ledger, refresh=refresh,
                )
            except BudgetExhausted as error:
                # Our own limit, hit before the site's. This is the good outcome:
                # we stopped ourselves. Treated exactly like a block so no
                # partial CSV can be produced from a truncated population.
                quota_hit = (year, name, error)
                break
            except QuotaExhausted as error:
                # Stop the entire run. Every further request would 403 too, and
                # hammering an IP that has been told to stop is exactly what the
                # politeness commitments in the README rule out. The cache means
                # a later re-run resumes here for free.
                if wait_on_quota:
                    print(f"\n  quota reached at {name}; sleeping "
                          f"{wait_on_quota} min before continuing", flush=True)
                    time.sleep(wait_on_quota * 60)
                    try:
                        result = team_page.fetch_team_season(
                            session, year=year, division=division, team_id=team_id,
                            ncaa_name=name, ledger=ledger, refresh=refresh,
                        )
                    except QuotaExhausted as retry_error:
                        quota_hit = (year, name, retry_error)
                        break
                else:
                    quota_hit = (year, name, error)
                    break
            except StatIdAnomaly as error:
                for category in schema.CATEGORIES:
                    coverage.append({**base, "category": category,
                                     "status": "stat_id_anomaly", "error": str(error),
                                     "parsed_rows": 0, "filtered_rows": 0, "skipped_rows": 0})
                print(f"  ! {name}: {error}", flush=True)
                continue
            except Exception as error:  # noqa: BLE001
                for category in schema.CATEGORIES:
                    coverage.append({**base, "category": category,
                                     "status": "fetch_failed", "error": str(error),
                                     "parsed_rows": 0, "filtered_rows": 0, "skipped_rows": 0})
                print(f"  ! {name}: {error}", flush=True)
                continue

            unknown_headers.update(result["unknown_headers"])

            for category, sink in (("batting", batting_rows), ("pitching", pitching_rows)):
                rows = result[category]
                for row in rows:
                    row["conference"] = conference
                    row["team"] = mapping[team_id]
                sink.extend(rows)

                # Count only this team-season's skips. The ledger is cumulative
                # across the whole run, so a raw count here would grow
                # monotonically and make every late team look broken.
                mine = [e for e in ledger.entries
                        if e["team_id"] == team_id and e["year"] == year
                        and e["category"] == category]
                coverage.append({
                    **base,
                    "category": category,
                    "status": "ok" if rows else "parse_empty",
                    "parsed_rows": len(rows),
                    "filtered_rows": sum(1 for e in mine
                                         if e["reason"] == "no_player_link"),
                    "skipped_rows": sum(1 for e in mine
                                        if e["reason"] not in SkipLedger.EXPECTED),
                    "header_fingerprint": result["header_fingerprint"].get(category),
                    "ysc_ids": ",".join(str(i) for i in (result["ysc_ids"] or [])),
                })

            progress.record(was_cached=result.get("from_cache", False))
            if progress.due() or i == len(teams):
                print(progress.line(), flush=True)

    meta = {
        "coverage": pd.DataFrame(coverage),
        "ledger": ledger,
        "failures": all_failures,
        "relaxed": all_relaxed,
        "unknown_headers": sorted(unknown_headers),
        "quota_hit": quota_hit,
        "progress": progress,
    }
    return batting_rows, pitching_rows, meta


def _report_quota(quota_hit, division, budget=None):
    """Explain why the run stopped and how much is banked."""
    year, name, error = quota_hit
    cached = 0
    for _root, _dirs, files in os.walk(config.CACHE_DIR):
        cached += sum(1 for f in files if f.endswith("_batting.html.gz"))

    from validate.acceptance import EXPECTED_TEAMS

    ours = isinstance(error, BudgetExhausted)
    total = sum(EXPECTED_TEAMS.get(y, 0) for y in config.YEARS)

    print(f"\n{'=' * 68}")
    if ours:
        print("PAUSED: our own request budget is spent (this is the good case)")
    else:
        print("STOPPED: stats.ncaa.org denied the request (HTTP 403)")
    print(f"{'=' * 68}")
    print(f"  stopped at    {year} {name}")
    print(f"  reason        {error if ours else error.last_error}")
    print(f"  cached        {cached} of ~{total} team-seasons"
          f" ({100 * cached / total:.0f}%)" if total else f"  cached {cached}")
    if budget is not None:
        print(f"  budget        {budget.summary()}")
    print()
    if ours:
        print("  We stopped ourselves before the site had to. Re-run the same")
        print("  command whenever you like -- the rolling hourly cap persists on")
        print("  disk, so it will pace itself and pick up where it left off.")
    else:
        print("  Measured on 2026-08-11: ~603 requests in 43 min earned a block")
        print("  on every URL, robots.txt included, lasting 16+ hours. If this")
        print("  happened despite the budget, lower --max-per-hour before retrying")
        print("  and do not poll the site to check.")
    print()
    print("  Cached pages cost no requests, so nothing already fetched is redone.")
    print(f"{'=' * 68}")


# ---------------------------------------------------------------------------
# Frame construction
# ---------------------------------------------------------------------------

IDENTITY = ("playerid", "name", "class", "team", "conference", "year",
            "division", "ncaa_team_id")


def _fold_accents(text):
    import unicodedata
    if text is None:
        return None
    decomposed = unicodedata.normalize("NFKD", str(text))
    return "".join(c for c in decomposed if not unicodedata.combining(c))


def _identity(record):
    name = record.get("name")
    class_year = record.get("class")
    return {
        "playerid": record["playerid"],
        # NCAA re-mints playerid every season, so it cannot group a player across
        # years. person_id is minted in sources/identity.py and is what the
        # eligibility season-count must use. None on the live path, which has no
        # roster file to link against.
        "person_id": record.get("person_id"),
        "name": name,
        # Blank class is genuinely unknown, unlike a blank counting stat.
        "class": class_year if class_year in config.VALID_CLASSES else None,
        "nameascii": _fold_accents(name),
        "team": record["team"],
        "conference": record.get("conference"),
        "year": record["year"],
        "division": record["division"],
        "ncaa_team_id": record["ncaa_team_id"],
        "pos": record.get("pos"),
    }


def build_batting_frame(rows):
    records = []
    for row in rows:
        counts = rates.batting_counts(row, row["year"])
        records.append({**_identity(row), **counts,
                        "opp_dp": rates.to_int(row.get("opp_dp")),
                        "ncaa_ba": rates.to_float(row.get("ncaa_ba")),
                        "tb": rates.to_int(row.get("tb"))})
    return pd.DataFrame(records)


def build_pitching_frame(rows):
    records = []
    for row in rows:
        counts = rates.pitching_counts(row, row["year"])
        records.append({**_identity(row), **counts,
                        "ncaa_era": rates.to_float(row.get("ncaa_era"))})
    return pd.DataFrame(records)


def reconcile_names(batting, pitching):
    """Give every ``playerid`` one canonical name across both files.

    NCAA spells the same player differently on the batting and pitching pages of
    the same team-season. Observed in 2025:

        playerid 8878816   'AJ Kostic'           vs 'A.J. Kostic'
        playerid 9300432   'McNish-Heider Ethan' vs 'Ethan McNish-Heider'

    Same id, same team, same class -- so identity is not in doubt, and the
    downstream merge in ``build_2026_combined.py`` joins on ``playerid`` and is
    unaffected. But ``masterDraft.py`` matches draft records by *name*, so a
    two-way player carrying two spellings could match on one file and miss on the
    other. That is a silent loss of a positive label, which matters here.

    Rules, in order: most frequent spelling wins; then the one whose word order
    matches the corpus convention; then the longer string.

    The word-order rule exists because the first version got this wrong. The two
    spellings of playerid 9300432 are the same tokens reversed and both 19
    characters, so a length tie-break was a coin flip -- and it kept
    'McNish-Heider Ethan', the Last-First form, which is precisely what breaks
    name-based draft matching. So when two candidates have the same token
    multiset, each is scored by how often its tokens appear in that position
    across all ~10k scraped names: 'Ethan' is common in first position, so
    'Ethan McNish-Heider' wins on evidence rather than on luck.

    Ties beyond that fall back to length, which keeps 'A.J. Kostic' over
    'AJ Kostic' and preserves punctuation. Every change is written to
    ``out/reports/name_reconciliation.csv``.

    This only makes the two files agree with each other. A name NCAA reports
    reversed on *both* pages still comes through reversed -- there is nothing to
    compare it against -- and downstream draft matching has to cope with that the
    same way it already does for the FanGraphs data.
    """
    combined = pd.concat([
        batting[["playerid", "name"]].assign(src="batting"),
        pitching[["playerid", "name"]].assign(src="pitching"),
    ])

    # Positional token frequencies over every name we scraped, used only to
    # break word-order ties.
    first_tokens = {}
    last_tokens = {}
    for name in combined["name"].dropna():
        parts = str(name).split()
        if len(parts) < 2:
            continue
        first_tokens[parts[0]] = first_tokens.get(parts[0], 0) + 1
        last_tokens[parts[-1]] = last_tokens.get(parts[-1], 0) + 1

    def order_score(name):
        """How well this word order matches the corpus convention."""
        parts = str(name).split()
        if len(parts) < 2:
            return 0
        return first_tokens.get(parts[0], 0) + last_tokens.get(parts[-1], 0)

    canonical = {}
    changes = []
    for playerid, group in combined.groupby("playerid"):
        spellings = group["name"].dropna().unique()
        if len(spellings) < 2:
            continue
        counts = group["name"].value_counts()

        # The word-order score only arbitrates genuine reorderings -- candidates
        # built from the same tokens. 'Matt Tarr' vs 'Matthew Tarr' is a
        # nickname, not a reordering, and there the corpus frequency of 'Matt'
        # says nothing about this player; length wins instead, preserving the
        # fuller form and any punctuation.
        token_sets = {frozenset(str(n).split()) for n in counts.index}
        reordering = len(token_sets) == 1

        best = sorted(
            counts.index,
            key=lambda n: (
                (-counts[n], -order_score(n), -len(n)) if reordering
                else (-counts[n], -len(n))
            ),
        )[0]
        canonical[playerid] = best
        for spelling in spellings:
            if spelling != best:
                changes.append({"playerid": playerid, "dropped": spelling,
                                "kept": best})

    if canonical:
        for frame in (batting, pitching):
            mapped = frame["playerid"].map(canonical)
            frame.loc[mapped.notna(), "name"] = mapped[mapped.notna()]
            frame.loc[mapped.notna(), "nameascii"] = (
                mapped[mapped.notna()].map(_fold_accents)
            )

    return batting, pitching, changes


def apply_derived(batting, pitching, constants, scope):
    """Attach rate and league-relative columns. Mutates copies, returns them."""
    index = constants_mod.lookup(constants, scope)

    def key(row):
        return (row["year"], row["conference"] if scope == "conference" else "ALL")

    batting = batting.copy()
    derived = []
    for record in batting.to_dict("records"):
        rate = rates.batting_rates(record)
        group = index.get(key(record))
        if group is None:
            raise KeyError(
                f"no league constants for {key(record)} -- a team's conference is "
                "missing from the constants table, which should be impossible"
            )
        derived.append({
            **{k: v for k, v in rate.items() if not k.startswith("_")},
            "spd": rates.speed_score(record),
            **advanced.add_batting_advanced(record, group),
        })
    if not derived:
        raise ValueError("no rows to derive from")
    for column in derived[0]:
        batting[column] = [d[column] for d in derived]

    pitching = pitching.copy()
    derived = []
    for record in pitching.to_dict("records"):
        group = index.get(key(record))
        if group is None:
            raise KeyError(f"no league constants for {key(record)}")
        derived.append(rates.pitching_rates(record, group["cfip"]))
    if not derived:
        raise ValueError("no rows to derive from")
    for column in derived[0]:
        pitching[column] = [d[column] for d in derived]

    return batting, pitching


# ---------------------------------------------------------------------------
# Emission
# ---------------------------------------------------------------------------

def emit(batting, pitching, coverage, *, out_dir, partial_reason=None):
    """Write the CSVs -- but only if every coverage row is `ok`.

    A short file must never be able to acquire the real filename. If anything
    failed, the output goes to ``*.PARTIAL.csv`` and the caller exits non-zero.

    ``partial_reason`` forces the PARTIAL name even when every coverage row is
    `ok`. That covers the case coverage cannot see: ``--limit-teams`` and
    ``--year`` produce a deliberately incomplete population, and coverage only
    records the teams the run actually attempted, so it looks complete. A
    40-team smoke test writing ``batting_combined_all.csv`` is exactly the
    silent-truncation failure this function exists to prevent.
    """
    os.makedirs(out_dir, exist_ok=True)
    bad = coverage[coverage["status"] != "ok"] if len(coverage) else coverage
    if len(bad):
        # Acknowledged gaps still appear in the report, but do not force the
        # PARTIAL name: they are permanent absences in the public sources, not
        # something a re-run can fix. Each one costs an entry with a reason in
        # config.KNOWN_EMPTY_TEAM_SEASONS.
        known = [
            (row.year, row.mapped_acronym, row.category)
            in config.KNOWN_EMPTY_TEAM_SEASONS
            for row in bad.itertuples()
        ]
        bad = bad[[not flag for flag in known]]
    complete = len(bad) == 0 and partial_reason is None

    suffix = "" if complete else ".PARTIAL"
    paths = {}
    for name, frame, columns in (
        ("batting", batting, config.BATTING_COLUMNS),
        ("pitching", pitching, config.PITCHING_COLUMNS),
    ):
        missing = [c for c in columns if c not in frame.columns]
        if missing:
            raise KeyError(f"{name}: derived frame is missing {missing}")
        path = os.path.join(out_dir, f"{name}_combined_all{suffix}.csv")
        frame[columns].to_csv(path, index=False)
        paths[name] = path

    return paths, complete, bad


# ---------------------------------------------------------------------------
# Probes
# ---------------------------------------------------------------------------

def selftest(sleep_ok=None):
    """Fetch one known team page and assert it is parseable. ~5 seconds.

    Two jobs: it is the canary for Akamai rotating the interstitial, and it is
    the cheapest way to check whether an IP block has lifted -- one request
    rather than a whole run that aborts on team one.
    """
    # Counted against the rolling window like any other request -- a budget that
    # ignores some of its own traffic is not a budget.
    budget = RequestBudget(
        path=os.path.join(config.CACHE_DIR, config.REQUEST_LOG),
        max_per_hour=config.MAX_REQUESTS_PER_HOUR,
        session_max=0,          # no per-run cap; this is 3 requests
        jitter=config.JITTER,
        min_sleep=sleep_ok or 0.0,
        verbose=False,
    )
    session = NcaaSession(sleep_ok=sleep_ok, budget=budget)
    team_id = 596471  # A&M-Corpus Christi, 2025
    print(f"GET /teams/{team_id}/season_to_date_stats ...", flush=True)

    try:
        html = session.get(team_page.batting_url(team_id))
    except QuotaExhausted:
        # The common case when resuming, so answer it plainly rather than with a
        # traceback -- this is the check you run to decide whether to bother.
        print("\n  STILL BLOCKED (HTTP 403)")
        print("  The IP has not been unblocked yet. Nothing to do but wait; do")
        print("  not loop on this check. See 'Resuming after a block' in README.md.")
        return 1
    except ChallengeError as error:
        print("\n  CHALLENGE NOT CLEARED")
        print(f"  {error.last_error}")
        print("  Akamai has probably rotated the interstitial. Save the challenge")
        print("  page and update the regexes in ncaa/session.py.")
        return 1
    except FetchError as error:
        print(f"\n  FETCH FAILED: {error.last_error}")
        return 1

    print(f"  {len(html):,} bytes, {session.requests_made} requests, "
          f"{session.solves} challenge solve(s)")

    ysc = team_page.extract_ysc_ids(html)
    print(f"  year_stat_category_id: {ysc}")

    ledger = SkipLedger()
    rows, unknown = team_page.parse_grid(
        html, year=2025, division=1, team_id=team_id,
        ncaa_name="A&M-Corpus Christi", category="batting", ledger=ledger,
    )
    print(f"  {len(rows)} player rows, {ledger.count('no_player_link')} filtered, "
          f"{len(ledger.unexpected)} unexpected skips, unknown headers: {unknown}")

    problems = []
    if len(rows) < 15:
        problems.append(f"only {len(rows)} player rows (expected >= 15)")
    if unknown:
        problems.append(f"unrecognized headers: {unknown}")
    if ledger.unexpected:
        problems.append(f"{len(ledger.unexpected)} unexpected skips")

    if problems:
        print("\nSELFTEST FAILED:")
        for problem in problems:
            print(f"  - {problem}")
        return 1
    print("\nSELFTEST PASSED")
    return 0


def probe_rows(years, division, n_teams):
    """Verify the /players/{id} row filter across seasons (risk 3)."""
    session = NcaaSession()
    print(f"{'year':>6} {'cat':<9} {'tr':>5} {'w/link':>7} {'ids':>5} {'match':>6}")
    ok = True
    for year in years:
        teams = discovery.discover(session, year, division)
        for team in teams[:n_teams]:
            ledger = SkipLedger()
            result = team_page.fetch_team_season(
                session, year=year, division=division, team_id=team["team_id"],
                ncaa_name=team["ncaa_name"], ledger=ledger,
            )
            for category in schema.CATEGORIES:
                rows = result[category]
                html = cache.read_html(
                    cache.html_path(year, division, team["team_id"], category))
                total = len(re.findall(r"<tr", html or ""))
                ids = len(set(re.findall(r"/players/(\d+)", html or "")))
                match = len(rows) == ids
                ok &= match
                print(f"{year:>6} {category:<9} {total:>5} {len(rows):>7} {ids:>5} "
                      f"{'yes' if match else 'NO':>6}")
    print("\nrows_with_link == distinct_player_ids everywhere:", ok)
    return 0 if ok else 1


def probe_playerid_stability(division=1):
    """Do NCAA player ids persist across seasons? (risk 6)

    Downstream code groups on ``playerid`` to count a player's eligibility
    seasons, so per-season ids would corrupt that silently.
    """
    session = NcaaSession()
    pairs = [(2024, 2025), (2023, 2024)]
    for earlier, later in pairs:
        early_teams = {t["ncaa_name"]: t["team_id"]
                       for t in discovery.discover(session, earlier, division)}
        late_teams = {t["ncaa_name"]: t["team_id"]
                      for t in discovery.discover(session, later, division)}
        shared = sorted(set(early_teams) & set(late_teams))[:3]

        for name in shared:
            ids = {}
            for year, table in ((earlier, early_teams), (later, late_teams)):
                ledger = SkipLedger()
                result = team_page.fetch_team_season(
                    session, year=year, division=division, team_id=table[name],
                    ncaa_name=name, ledger=ledger,
                )
                ids[year] = {r["playerid"]: r["name"] for r in result["batting"]}

            overlap = set(ids[earlier]) & set(ids[later])
            names_early = set(ids[earlier].values())
            names_late = set(ids[later].values())
            name_overlap = names_early & names_late

            print(f"{name}: {earlier}={len(ids[earlier])} {later}={len(ids[later])} "
                  f"| shared ids={len(overlap)} shared names={len(name_overlap)}")
            if name_overlap and not overlap:
                print("   ids do NOT persist -- a minted stable key is required")
            elif overlap:
                sample = sorted(overlap)[:3]
                print(f"   ids persist, e.g. {[(i, ids[earlier][i]) for i in sample]}")
    return 0


def probe_gdp_2021(division=1, n_teams=20):
    """Is 2021's `OPP DP` the same statistic as FanGraphs' 2021 `gdp`?

    2021 is the one season whose batting grid has no `GDP` column. If `OPP DP`
    matches, 2021 `gdp` is recoverable; if not, it must stay null and be
    documented as such.
    """
    if not os.path.exists(config.FG_BATTING):
        print(f"skipped: {config.FG_BATTING} not present (needs FanGraphs access)")
        return 0

    session = NcaaSession()
    teams = discovery.discover(session, 2021, division)[:n_teams]
    scraped = []
    for team in teams:
        ledger = SkipLedger()
        result = team_page.fetch_team_season(
            session, year=2021, division=division, team_id=team["team_id"],
            ncaa_name=team["ncaa_name"], ledger=ledger,
        )
        acronym_value, _ = acronym.to_fg_acronym(team["ncaa_name"], 2021, division)
        for row in result["batting"]:
            scraped.append({
                "key": _join_key(row["name"], acronym_value, 2021),
                "opp_dp": rates.to_int(row.get("opp_dp")),
            })

    ours = pd.DataFrame(scraped).drop_duplicates("key").set_index("key")
    fg = pd.read_csv(config.FG_BATTING)
    fg = fg[fg["year"] == 2021].copy()
    fg["key"] = [_join_key(n, t, 2021) for n, t in zip(fg["name"], fg["team"])]
    fg = fg.drop_duplicates("key").set_index("key")

    joined = ours.join(fg[["gdp"]], how="inner")
    if joined.empty:
        print("no overlap -- check the join key")
        return 1
    agree = (joined["opp_dp"] == joined["gdp"]).mean()
    print(f"joined {len(joined)} players; OPP DP == FanGraphs gdp on {agree:.1%}")
    print(joined.head(12).to_string())
    print()
    if agree >= 0.95:
        print("VERDICT: OPP DP is the same statistic. Map it as 2021 `gdp`.")
    else:
        print("VERDICT: different statistic. 2021 `gdp` stays null; document it.")
    return 0


def _join_key(name, team, year):
    """Normalized (name, team, year) key for matching against FanGraphs."""
    text = _fold_accents(name or "").lower()
    text = re.sub(r"\b(jr|sr|ii|iii|iv)\b", "", text)
    text = re.sub(r"[.'`]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return f"{text}|{team}|{year}"


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def _parse_years(spec):
    """'2021-2025' or '2021,2023' -> [2021, ...]."""
    if "-" in spec:
        start, _, end = spec.partition("-")
        return list(range(int(start), int(end) + 1))
    return [int(part) for part in spec.split(",") if part.strip()]


def pipeline(args):
    if args.year:
        years = [args.year]
    elif args.years:
        years = _parse_years(args.years)
    else:
        years = list(config.YEARS)
    division = args.division
    scope = args.scope

    os.makedirs(config.REPORT_DIR, exist_ok=True)

    plan = sources.registry.resolve(years, division, args)
    if args.list_sources:
        print(sources.registry.describe(plan))
        return 0

    # Years served from a mirror or the local cache need no network session at
    # all. Building one anyway would arm the budget and the 403 handling for a run
    # that never touches stats.ncaa.org -- and on a blocked IP the session's own
    # constructor is the last thing that should run.
    live_years = [year for year in years if "live" in plan[year]]

    session = None
    budget = None
    if live_years and not args.offline:
        budget = RequestBudget(
            path=os.path.join(config.CACHE_DIR, config.REQUEST_LOG),
            max_per_hour=args.max_per_hour,
            session_max=args.request_budget,
            jitter=config.JITTER,
            min_sleep=args.sleep,
        )
        session = NcaaSession(sleep_ok=args.sleep, budget=budget)
        interval = max(budget.min_interval, args.sleep)
        print(f"budget: {args.max_per_hour}/hour rolling (persists across runs), "
              f"{args.request_budget} max this run, ~{interval:.0f}s between "
              f"requests (~{2 * interval + 0.7:.0f}s per team-season)")
        print(f"        {budget.in_window()} request(s) already logged in the "
              f"last hour", flush=True)

    if live_years == years:
        batting_rows, pitching_rows, meta = collect(
            years, division, session=session, refresh=args.refresh,
            limit_teams=args.limit_teams, wait_on_quota=args.wait_on_quota,
        )
    elif live_years:
        raise SystemExit(
            "mixing live and offline years in one run is not supported: the "
            "live leg can abort on a quota mid-run, which would leave the "
            "offline years' league constants fitted to a partial population. "
            f"Run them separately -- live years here are {live_years}.")
    else:
        batting_rows, pitching_rows, meta = sources.registry.collect(
            plan, division=division, offline=args.offline,
            refresh=args.refresh, pins=_parse_pins(args.bulk_pin),
            bulk_refresh=args.bulk_refresh,
        )

    if meta.get("progress") is not None:
        print()
        print(meta["progress"].summary(), flush=True)

    if meta["quota_hit"]:
        _report_quota(meta["quota_hit"], division, budget=budget)
        # Exit 2, distinct from 1: the data is incomplete for an external
        # reason, not because anything failed validation. Do not emit CSVs --
        # even PARTIAL ones -- since the population would be a
        # geographically-arbitrary alphabetical prefix and the league constants
        # fitted to it would be quietly wrong.
        return 2

    coverage = meta["coverage"]
    coverage.to_csv(os.path.join(config.REPORT_DIR, "coverage.csv"), index=False)

    ledger = meta["ledger"]
    if ledger.entries:
        pd.DataFrame(ledger.entries).to_csv(
            os.path.join(config.REPORT_DIR, "skips.csv"), index=False)

    unmapped_path = os.path.join(config.REPORT_DIR, "unmapped_teams.csv")
    if meta["failures"]:
        pd.DataFrame(meta["failures"]).to_csv(unmapped_path, index=False)
    if meta["relaxed"]:
        pd.DataFrame(meta["relaxed"]).to_csv(
            os.path.join(config.REPORT_DIR, "relaxed_team_lookups.csv"), index=False)
        print(f"\n{len(meta['relaxed'])} team-seasons needed the relaxed "
              f"(no-division) registry lookup; see relaxed_team_lookups.csv")

    if meta["unknown_headers"]:
        print(f"\nUNRECOGNIZED HEADERS: {meta['unknown_headers']}")
        print("Add them to ncaa/schema.py, either to a *_MAP or to IGNORED_*.")
        return 1

    acronym.raise_for_failures(meta["failures"], unmapped_path)

    if not batting_rows:
        print("no rows collected")
        return 1

    print(f"\nparsed {len(batting_rows):,} batting and {len(pitching_rows):,} "
          f"pitching rows", flush=True)

    batting = build_batting_frame(batting_rows)
    pitching = build_pitching_frame(pitching_rows)

    batting, pitching, name_changes = reconcile_names(batting, pitching)
    if name_changes:
        pd.DataFrame(name_changes).to_csv(
            os.path.join(config.REPORT_DIR, "name_reconciliation.csv"), index=False)
        print(f"reconciled {len(name_changes)} name spelling(s) across the two "
              f"files; see out/reports/name_reconciliation.csv")

    # League constants come from the FULL population, before the phantom-row
    # drop below. A pinch runner can have PA=0 and R>0, and those runs belong in
    # the league total even though the row itself is not emitted.
    print(f"fitting league constants (scope={scope}) ...", flush=True)
    constants = constants_mod.build(batting, pitching, scope=scope)
    constants.to_csv(os.path.join(config.REPORT_DIR, "league_constants.csv"), index=False)

    # Phantom rows: the batting grid lists pitchers with no plate appearances,
    # and (pre-2024, where the grid was keyed on games played) the pitching grid
    # lists position players who never pitched. Emitting them would create fake
    # "Two-Way" players in build_2026_combined.py:136.
    #
    # The pitching test is `tbf == 0`, NOT `ip_true == 0`. A reliever can face
    # batters, allow hits and walks, and be pulled before recording an out --
    # 32 such pitchers exist in 2025 alone, and an earlier `ip_true > 0` rule
    # silently discarded every one of them. They are real appearances; their
    # innings-denominated rates are simply null. You cannot face a batter without
    # pitching, so tbf separates the two cases cleanly where innings do not.
    batting_phantom = int((batting["pa"] == 0).sum())
    pitching_phantom = int((pitching["tbf"] == 0).sum())
    zero_ip_kept = int(((pitching["tbf"] > 0) & (pitching["ip_true"] == 0)).sum())
    batting = batting[batting["pa"] > 0].reset_index(drop=True)
    pitching = pitching[pitching["tbf"] > 0].reset_index(drop=True)
    print(f"dropped {batting_phantom:,} zero-PA batting and {pitching_phantom:,} "
          f"no-batters-faced pitching rows "
          f"(kept {zero_ip_kept:,} appearances with 0 innings)")

    batting, pitching = apply_derived(batting, pitching, constants, scope)

    # Anything less than every configured year and every discovered team is a
    # deliberately partial population, and must not claim the real filename.
    #
    # Two definitions of "every year", because there are two deliverables.
    # config.YEARS (2021-2026) is the eventual target and what a live scrape
    # should cover. config.PUBLIC_YEARS (2021-2025) is what public sources can
    # actually supply today, since the mirror stopped updating mid-2026. A build
    # covering PUBLIC_YEARS is the complete *public* dataset and earns the real
    # filename; anything less does not.
    expected = set(config.PUBLIC_YEARS)
    partial_reason = None
    if args.limit_teams:
        partial_reason = f"--limit-teams {args.limit_teams}"
    elif not expected.issubset(set(years)):
        missing = sorted(expected - set(years))
        partial_reason = (f"only year(s) {years}; the public dataset needs "
                          f"{sorted(expected)} and is missing {missing}")

    paths, complete, bad = emit(batting, pitching, coverage,
                                out_dir=config.OUT_DIR,
                                partial_reason=partial_reason)
    for name, path in paths.items():
        print(f"wrote {path} ({len(batting if name == 'batting' else pitching):,} rows)")

    if partial_reason and len(bad) == 0:
        print(f"\nwritten as *.PARTIAL.csv: {partial_reason}. Every team fetched "
              f"succeeded, but this is not the full population -- league constants "
              f"fitted to a subset are not the ones the final CSVs should carry.")
        return 1

    if not complete:
        print(f"\n{len(bad)} coverage rows are not `ok` -- output written as "
              f"*.PARTIAL.csv. See out/reports/coverage.csv")
        print(bad["status"].value_counts().to_string())
        return 1

    unexpected = len(ledger.unexpected)
    if unexpected:
        print(f"\n{unexpected} unexpected row skips; see out/reports/skips.csv")
        return 1

    from validate import acceptance
    return acceptance.run(batting, pitching, constants, coverage, scope=scope)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--year", type=int, help="scrape a single season")
    parser.add_argument("--years", metavar="A-B|A,B,C",
                        help="build a subset of seasons, e.g. 2021-2025. The "
                             "league constants are fitted to exactly the years "
                             "built, so a subset is a different fit -- emit() "
                             "marks the output PARTIAL unless it covers "
                             "config.YEARS.")
    parser.add_argument("--division", type=int, default=config.DIVISION)
    parser.add_argument("--scope", default=config.LEAGUE_SCOPE,
                        choices=["conference", "division"],
                        help="league scope for wOBA/wRC+/FIP constants")
    parser.add_argument("--workers", type=int, default=config.WORKERS,
                        help="parallel fetchers (hard cap 3; 1 is the polite default)")
    parser.add_argument("--sleep", type=float, default=config.SLEEP_OK,
                        metavar="SECONDS",
                        help=f"floor between requests (default {config.SLEEP_OK:g}); "
                             f"--max-per-hour usually governs instead")
    parser.add_argument("--max-per-hour", type=int, default=config.MAX_REQUESTS_PER_HOUR,
                        metavar="N",
                        help=f"rolling hourly request cap, persisted across runs "
                             f"(default {config.MAX_REQUESTS_PER_HOUR}). ~840/hour "
                             f"earned a 16h+ IP block, so raise this at your peril.")
    parser.add_argument("--request-budget", type=int,
                        default=config.SESSION_REQUEST_BUDGET, metavar="N",
                        help=f"stop voluntarily after this many requests in one run "
                             f"(default {config.SESSION_REQUEST_BUDGET}; ~603 tripped "
                             f"the block)")
    parser.add_argument("--refresh", action="store_true", help="ignore the cache")
    parser.add_argument("--wait-on-quota", type=int, default=0, metavar="MINUTES",
                        help="on a per-IP quota 403, sleep this many minutes and "
                             "continue instead of stopping (try 60)")
    parser.add_argument("--offline", action="store_true",
                        help="cache only; fail rather than fetch")
    parser.add_argument("--limit-teams", type=int,
                        help="only the first N teams per year (for smoke tests)")

    # Offline sources. stats.ncaa.org blocks this IP at the Akamai layer, so the
    # bulk mirrors are the default route, not a fallback.
    parser.add_argument("--source", default="auto",
                        choices=["auto", "bulk", "live", "rich", "lean",
                                 "legacy", "cache"],
                        help="where rows come from (default auto: the per-year "
                             "plan in config.BULK_YEAR_SOURCES). 'live' scrapes "
                             "stats.ncaa.org and needs an unblocked IP; 'bulk' "
                             "forces every year onto a mirror, including 2025, "
                             "which is how the mirror-vs-local-cache A/B is run.")
    parser.add_argument("--source-for", action="append", metavar="YEAR=SPEC",
                        help="override one year, e.g. --source-for 2026=lean or "
                             "2022=rich+legacy_patch (repeatable)")
    parser.add_argument("--bulk-refresh", action="store_true",
                        help="re-resolve each mirror's HEAD commit and re-download. "
                             "Moves the pin, so every derived number can move too; "
                             "the old SHA is retained in MANIFEST.json.")
    parser.add_argument("--bulk-pin", action="append", metavar="REPO=SHA",
                        help="pin a mirror to an explicit commit (repeatable)")
    parser.add_argument("--verify-bulk", action="store_true",
                        help="re-hash every cached bulk file against MANIFEST.json")
    parser.add_argument("--list-sources", action="store_true",
                        help="print the resolved per-year source plan and exit")
    parser.add_argument("--selftest", action="store_true")
    parser.add_argument("--probe-rows", action="store_true")
    parser.add_argument("--probe-playerid-stability", action="store_true")
    parser.add_argument("--probe-gdp-2021", action="store_true")
    parser.add_argument("--teams", type=int, default=5, help="team count for probes")
    parser.add_argument("--validate", action="store_true",
                        help="run acceptance gates against existing out/ CSVs")
    parser.add_argument("--validate-against-fangraphs", action="store_true")
    parser.add_argument("--prune-html", action="store_true",
                        help="delete cached HTML, keep parsed rows")
    args = parser.parse_args(argv)

    if args.workers > 3:
        parser.error("--workers is capped at 3; see the politeness note in config.py")

    if args.selftest:
        return selftest(sleep_ok=args.sleep)
    if args.probe_rows:
        years = [args.year] if args.year else list(config.YEARS)
        return probe_rows(years, args.division, args.teams)
    if args.probe_playerid_stability:
        return probe_playerid_stability(args.division)
    if args.probe_gdp_2021:
        return probe_gdp_2021(args.division, args.teams)
    if args.prune_html:
        reclaimed = cache.prune_html(args.division)
        print(f"reclaimed {reclaimed / 1e6:.1f} MB")
        return 0
    if args.verify_bulk:
        from sources import manifest as manifest_mod
        problems = manifest_mod.verify()
        loaded = manifest_mod.load()
        if not loaded.get("files"):
            print("no bulk manifest yet; run a bulk build first")
            return 1
        for alias, entry in sorted(loaded["resolved"].items()):
            print(f"{alias}: {entry['owner']}/{entry['repo']}@{entry['sha'][:12]} "
                  f"(resolved {entry['resolved_at']})")
        if problems:
            for key, kind, detail in problems:
                print(f"  FAIL {key}: {kind} -- {detail}")
            return 1
        print(f"  {len(loaded['files'])} file(s) verified, all sha256 match")
        return 0
    if args.validate_against_fangraphs:
        from validate import against_fangraphs
        return against_fangraphs.run(year=args.year)
    if args.validate:
        from validate import acceptance
        return acceptance.run_from_disk(scope=args.scope)

    return pipeline(args)


if __name__ == "__main__":
    sys.exit(main())
