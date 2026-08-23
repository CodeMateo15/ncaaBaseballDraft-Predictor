"""Configuration for the NCAA player-stat scrape.

The knobs at the top are the ones you change. Everything below them is a
constant of the NCAA's site, not a preference.
"""

# ---------------------------------------------------------------------------
# The knobs
# ---------------------------------------------------------------------------

YEARS = range(2021, 2027)      # <- change this line to backfill earlier seasons
DIVISION = 1                   # <- and this one

# The seasons public sources can actually supply a *finished* season for.
#
# 2026 is included as of 2026-08-22, scraped live from stats.ncaa.org over eight
# budget-limited sessions. It is deliberately NOT built from the bulk mirror,
# which stopped being updated on 2026-04-12 with its 2026 files written
# mid-season -- those understate at-bats by ~48 per player, and
# sources/bulk.py::stale_seasons still refuses them. Always pass
# `--source-for 2026=live` so 2026 comes from the live cache, not the mirror.
#
# 2026 covers all 308 D1 team-seasons.
PUBLIC_YEARS = range(2021, 2027)

LEAGUE_SCOPE = "conference"    # "conference" (matches FanGraphs) or "division"

# ---------------------------------------------------------------------------
# Politeness. Do not raise these casually.
#
# stats.ncaa.org's own bot-challenge page references
# `request_quota_reached.html`, so per-IP quotas exist and the constraint is
# server load rather than throughput. One request per second, single worker,
# and every page cached so it is fetched exactly once.
# ---------------------------------------------------------------------------

SLEEP_OK = 1.0                 # floor between requests; the budget usually governs
SLEEP_FAIL = 5.0               # seconds after a failed request
MAX_RETRIES = 3
TIMEOUT = 30                   # seconds
WORKERS = 1                    # --workers raises this; hard cap of 3 in run.py

# --- Request budget -------------------------------------------------------
#
# On 2026-08-11 a run at 1 req/s completed ~603 requests in 43 minutes (~840
# requests/hour sustained) and the IP was then denied on every URL for at least
# 16 hours, robots.txt included. So the trigger is volume over a window, not
# per-request speed.
#
# MAX_REQUESTS_PER_HOUR is therefore the setting that matters, and it is enforced
# over a rolling hour that PERSISTS ACROSS RUNS (ncaa/budget.py). 300/hour is
# about 2.8x under the rate that got us blocked. That margin is a judgement call,
# not a known-safe figure -- the real threshold is unpublished.
#
# Implied pacing: 3600/300 = 12 s between requests, jittered. Six years of D1
# (~3,632 requests) therefore takes ~12 h of mostly waiting. That is the price of
# not getting blocked again; raise it only if you are willing to risk the block.
MAX_REQUESTS_PER_HOUR = 300

# Voluntary stop well before the ~603 requests that tripped it, so a single run
# cannot walk into the wall even if MAX_REQUESTS_PER_HOUR is raised carelessly.
SESSION_REQUEST_BUDGET = 400

# Multiplied into each interval. Perfectly regular timing is a bot signature.
JITTER = (0.85, 1.25)

REQUEST_LOG = "_request_log.json"   # inside CACHE_DIR

# ---------------------------------------------------------------------------
# NCAA site constants
# ---------------------------------------------------------------------------

BASE = "https://stats.ncaa.org"
SPORT_CODE = "MBA"
VERIFY_URL = f"{BASE}/_sec/verify?provider=interstitial"

# stat_seq 484.0 is Hits -- an arbitrary but always-populated category. We only
# use institution_trends to enumerate teams, never to read the stat itself.
DISCOVERY_STAT_SEQ = 484.0

# The ranking period is the "as of" week for season_to_date_stats -- it selects
# the final regular-season ranking. It is not derivable from the year; look a
# new one up at
#   /rankings/change_sport_year_div?sport_code=MBA&academic_year={y}.0&division={d}.0
# and add it here.
#
# Copied verbatim from ncaa_bbStats/team_stats.py:1047-1072 (upstream SHA
# 24b3050). D1 2021 (96) and 2025 (104) are confirmed to return live data.
# Keyed {year: {division: period}}.
RANKING_PERIODS = {
    2002: {1: 12.0, 2: 9.0, 3: 9.0},
    2003: {1: 12.0, 2: 8.0, 3: 8.0},
    2004: {1: 11.0, 2: 8.0, 3: 9.0},
    2005: {1: 13.0, 2: 11.0, 3: 12.0},
    2006: {1: 12.0, 2: 12.0, 3: 11.0},
    2007: {1: 13.0, 2: 12.0, 3: 11.0},
    2008: {1: 17.0, 2: 13.0, 3: 12.0},
    2009: {1: 16.0, 2: 13.0, 3: 12.0},
    2010: {1: 16.0, 2: 12.0, 3: 12.0},
    2011: {1: 17.0, 2: 13.0, 3: 12.0},
    2012: {1: 35.0, 2: 16.0, 3: 15.0},
    2013: {1: 40.0, 2: 14.0, 3: 13.0},
    2014: {1: 43.0, 2: 14.0, 3: 13.0},
    2015: {1: 95.0, 2: 24.0, 3: 30.0},
    2016: {1: 100.0, 2: 32.0, 3: 36.0},
    2017: {1: 98.0, 2: 30.0, 3: 44.0},
    2018: {1: 95.0, 2: 47.0, 3: 71.0},
    2019: {1: 93.0, 2: 57.0, 3: 67.0},
    2020: {1: 23.0, 2: 23.0, 3: 26.0},   # COVID-shortened season
    2021: {1: 96.0, 2: 73.0, 3: 98.0},
    2022: {1: 90.0, 2: 78.0, 3: 103.0},
    2023: {1: 94.0, 2: 76.0, 3: 104.0},
    2024: {1: 108.0, 2: 79.0, 3: 105.0},
    2025: {1: 104.0, 2: 79.0, 3: 101.0},
    2026: {1: 111.0, 2: 119.0, 3: 105.0},
}

# Sanity band on the number of D1 programs a discovery page should return.
# Live: 293 (2021) through 307 (2025).
TEAM_COUNT_BAND = (270, 330)

# Rows per team-season outside this band get flagged, not dropped.
ROWS_PER_TEAM_BAND = (15, 80)

# ---------------------------------------------------------------------------
# Output schema.
#
# FanGraphs' column order, with four deliberate changes:
#   * `age` -> `class`   NCAA publishes class year, not date of birth.
#   * `mlbamid` removed  Unfillable from NCAA, and make_public_data.py already
#                        drops it via FG_IDENT_DROP.
#   * `person_id` added  `playerid` is NCAA's key, and NCAA mints a *new* one
#                        every season -- consecutive roster years share exactly
#                        zero ids. So `playerid` cannot group a player across
#                        seasons, which the draft-eligibility logic depends on.
#                        `person_id` is our own minted cross-season key; see
#                        sources/identity.py for how links are earned and
#                        out/reports/person_links.csv for the evidence.
# `w l cg sho sv` are KEPT, after briefly being dropped.
#
# The argument for dropping them was consistency: they are absent from the
# 2025/2026 source generation and the per-game box scores carry no decision
# column, so 2026 cannot have them at any price. But PUBLIC_YEARS is 2021-2025 --
# 2026 is excluded anyway because its mirror files are a mid-season snapshot --
# and across 2021-2025 all five are fully populated. Dropping them therefore
# discarded real data to stay consistent with a season the dataset does not
# contain. If a live 2026 scrape ever lands, PITCH_DECISIONS_ABSENT_YEARS already
# makes them arrive as null rather than a fabricated zero.
#
# Counts: 40 batting and 39 pitching, against the FanGraphs files' 40 and 39.
# ---------------------------------------------------------------------------

BATTING_COLUMNS = [
    "name", "team", "class", "nameascii", "playerid", "person_id", "year",
    "g", "ab", "pa", "h", "1b", "2b", "3b", "hr", "r", "rbi", "bb", "so",
    "hbp", "sf", "sh", "gdp", "sb", "cs",
    "avg", "bb%", "k%", "bb/k", "obp", "slg", "ops", "iso", "spd", "babip",
    "wsb", "wrc", "wraa", "woba", "wrc+",
]

PITCHING_COLUMNS = [
    "name", "team", "class", "nameascii", "playerid", "person_id", "year",
    "w", "l", "era", "g", "gs", "cg", "sho", "sv", "ip", "tbf",
    "h", "r", "er", "hr", "bb", "hbp", "wp", "bk", "so",
    "k/9", "bb/9", "k/bb", "hr/9", "k%", "bb%", "k-bb%",
    "avg", "whip", "babip", "lob%", "fip", "e-f",
]

VALID_CLASSES = {"Fr", "So", "Jr", "Sr", "Gr"}

# Ordinal encoding of `class`, which is the public substitute for FanGraphs' `age`.
# NCAA has no date of birth, so there is nothing to compute an age from. This is
# not a downgrade as large as it looks: FanGraphs' own `age` is 45-64% null
# depending on the season, while `class` is populated for all but a handful of
# rows per year and is the criterion draft eligibility actually turns on.
#
# `Gr` is in VALID_CLASSES but NCAA has never emitted it in 2021-2026; it is kept
# so a future season that does emit it maps rather than silently becoming null.
CLASS_ORDINAL = {"Fr": 1, "So": 2, "Jr": 3, "Sr": 4, "Gr": 5}

# ---------------------------------------------------------------------------
# Per-year data absences.
#
# These exist because the public sources are not uniform across seasons, and a
# missing column must never arrive as a zero -- a zero is a claim we cannot
# support. `derive/rates.py` and `ncaa/schema.py` both read these, so there is
# one place to change when a source improves.
# ---------------------------------------------------------------------------

# 2021: the legacy mirror carries a `GDP` column that is 0% nonzero, and the 2021
#       NCAA grid never had it either.
# 2026: the "lean" mirror generation dropped GDP entirely. `OPP DP` is present in
#       both but is a different stat (63.9% exact vs FanGraphs `gdp`, r=0.92), so
#       it is not a substitute.
GDP_ABSENT_YEARS = {2021, 2026}

# W/L/SV/CG/SHO. The lean generation omits them and the per-game box scores carry
# no win/loss/save decision column, so for 2026 they are unrecoverable offline.
# They are dropped from the emitted schema entirely (see PITCHING_COLUMNS), but
# the constant stays so the normalised intermediates can mark them absent rather
# than zero.
PITCH_DECISIONS_ABSENT_YEARS = {2026}

PITCH_DECISION_COLUMNS = ("w", "l", "sv", "cg", "sho")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

import os

HERE = os.path.dirname(os.path.abspath(__file__))
CACHE_DIR = os.path.join(HERE, "cache")
OUT_DIR = os.path.join(HERE, "out")
REPORT_DIR = os.path.join(OUT_DIR, "reports")
BULK_CACHE_DIR = os.path.join(HERE, "bulk_cache")
BULK_MANIFEST = os.path.join(BULK_CACHE_DIR, "MANIFEST.json")

# ---------------------------------------------------------------------------
# Bulk mirrors.
#
# stats.ncaa.org blocks this IP at the Akamai layer -- both plain `requests` and
# curl_cffi Chrome impersonation get a flat 403, including on robots.txt, so it
# is the address that is blocked and not the client fingerprint. These two
# repositories publish the same pages already scraped and parsed, which is what
# makes a complete 2021-2026 build possible without touching the NCAA at all.
#
# Fetched by commit SHA, never by `main`: a branch name is mutable and a paper
# needs to cite something that is not. bulk.py resolves the SHA once and records
# it in MANIFEST.json.
#
# `ncaa_baseball_data` is ~807 MB and `NCAA_Baseball_repository` ~8 GB, but we
# take individual files over raw.githubusercontent -- roughly 100 MB in total,
# and neither repository is ever cloned.
# ---------------------------------------------------------------------------

BULK_REPOS = {
    # 2022-2026 season stats, and the rosters that carry identity for every year.
    "modern": {"owner": "armstjc", "repo": "ncaa_baseball_data", "branch": "main"},
    # 2021 -- the only source for it -- plus the seven 2022 teams the modern
    # repository is missing. Last pushed 2024-09-16, so it will never gain 2025+.
    "legacy": {"owner": "armstjc", "repo": "NCAA_Baseball_repository", "branch": "main"},
}

GITHUB_RAW = "https://raw.githubusercontent.com/{owner}/{repo}/{sha}/{path}"
GITHUB_COMMIT_API = "https://api.github.com/repos/{owner}/{repo}/commits/{branch}"

# Which upstream file backs which (year, category). "modern"/"legacy" selects the
# repository above. Season stats for 2021 come only from legacy; rosters only from
# modern (legacy's rosters are used for 2021 names, see sources/legacy.py).
BULK_FILES = {
    ("modern", "season"): "combined_files/season_stats/player/{year}_season_{category}_stats.csv",
    ("modern", "roster"): "combined_files/rosters/{year}_rosters.csv",
    ("legacy", "season"): "season_stats/player/{category}_season_stats/csv/{year}_{category}.csv",
    ("legacy", "roster"): "TeamRosters/{year}_roster.csv",
}

# Per-year adapter selection. See sources/registry.py; --source-for overrides it.
#   rich   2022-2024 modern generation, 43/51 columns, near-exact vs FanGraphs
#   lean   2025-2026 modern generation, 28/30 columns, season *segments*
#   legacy 2021 (and the 2022 patch) from the legacy repository
#   cache  the local pre-block scrape in out/*.PARTIAL.csv -- complete for 2025
BULK_YEAR_SOURCES = {
    # The pre-block cache holds 175 of 293 team-seasons and is authoritative for
    # them; the legacy mirror's 2021 batting file is missing ~347 players who had
    # real plate appearances, so it fills only the teams the cache lacks.
    2021: ("cache", "legacy_fill"),
    2022: ("rich", "legacy_patch"),
    2023: ("rich",),
    2024: ("rich",),
    2025: ("cache",),   # complete on disk (307/307); the lean mirror runs short
    2026: ("lean",),
}

# The seven 2022 teams absent from the modern repository but present in legacy.
# Stonehill (STMN) is absent from both -- a 2022 D2->D1 transition program that
# neither mirror captured -- so 2022 tops out at 300 of FanGraphs' 301 teams.
BULK_2022_PATCH_TEAMS = ("BELL", "CBU", "MRMK", "TAR", "UCSD", "UNA", "UTU")
BULK_UNRECOVERABLE_TEAMS = {2022: ("STMN",)}

# Years knowingly built from a mid-season mirror snapshot.
#
# Empty on purpose. The modern mirror stopped being updated on 2026-04-12 -- its
# final commit is titled "That's all folks" -- and its 2026 season files were last
# written on 2026-04-09, roughly 60% of the way through the season. Building 2026
# from them yields totals averaging 47.8 at-bats and 56.7 batters-faced short of
# the full season, which is not a small bias but a different quantity.
#
# There is no fork and no other public mirror covering the finished 2026 season,
# so the only routes to a real 2026 are a live scrape from an unblocked IP
# (~616 requests) or the private FanGraphs export. Adding 2026 here would ship the
# partial season; do that only with the limitation written into the paper.
BULK_PARTIAL_SEASON_OK = frozenset()

# Team-season categories that no public source has, acknowledged individually.
#
# Listing one costs a stated reason, which is the point: the alternative is either
# a permanently PARTIAL filename or a blanket "ignore empty teams" flag that would
# also hide a real regression. Keyed (year, fg_acronym, category) -> reason.
KNOWN_EMPTY_TEAM_SEASONS = {
    (2021, "TXSO", "batting"): (
        "Texas Southern's 2021 batting grid is in neither public source: the "
        "legacy mirror's 2021 batting file omits TXSO entirely (along with LAS and "
        "NCCU, both of which the pre-block cache does cover), and TXSO is one of "
        "the 118 team-seasons the cache never reached before the IP was blocked. "
        "Its pitching is present, from the legacy mirror. One team-season of 1,465."),
}

# The private FanGraphs originals, used only by validate/against_fangraphs.py.
# Absent for anyone without FanGraphs access; the validator skips cleanly.
_CSV_ROOT = os.path.dirname(HERE)
FG_BATTING = os.path.join(_CSV_ROOT, "ncaa_battingNoMinCSV", "batting_combined_all.csv")
FG_PITCHING = os.path.join(_CSV_ROOT, "ncaa_pitchingNoMinCSV", "pitching_combined_all.csv")
UNIQUE_TEAMS = os.path.join(_CSV_ROOT, "standardized", "unique_teams.csv")


def ranking_period(year: int, division: int) -> int:
    """Look up the ranking period, failing with instructions rather than a KeyError."""
    try:
        return RANKING_PERIODS[year][division]
    except KeyError:
        raise SystemExit(
            f"No ranking period for year={year} division={division}.\n"
            f"Add it to RANKING_PERIODS in {__file__}.\n"
            f"Find the value at {BASE}/rankings/change_sport_year_div"
            f"?sport_code={SPORT_CODE}&academic_year={float(year)}&division={float(division)}"
        )
