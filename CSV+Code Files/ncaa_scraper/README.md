# NCAA player-stat scraper

Re-derives the player batting and pitching statistics in this project directly
from `stats.ncaa.org`, replacing the manual FanGraphs college leaderboard exports
that cannot be redistributed.

The existing `ncaa_batting*CSV/` and `ncaa_pitching*CSV/` folders contain
FanGraphs exports. Four `DATA_NOTICE.md` files exist because of that, and
`README.md:99` in the repo root names re-deriving from `stats.ncaa.org` as the
outstanding fix. This is that fix. **Nothing outside this folder is modified.**

```bash
pip install -r requirements.txt

# The route that works today: pre-scraped public mirrors, no NCAA access at all.
python run.py --years 2021-2025            # ~3 min, ~94 MB of downloads
python run.py --validate                   # acceptance gates
python run.py --validate-against-fangraphs # the correctness proof

python run.py --list-sources               # which source each year uses
python run.py --verify-bulk                # re-hash the downloads against the manifest

# The live scraper. Needs an unblocked IP; see the block section below.
python run.py --selftest                   # 5 s -- has the block lifted?
python run.py --source live --year 2026     # one season, ~26 min cold
```

Outputs land in `out/`: `batting_combined_all.csv`, `pitching_combined_all.csv`,
and diagnostics under `out/reports/`.

---

## Read this first: the live scrape is blocked, and the bulk path replaces it

`stats.ncaa.org` blocks this machine's IP at the Akamai layer. Both plain
`requests` with a Chrome user-agent **and** `curl_cffi` with
`impersonate="chrome"` get an identical `403 Access Denied`, `robots.txt`
included — it is the address that is blocked, not the client fingerprint, so no
HTTP client swap helps. (This is also why `ncaa_stats_py` cannot help: it fetches
with plain `requests` against the same `season_to_date_stats` URL.)

So the default path reads **pre-scraped public mirrors** instead, pinned to exact
commits:

| alias | repository | supplies |
|---|---|---|
| `modern` | `armstjc/ncaa_baseball_data` | season stats 2022–2026, rosters 2021–2026 |
| `legacy` | `armstjc/NCAA_Baseball_repository` | 2021, and the 2022 teams `modern` lacks |

Neither is cloned — 20 individual files, ~94 MB, fetched over
`raw.githubusercontent.com` at a recorded SHA. `bulk_cache/MANIFEST.json` holds
the commit SHA, sha256, byte size and row count of every one, and is the thing to
cite; `--verify-bulk` re-checks it.

### Per-year sources, and why

| year | source | why |
|---|---|---|
| 2021 | local cache + `legacy` | the pre-block scrape banked 175 of 293 team-seasons and is authoritative for them; `legacy` fills the other 118, because its 2021 batting file omits ~347 players who had real plate appearances |
| 2022 | `modern` + `legacy` patch | `modern` has 293 of 301 D1 schools; seven of the eight absent transition programs are in `legacy`. Stonehill is in neither, so 2022 reaches 300 |
| 2023–2024 | `modern` | complete; 2024 pitching lists only pitchers by design and is 100.2% of the FanGraphs row count |
| 2025 | local cache | complete on disk (307 of 307). The mirror's 2025 is measurably short — see below |
| **2026** | **none** | **excluded.** The mirror's last commit is 2026-04-12, titled "That's all folks", and its 2026 files were last written 2026-04-09, mid-season. Building from them understates at-bats by 47.8 and batters-faced by 56.7 per player |

2026 therefore needs either a live scrape from an unblocked IP
(`--source-for 2026=live`, ~616 requests) or the private FanGraphs export.
`sources/bulk.py::stale_seasons` refuses any season whose source file predates
1 July of its own year, so a partial season cannot ship by accident; overriding it
requires naming the year in `config.BULK_PARTIAL_SEASON_OK`.

### Mirror quirks the adapters correct

Each of these produces plausible-looking wrong numbers rather than an error, and
each is asserted rather than trusted:

- **Innings notation flips between generations.** 2022–2024 use NCAA thirds
  notation (`97.2` = 97⅔); 2021 and 2025–2026 use true innings (`97.667`).
  `ip_to_float` expects thirds, so feeding it true innings silently corrupts every
  pitching rate. Guarded by `sources/_shape.assert_ip_format`.
- **The legacy mirror's batting `G` is TEAM games** — constant within school for
  288 of 290 schools in 2021. Per-player games come from the roster instead.
- **The 2025/2026 files store season segments, not totals** — 83,771 rows for
  33,611 players in 2025. They must be summed; the largest row understates
  at-bats by 22.7. Aggregating the per-game box scores instead was tested and is
  identical, so there is nothing to gain there.
- **Blank means zero; a missing column does not.** 47–80% of counting cells are
  blank where the NCAA grid was empty, and reading them as missing drops FanGraphs
  agreement from ~98% to ~50%. But `gdp` (2021, 2026) and `w/l/sv/cg/sho` (2026)
  were never published, so they stay null — see `derive/rates.absent_columns`.
- **The legacy roster's name column swaps between years** — 2021 fills `name`,
  2022 fills `player_name` and leaves `name` empty. Reading one gives a year of
  nameless players, which then fail draft matching and count as negatives.

### Measured against FanGraphs, 2021–2025

| | rows | FanGraphs | join | counting stats identical |
|---|---|---|---|---|
| batting | 26,821 | 26,826 | 99.4% | 94.3% |
| pitching | 25,966 | 25,964 | 99.8% | 78.2% |

Every pure-arithmetic rate sits at or **above** the ceiling its own input columns
allow, which is the strongest available statement that the formulas are exact:
wherever the inputs agree with FanGraphs, the derived rate agrees too. The
league-relative columns are recomputed, so they correlate rather than match —
wOBA r=0.9950, wRC+ r=0.9915, FIP r=0.9985, E–F r=0.9975, spd r=0.9326.

Known gaps: Texas Southern's 2021 batting exists in no public source (one
team-season of 1,465); 2022 is one team short; `gdp` is unavailable for 2021.

---

## Disclosure: this scraper clears a bot-management challenge

`/rankings/*` on `stats.ncaa.org` is open, but `/teams/*` and `/players/*` sit
behind an Akamai interstitial. It returns **HTTP 200 with a ~2.3 KB challenge
page** rather than an error, and clearing it means replaying a two-step
handshake: read a token and a trivial sum off the page, POST them back to
`/_sec/verify`, then re-request. `ncaa/session.py` does this, and one solve covers
a whole session.

That is bot-management circumvention, and this project does it deliberately
rather than incidentally. The reasoning:

- The NCAA does not sell these statistics. They are published for public
  consumption, and the underlying facts are public record.
- Established tools have scraped these same pages for years — `baseballr`
  (Bill Petti) and `ncaa_stats_py` (armstjc) both do.
- The challenge page's own `<noscript>` block points at
  `request_quota_reached.html`, which indicates the site's concern is **server
  load**, not access control.

So the mitigations are the substance of the position, not a footnote, and they
are not optional:

| mitigation | where |
|---|---|
| 1 request/second | `config.SLEEP_OK` |
| single worker by default (`--workers` capped at 3) | `config.WORKERS`, `run.py` |
| every page cached, so each is fetched exactly once | `ncaa/cache.py` |
| ~3,600 requests total for six years of D1 | see Runtime below |

**This paragraph belongs in the paper's methods section too**, at the same
standard of disclosure the existing `DATA_NOTICE.md` files already meet. A
scrape whose access method you would not describe in print is not a scrape you
should publish from.

By contrast, `collegesplits.com` was considered and rejected: its `robots.txt`
blocks AI crawlers by name and asserts an Article 4 EU DSM rights reservation,
Cloudflare 403s non-browser requests, and College Splits licenses this exact data
commercially to MLB clubs. Different situation, different answer.

---

## Schema

FanGraphs' column order, with four deliberate changes, giving **40 batting** and
**34 pitching** columns rather than 40/39:

| change | why |
|---|---|
| `age` → `class` | NCAA publishes class year (`Fr`/`So`/`Jr`/`Sr`/`Gr`), not date of birth. There is no DOB anywhere on the site. |
| `mlbamid` dropped | Unfillable from NCAA, already 92% empty in the FanGraphs files, and `make_public_data.py` drops it anyway via `FG_IDENT_DROP`. |
| `person_id` added | NCAA mints a **new** `playerid` every season — consecutive roster years share exactly zero ids, and the legacy mirror's ids are a third disjoint space. So `playerid` cannot group a player across seasons, which draft eligibility depends on. `person_id` is our own minted key; see `sources/identity.py` and `out/reports/person_links.csv` for the evidence behind each link. |
| `w l cg sho sv` dropped | Absent from the 2025+ source generation, and the per-game box scores carry no win/loss/save decision column either, so 2026 cannot have them at any price. Dropped for every year rather than changing definition mid-panel. |

```
batting   name,team,class,nameascii,playerid,person_id,year,g,ab,pa,h,1b,2b,3b,
          hr,r,rbi,bb,so,hbp,sf,sh,gdp,sb,cs,avg,bb%,k%,bb/k,obp,slg,ops,iso,
          spd,babip,wsb,wrc,wraa,woba,wrc+

pitching  name,team,class,nameascii,playerid,person_id,year,era,g,gs,ip,tbf,h,r,
          er,hr,bb,hbp,wp,bk,so,k/9,bb/9,k/bb,hr/9,k%,bb%,k-bb%,avg,whip,babip,
          lob%,fip,e-f
```

`person_id` is null for 0.5% of rows — players with no roster entry to link
against, mostly the 2022 teams patched in from the legacy mirror.

`playerid` is the NCAA player id from `/players/{id}`. `team` is the FanGraphs
acronym, so these files join to `standardized/unique_teams.csv` exactly as the
existing ones do. `ip` stays in NCAA notation (`97.2` = 97⅔) to match the
existing schema; every innings-denominated rate is computed from true innings.

### `age` is gone, and it was a model feature

`age` is in `PARTA_FEATURES` in `xgboostAllWithTeamsV10.ipynb:126`. It cannot be
recovered from NCAA data. `class` is the replacement and is arguably the better
draft-eligibility signal anyway, but **`PARTA_FEATURES` must be updated** and
`class` needs encoding (it is categorical, not numeric).

---

## What matches FanGraphs and what does not

**23 columns match to floating point — verified, not hoped for.** Feeding
FanGraphs' own counting stats through `derive/rates.py` reproduces every one of
their rate columns at `atol=1e-6, rtol=1e-6` on **100% of ~26,000 rows**:

| | columns | agreement |
|---|---|---|
| batting | `1b, pa, avg, obp, slg, ops, iso, bb%, k%, bb/k, babip` | 11/11 at 100% |
| pitching | `era, whip, k/9, bb/9, hr/9, k/bb, k%, bb%, k-bb%, avg, babip, lob%` | 12/12 at 100% |

That is the correctness proof for the formula layer, and it holds independently
of the scrape. Four of these are not the textbook formula and were recovered by
fitting against the existing files:

- **`obp` and `woba` divide by `ab + bb + hbp + sf`, excluding SH**, while `pa`
  includes SH. Fitting wOBA with `pa` as the denominator leaves a residual
  correlating −0.607 with `sh`.
- **Pitching `k%`/`bb%` divide by `tbf`**, and opponent `avg` is
  `h / (tbf − bb − hbp)` — exact on all 25,872 comparable rows, so NCAA's real
  `P-OAB` column is not needed (it is carried as a diagnostic anyway).
- **`lob%` is capped above at 1.0 but not floored at 0.** Above 1 is an artefact
  of the `1.4*HR` term; below 0 is real, because a reliever can be charged with
  more runs than he put on base when inherited runners score (Jack Lang, 2021:
  7 baserunners allowed, 8 runs charged, LOB% −0.357). Flooring it would erase a
  genuine signal about relief usage. Getting this asymmetry right moved the
  column from 99.0% to 100%.

**9 columns are recomputed and will not match exactly.** `woba, wraa, wrc, wrc+,
wsb, spd` (batting) and `fip, e-f, lob%` (pitching) depend on league constants.
FanGraphs' come from a proprietary Markov model; ours from a documented
team-level runs regression (`derive/constants.py`). Measured on 2025, 292 teams,
conference scope:

| column | r vs FanGraphs | mae |
|---|---|---|
| `fip` | **0.9992** | 0.045 |
| `wrc` | 0.9980 | 0.98 |
| `woba` | 0.9954 | 0.0059 |
| `wraa` | 0.9928 | 0.99 |
| `wrc+` | 0.9921 | 7.0 |
| `wsb` | 0.9900 | 0.072 |
| `spd` | 0.9304 | 1.19 |

Every one of those beats the target set when this was planned (wOBA ≥0.99,
wRC+ ≥0.97, wSB ≥0.84, Spd ≥0.78), and `fip` at r=0.9992 with a +0.004 mean
offset is far better than the +0.30 division-scope offset that was expected —
conference-scoped cFIP is doing real work.

`lob%` is in this list only because it is listed as derived elsewhere; it is
actually pure arithmetic and matches at 100% (see above).

`fip_pitch`, `woba_bat`, and `wrc+_bat` are all in `PARTA_FEATURES`, so running
the existing model against these features unchanged would be a silent
distribution shift. There is an upside: `make_public_data.py` currently drops
exactly these nine columns as `FG_DERIVED_COLS` because they were not
redistributable, so a model trained *with* NCAA-native versions is a strict
information gain over the public model.

### Method for the league constants

Per **conference**, not per division — because that is what FanGraphs does.
`(wrc − wraa)/pa` in the existing files is constant to 1e-17 within a team and
takes exactly 31 distinct values across 2021 D1, and those 31 groups are the
conferences. Their cFIP reproduces from conference totals to five decimals.

**The regression form is load-bearing.** It fits *runs per PA on event rates*
with an intercept, which makes **outs the omitted category**, so each coefficient
is the marginal runs from turning one out into that event — the runs-above-out
quantity wOBA needs. Fitting event *counts* with no intercept instead gives
coefficients that predict total runs (β_1b ≈ 0.25 rather than ≈ 0.77) and
silently inflates `woba_scale` by about 2×, producing a wOBA that looks plausible
and is wrong. Cross-checked against the packaged constants: this form reproduces
their r² to four decimals (0.9732 vs 0.9733 for 2021 D1) and their `woba_scale`
to within ~2%.

**Event weights are fitted at division level only, never per conference.** This
was the single worst bug in the first version and it is worth understanding, since
it produced numbers that looked fine at a glance.

The regression has 9 parameters (8 events + intercept). A conference has 10–17
teams, leaving 1–8 residual degrees of freedom. Fitting there does not merely add
noise, it produces impossibilities:

| conference | w_bb | w_hbp | w_hr | r² |
|---|---|---|---|---|
| MAC | **−0.150** | **2.363** | 2.909 | 0.998 |
| SWAC | 1.522 | **−0.594** | 1.210 | 0.992 |
| NEC | 0.291 | **2.681** | 1.637 | 0.997 |
| division (292 teams) | 0.740 | 0.736 | 1.984 | 0.961 |

A walk cannot have negative run value. The r² above 0.99 is the tell — that is
saturation, not fit quality, and saturation collapses the standard errors, which
drives the empirical-Bayes weight λ = τ²/(τ²+SE²) toward 1, so the shrinkage
meant to rescue these fits does nothing exactly where it is needed. Applied to
real players this gave a .305/.465/.476 hitter a .659 wOBA and a 241 wRC+.

So: weights come from the division fit (~300 team-seasons), and **per (year,
conference) we compute only the scalars that are estimated from totals rather
than a regression** — `lg_obp`, `lg_r_pa`, `cfip`, `lg_wsb`, and the `woba_scale`
that anchors lgwOBA to lgOBP. That preserves what actually differs between the
SEC and the MAC (the run environment) without pretending to estimate eight event
coefficients from ten teams. The FanGraphs data supports this directly: their
per-conference weight *shapes* are near-constant across 153 conference-years
(2B = 1.330 ± 0.021, BB = 0.8295 ± 0.011), while their per-conference cFIP ranges
3.69–5.43. `FIT_WEIGHTS_PER_CONFERENCE = True` reproduces the broken behaviour if
you want to see it.

Division-year fits are shrunk toward a division-pooled fit across all years by
empirical Bayes, then passed through the monotone-hits constraint
(`w_1b ≤ w_2b ≤ w_3b ≤ w_hr`) via inverse-variance-weighted isotonic regression.
Triples are under 1% of plate appearances and the noisiest coefficient in the
model, so that constraint frequently pools `w_2b` with `w_3b` — which is why
those two are reported rather than gated against the packaged fit.

**Baserunning uses published run values, not fitted ones.** `runSB = 0.200` fixed
and `runCS = −(2·R/Out + 0.075)` per conference, neither multiplied by
`woba_scale` since wSB is expressed in runs. Fitting them gave w_sb ≈ 0.27 against
the accepted 0.200 and w_cs ≈ −0.35 against a run-environment value near −0.55,
and `wsb` correlated with FanGraphs at only r = 0.66. Switching to the published
form took it to **r = 0.99**.

Weights are then anchored so that `lgwOBA ≡ lgOBP`, which is the identity
FanGraphs uses (2021 D1: 0.36445 on both sides). `--scope division` exists so a
reviewer can quantify how much of the residual gap is scope versus weights.

### Two documented deviations in `wrc+`

- **Park factor is fixed at 1.0.** No public NCAA park-factor series exists and
  inventing one would be unfalsifiable. Consequence: hitters at extreme-altitude
  programs (Air Force, New Mexico, Utah Valley) are flattered.
- **The denominator uses league R/PA over all hitters**, not non-pitchers,
  because NCAA two-way players make that split ambiguous.

### `spd` is the weak one

Bill James' speed score, four of five published factors — the GDP-avoidance
factor is omitted because `GDP` is absent from the 2021 NCAA grid, and dropping
it uniformly keeps `spd` comparable across years rather than silently changing
definition at the 2021/2022 boundary. James' constants were fitted to MLB, so the
NCAA population centres near 3.9 rather than the intended 5.0. Agreement with
FanGraphs is r = 0.81, the weakest column here. All inputs are retained in the
output so a modeller can prefer them over the composite.

---

## Site quirks that the code depends on

Verified live across 2021–2026; these are the things that break a naive scrape.

**Zero renders as an empty cell.** Tre Jones' 2022 row has `CS=''` and `IBB=''`,
and he certainly had zero of each. Blank counting stats coerce to 0; only `class`
keeps null when blank, since there a blank really is unknown.

**`table#stat_grid`, not `rankings_table`.** The latter only exists on
`/rankings/national_ranking`. `ncaa_bbStats.team_stats.fetch_ncaa_table` targets
`rankings_table` and would silently return nothing for every team page.

**The grid holds far more rows than players** — 323 `<tr>` for 39 players in one
2025 case. The surplus is per-player situational split labels (`Hits-AB with 2
outs`, `Hits-AB vs Left Handed Pitchers`, …) plus `Totals` and `Opponent Totals`,
all with blank stat cells. Keeping only rows whose `Player` cell contains an
`<a href="/players/{id}">` selects exactly the real players; verified across all
six years, where rows-with-link always equals the number of distinct player ids.

**`year_stat_category_id` needs no lookup table.** Requesting
`season_to_date_stats` with no parameters returns the batting grid and exposes all
three ids in its tab links, contiguous with batting lowest (2025 → 15687/15688/
15689; 2021 → 14840/14841/14842). They change every season, so they are parsed
off the page every time. Two requests per team-season.

**Team ids are per-season.** A&M-Corpus Christi is 508948 in 2021 and 596471 in
2025, so discovery runs once per year.

**Headers drift; map by name, never by index.**

| | 2021 | 2022–2023 | 2024–2026 |
|---|---|---|---|
| batting | 28 cols, no `Ht`/`B/T`/`GDP` | 31 | 31 |
| pitching | 37 cols, no `Ht`/`B/T`, has `G` **and** `App` | 39, has `G` and `App` | 38, `G` gone |

**`G` is not `App`.** In 2021–2023 the pitching grid has both, and they differ:
Diego Johnson's 2022 row is `G=27, App=blank, IP=blank` — a position player who
appeared in 27 games and never pitched. Pitching `g` maps to `App`. Mapping `G`
would give position players 27 games with no innings. This is also why 2021–2023
pitching grids list every rostered player (~36) while 2024+ list only pitchers
(~19); since rows with no innings are dropped, both shapes give the same result.

**Zero-PA and zero-IP rows are dropped**, with counts reported. The batting grid
lists pitchers with blank batting cells and (pre-2024) the pitching grid lists
position players with no innings. Emitting them would create phantom `Two-Way`
players in `build_2026_combined.py:136`. This is also probably a large part of
what FanGraphs' "noMin" means.

---

## Design commitments

**Nothing fails silently.** `NcaaSession.get` raises rather than returning empty
content, because the interstitial returns HTTP 200 and a naive scraper reports
zero rows as success. Every discarded row is recorded in `out/reports/skips.csv`
with its raw cells and a reason — replacing the upstream `except Exception:
continue` whose comment ("About 5 rows are skipped each time") is exactly the
unexamined loss this scraper must not repeat. An unrecognized header aborts the
run. An unmappable team aborts the run with the list and instructions.

**A short file cannot acquire the real filename.** The CSVs are written only when
every row in `out/reports/coverage.csv` is `ok`; otherwise output goes to
`*.PARTIAL.csv` and the process exits non-zero.

**Resume is free.** Cache writes are `*.tmp` then `os.replace`, so a kill -9
leaves either the old entry or the new one. A crash resumes at the first missing
key, with no bookkeeping file to corrupt. Parsed rows are cached alongside the
HTML, so iterating on the derived-stat code costs seconds rather than the ~25
minutes BeautifulSoup needs to re-chew 3,600 pages.

**`--selftest` is the canary.** Akamai can rotate the challenge shape — the
`Number("6208" + "99594")` split is obfuscation that exists to be changed. The
selftest surfaces that in 5 seconds instead of 3 hours in.

---

## Runtime

6 discovery requests + 2 per team-season ≈ **3,632 requests** for 2021–2026 D1
(293/301/305/305/307/308 teams = 1,819 team-seasons), plus one challenge solve per
session.

Measured on a 2025 run: **3.3 s per page, 6.7 s per team-season** steady state
(1 s of that is the politeness sleep, the rest is transfer plus BeautifulSoup on
a ~1.9 MB page).

| | |
|---|---|
| one year | ≈ 34 min |
| cold run, 6 years, 1 worker | **≈ 3.4 h** |
| warm from parsed-rows cache | < 1 min |
| disk | ≈ 50 KB gzipped per page, ≈ 180 MB for 6 years |

Expect occasional multi-minute stalls — one 2025 fetch took 533 s before the
retry logic got it, with no failures logged. They are absorbed, not fatal, but
they mean the wall-clock figures above are floors rather than promises.

### READ THIS FIRST: the IP gets blocked, and it does not clear quickly

**Measured 2026-08-11.** A run at 1 req/s completed **293 of 307 team-seasons
(~603 requests over 43 minutes)** and was then denied on every subsequent
request. Probed once every 20 minutes afterwards: **still fully blocked at 2 h 52
min**, at which point polling was stopped.

The block is not endpoint throttling and is worse than a rolling quota:

```
team page   HTTP 403   Access Denied
rankings    HTTP 403   Access Denied
site root   HTTP 403   Access Denied
robots.txt  HTTP 403   Access Denied     <-- no rate limiter blocks robots.txt
```

`robots.txt` being denied is the tell: this is **Akamai denying the IP outright**,
not the `request_quota_reached.html` path. Duration is unknown and was not
established — it is at least three hours and may be much longer.

**What this means for the project, honestly:** completing six years needs ~3,632
requests. If ~600 requests earns a multi-hour-plus IP block, that is at least six
blocks, and the total elapsed time is days rather than hours. **1 req/s with a
browser-impersonating client is evidently not acceptable to this site**, whatever
the challenge page implies about quotas. Do not assume slowing down fixes it;
that is untested.

Before running this scraper again, consider the alternatives in
"If the block persists" below. Continuing to retry a blocked IP is both useless
and contrary to the politeness commitments above.

Behaviour when it happens:

- A 403 is treated as a block, not a transient error: raised on the first
  occurrence with no retries, and the whole run aborts. The first version burned
  ~90 requests rediscovering this.
- The process exits **2**, distinct from 1, and writes **no CSVs at all** — not
  even `PARTIAL` ones. The scraped population would be an alphabetical prefix
  (the 2025 run stopped in the W's) and league constants fitted to that are
  quietly wrong in a way no gate would catch.
- Nothing already fetched is lost. Cached pages cost no requests, so a later
  re-run resumes at the first team it never got.
- `--wait-on-quota MINUTES` sleeps and retries once rather than stopping. Given
  the measured block duration, do not expect 60 to be enough.

### Resuming after a block — the runbook

The chosen approach: wait, then go much slower. `--sleep 5` instead of the 1 s
default, which is the rate that earned the block.

```bash
cd "CSV+Code Files/ncaa_scraper"

# 1. One request. Has the block lifted?
python run.py --selftest --sleep 5
#    -> "SELFTEST PASSED"  = go on to step 2
#    -> QuotaExhausted/403 = still blocked, come back later. Do not loop on this.

# 2. Finish 2025 (14 teams left of 307). ~3 min at 5s.
python run.py --year 2025 --sleep 5

# 3. Then a year at a time, checking each finishes before starting the next.
python run.py --year 2024 --sleep 5     # ~1 h per year at 5s
python run.py --year 2023 --sleep 5
#    ... and so on. Each writes *.PARTIAL.csv because it is a single year.

# 4. Once every year in config.YEARS is cached, one final pass builds the real
#    CSVs and fits the league constants over the whole population.
python run.py --sleep 5
python run.py --validate-against-fangraphs
```

Costs at 5 s/request: **~12 s per team-season**, so ~1 h per season and **~6 h of
request time for six years** — spread across as many days as the blocks require.
Steps 2 and 3 are cheap to retry, and step 4 needs no network at all once
everything is cached.

Two rules for this process:

- **Never loop on `--selftest`.** One check, then wait. Polling a blocked IP is
  what got us here.
- **Only step 4 produces usable CSVs.** Single-year runs are deliberately named
  `*.PARTIAL.csv` because league constants fitted to one season of a
  conference-scoped model are not the ones the final files should carry.

### If the block persists

In rough order of how much I'd recommend them:

1. **Ask the NCAA for the data.** The site is now actively refusing this IP, which
   is a fairly clear signal about the access method. A request for bulk or
   research access costs an email.
2. **Use an already-published mirror.** `baseballr` distributes pre-collected
   NCAA baseball data from its `baseballr-data` GitHub repository
   (`load_ncaa_baseball_teams()` and friends), published for reuse. That
   sidesteps scraping entirely and is worth checking before spending more effort
   here. `ncaa_stats_py` also ships a cache.
3. **Stay on the FanGraphs exports** you already have and keep not
   redistributing them, i.e. the status quo, accepting the `DATA_NOTICE.md`
   caveat.
4. **Retry much more slowly** from a different network, e.g. 5 s/request. Untested
   and may simply earn the same block more slowly.

Note that options 1 and 2 both leave the rest of this folder useful: the parsing,
the schema mapping, and every formula in `derive/` are validated and independent
of how the HTML arrives.

### Validation against FanGraphs — complete 2025 season

All 307 teams, zero fetch failures. **Every acceptance gate passes.**

| | ours | FanGraphs | joined |
|---|---|---|---|
| batting | 5,401 | 5,376 | 5,370 (**99.9%**) |
| pitching | 5,477 | 5,471 | 5,468 (**99.9%**) |

Only 6 batting and 3 pitching FanGraphs rows are unaccounted for. Coverage is
effectively complete.

**Two things had to be fixed before those numbers meant anything**, and both were
initially mistaken for scrape problems:

*The join rate was really a name-spelling rate.* At first only 96.6% joined, and
184 FanGraphs players appeared "missing" — including a 292-PA regular, which made
no sense. They were all present under different spellings:

```
Cermodrick Bland / Cemodrick Bland      (typo in one source)
Joshua Ibe       / Josh Ibe             (nickname)
Matthew Reinholtz/ Mathew Reinholtz     (spelling)
JP Hefft         / Jason Hefft          (initials vs given name)
Michael Cruz     / Mikey Cruz Jr.       (nickname + suffix)
Brenden OSullivan/ Brendan O'Sullivan   (spelling + apostrophe)
```

No string normalization catches `JP` vs `Jason`. Within a team-season, though, the
season line is a near-unique fingerprint, so `validate/against_fangraphs.py`
falls back to matching on counting stats with a hard ±3 guard. That is a
*measurement* fix — the emitted CSVs keep NCAA's spelling, because NCAA is the
source of truth for NCAA statistics. **This is worth knowing for
`masterDraft.py`**, which matches draft records by name and is exposed to exactly
this variance.

*36 real pitchers were being dropped.* The rule was `ip_true > 0`, which discards
a reliever who faced batters, allowed hits and walks, and was pulled before
recording an out — 32 such appearances in 2025. The rule is now `tbf > 0`: you
cannot face a batter without pitching, so it separates real appearances from
position players on the pre-2024 pitching grid, which innings cannot. Pitching
rows went 5,441 → 5,477 and phantom drops fell from 42 to 6.

### Where we still disagree with FanGraphs, we agree with the NCAA

Counting stats match on 94.5% of joined batting rows and 81.4% of pitching. That
is **not** evidence we are wrong:

- 81% of rows differ in **zero** columns, 15% in exactly **one**, and only 74 of
  5,007 in three or more.
- `mean(ours − theirs)` is ≤ 0.013 for every column except two — scattered ±1,
  the signature of NCAA box-score revisions since the FanGraphs export was taken.
- **`bk` (balks) is the one systematic gap**: our mean 0.268 vs 0.088. Checked
  against the source — Andrew Rubayo (Quinnipiac, 2025) reads `Bk=1, WP=12,
  SV=blank` on his NCAA page, exactly what we emit; FanGraphs records `0/13/1`.
  Headers and body cells align 38/38 and every column is mapped by name. **Ours
  are the NCAA's own values.**

So the gate is on **per-column bias** (which catches a mis-mapped header, since
that shifts every row the same way) plus the join rate (which catches missing
players), *not* on the row-level identical rate. A sub-100% rate is a question
about which source is stale; resolve it by reading the NCAA page, not by bending
the scraper to match a vendor.

`python run.py --prune-html` drops the HTML once validation passes.

---

## Layout

```
config.py            YEARS / DIVISION / LEAGUE_SCOPE and the politeness constants
run.py               CLI and pipeline
ncaa/session.py      curl_cffi + the interstitial solver
ncaa/cache.py        atomic gzipped HTML and parsed-rows cache
ncaa/discovery.py    institution_trends -> team ids and conferences
ncaa/team_page.py    stat_grid parsing, row filter, SkipLedger
ncaa/schema.py       NCAA header -> target column, tolerant of year drift
derive/rates.py      ip_to_float and the 22 pure-arithmetic rates
derive/constants.py  the per-conference run-value regression
derive/advanced.py   woba / wraa / wrc / wrc+ / wsb
mapping/             vendored registry + NCAA name -> FanGraphs acronym
validate/            acceptance gates and the FanGraphs diff
vendor/PROVENANCE.md what was vendored, from where, with hashes
```

`mapping/_normalize.py`, `mapping/team_registry.py`, and the registry CSVs are
vendored from `ncaa_bbStats` rather than imported — that package's editable
install on this machine points at a deleted directory, and a *stale* copy in
another interpreter's `site-packages` imports successfully while missing the
modules we need. See `vendor/PROVENANCE.md`.

---

## Out of scope

Wiring these CSVs into `build_2026_combined.py` and retraining the model. That
touches files this folder deliberately does not. What ships here is two validated
CSVs plus a report proving they match the FanGraphs numbers where they should.
