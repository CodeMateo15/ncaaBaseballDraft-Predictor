# Public NCAA player-season data, 2021–2025

These two files replace the non-redistributable FanGraphs college leaderboard
exports in `ncaa_batting*CSV/` and `ncaa_pitching*CSV/`. They are derived entirely
from public NCAA data and **are** redistributable, which is the point of them.

| file | rows | columns |
|---|---|---|
| `batting_combined_all.csv` | 26,821 | 40 |
| `pitching_combined_all.csv` | 25,966 | 34 |

`BULK_MANIFEST.json` pins the exact upstream commit, sha256 and row count of every
source file, so the build is reproducible against a fixed snapshot rather than a
moving branch.

Regenerate with:

```bash
cd ../ncaa_scraper
python run.py --years 2021-2025          # ~3 min, no NCAA access needed
python run.py --validate                 # acceptance gates
python run.py --validate-against-fangraphs   # needs the private files
cp out/batting_combined_all.csv out/pitching_combined_all.csv ../ncaa_public/
cp bulk_cache/MANIFEST.json ../ncaa_public/BULK_MANIFEST.json
```

## How close is this to the FanGraphs data it replaces?

Measured against the local FanGraphs export for the same seasons:

| | rows | FanGraphs | join | counting stats identical |
|---|---|---|---|---|
| batting | 26,821 | 26,826 | 99.4% | 94.3% |
| pitching | 25,966 | 25,964 | 99.8% | 78.2% |

Every pure-arithmetic rate (`avg`, `obp`, `slg`, `ops`, `iso`, `bb%`, `k%`,
`bb/k`, `babip`, `era`, `whip`, `k/9`, `bb/9`, `hr/9`, `k/bb`, `k-bb%`, `lob%`)
agrees at or **above** the ceiling its own input columns allow. That is the
strongest available statement: wherever the inputs agree with FanGraphs, the
derived rate agrees exactly, so the remaining disagreement is source freshness
rather than formula error. The largest single contributor is pitching `tbf`, which
NCAA revises after FanGraphs takes an export.

The nine league-relative columns are **recomputed from our own fitted constants**,
so they correlate rather than match: wOBA r=0.9950, wRC+ r=0.9915, FIP r=0.9985,
E–F r=0.9975, wSB r=0.9900, wRC r=0.9980, wRAA r=0.9928, spd r=0.9326. They are a
level shift, not noise — **any model consuming them must be retrained, not
re-scored.**

## Schema differences from the FanGraphs files

| change | why |
|---|---|
| `age` → `class` | NCAA publishes no date of birth. Note FanGraphs' own `age` was 45–64% null depending on season, so this loses less than it appears to. |
| `mlbamid` dropped | No NCAA equivalent. |
| `person_id` added | NCAA mints a **new** `playerid` every season — consecutive years share zero ids — so `playerid` cannot group a player across seasons. `person_id` is a minted cross-season key; every link is recorded with its evidence in `ncaa_scraper/out/reports/person_links.csv`. Null for 0.5% of rows. |
| `w l cg sho sv` dropped | Absent from the 2025+ source generation, and the per-game box scores carry no decision column, so they cannot be had for every year. |

## Known gaps

- **2026 is absent.** The upstream mirror stopped updating on 2026-04-12 and its
  2026 files were last written mid-season, understating at-bats by ~48 per player.
  A real 2026 needs a live scrape from an unblocked IP or the private export.
- Texas Southern's 2021 **batting** is in no public source (its pitching is
  present). One team-season of 1,465.
- 2022 has 300 of 301 D1 teams; Stonehill is in neither upstream mirror.
- `gdp` is null for 2021 — the 2021 NCAA grid did not carry it.

## Provenance and licence

Source data is NCAA game and season statistics, published by the NCAA and
mirrored by `armstjc/ncaa_baseball_data` and `armstjc/NCAA_Baseball_repository`.
Statistics are facts, not creative expression. The derived columns
(`1b`, `pa`, all rates, and the nine league-relative columns) are computed in
`ncaa_scraper/derive/` and are this project's own work. See
`ncaa_scraper/README.md` for the full method and `ncaa_scraper/vendor/PROVENANCE.md`
for the vendoring record.
