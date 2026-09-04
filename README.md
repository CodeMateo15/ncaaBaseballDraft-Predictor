# Predicting the MLB Draft from Public NCAA Data

TLDR: Go to paper/ folder and run the notebook there

A college baseball player deciding whether to enter the MLB Draft wants to know three things: will they be drafted, where will they go, and what will they be paid. Existing college baseball and MLB Draft modeling relies on data that cannot be readily redistributed or reproduced: the sabermetric leaderboards most college baseball work relies on are not free, so results cannot be checked by the public. The first contribution of this work is to construct and validate a public NCAA-derived dataset, consisting of 61,270 Division~I player-seasons from 2021 through 2026 (every player with any playing time), of whom 2,565 (4.19%) were drafted.

We then use MLB Draft prediction as a substantive benchmark of what can, and cannot, be inferred from public performance data alone. The headline test is a 2026 draft board built with no hindsight: every model retrained on 2021--2025 alone, every field that exists only after a player is drafted removed, and all 10,434 2026 player-seasons ranked from scratch. Of the top 50 names, 48 were actually drafted, matching MLB Pipeline's college-only top 50. Our board runs 261 deep and is still 71% accurate at that depth, where the public scouting board stops at 142 college players.

This paper contributes both an open dataset that matches batter and pitcher data of a paid source at 99% and a predictive pipeline demonstrating that data's utility in modeling if a player is drafted, where they will be drafted, and their signing bonus. We find that public college-level data is not a replacement for expert opinions.

We rebuild every input column from public NCAA statistics, recompute the nine league-relative sabermetric columns — wOBA, wRC+, wRAA, wSB, wRC, Speed Score, FIP, E–F, LOB% — from run values we fit ourselves, and validate the result against the paid version column by column.

**Mateo Biggs and Eric Gerber** — Khoury College of Computer Sciences, Northeastern University.

The same models power a public site where you can browse the board, look up a player, or score a stat line of your own: https://codemateo15-ncaa-draft-app.share.connect.posit.cloud/

## What is where

| path | what's in it |
|---|---|
| `paper/` | the manuscript and the notebook behind it |
| `CSV+Code Files/Main XGBoost Files/` | `draft_model.ipynb` — the paper's notebook — with its input matrix, `figures/`, and 2026 draft board |
| `.../csv_editing_scripts/` | scripts that build the modelling matrix from the source data |
| `.../MLBStatsAPIDraftDataAccess/` | MLB Stats API draft results: picks, signing bonuses, slot values |
| `.../mlb_draft_prospects/` | MLB Pipeline top-250 prospect lists, 2021–2026 — the human benchmark |
| `.../ncaa_rpiYears/` | Warren Nolan RPI, strength of schedule, and quadrant records |
| `.../EADA Data/` | program-finance features from the federal EADA survey (`FEATURES.txt`; the source workbooks are large and not committed) |
| `CSV+Code Files/ncaa_public/` | the public NCAA player-season data, plus `BULK_MANIFEST.json` pinning every upstream commit and checksum — [README](CSV+Code%20Files/ncaa_public/README.md) |
| `CSV+Code Files/ncaa_scraper/` | the code that builds that data from public NCAA sources, including the recompute logic for the nine sabermetric columns — [README](CSV+Code%20Files/ncaa_scraper/README.md) |
| `CSV+Code Files/standardized/` | team and conference name lookup tables |
| `CSV+Code Files/all_drafts.json` | historical MLB draft results |
| `CSV+Code Files/ncaabb_dataset.csv` | team-level NCAA season statistics |
| `CSV+Code Files/RISE Expo Material/` | 2026 RISE Expo poster |
| `CSV+Code Files/archive/` | superseded notebooks, figure sets, and boards from earlier runs; not used by the paper |

## Running it

Python 3.12, with `xgboost`, `shap`, `scikit-learn`, `pandas`, `numpy`, `matplotlib`, and
`codecarbon`.

The paper's results come from `CSV+Code Files/Main XGBoost Files/draft_model.ipynb`. Its outputs are
committed, so every number can be checked without re-running anything; the notebook also prints a
machine-readable metrics ledger. It writes its figures to `figures/` and its draft board to
`2026_simulated_board.csv`, both beside it.

The notebook is generated, not hand-written: `csv_editing_scripts/make_v7_public_notebooks.py`
derives the public variant from the private original, then `apply_app_stage1.py` refits Stage 1 the
way the deployed app does. Both originals live in `CSV+Code Files/archive/old_jupyterFiles/`, and
both scripts support `--check`. To regenerate the underlying NCAA data rather than use the committed
copy, see `CSV+Code Files/ncaa_scraper/README.md`.

## Data sources

All freely available, no paywall.

| source | what it supplies |
|---|---|
| NCAA statistics, via public mirrors at pinned commits | player and team season statistics |
| MLB Stats API | draft picks, signing bonuses, slot values |
| MLB Pipeline top-250 | the human benchmark |
| Warren Nolan | RPI, strength of schedule, quadrant records |
| U.S. Dept. of Education EADA survey | program budget and roster features |

## Data use

The sabermetric columns in this repository are recomputed from run values fit on the public NCAA
data, not copied from any commercial source. MLB Stats API data is accessed under MLB Advanced
Media's public data notice; this work is not affiliated with or endorsed by MLB or any MLB team. All
data is used for non-commercial research.

## License

Code is released under the MIT License (see `LICENSE`). Data redistribution rights follow the
upstream sources listed above.
