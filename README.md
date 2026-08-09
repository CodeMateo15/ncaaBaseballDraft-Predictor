# ncaaBaseballDraft-Predictor

Data, code, and figures supporting **Biggs & Gerber (2026), *Predicting MLB draft outcomes for NCAA baseball players: a three-stage XGBoost pipeline with SHAP-based prediction explanations***.

**Authors.** Mateo Biggs and Eric Gerber — Khoury College of Computer Sciences, Northeastern University.

The draft model (using only public data, not private from the paper) powers a public site where you can browse a board, look up a player, or score a stat line of your own: https://codemateo15-ncaa-draft-app.share.connect.posit.cloud/

The paper (`Predicting_MLB_draft_success__an_XGBoost_model_for_NCAA_baseball_players.pdf`, in the repo root) is the canonical reference for methods, results, and discussion. **This README documents repository layout and reproduction only.**

---

## Primary research artifact

The full three-stage pipeline reported in §5 of the paper — the binary draft classifier (Stage 1), college-draft-order regressor (Stage 2), signing-bonus regressor (Stage 3), and the SHAP-based scouting report layer (§4.5) — is implemented in a single notebook:

> **`CSV+Code Files/Main XGBoost Files/xgboostAllWithTeamsV7.ipynb`**

`xgboostAllWithTeamsV6_withRankFeature.ipynb` in the same directory is the immediately prior version, retained for reference. Earlier iterations (V1–V5) and exploratory work are under `CSV+Code Files/archive/`.

---

## Repository layout

```
ncaaBaseballDraft-Predictor/
├── README.md
├── LICENSE
├── .gitignore
├── Predicting_MLB_draft_success__...pdf       # the paper
├── citations_papers/                          # works cited in the paper + extras
└── CSV+Code Files/
    ├── Main XGBoost Files/                    # PRIMARY RESEARCH ARTIFACT
    │   ├── xgboostAllWithTeamsV7.ipynb        # published pipeline (Stages 1–3 + scouting report)
    │   ├── xgboostAllWithTeamsV6_withRankFeature.ipynb   # prior version, retained
    │   ├── batting_pitching_combined_with_rpi_public.csv # PUBLIC: input matrix, 173 cols, FG-derived metrics stripped
    │   ├── batting_pitching_combined_with_rpi_2026_eada.csv # PRIVATE (gitignored): full matrix, 183 cols, paper §3.4 / Table 2
    │   ├── batting_pitching_combined_with_rpi_2026.csv   # PRIVATE (gitignored): same, pre-EADA merge
    │   ├── emissions.csv                                 # CodeCarbon training-energy log
    │   ├── csv_editing_scripts/                          # preprocessing (team-RPI merge, EADA merge, acronym fixes, public strip)
    │   ├── MLBStatsAPIDraftDataAccess/                   # MLB Stats API enrichment (per-player physicals, signing bonus, slot value)
    │   ├── mlb_draft_prospects/                          # MLB Pipeline top-250 scrape — Stage-2 benchmark (paper §3.7, §5.3)
    │   ├── ncaa_rpiYears/                                # Warren Nolan RPI tables — source of rpi_team, SOS_team, Q1–Q4 splits (Table 2)
    │   ├── 2026 data/                                    # PRIVATE (gitignored): raw FanGraphs 2026 leaderboard exports
    │   ├── EADA Data/                                    # EADA source workbooks (gitignored, ~100 MB/yr) + FEATURES.txt
    │   └── figures/                                      # publication figures (see mapping below)
    ├── ncaa_battingQualifiedCSV/        # FanGraphs batting (qualified PA) — CSVs gitignored, see folder DATA_NOTICE.md
    ├── ncaa_battingNoMinCSV/            # FanGraphs batting (no-min) — CSVs gitignored, see folder DATA_NOTICE.md
    ├── ncaa_pitchingQualifiedCSV/       # FanGraphs pitching (qualified IP) — CSVs gitignored, see folder DATA_NOTICE.md
    ├── ncaa_pitchingNoMinCSV/           # FanGraphs pitching (no-min) — CSVs gitignored, see folder DATA_NOTICE.md
    ├── standardized/                    # team/conference name standardization lookup tables
    ├── RISE Expo Material/              # 2026 RISE Expo poster + abstract
    ├── all_drafts.json                  # historical MLB draft results (Baseball Almanac)
    ├── ncaabb_dataset.csv               # master team-level stats
    └── archive/                         # NOT used in the published pipeline
        ├── old_jupyterFiles/            # V1–V6 prior notebook versions
        ├── trial_sklearn/               # early scikit-learn prototypes
        └── pythagorean_expectation/     # exploratory PE notebooks
```

---

## Data sources

The mapping below mirrors Table 1 of the paper. All upstream collection is handled by the open-source `ncaa_bbStats` package (Biggs 2025), available separately at https://github.com/CodeMateo15/CollegeBaseballStatsPackage. The CSVs in this repo are the *output* of that pipeline; you do not need to re-scrape to reproduce.

| Source (paper Table 1) | Where it lives in this repo |
|---|---|
| NCAA team statistics (2002–2025) | `CSV+Code Files/ncaabb_dataset.csv` |
| FanGraphs player statistics (2021–2025) | `CSV+Code Files/ncaa_batting*CSV/`, `ncaa_pitching*CSV/` |
| Baseball Almanac draft history | `CSV+Code Files/all_drafts.json` |
| Warren Nolan RPI / SOS / split-record win % | `CSV+Code Files/Main XGBoost Files/ncaa_rpiYears/` |
| MLB Stats API (drafted-player physicals, signing bonuses) | `CSV+Code Files/Main XGBoost Files/MLBStatsAPIDraftDataAccess/` |
| MLB Pipeline top-250 (benchmark only, §3.7) | `CSV+Code Files/Main XGBoost Files/mlb_draft_prospects/` |

---

## Data redistribution: public vs. private files

FanGraphs' terms of use do not permit redistribution of their bulk leaderboard data. To respect that while keeping the repository as reproducible as possible, this release uses a two-version layout:

- **Private (gitignored, authors' machine only):** the raw FanGraphs CSVs in the four `ncaa_*CSV/` folders and in `Main XGBoost Files/2026 data/`, plus the full combined matrices `batting_pitching_combined_with_rpi_2026.csv` / `..._2026_eada.csv`. Each of the four raw-data folders contains a `DATA_NOTICE.md` instead of the CSVs in the public clone.
- **Public (in the repo):** `batting_pitching_combined_with_rpi_public.csv` — the private matrix with 10 columns removed and one re-keyed, leaving 173 of 183.

### Where the line falls, and why

This repo applies the same rule as the `ncaa_bbStats` package, whose [`DATA_PROVENANCE.md`](https://github.com/CodeMateo15/CollegeBaseballStatsPackage/blob/main/DATA_PROVENANCE.md) sets out the reasoning in full. Facts about sporting events are not copyrightable in the United States (*Feist Publications v. Rural Telephone Service*, 499 U.S. 340 (1991)); what a compiler owns is its original selection, arrangement, and **derived analytics**. Two categories come out, and nothing else:

**1. FanGraphs-derived metrics (9 columns dropped).** `fip_pitch`, `e-f_pitch`, `lob%_pitch`, `wrc_bat`, `wraa_bat`, `woba_bat`, `wrc+_bat`, `wsb_bat`, `spd_bat` — each computed from FanGraphs' own NCAA linear weights, league constants and park factors. That is their analytical product, not a fact.

**2. FanGraphs identifiers.** `mlbamid` is dropped. `playerid` is FanGraphs' internal key (values like `sa3028661`), so it is **replaced in place** by an opaque surrogate (`p00001`, …) rather than removed — the notebook groups on it to count a player's eligibility seasons, and the public file needs *a* stable player key, just not FanGraphs'. The mapping is one-to-one and deterministic, so rows for the same player still group identically; the eligibility-season count is row-for-row identical to the private matrix across all 20,220 rows.

The surrogate is internal to this file by design: it will **not** join to raw FanGraphs leaderboards, which still carry the `sa…` keys. The notebook's season-count cell supplements the modelling file with the no-minimum leaderboards for exactly that kind of lookup, and that step needs the private data anyway. Anyone with FanGraphs access should work from the private matrix; the public file is self-contained.

**What is retained.** All 56 remaining player columns: the raw counting statistics (G, AB, PA, H, 1B–HR, BB, SO, IP, ER, …), which are records of what happened on the field, and the pure-arithmetic rates built from them — AVG, OBP, SLG, OPS, ISO, BABIP, BB%, K%, ERA, WHIP, K/9, BB/9, K-BB% — which reproduce exactly from those counts. Also retained: all draft outcomes, the `role` column, every team-level NCAA / Warren Nolan / RPI feature, and the 12 `*_eada_team` program-finance features (EADA is a U.S. federal government work and therefore public domain).

No substitute metrics are computed to fill the nine gaps; this is a strip, not a substitution.

**The honest caveat.** The retained counting statistics still *reach this repo* via a FanGraphs export, even though the underlying facts are not FanGraphs'. Re-deriving them directly from stats.ncaa.org individual-player pages would remove the dependency; until that lands, this is the accurate description of where the numbers came from.

The strip is reproducible via `Main XGBoost Files/csv_editing_scripts/make_public_data.py`, which fails loudly if any expected column has been renamed upstream rather than silently shipping it.

---

## Reproducing the pipeline

### 1. Environment

Python **3.12**. Required packages (from the notebook's import block):

- `xgboost`, `shap`, `scikit-learn`, `scipy`, `pandas`, `numpy`
- `matplotlib`, `plotly`
- `codecarbon`, `pynvml` (training-energy logging)

For optional re-scraping of MLB Stats API data: `requests`, `tenacity`.

### 2. Data

The notebook reads `batting_pitching_combined_with_rpi_2026.csv` (the **private** full matrix) via a relative path from its own directory. This file is **not** in the public release — see the *Data redistribution* section above. To reproduce the paper's results exactly you must either (a) have access to the authors' private working copy, or (b) regenerate the matrix from upstream sources via the `ncaa_bbStats` package (FanGraphs access required).

`batting_pitching_combined_with_rpi_public.csv` gets you most of the way: it carries 56 of the 65 player-level columns, all identifiers, all targets, and every team-level feature, so the pipeline can be recreated against it. What it cannot reproduce is the contribution of the nine dropped FanGraphs-derived metrics, which the notebook does use as Stage-1 and Stage-2 features — expect the feature list to need trimming and the published numbers not to match. The pure-arithmetic rates are all present, so only genuinely FanGraphs-original signal is missing.

### 3. Run

Open `CSV+Code Files/Main XGBoost Files/xgboostAllWithTeamsV7.ipynb` in Jupyter and execute top-to-bottom. The notebook runs the three pipeline stages in order — Stage 1 classifier, Stage 2 college-draft-order regressor (single-year hold-out and multi-year LOYO), Stage 3 signing-bonus regressor — followed by SHAP attribution, partial-dependence plots, the scouting-report layer, and the Anderson / LaViolette / Clark case-study cells from §5.4 and §5.7.

### 4. Optional: rebuild the input matrix from raw sources

If you want to regenerate `batting_pitching_combined_with_rpi_2026_eada.csv` from the FanGraphs CSVs and Warren Nolan RPI tables, the relevant scripts are in `CSV+Code Files/Main XGBoost Files/csv_editing_scripts/`:

- `merge_columns.py`, `build_2026_combined.py` — merge batting + pitching player-season CSVs
- `add_rpi.py`, `add_team_rpi.py` — join team-level RPI/SOS/conference data from `ncaa_rpiYears/`
- `add_team_eada.py` — join the 12 EADA program-finance features (see `EADA Data/FEATURES.txt`)
- `fix_acronym_bugs.py`, `fix_swapped_team_identities.py` — repair team-name standardization issues
- `check_team_rpi.py` — validate team RPI mappings
- `make_public_data.py` — produce the public copy: drops the 9 FanGraphs-derived metrics and `mlbamid`, re-keys `playerid` (see *Data redistribution* above)

The MLB Stats API draft enrichment lives separately in `MLBStatsAPIDraftDataAccess/enrich_draft_data.py` (and a simplified `enrich_draft_dataV2.py`).

---

## Figures

Publication figures are in `CSV+Code Files/Main XGBoost Files/figures/`. Mapping to the paper:

| File | Paper |
|---|---|
| `s1_pr_curve.pdf` | Figure 1 — Stage 1 precision-recall curve |
| `s1_pdp.{pdf,png}` | Figure 3 — Stage 1 partial-dependence plots |
| `pitching_pcp_drafted.pdf` | Figure 4 — pitching parallel-coordinates (actual draft status) |
| `pitching_pcp_predicted.pdf` | Figure 5 — pitching parallel-coordinates (predicted draft status) |
| `rank_scatter_full.{pdf,png}` | Figure 6 — Stage 2 predicted vs. actual draft order (full class) |
| `rank_scatter_zoom.{pdf,png}` | Figure 7 — Stage 2 top-50 zoom with player labels |
| `s3_scatter.{pdf,png}` | Figure 8 — Stage 3 predicted vs. actual signing bonus |

Figure 2 (top-10 features by gain) is generated inline by the notebook.

---

## Data use

MLB Stats API data is accessed under MLB Advanced Media's public data notice. This work is not affiliated with or endorsed by MLB or any MLB team. NCAA, FanGraphs, Baseball Almanac, Warren Nolan, and MLB Pipeline data are scraped from public sources for non-commercial research.

## License

Code is released under the MIT License (see `LICENSE`). Data redistribution rights follow the upstream sources listed under *Data sources* above.
