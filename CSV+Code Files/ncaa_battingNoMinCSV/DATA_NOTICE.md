# Data not included

This folder contained per-year FanGraphs **no-minimum** college-batting leaderboards (2021–2025) that the authors cannot redistribute under FanGraphs' terms of use. The CSVs are present in the authors' local working copy but are gitignored at the repo root.

**To regenerate this data**, you need FanGraphs leaderboard access and the `ncaa_bbStats` package: https://github.com/CodeMateo15/CollegeBaseballStatsPackage. The original column schema is documented in the paper (§3.4, Table 2) and the variable names used downstream are visible in `Main XGBoost Files/csv_editing_scripts/make_public_data.py`.

The publicly-released copy of the combined input matrix (`Main XGBoost Files/batting_pitching_combined_with_rpi_public.csv`) has all FG-sourced player-level columns stripped. See the main `README.md` for details.
