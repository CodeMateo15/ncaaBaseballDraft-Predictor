"""Add backward-looking tenure features to a player-season matrix.

Two columns, both derived from *when a player first shows up in college baseball*
rather than from anything about the season being scored:

    seasons_elapsed   year - (first year this player appears in the panel)
    first_class_ord   class ordinal in that first appearance   [public only]

Why these exist. The private matrix carries FanGraphs' `age`, which is the single
strongest Stage 1 feature; the public matrix cannot, because the NCAA publishes no
date of birth, and `class_ord` recovers only about 73% of age's signal. Class is
coarse exactly where the draft is sharp: the draft rate peaks at age 21 (31.4%),
but sophomores average 20.4 and juniors 21.3, so 26% of underclassmen are already
21+ and class scores them as ineligible. `seasons_elapsed` splits that difference
without needing a birth date -- it reproduces the age-21 peak almost identically in
both datasets:

    seasons_elapsed   0      1      2       3      4
    public          6.9%   8.0%  17.3%   8.6%   8.2%
    private         6.8%   8.3%  17.7%   8.8%   5.2%

`first_class_ord` is the weaker of the two and is public-only (the private matrix
has no `class` column at all). It is close to redundant -- `class_ord -
seasons_elapsed` already approximates it, so it is worth +0.004 PR-AUC alone and
+0.001 on top of `seasons_elapsed`. Kept because it is free and interpretable
(players entering as freshmen are drafted 10.4% of the time, JUCO/late transfers
entering as seniors 5.5%), not because it carries the result.

TWO CONSTRAINTS, both of which produce a silently wrong feature if missed:

1. **Count from the no-minimum panel, never from the modeling matrix.** The matrix
   is qualified-only, so a player's first row in it is their first *qualified*
   season, which is a different and outcome-correlated quantity -- a player good
   enough to qualify as a freshman is not the same population as one who did not.

2. **Never count seasons panel-wide.** A count of a player's distinct seasons
   across all of 2021-2025 encodes the label: a junior drafted in 2023 has no 2024
   or 2025 row, so a truncated count says "drafted". Measured at +0.080 PR-AUC of
   pure leakage. Only quantities fixed at or before the row's own season are
   admissible, which is why this module derives everything from the player's
   *first* year and never from their last or their total.

`seasons_elapsed` is left-censored at the start of the panel: every 2021 row is 0,
because no player has a prior season to be seen in. The feature is therefore
uninformative in 2021 and strongest in 2024-25. This is symmetric across the public
and private builds, so it does not distort the comparison between them, but it is a
real limitation and belongs in the paper rather than in a footnote here.

Usage:
    python csv_editing_scripts/add_tenure.py [--input X.csv] [--output Y.csv]
"""

import argparse
import glob
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent                      # Main XGBoost Files
CSV_ROOT = ROOT.parent                  # CSV+Code Files

INPUT_PATH = ROOT / "batting_pitching_combined_with_rpi_2026_eada.csv"
OUTPUT_PATH = INPUT_PATH

CLASS_ORDINAL = {"Fr": 1, "So": 2, "Jr": 3, "Sr": 4, "Gr": 5}

# The private panel: FanGraphs' no-minimum leaderboards, which is the same source
# the V7 notebook's eligibility cell counts seasons from. The 2026 exports are
# single-season files with no `year` column, so the year is implicit and has to be
# supplied -- without it the 2026 cohort's first appearances are wrong.
PRIVATE_PANEL = [
    (str(CSV_ROOT / "ncaa_battingNoMinCSV" / "batting_combined_all.csv"), None),
    (str(CSV_ROOT / "ncaa_pitchingNoMinCSV" / "pitching_combined_all.csv"), None),
    (str(ROOT / "2026 data" / "noMin" / "*.csv"), 2026),
]


def load_panel(sources, key):
    """Read (key, year[, class, team]) tuples from every no-minimum file we have."""
    frames = []
    for pattern, implicit_year in sources:
        for path in sorted(glob.glob(pattern)):
            frame = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)
            frame.columns = [c.strip().lower() for c in frame.columns]
            if key not in frame.columns:
                continue
            if "year" in frame.columns:
                years = frame["year"]
            elif implicit_year is not None:
                years = pd.Series(implicit_year, index=frame.index)
            else:
                continue
            block = pd.DataFrame({key: frame[key], "year": years})
            block["class"] = (frame["class"] if "class" in frame.columns else None)
            # Carried so colliding_ids() can tell one career from two players
            # who share a name; nothing else here reads it.
            block["team"] = (frame["team"] if "team" in frame.columns else None)
            frames.append(block)
    if not frames:
        raise SystemExit(f"no panel files carried a `{key}` column: {sources}")
    panel = pd.concat(frames, ignore_index=True)
    panel[key] = panel[key].astype(str).str.strip()
    panel = panel[panel[key].str.lower().ne("nan") & panel[key].ne("")]
    panel = panel.dropna(subset=["year"])
    panel["year"] = panel["year"].astype(int)
    return panel


def colliding_ids(panel, key, team_column="team"):
    """Ids that appear for more than one school in a single season.

    NCAA baseball has no meaningful mid-season transfer, so one id against two
    schools in one year is two people, not one career -- the record linkage in
    ``ncaa_scraper/sources/identity.py`` over-merged on a shared name. Measured
    on the 2021-2026 public panel: 23 ids, e.g. `q001326` in 2023 is both a Cal
    Baptist pitcher (54.2 IP) and a Grand Canyon batter taken 6th overall.

    Left alone this is not cosmetic. ``first_appearance`` takes the minimum year
    per id, so the earlier player's debut is attributed to the later one and
    ``seasons_elapsed`` is overstated -- by up to 4 seasons across 43 rows,
    including freshmen credited with prior seasons they never played.

    The correction is deliberately blunt and is not free. Qualifying *every* row
    of an affected id by school also splits any genuine transfer inside that id,
    resetting the transferring player's tenure to 0 instead of carrying it
    across schools. That trades an overstatement for an understatement on the
    same ~110 rows. It is worth it only because these ids are already known to
    be unreliable; the real fix is to stop the over-merge in
    ``ncaa_scraper/sources/identity.py``, which links on name plus weak
    evidence tiers. Ids that never collide are untouched either way.
    """
    if team_column not in panel.columns:
        return frozenset()
    per_year = panel.groupby([key, "year"])[team_column].nunique()
    return frozenset(per_year[per_year > 1].index.get_level_values(0).unique())


def effective_key(frame, key, split, team_column="team"):
    """The grouping key, with colliding ids qualified by school.

    Derived identically on the panel and on the matrix, so the join in
    :func:`attach` still lines up.
    """
    ids = frame[key].astype(str).str.strip()
    if not split or team_column not in frame.columns:
        return ids
    teams = frame[team_column].astype(str).str.strip()
    return ids.where(~ids.isin(split), ids + "|" + teams)


def first_appearance(panel, key, with_class=False, split=frozenset()):
    """Per player: the first year they appear, and their class in that year.

    Deliberately uses only the *minimum* year. Any statistic over the player's
    whole span (season count, last year, span length) would read forward from the
    row being scored -- see constraint 2 in the module docstring.
    """
    panel = panel.copy()
    panel["class_ord"] = panel["class"].map(CLASS_ORDINAL)
    # Sorting by class ordinal as well makes the tie-break deterministic when a
    # player has both a batting and a pitching row in their first season.
    panel = panel.sort_values(["year", "class_ord"])
    panel["_key"] = effective_key(panel, key, split)
    grouped = panel.groupby("_key", sort=False)
    table = grouped["year"].min().to_frame("first_year")
    if with_class:
        # GroupBy.first() skips nulls, so a blank class in the batting row still
        # picks up the pitching row's value rather than propagating NaN.
        table["first_class_ord"] = grouped["class_ord"].first()
    return table


def attach(frame, table, key, split=frozenset()):
    """Join the tenure table onto a player-season matrix and derive the delta."""
    keys = effective_key(frame, key, split)
    mapped_first = keys.map(table["first_year"])
    frame["seasons_elapsed"] = frame["year"] - mapped_first
    if "first_class_ord" in table.columns:
        frame["first_class_ord"] = keys.map(table["first_class_ord"])

    negative = (frame["seasons_elapsed"] < 0).sum()
    if negative:
        raise ValueError(
            f"{negative} rows have a season before their first appearance, which "
            f"means the `{key}` join is wrong, not that the data is odd.")
    return frame


def report(frame, label=""):
    null = frame["seasons_elapsed"].isna().mean()
    print(f"  {label}seasons_elapsed: {null:.2%} null, "
          f"range {frame['seasons_elapsed'].min():.0f}-"
          f"{frame['seasons_elapsed'].max():.0f}")
    if "first_class_ord" in frame.columns:
        print(f"  {label}first_class_ord: "
              f"{frame['first_class_ord'].isna().mean():.2%} null")
    by_year = frame.groupby("year")["seasons_elapsed"].mean()
    print(f"  {label}mean by year: "
          + ", ".join(f"{y}={v:.2f}" for y, v in by_year.items())
          + "   (year one is 0 by construction -- left-censored panel)")


def main(input_path=None, output_path=None):
    # Overridable so the public build can call this with its own paths; the
    # backward-looking discipline above is the part worth reusing rather than
    # reimplementing.
    input_path = Path(input_path) if input_path else INPUT_PATH
    output_path = Path(output_path) if output_path else OUTPUT_PATH

    frame = pd.read_csv(input_path, low_memory=False)
    frame.columns = frame.columns.str.strip()

    panel = load_panel(PRIVATE_PANEL, "playerid")
    print(f"panel: {len(panel):,} player-seasons, "
          f"{panel['playerid'].nunique():,} players, "
          f"years {panel['year'].min()}-{panel['year'].max()}")

    table = first_appearance(panel, "playerid", with_class=False)
    n_before = len(frame)
    frame = attach(frame, table, "playerid")
    assert len(frame) == n_before, f"row count changed: {n_before} -> {len(frame)}"

    report(frame)
    frame.to_csv(output_path, index=False)
    print(f"Wrote {output_path} ({len(frame):,} rows, {frame.shape[1]} columns)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input", default=None)
    parser.add_argument("--output", default=None)
    args = parser.parse_args()
    main(input_path=args.input, output_path=args.output)
