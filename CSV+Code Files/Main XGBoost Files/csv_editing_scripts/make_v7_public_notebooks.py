"""Generate the public-data variants of xgboostAllWithTeamsV7.ipynb.

Step 1 of 2 in building the shipping notebook. All three files here are
superseded intermediates and live in `archive/old_jupyterFiles/`; step 2 is
`apply_app_stage1.py`, which turns the no-minimum variant into the notebook the
paper actually reports, `draft_model.ipynb`.

Two outputs, differing only in which public matrix they read:

    xgboostAllWithTeamsV7_public.ipynb        qualified population  (17,105 rows)
    xgboostAllWithTeamsV7_public_nomin.ipynb  no-minimum population (50,836 rows)

Generated rather than hand-edited so the diff against the private original stays
visible and re-derivable: a public variant that had silently drifted from it
would be worse than no variant. Every edit below is declared as an exact
(cell, old, new) triple and asserted to apply exactly once, so a change upstream
that invalidates one of them fails here instead of producing a subtly different
notebook.

What actually changes, and why:

1. **`age` -> `class_ord`.** NCAA publishes no date of birth. Class year is the
   criterion draft eligibility genuinely turns on, and it carries the same signal:
   juniors are drafted at 17.2% against freshmen at 0.5%, the same non-monotonic
   shape as age 21 (31%) versus 22 (21%). It is also far better populated --
   FanGraphs' `age` is 43% null over these seasons, `class_ord` is 0.1%.

2. **`first_class_ord` added.** The class a player was in the first season they
   appear anywhere in the no-minimum panel. `class_ord` alone recovers only ~73%
   of `age`'s signal because class is coarse where the draft is sharp -- the draft
   rate peaks at age 21, which straddles the sophomore/junior line, so 26% of
   underclassmen are already 21+. `seasons_elapsed` (in V7 itself, so both builds
   carry it) does most of the repair; `first_class_ord` is public-only because the
   private matrix has no `class` column at all. See add_tenure.py.

3. **`w/l/cg/sho/sv_pitch` are NOT dropped.** They were, while the public source
   generation lacked them. It no longer does -- all five are present at 100%
   coverage for 2021-2025 -- so the public matrix carries them and the player
   feature block matches V7 one for one. Player features go 68 -> 69 (the extra is
   `first_class_ord`), total features 150 -> 151.

4. **`mlbamid` is not dropped**, because it does not exist to drop.

5. **Eligibility counts seasons by `person_id`, not `playerid`.** This is the one
   change that would be a silent bug if missed: NCAA mints a new `playerid` every
   season, so counting distinct seasons per `playerid` returns 1 for every player,
   the eligibility filter would treat the entire population as underclassmen, and
   the board would be gated on class alone. `person_id` is the minted cross-season
   key; see ncaa_scraper/sources/identity.py.

6. **Season sources point at `ncaa_public/`** rather than the FanGraphs
   no-minimum exports.

Everything else -- hyperparameters, seeds, splits, gates, figures -- is untouched.

Usage:
    python csv_editing_scripts/make_v7_public_notebooks.py [--check]
"""

import argparse
import copy
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent

# The V7 originals and the intermediate public variants are superseded by
# draft_model.ipynb and live in the archive; only the final notebook ships.
ARCHIVE = ROOT.parent / "archive" / "old_jupyterFiles"

SOURCE = ARCHIVE / "xgboostAllWithTeamsV7.ipynb"

VARIANTS = {
    "public": {
        "notebook": ARCHIVE / "xgboostAllWithTeamsV7_public.ipynb",
        "data_file": "batting_pitching_combined_with_rpi_public_v2.csv",
        "fig_dir": "figures_v7_public",
        "run_tag": "public",
        "note": "qualified population, 20,763 rows (the private matrix has 16,728)",
    },
    "public_nomin": {
        "notebook": ARCHIVE / "xgboostAllWithTeamsV7_public_nomin.ipynb",
        "data_file": "batting_pitching_combined_with_rpi_public_v2_nomin.csv",
        "fig_dir": "figures_v7_public_nomin",
        "run_tag": "public_nomin",
        "note": "full no-minimum population, 61,270 rows and a 4.2% base rate",
    },
}

# The 69 player features: V7's 68 with `age` -> `class_ord` and `first_class_ord`
# added.
# --- Cell 108: eligibility, rewritten to count seasons by person_id -----------
ELIG_V7 = """# playerid was dropped from df (cell 4); reload it (+ age) for the eligibility join.
_ids = pd.read_csv(DATA_FILE, usecols=['nameascii', 'year', 'playerid', 'age'])
_ids['playerid'] = _ids['playerid'].astype(str).str.strip()

# Season count per playerid: no-minimum leaderboards (recover low-PA/IP seasons)
# UNIONed with the modeling dataset. >=3 seasons => junior+ (draft-eligible by class).
seasons_by_pid = {}
def _add_seasons(path, year=None):
    d = pd.read_csv(path, encoding='utf-8-sig', low_memory=False)
    d.columns = [c.strip().lower() for c in d.columns]
    if 'playerid' not in d.columns:
        return
    if 'year' in d.columns:
        pairs = zip(d['playerid'].astype(str).str.strip(), d['year'])
    elif year is not None:
        pairs = zip(d['playerid'].astype(str).str.strip(), [year] * len(d))
    else:
        return
    for pid, yr in pairs:
        # isinstance guard, not just a truthiness test: pandas 3 leaves missing
        # values as NaN through .astype(str) instead of producing the string
        # "nan", so pid can be a float here and float has no .lower().
        if isinstance(pid, str) and pid and pid.lower() != 'nan' and pd.notna(yr):
            seasons_by_pid.setdefault(pid, set()).add(int(yr))

# Multi-year no-minimum leaderboards (2021-2025).
for _g in ['../ncaa_battingNoMinCSV/batting_combined_all.csv',
           '../ncaa_pitchingNoMinCSV/pitching_combined_all.csv']:
    for _p in glob.glob(_g):
        _add_seasons(_p)
# The 2026 no-minimum files are single-season exports with no `year` column, so the
# year is implicit. Without this the 2026 cohort's season counts are undercounted and
# the eligibility filter over-removes.
for _p in glob.glob('2026 data/noMin/*.csv'):
    _add_seasons(_p, year=2026)
print(f"season counts loaded for {len(seasons_by_pid):,} playerids "
      f"(years {min(min(v) for v in seasons_by_pid.values())}-"
      f"{max(max(v) for v in seasons_by_pid.values())})")"""

ELIG_PUBLIC = """# person_id was dropped from df (cell 5); reload it for the eligibility join.
#
# It has to be person_id and NOT playerid. NCAA mints a new playerid every season --
# consecutive roster years share exactly zero ids -- so counting distinct seasons per
# playerid returns 1 for everyone, which would mark the whole population as
# underclassmen and silently reduce the board to a class-only filter. person_id is
# the minted cross-season key; see ncaa_scraper/sources/identity.py for how each
# link is earned and out/reports/person_links.csv for the evidence.
_ids = pd.read_csv(DATA_FILE, usecols=['nameascii', 'year', 'person_id'])
_ids['person_id'] = _ids['person_id'].astype(str).str.strip()

# Season count per person: the public no-minimum player files, which cover every
# player with any playing time. >=3 seasons => junior+ (draft-eligible by class).
seasons_by_pid = {}
def _add_seasons(path, year=None):
    d = pd.read_csv(path, encoding='utf-8-sig', low_memory=False)
    d.columns = [c.strip().lower() for c in d.columns]
    if 'person_id' not in d.columns:
        return
    if 'year' in d.columns:
        pairs = zip(d['person_id'].astype(str).str.strip(), d['year'])
    elif year is not None:
        pairs = zip(d['person_id'].astype(str).str.strip(), [year] * len(d))
    else:
        return
    for pid, yr in pairs:
        # isinstance guard, not just a truthiness test: pandas 3 leaves missing
        # values as NaN through .astype(str) instead of producing the string
        # "nan", so pid can be a float here and float has no .lower().
        if isinstance(pid, str) and pid and pid.lower() != 'nan' and pd.notna(yr):
            seasons_by_pid.setdefault(pid, set()).add(int(yr))

for _g in ['../ncaa_public/batting_combined_all.csv',
           '../ncaa_public/pitching_combined_all.csv']:
    for _p in glob.glob(_g):
        _add_seasons(_p)
print(f"season counts loaded for {len(seasons_by_pid):,} person_ids "
      f"(years {min(min(v) for v in seasons_by_pid.values())}-"
      f"{max(max(v) for v in seasons_by_pid.values())})")"""

ELIG_BASIS_V7 = """# Eligibility: age >= 21 OR >= 3 college seasons.
_id_yr = _ids[_ids['year'] == SIM_TEST_YEAR].drop_duplicates('nameascii').set_index('nameascii')
sim['playerid'] = sim['nameascii'].map(_id_yr['playerid'])
sim['total_college_seasons'] = sim['playerid'].map(
    lambda p: len(seasons_by_pid.get(str(p), set())))

def _elig_basis(r):
    if r['total_college_seasons'] >= CLASS_ELIGIBLE_SEASONS: return 'class'
    if pd.notna(r['age']) and r['age'] >= AGE_ELIGIBLE:       return 'age'
    if pd.isna(r['age']):                                     return 'unknown'
    return 'ineligible'
sim['eligibility_basis'] = sim.apply(_elig_basis, axis=1)
sim['eligible'] = sim['eligibility_basis'].isin(['class', 'age'])"""

ELIG_BASIS_PUBLIC = """# Eligibility: junior or above by class, OR >= 3 counted college seasons.
# The two bases disagree for transfers and redshirts, so both are kept, exactly as
# V7 keeps age and season count.
_id_yr = _ids[_ids['year'] == SIM_TEST_YEAR].drop_duplicates('nameascii').set_index('nameascii')
sim['person_id'] = sim['nameascii'].map(_id_yr['person_id'])
sim['total_college_seasons'] = sim['person_id'].map(
    lambda p: len(seasons_by_pid.get(str(p), set())))

def _elig_basis(r):
    if r['total_college_seasons'] >= CLASS_ELIGIBLE_SEASONS: return 'seasons'
    if pd.notna(r['class_ord']) and r['class_ord'] >= CLASS_ELIGIBLE_ORD: return 'class'
    if pd.isna(r['class_ord']):                                           return 'unknown'
    return 'ineligible'
sim['eligibility_basis'] = sim.apply(_elig_basis, axis=1)
sim['eligible'] = sim['eligibility_basis'].isin(['seasons', 'class'])"""

# Edits keyed by cell index. Each must apply exactly once.
EDITS = [
    (5, "'team_old', 'league_team', 'name', 'playerid', 'mlbamid', 'team',",
        "'team_old', 'league_team', 'name', 'playerid', 'person_id', 'class', 'team',"),
    # Three token edits rather than one block replacement: the source has trailing
    # whitespace inside the list, so matching the whole block is brittle.
    (9, "'age', 'role', ", "'class_ord', 'role', "),
    (9, "'role', 'seasons_elapsed', ",
        "'role', 'seasons_elapsed', 'first_class_ord', "),
    (12, "if col == 'age':          return 'Age'",
         "if col == 'class_ord':    return 'Class (1=Fr .. 4=Sr)'"),
    (88, "'age': 'Age', 'role': 'Role',",
         "'class_ord': 'Class (1=Fr .. 4=Sr)', 'role': 'Role',"),
    # Scouting-report templates and the custom-prediction entry point.
    (97, "    'age': 21, 'seasons_elapsed': 2, 'era_pitch': 3.50,",
         "    'class_ord': 3, 'seasons_elapsed': 2, 'first_class_ord': 1, 'era_pitch': 3.50,"),
    (97, "    'age': 21, 'seasons_elapsed': 2, 'avg_bat': 0.300,",
         "    'class_ord': 3, 'seasons_elapsed': 2, 'first_class_ord': 1, 'avg_bat': 0.300,"),
    (97, "def predict_from_stats(role, age, stats_dict, team_stats_dict=None,",
         "def predict_from_stats(role, class_ord, stats_dict, team_stats_dict=None,"),
    (97, "    row['age'] = age; row['role'] = role_num",
         "    row['class_ord'] = class_ord; row['role'] = role_num"),
    (97, 'header = f"Custom {role} Prediction (age {age})"',
         'header = f"Custom {role} Prediction (class_ord {class_ord})"'),
    (97, "        full_row['age'] = age; full_row['role'] = role_num",
         "        full_row['class_ord'] = class_ord; full_row['role'] = role_num"),
    (97, 'name=name or f"Custom {role} (age {age})",',
         'name=name or f"Custom {role} (class_ord {class_ord})",'),
    # Keep two of the raw RPI columns the notebook otherwise discards after
    # deriving the *_WP_team rates from them. Both carry information the rates
    # do not -- NC_RPI_team is a national *rank*, Q1_Wins_team a *count* -- and
    # the package's feature contract has always used them. Dropping them here
    # while listing them as features is what raised
    # KeyError: "['NC_RPI_team', 'Q1_Wins_team'] not in index".
    (5, "'NC_Rec_Wins_team', 'NC_Rec_Losses_team', 'NC_RPI_team', 'NC_SOS_team',",
        "'NC_Rec_Wins_team', 'NC_Rec_Losses_team', 'NC_SOS_team',"),
    (5, "'Q1_Wins_team', 'Q1_Losses_team',", "'Q1_Losses_team',"),
    # Derive the two package features that are not columns in the file.
    # `Overall_WP_team` is wins over decided games, matching the other *_WP_team
    # columns; WPCT_team is the published percentage and counts ties, so the two
    # differ on the 1,457 rows that have one. `tb_bat` is exact from the hit
    # breakdown, and is masked back to NaN for players who never batted so a
    # pitcher does not get a real-looking 0.
    (4, """for col in columns:
    new_col = col.replace('_team', '') + '_WP_team'
    df[new_col] = df[col].apply(calc_wp)""",
        """for col in columns:
    new_col = col.replace('_team', '') + '_WP_team'
    df[new_col] = df[col].apply(calc_wp)

_w = pd.to_numeric(df['W_team'], errors='coerce')
_l = pd.to_numeric(df['L_team'], errors='coerce')
df['Overall_WP_team'] = _w / (_w + _l).replace(0, np.nan)

df['tb_bat'] = (df['1b_bat'].fillna(0) + 2 * df['2b_bat'].fillna(0)
                + 3 * df['3b_bat'].fillna(0) + 4 * df['hr_bat'].fillna(0))
df.loc[df['ab_bat'].isna(), 'tb_bat'] = np.nan"""),
    # --- feature parity with the ncaa_bbStats package ------------------------
    # Four columns the package's feature contract carries and V7 did not. Added
    # so the notebook and the shipped model read an identical set; see
    # ncaa_bbStats/src/ncaa_bbStats/features.py::model_features.
    #
    # These change the notebook's reported figures. That is the point -- the two
    # were scoring the same data with different inputs -- but any paper figure
    # generated before this must be regenerated.
    (9, "rpi_team_features = ['rpi_team', 'SOS_team', 'Conference_Record_WP_team',",
        "rpi_team_features = ['rpi_team', 'SOS_team', 'NC_RPI_team', 'Q1_Wins_team', "
        "'Overall_WP_team', 'Conference_Record_WP_team',"),
    (9, "'role', 'seasons_elapsed', 'first_class_ord', ",
        "'role', 'seasons_elapsed', 'first_class_ord', 'tb_bat', "),
    # Simulation: eligibility basis constant, then the two blocks above.
    (107, "AGE_ELIGIBLE = 21", "CLASS_ELIGIBLE_ORD = 3        # junior or above"),
    (108, ELIG_V7, ELIG_PUBLIC),
    (108, ELIG_BASIS_V7, ELIG_BASIS_PUBLIC),
    (108, "'rank_raw', 'pred_bonus', 'pred_slot', 'age',",
          "'rank_raw', 'pred_bonus', 'pred_slot', 'class_ord',"),
    (108, "_out.to_csv(f'{SIM_TEST_YEAR}_simulated_board_v7.csv', index=False)",
          "_out.to_csv(f'{SIM_TEST_YEAR}_simulated_board_v7_{RUN_TAG}.csv', index=False)"),
    (108, 'print(f"Saved -> {SIM_TEST_YEAR}_simulated_board_v7.csv")',
          'print(f"Saved -> {SIM_TEST_YEAR}_simulated_board_v7_{RUN_TAG}.csv")'),
    # Display-only age references in the grading and trace cells.
    (109, 'print(f"  Actually-drafted players the filter wrongly cut (pick | name | age | seasons):")',
          'print(f"  Actually-drafted players the filter wrongly cut (pick | name | class | seasons):")'),
    (109, "print(f\"    {_pk:>4}  {x['nameascii']:22} age {x['age']}  {int(x['total_college_seasons'])} seasons\")",
          "print(f\"    {_pk:>4}  {x['nameascii']:22} class {x['class_ord']}  {int(x['total_college_seasons'])} seasons\")"),
    (110, "f\"age {r['age']}, {int(r['total_college_seasons'])} college seasons)\")",
          "f\"class {r['class_ord']}, {int(r['total_college_seasons'])} college seasons)\")"),
    (110, "f\"P={_r['draft_prob']*100:4.1f}%  age {_r['age']}  \"",
          "f\"P={_r['draft_prob']*100:4.1f}%  class {_r['class_ord']}  \""),
]

# Demo cells that call predict_from_stats positionally with an age of 21.
DEMO_CELLS = (101, 102, 103)


def apply_edits(cells, variant):
    """Apply every declared edit, asserting each fires exactly once."""
    applied = 0
    for index, old, new in EDITS:
        source = "".join(cells[index]["source"])
        count = source.count(old)
        if count != 1:
            raise SystemExit(
                f"cell {index}: expected exactly 1 occurrence of\n  {old[:90]!r}\n"
                f"found {count}. V7 has changed; update "
                f"csv_editing_scripts/make_v7_public_notebooks.py.")
        cells[index]["source"] = (source.replace(old, new)).splitlines(keepends=True)
        applied += 1

    # The demo cells pass age positionally; 21 becomes class_ord 3 (junior).
    for index in DEMO_CELLS:
        source = "".join(cells[index]["source"])
        before = source
        source = source.replace("predict_from_stats('Both', 21,",
                                "predict_from_stats('Both', 3,")
        source = source.replace("    'age': 21,\n", "    'class_ord': 3,\n")
        source = source.replace("'age': 21, 'avg_bat'", "'class_ord': 3, 'avg_bat'")
        if source == before:
            raise SystemExit(f"cell {index}: no demo edit applied; check DEMO_CELLS")
        cells[index]["source"] = source.splitlines(keepends=True)
        applied += 1

    # Settings cell: repoint the data file, figure directory and run tag.
    source = "".join(cells[2]["source"])
    replacements = [
        ("'batting_pitching_combined_with_rpi_2026_eada.csv'",
         f"'{variant['data_file']}'"),
        ("os.environ.get('V7_FIG_DIR', 'figures_v7')",
         f"os.environ.get('V7_FIG_DIR', '{variant['fig_dir']}')"),
        ("os.environ.get('V7_RUN_TAG', 'default')",
         f"os.environ.get('V7_RUN_TAG', '{variant['run_tag']}')"),
    ]
    for old, new in replacements:
        if source.count(old) != 1:
            raise SystemExit(f"cell 2: expected 1 occurrence of {old!r}")
        source = source.replace(old, new)
    header = (
        "# PUBLIC-DATA VARIANT of xgboostAllWithTeamsV7.ipynb -- generated by\n"
        "# csv_editing_scripts/make_v7_public_notebooks.py. Do not hand-edit; edit\n"
        "# the generator so the diff against V7 stays re-derivable.\n"
        f"#   population: {variant['note']}\n"
        "#   `age` -> `class_ord`, plus `first_class_ord` (70 player features,\n"
        "#   155 total); eligibility counts seasons by person_id, not playerid.\n"
        "#   The feature set is kept identical to the ncaa_bbStats package that\n"
        "#   serves the draft app, so both read the same 155 columns from this\n"
        "#   same file. See features.py::model_features.\n"
        "#   2026 is present: scraped live from stats.ncaa.org 2026-08-22, all\n"
        "#   308 D1 team-seasons. The public mirror is NOT used for it -- the\n"
        "#   mirror stopped mid-season and understates at-bats by ~48/player.\n\n"
    )
    cells[2]["source"] = (header + source).splitlines(keepends=True)
    applied += 1
    return applied


def build(name, variant, check=False):
    notebook = json.loads(SOURCE.read_text())
    cells = notebook["cells"]
    applied = apply_edits(cells, variant)

    # Outputs would be the private run's; clear them so the file is a clean recipe.
    for cell in cells:
        if cell["cell_type"] == "code":
            cell["outputs"] = []
            cell["execution_count"] = None

    text = json.dumps(notebook, indent=1, ensure_ascii=False) + "\n"
    if check:
        # Compare cell SOURCES only. Comparing whole files would report every
        # executed notebook as stale, since running one fills in outputs -- which
        # is the normal state for these files, not a defect.
        if not variant["notebook"].exists():
            print(f"  {variant['notebook'].name}: MISSING")
            return False
        current = json.loads(variant["notebook"].read_text())
        theirs = ["".join(c["source"]) for c in current["cells"]]
        ours = ["".join(c["source"]) for c in cells]
        if theirs == ours:
            print(f"  {variant['notebook'].name}: up to date ({applied} edits)")
            return True
        drift = [i for i, (a, b) in enumerate(zip(theirs, ours)) if a != b]
        print(f"  {variant['notebook'].name}: STALE -- cells {drift[:10]} differ "
              f"from what the generator produces")
        return False

    variant["notebook"].write_text(text)
    print(f"  wrote {variant['notebook'].name} ({applied} edits, "
          f"{len(cells)} cells)")
    return True


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--check", action="store_true",
                        help="verify the generated notebooks match, do not write")
    args = parser.parse_args()

    if not SOURCE.exists():
        raise SystemExit(f"missing {SOURCE}")
    ok = True
    for name, variant in VARIANTS.items():
        ok &= build(name, variant, check=args.check)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
