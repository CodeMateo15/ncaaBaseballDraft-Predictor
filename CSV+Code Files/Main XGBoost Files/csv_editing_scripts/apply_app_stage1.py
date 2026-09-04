"""Turn the generated public no-minimum notebook into the app-Stage-1 variant.

The paper's Stage 1 originally undersampled negatives to 1:1. That makes its output a
ranking score rather than a probability: the same player reads 0.998 there and 0.79 on
the deployed app. The app (`ncaa_bbStats`) fits Stage 1 on the full population instead,
so its output is a population rate. This script applies that change, and nothing else,
so any difference against the paper notebook is attributable to Stage 1 alone.

What changes:
  1. no undersampling; Stage 1 fits the full 61,270-row population (4.19% positive)
  2. Stage 1 hyperparameters match ncaa_bbStats STAGE1_PARAMS (400 trees, seed 0)
  3. the operational evaluation reads the stratified hold-out directly, since with
     full-population training the hold-out is already at the natural rate
  4. the pre-draft board gate moves from P >= 0.90 to P >= 0.50 (app REAL_CHANCE)
  5. a pooled leave-one-season-out cell is added, the app's validation scheme

Run after make_v7_public_notebooks.py. Every edit is asserted, so a change upstream that
invalidates one fails here rather than producing a subtly different notebook.

Usage:
    python csv_editing_scripts/apply_app_stage1.py [--check]
"""

import argparse
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent


def _locate(name):
    """Find a notebook whether we are run from the analysis dir or a repo root.

    The script lives beside the notebooks in the working project and under `code/`
    in the published repository, where the notebooks sit in `analysis/`.
    """
    for base in (HERE, HERE.parent, HERE.parent / "analysis", Path.cwd(),
                 Path.cwd() / "analysis"):
        candidate = base / name
        if candidate.exists():
            return candidate
    return HERE.parent / name


SOURCE = _locate("xgboostAllWithTeamsV7_public_nomin.ipynb")
TARGET = _locate("xgboostAllWithTeamsV7_public_nomin_appS1.ipynb")

CELL13 = '''# APP-STAGE-1: no undersampling. The app fits Stage 1 on the full population,
# which is what makes its output a probability rather than a ranking score.
drafted_indices = df[df['Drafted?'] == 1].index
not_drafted_indices = df[df['Drafted?'] == 0].index

print("Class distribution (full population, no resampling):")
print(f"  Class 0 (Not Drafted): {len(not_drafted_indices)}")
print(f"  Class 1 (Drafted): {len(drafted_indices)}")
print(f"  Positive rate: {len(drafted_indices) / len(df) * 100:.2f}%")

# Names kept so the downstream cells need no edits; the "balanced" pool is now
# simply the whole population.
X_balanced = X
y_balanced = y
undersampled_not_drafted = np.array([], dtype=not_drafted_indices.dtype)
'''

LOSO = '''# APP-STAGE-1: pooled leave-one-season-out for Stage 1, the app's validation.
# Each season is scored by a model fitted without it. Directly comparable to the
# app's published 0.602 PR-AUC at a 4.2% base rate.
_S1P = dict(n_estimators=400, learning_rate=0.05, max_depth=6, subsample=0.8,
            colsample_bytree=0.8, random_state=0)
_oof = pd.Series(index=df.index, dtype=float)
for _yr in sorted(df['year'].unique()):
    _tr = df[df['year'] != _yr]
    _te = df[df['year'] == _yr]
    _m = xgb.XGBClassifier(**_S1P)
    _m.fit(_tr[features], _tr['Drafted?'])
    _oof.loc[_te.index] = _m.predict_proba(_te[features])[:, 1]

_yt = df['Drafted?'].astype(int)
_base = float(_yt.mean())
_prauc = float(average_precision_score(_yt, _oof))
_rocauc = float(roc_auc_score(_yt, _oof))
print("STAGE 1 -- leave-one-season-out, pooled over all seasons")
print(f"  n              {len(_oof):,}   positives {int(_yt.sum()):,} ({_base*100:.2f}%)")
print(f"  PR-AUC         {_prauc:.4f}   (base rate {_base:.4f}, lift {_prauc/_base:.1f}x)")
print(f"  ROC-AUC        {_rocauc:.4f}")
print("  per season:")
for _yr in sorted(df['year'].unique()):
    _m2 = df['year'] == _yr
    print(f"    {_yr}  PR-AUC {average_precision_score(_yt[_m2], _oof[_m2]):.4f}"
          f"   base {_yt[_m2].mean()*100:.2f}%")
METRICS['s1_loso'] = dict(n=int(len(_oof)), n_pos=int(_yt.sum()), base_rate=_base,
                          pr_auc=_prauc, roc_auc=_rocauc, pr_lift=_prauc/_base)
'''

# (cell-locating substring, old, new) -- located by content, never by index, so an
# inserted cell upstream cannot silently retarget an edit.
EDITS = [
    ("draft_model = xgb.XGBClassifier(",
     """draft_model = xgb.XGBClassifier(
    n_estimators=500,
    learning_rate=0.05,
    max_depth=6,
    subsample=0.8,
    colsample_bytree=0.8,
    random_state=42
)""",
     """# APP-STAGE-1: ncaa_bbStats STAGE1_PARAMS.
draft_model = xgb.XGBClassifier(
    n_estimators=400,
    learning_rate=0.05,
    max_depth=6,
    subsample=0.8,
    colsample_bytree=0.8,
    random_state=0
)"""),
    ("Drafted? Classifier Results (Balanced Dataset)",
     'print("\\nDrafted? Classifier Results (Balanced Dataset):")',
     'print("\\nDrafted? Classifier Results (full-population hold-out, 20%):")'),
    ("PROB_GATE = 0.90",
     "PROB_GATE = 0.90              # keep players with P(drafted) >= this on the board",
     "PROB_GATE = 0.50              # APP-STAGE-1: app REAL_CHANCE, better-than-even odds"),
    ("draft_model_sim = xgb.XGBClassifier",
     """_hist = df[df[year_col] < SIM_TEST_YEAR]
_pos = _hist[_hist['Drafted?'] == 1].index
_neg = _hist[_hist['Drafted?'] == 0].index
np.random.seed(40)
_neg_us = np.random.choice(_neg, size=len(_pos), replace=False)
_bal = np.concatenate([_pos.values, _neg_us]); np.random.shuffle(_bal)
draft_model_sim = xgb.XGBClassifier(n_estimators=500, learning_rate=0.05, max_depth=6,
                                    subsample=0.8, colsample_bytree=0.8, random_state=42)
draft_model_sim.fit(df.loc[_bal, sim_cls_features], df.loc[_bal, 'Drafted?'])""",
     """# APP-STAGE-1: fit the full history, no undersampling, app hyperparameters.
_hist = df[df[year_col] < SIM_TEST_YEAR]
draft_model_sim = xgb.XGBClassifier(n_estimators=400, learning_rate=0.05, max_depth=6,
                                    subsample=0.8, colsample_bytree=0.8, random_state=0)
draft_model_sim.fit(_hist[sim_cls_features], _hist['Drafted?'])"""),
    ("Stage1 balanced n=",
     'f"Stage1 balanced n={len(_bal)}, Stage2 n={len(sim_drafted_train)}, "',
     'f"Stage1 full-population n={len(_hist)}, Stage2 n={len(sim_drafted_train)}, "'),
]


def _find(cells, needle):
    hits = [i for i, c in enumerate(cells) if needle in "".join(c.get("source", ""))]
    if len(hits) != 1:
        raise SystemExit(f"expected exactly one cell containing {needle!r}, found {hits}")
    return hits[0]


def build(check=False):
    notebook = json.loads(SOURCE.read_text())
    cells = notebook["cells"]

    # 1. replace the undersampling cell wholesale
    i = _find(cells, "undersampled_not_drafted = np.random.choice")
    cells[i]["source"] = CELL13.splitlines(keepends=True)

    # 2. rewrite the operational cell to read the hold-out directly
    i = _find(cells, "Operational (imbalanced) evaluation for Stage 1")
    src = "".join(cells[i]["source"])
    marker = "# Step 3"
    if marker not in src:
        raise SystemExit("operational cell: '# Step 3' marker missing")
    tail = marker + src.split(marker, 1)[1]
    cells[i]["source"] = ('''# APP-STAGE-1: no reconstruction needed. Training used the full population, so
# the stratified 20% hold-out is already at the natural positive rate and every
# row in it was unseen. Same METRICS keys as the paper notebook so the ledgers line up.
X_op_test = X_test
y_op_test = y_test
target_positive_rate = drafted_indices.shape[0] / df.shape[0]
print(f"Target Positive Rate: {target_positive_rate*100:.2f}%")
print(f"\\nOperational test set (= the stratified hold-out):")
print(f"  Total samples:   {len(y_op_test)}")
print(f"  Positives:       {y_op_test.sum()} ({y_op_test.mean()*100:.2f}%)")

''' + tail).splitlines(keepends=True)

    # 3. the small in-place substitutions
    for needle, old, new in EDITS:
        j = _find(cells, needle)
        s = "".join(cells[j]["source"])
        if old not in s:
            raise SystemExit(f"cell {j}: anchor not found for {needle!r}")
        cells[j]["source"] = s.replace(old, new, 1).splitlines(keepends=True)

    # 4. insert the leave-one-season-out cell after the operational evaluation
    i = _find(cells, "APP-STAGE-1: no reconstruction needed")
    cells.insert(i + 1, {"cell_type": "code", "execution_count": None, "metadata": {},
                         "outputs": [], "source": LOSO.splitlines(keepends=True)})

    # 5. header note
    i = _find(cells, "PUBLIC-DATA VARIANT")
    cells[i]["source"] = ("".join(cells[i]["source"]).replace(
        "PUBLIC-DATA VARIANT", "APP-STAGE-1 VARIANT of the PUBLIC-DATA VARIANT", 1)
        + "\n# Stage 1 refitted to match the deployed app; see apply_app_stage1.py.\n"
    ).splitlines(keepends=True)

    for c in cells:
        if c["cell_type"] == "code":
            c["outputs"] = []
            c["execution_count"] = None

    text = json.dumps(notebook, indent=1, ensure_ascii=False) + "\n"
    if check:
        if not TARGET.exists():
            print(f"  {TARGET.name}: MISSING")
            return False
        theirs = ["".join(c["source"]) for c in json.loads(TARGET.read_text())["cells"]]
        ours = ["".join(c["source"]) for c in cells]
        if theirs == ours:
            print(f"  {TARGET.name}: up to date ({len(cells)} cells)")
            return True
        drift = [i for i, (a, b) in enumerate(zip(theirs, ours)) if a != b]
        print(f"  {TARGET.name}: STALE -- cells {drift[:10]} differ "
              f"(lengths {len(theirs)} vs {len(ours)})")
        return False

    TARGET.write_text(text)
    print(f"  wrote {TARGET.name} ({len(cells)} cells)")
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true",
                        help="verify the variant matches, do not write")
    args = parser.parse_args()
    if not SOURCE.exists():
        raise SystemExit(f"missing {SOURCE}")
    sys.exit(0 if build(check=args.check) else 1)


if __name__ == "__main__":
    main()
