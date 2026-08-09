"""Train V7-public and report how it differs from the published V7.

    python train_v7_public.py --report     # metrics and comparison, no write
    python train_v7_public.py              # retrain and write artifacts

V7-public is a separate lineage, not a revision of V7. It keeps V7's staged
design -- classify who is drafted, then order the survivors -- but refits it on
inputs that can be redistributed, so the model behind the public website can be
reproduced by anyone. The published V7 notebook is not touched by this script
and remains the artifact the paper describes.

What differs is listed in `ncaa_bbStats.model_store.LINEAGE` and surfaced
through `model_card()`. The short version: the nine vendor-proprietary metrics
are replaced by college-calibrated equivalents, the draft labels are anchored by
school as well as name, the population is keyed on a stable player id, and the
hyperparameters are the package's rather than V7's. Because all four changed at
once, the gap between V7 and V7-public is not attributable to any one of them.

Training lives in `ncaa_bbStats.model_store`, which is also what the website
loads at runtime -- so there is exactly one implementation, and this script
cannot drift away from what is being served.
"""

import argparse
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
PAPER_BOARD = HERE.parent / "2026_simulated_board_v7.csv"

try:
    import pandas as pd
    from ncaa_bbStats import model_store as ms
    from ncaa_bbStats import scouting
    from ncaa_bbStats._paths import data_path
except ImportError as exc:  # pragma: no cover - environment guidance
    raise SystemExit(
        f"{exc}\n\nInstall the package first:  pip install 'ncaa_bbStats[explain]'\n"
        "or point PYTHONPATH at a source checkout of CollegeBaseballStatsPackage/src."
    )


def compare_to_paper_board(season=2026, top=None):
    """Overlap between the V7-public board and the board V7 published.

    Reported rather than asserted. The two models are fitted on different
    features, labels and populations, so they are not expected to agree; the
    number is here to keep the size of the disagreement visible.
    """
    if not PAPER_BOARD.is_file():
        return None
    paper = pd.read_csv(PAPER_BOARD)
    paper_names = paper["nameascii"].tolist()

    rows = scouting.draft_board(season, n=len(paper_names))
    public_names = [r["name"] for r in rows]

    out = {"paper_board_rows": len(paper_names), "public_board_rows": len(public_names)}
    for n in (top or (50, 100, 300)):
        shared = set(paper_names[:n]) & set(public_names[:n])
        out[f"top_{n}_overlap"] = f"{len(shared)}/{min(n, len(paper_names))}"
    return out


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--report", action="store_true",
                        help="Report without retraining or writing artifacts.")
    parser.add_argument("--out", default=None,
                        help="Artifact directory (defaults to the package's).")
    parser.add_argument("--test-year", type=int, default=ms.TEST_YEAR)
    args = parser.parse_args(argv)

    if args.report:
        card = scouting.model_card()
        print(f"model version : {card['model_version']}")
        print(f"trained on    : {card['train_years']}")
        print(f"held out      : {card['test_year']}")
        print(f"scored years  : {card.get('scored_years')}")
        print(f"\nstage 1: {card['stage1']['metrics']}")
        print(f"stage 2: {card['stage2']['metrics']}")

        reference = card["reference_implementation"]
        print(f"\n{reference['name']} for orientation only "
              f"(NOT this model's numbers):")
        print(f"  stage1 PR-AUC   {reference['stage1_pr_auc']}")
        print(f"  stage2 Spearman {reference['stage2_spearman']}")

        print("\ndifferences from V7:")
        for line in card["lineage"]["differences"]:
            print(f"  - {line}")

        overlap = compare_to_paper_board()
        if overlap:
            print(f"\n2026 board vs the published V7 board: {overlap}")
        return 0

    print("building matrix ...")
    matrix = ms.build_matrix()
    print(f"  {len(matrix):,} player-seasons, "
          f"{int(matrix['eligible'].sum()):,} draft-eligible, "
          f"{int(matrix['drafted'].sum()):,} drafted")

    out_dir = args.out or data_path("models")
    ms.write_matrix(matrix, out_dir)
    manifest = ms.train(matrix, out_dir, test_year=args.test_year)
    print(f"\n{manifest['model_version']}")
    print(f"  stage 1: {manifest['stage1']['metrics']}")
    print(f"  stage 2: {manifest['stage2']['metrics']}")
    print(f"\nwrote -> {out_dir}")
    print(json.dumps(compare_to_paper_board() or {}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
