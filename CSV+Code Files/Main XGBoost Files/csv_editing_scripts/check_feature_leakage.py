"""Fail the build if a feature's *missingness* predicts the draft label.

This exists because of `age`. FanGraphs publishes a date of birth only for players
it has already linked to a professional record, so in the private matrix `age` is
null for 43% of rows but only 0.3% of drafted rows:

    P(drafted | age present) = 16.9%
    P(drafted | age absent)  =  0.07%

XGBoost routes NaN down a learned default branch, so "age is missing" is readable
as "not drafted" and the model takes it. Masking the pattern while keeping every
real age value -- random-imputing the nulls from the observed distribution -- costs
0.100 Stage 1 PR-AUC. That is not a modelling choice anyone made; it is an artefact
of which players a vendor happens to have birth dates for, and it inflated the
measured gap between the public and private builds from ~0.04 to 0.11.

Nothing about that is specific to `age`. Any column sourced from a vendor that
tracks professional players will have the same shape, and the failure is silent:
metrics improve, so the only signal is that they improved for the wrong reason.
Hence a standing check rather than a one-time audit.

What it does NOT catch: leakage through a column's *values* (a post-draft
statistic), or through row membership. Those need different tests. This one covers
exactly the missingness channel.

Usage:
    python csv_editing_scripts/check_feature_leakage.py [--threshold 0.10] [csv ...]
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent

DEFAULT_TARGETS = [
    ROOT / "batting_pitching_combined_with_rpi_2026_eada.csv",
    ROOT / "batting_pitching_combined_with_rpi_public_v2.csv",
    ROOT / "batting_pitching_combined_with_rpi_public_v2_nomin.csv",
]

# Columns that are the label, describe the draft itself, or identify the player.
# None reach a model, so their missingness is allowed to track the outcome -- for
# most of them it defines it.
NOT_FEATURES = {
    "Drafted?", "Round", "Pick", "Drafted By", "Drafted From",
    "name", "nameascii", "team", "playerid", "person_id", "mlbamid", "class",
    "year", "Acronym", "Full Team Name", "Full Name_team", "id_team",
    "team_old", "team_new", "team_teamstats", "league_team", "division_team",
}

# Columns that trip the check but are known, documented, and gated OFF by default.
# Reported every run so they stay visible, but they do not fail the build -- the
# value is in noticing a NEW one. Removing an entry here is how you re-arm it.
GATED = {
    "age": "not a model feature unless V7_AGE=1 (cell 2 of "
           "xgboostAllWithTeamsV7.ipynb); still read by the simulation's "
           "eligibility rule, which is a draft rule, not a learned signal",
}

# A column null for almost nobody or almost everybody cannot carry much through
# this channel, and the rate estimate on the rare side is too noisy to act on.
MIN_NULL_RATE = 0.02
MAX_NULL_RATE = 0.98


def audit(path, threshold):
    frame = pd.read_csv(path, low_memory=False)
    label = frame["Drafted?"].astype(str).isin(["1", "1.0", "True", "true"])
    base = label.mean()

    findings = []
    for column in frame.columns:
        if column in NOT_FEATURES or column.startswith("api_"):
            continue
        absent = frame[column].isna()
        rate = absent.mean()
        if not (MIN_NULL_RATE <= rate <= MAX_NULL_RATE):
            continue
        present_rate = label[~absent].mean()
        absent_rate = label[absent].mean()
        gap = abs(present_rate - absent_rate)
        if gap >= threshold:
            findings.append((column, rate, present_rate, absent_rate, gap))

    findings.sort(key=lambda r: -r[4])
    gated = [f for f in findings if f[0] in GATED]
    findings = [f for f in findings if f[0] not in GATED]

    print(f"\n{path.name}")
    print(f"  {len(frame):,} rows, base rate {base:.2%}, threshold {threshold:.2f}")
    for column, rate, present_rate, absent_rate, gap in gated:
        print(f"  gated: {column} (gap {gap:.3f}, {rate:.0%} null) -- {GATED[column]}")
    if not findings:
        print("  OK -- no ungated feature's missingness predicts the label")
        return True
    print(f"  {len(findings)} FAILING column(s):")
    print(f"    {'column':<26} {'null%':>7} {'P|present':>10} {'P|absent':>9} {'gap':>7}")
    for column, rate, present_rate, absent_rate, gap in findings:
        print(f"    {column:<26} {rate:>6.1%} {present_rate:>10.2%} "
              f"{absent_rate:>9.2%} {gap:>7.3f}")
    return False


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("csv", nargs="*", type=Path, default=None)
    parser.add_argument("--threshold", type=float, default=0.10,
                        help="max allowed |P(drafted|present) - P(drafted|absent)|")
    args = parser.parse_args(argv)

    targets = args.csv or [p for p in DEFAULT_TARGETS if p.exists()]
    if not targets:
        raise SystemExit("no matrices found to audit")

    ok = all([audit(path, args.threshold) for path in targets])
    if not ok:
        print("\nFAILED. A failing column is not necessarily unusable -- the fix is "
              "to mask the pattern (impute the nulls from the observed "
              "distribution) rather than to drop the column, which throws away "
              "real signal along with the artefact. Do NOT add an `is_missing` "
              "indicator; that reintroduces exactly what this check is for.")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
