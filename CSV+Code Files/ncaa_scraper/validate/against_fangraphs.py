"""The correctness proof: join to the FanGraphs CSVs and diff.

The counting statistics are facts about games that happened. If our scrape and
FanGraphs' export disagree about them, one of us has a bug -- and since NCAA is
the upstream source for both, it is almost certainly us. That agreement rate is
the single number that says whether this scraper works.

The 22 pure-arithmetic rate columns should likewise match to floating point. The
nine league-relative columns will NOT match and are reported as correlations
instead; see ``derive/advanced.py`` for why.

Runs against the gitignored private originals and skips cleanly when they are
absent, so the folder still works for anyone without FanGraphs access.
"""

import os
import re
import unicodedata

import numpy as np
import pandas as pd

import config

BATTING_COUNTS = ["g", "ab", "h", "2b", "3b", "hr", "r", "rbi", "bb", "so",
                  "hbp", "sf", "sh", "sb", "cs"]
PITCHING_COUNTS = ["w", "l", "g", "gs", "cg", "sho", "sv", "ip", "tbf", "h",
                   "r", "er", "hr", "bb", "hbp", "wp", "bk", "so"]

BATTING_RATES = ["avg", "obp", "slg", "ops", "iso", "bb%", "k%", "bb/k", "babip"]
PITCHING_RATES = ["era", "whip", "k/9", "bb/9", "hr/9", "k/bb", "k%", "bb%",
                  "k-bb%", "avg", "babip", "lob%"]

# Recomputed from our own constants -- correlation, not equality.
DERIVED = {
    "batting": ["woba", "wrc", "wraa", "wrc+", "wsb", "spd"],
    "pitching": ["fip", "e-f"],
}

RATE_TOLERANCE = 1e-6

# Which counting stats each rate is computed from.
#
# This exists to make the rate gate self-calibrating. A rate is pure arithmetic
# over these inputs, so it *cannot* agree with FanGraphs on more rows than its
# inputs do. Gating a rate against a fixed floor therefore measures the source
# data's freshness, not our formula -- and it fails for reasons no code change can
# fix. Gating it against its own inputs' joint agreement measures exactly the
# thing we control.
#
# Concretely: pitching `tbf` agrees on 94.7% of 2023 rows because NCAA revises
# batters-faced after FanGraphs takes an export (the mirror's own BF disagrees
# with its own AB+BB+HBP+SH+SFA on 3.6% of rows, and every alternative definition
# scores worse than the reported column). So `avg`, `babip`, `k%`, `bb%` and
# `k-bb%` -- all of which divide by tbf -- inherit that ceiling. They are not
# broken, and lowering their floor to 0.92 would hide a real formula bug the next
# time one appears.
RATE_INPUTS = {
    "batting": {
        "avg": ["h", "ab"],
        "obp": ["h", "bb", "hbp", "ab", "sf"],
        "slg": ["h", "2b", "3b", "hr", "ab"],
        "ops": ["h", "bb", "hbp", "ab", "sf", "2b", "3b", "hr"],
        "iso": ["h", "2b", "3b", "hr", "ab"],
        "bb%": ["bb", "ab", "hbp", "sf", "sh"],
        "k%": ["so", "ab", "bb", "hbp", "sf", "sh"],
        "bb/k": ["bb", "so"],
        "babip": ["h", "hr", "ab", "so", "sf"],
    },
    "pitching": {
        "era": ["er", "ip"],
        "whip": ["h", "bb", "ip"],
        "k/9": ["so", "ip"],
        "bb/9": ["bb", "ip"],
        "hr/9": ["hr", "ip"],
        "k/bb": ["so", "bb"],
        "k%": ["so", "tbf"],
        "bb%": ["bb", "tbf"],
        "k-bb%": ["so", "bb", "tbf"],
        "avg": ["h", "tbf", "bb", "hbp"],
        "babip": ["h", "hr", "tbf", "bb", "hbp", "so"],
        "lob%": ["h", "bb", "hbp", "r", "hr"],
    },
}

# How far below its inputs' ceiling a rate may fall before it is a real failure.
# Small but nonzero: floating-point comparison at 1e-6 can differ on a row where
# the integer inputs match, e.g. when FanGraphs rounds a published rate.
RATE_CEILING_SLACK = 0.01

# Per-column bias exemptions, each requiring a measured reason.
#
# The bias gate exists to catch a mis-mapped header, which shifts every row in one
# direction. A known source discrepancy looks identical to the gate, so it has to
# be listed -- but listing it costs a written justification, which keeps this from
# becoming a way to silence the gate. Keyed (year, category, column); a `None`
# year applies to every year.
BIAS_EXEMPT = {
    (None, "pitching", "bk"): (
        "NCAA's own page agrees with us and FanGraphs does not: Andrew Rubayo "
        "(Quinnipiac, 2025) reads Bk=1 on stats.ncaa.org, which is what we emit, "
        "while FanGraphs records 0. Bias is +0.17 to +0.23 in every year."),
    (2021, "pitching", "g"): (
        "The legacy mirror's `App` over-counts appearances on the 118 teams the "
        "pre-block cache does not cover: measured +0.440 there against -0.098 on "
        "the 175 cache-sourced teams. `App` is that mirror's only appearances "
        "column, so there is nothing better to map, and nulling 1,912 real "
        "pitchers' `g` would be worse than a 0.44-appearance offset."),
    (2021, "pitching", "gs"): (
        "Same source and same 118 teams as `g`: legacy `GS` measures -0.248 there "
        "versus +0.000 on cache-sourced teams."),
    (2021, "pitching", "tbf"): (
        "Batters-faced is the noisiest column in every year (+0.03 in 2022, "
        "+0.05 in 2023, -0.08 in 2024) because NCAA revises it after FanGraphs "
        "takes an export; 2021 measures +0.13 on cache teams and +0.18 on "
        "legacy-filled ones, i.e. the same drift on both sides rather than a "
        "mapping difference. Every alternative definition of TBF scores worse "
        "than the reported column -- see the note on RATE_INPUTS."),
}


def _bias_exempt(year, category, column):
    """Return the documented reason this column's bias is allowed, or None."""
    for key in ((year, category, column), (None, category, column)):
        if key in BIAS_EXEMPT:
            return BIAS_EXEMPT[key]
    return None


def _fold(text):
    decomposed = unicodedata.normalize("NFKD", str(text or ""))
    return "".join(c for c in decomposed if not unicodedata.combining(c))


def join_key(name, team, year):
    text = _fold(name).lower()
    text = re.sub(r"\b(jr|sr|ii|iii|iv)\b", "", text)
    text = re.sub(r"[.'`]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return f"{text}|{team}|{year}"


def _match_within_group(ours, theirs, tiebreak):
    """Pair rows inside a colliding key group by nearest counting stats.

    (name, team, year) is not unique: the FanGraphs files each contain two
    genuine collisions -- `Cole Conn / UIC` in 2022 and 2023 under two different
    FanGraphs ids, and `Vince Reilly / GCU` in 2021 and 2022. A naive merge fans
    those out to four rows and inflates the apparent match rate, so groups of
    size > 1 are paired by minimum absolute difference on a few high-signal
    columns instead.
    """
    columns = [c for c in tiebreak if c in ours.columns and c in theirs.columns]
    pairs = []
    remaining = list(theirs.index)

    for our_index in ours.index:
        if not remaining:
            break
        best, best_cost = None, None
        for their_index in remaining:
            cost = 0.0
            for column in columns:
                a = ours.at[our_index, column]
                b = theirs.at[their_index, column]
                if pd.notna(a) and pd.notna(b):
                    cost += abs(float(a) - float(b))
                else:
                    cost += 1e6
            if best_cost is None or cost < best_cost:
                best, best_cost = their_index, cost
        pairs.append((our_index, best))
        remaining.remove(best)

    return pairs


def _second_pass(ours, theirs, our_left, their_left, tiebreak):
    """Match leftovers within a team-season by counting-stat proximity.

    The two sources disagree about spellings far more than about players. Real
    examples from 2025, all of them the same human:

        Cermodrick Bland / Cemodrick Bland      (a typo in one source)
        Joshua Ibe       / Josh Ibe             (nickname)
        Matthew Reinholtz/ Mathew Reinholtz     (spelling)
        JP Hefft         / Jason Hefft          (initials vs given name)
        Michael Cruz     / Mikey Cruz Jr.       (nickname plus a suffix)

    No amount of string normalization catches 'JP' vs 'Jason'. But counting stats
    do: within one team-season the season line is a near-unique fingerprint, so
    leftovers are paired on it instead. Guarded tightly -- AB, H and G must all
    agree within 3 -- because a loose match here would inflate the very agreement
    rate this module exists to measure.

    This is a measurement fix only. The emitted CSVs keep NCAA's spelling, since
    NCAA is the source of truth for NCAA statistics.
    """
    columns = [c for c in tiebreak if c in ours.columns and c in theirs.columns]
    if not columns:
        return []

    pairs = []
    ours_by_team = {}
    for idx in our_left:
        ours_by_team.setdefault(ours.at[idx, "team"], []).append(idx)

    theirs_by_team = {}
    for idx in their_left:
        theirs_by_team.setdefault(theirs.at[idx, "team"], []).append(idx)

    for team, their_indices in theirs_by_team.items():
        candidates = list(ours_by_team.get(team, []))
        if not candidates:
            continue
        for their_index in their_indices:
            best, best_cost = None, None
            for our_index in candidates:
                cost = 0.0
                ok = True
                for column in columns:
                    a = ours.at[our_index, column]
                    b = theirs.at[their_index, column]
                    if pd.isna(a) or pd.isna(b):
                        ok = False
                        break
                    delta = abs(float(a) - float(b))
                    if delta > 3:          # hard guard, not a soft penalty
                        ok = False
                        break
                    cost += delta
                if ok and (best_cost is None or cost < best_cost):
                    best, best_cost = our_index, cost
            if best is not None:
                pairs.append((best, their_index))
                candidates.remove(best)

    return pairs


def _align(ours, theirs, tiebreak):
    """Return index pairs aligning our rows to theirs, handling key collisions."""
    our_groups = ours.groupby("_key").indices
    their_groups = theirs.groupby("_key").indices

    pairs = []
    ours_only = []
    for key, our_positions in our_groups.items():
        their_positions = their_groups.get(key)
        if their_positions is None:
            ours_only.extend(ours.index[our_positions])
            continue
        our_slice = ours.iloc[our_positions]
        their_slice = theirs.iloc[their_positions]
        if len(our_slice) == 1 and len(their_slice) == 1:
            pairs.append((our_slice.index[0], their_slice.index[0]))
        else:
            pairs.extend(_match_within_group(our_slice, their_slice, tiebreak))

    theirs_only = [
        idx for key, positions in their_groups.items() if key not in our_groups
        for idx in theirs.index[positions]
    ]

    # Second pass on whatever the name key could not reach.
    recovered = _second_pass(ours, theirs, ours_only, theirs_only, tiebreak)
    if recovered:
        matched_ours = {p[0] for p in recovered}
        matched_theirs = {p[1] for p in recovered}
        pairs.extend(recovered)
        ours_only = [i for i in ours_only if i not in matched_ours]
        theirs_only = [i for i in theirs_only if i not in matched_theirs]

    return pairs, ours_only, theirs_only


def compare(ours, theirs, *, category, counts, rate_columns, tiebreak, year=None):
    ours = ours.copy()
    theirs = theirs.copy()

    if year is not None:
        ours = ours[ours["year"] == year]
        theirs = theirs[theirs["year"] == year]
    else:
        # Only compare years we actually scraped.
        theirs = theirs[theirs["year"].isin(set(ours["year"]))]

    ours["_key"] = [join_key(n, t, y) for n, t, y in
                    zip(ours["name"], ours["team"], ours["year"])]
    theirs["_key"] = [join_key(n, t, y) for n, t, y in
                      zip(theirs["name"], theirs["team"], theirs["year"])]

    pairs, ours_only, theirs_only = _align(ours, theirs, tiebreak)

    print(f"\n--- {category} ---")
    print(f"  our rows           {len(ours):,}")
    print(f"  FanGraphs rows     {len(theirs):,}")
    print(f"  joined             {len(pairs):,} "
          f"({len(pairs) / max(len(theirs), 1):.1%} of FanGraphs)")
    print(f"  FanGraphs unmatched {len(theirs_only):,} "
          f"({len(theirs_only) / max(len(theirs), 1):.1%})")
    print(f"  ours unmatched      {len(ours_only):,} "
          f"(expected to be large -- FanGraphs' noMin still filters)")

    if not pairs:
        print("  NOTHING JOINED -- check the join key or the team mapping")
        return {"joined": 0, "records": []}, 1

    our_indices = [p[0] for p in pairs]
    their_indices = [p[1] for p in pairs]
    left = ours.loc[our_indices].reset_index(drop=True)
    right = theirs.loc[their_indices].reset_index(drop=True)

    records = []
    per_column = {}
    all_agree = pd.Series(True, index=left.index)

    for column in counts:
        if column not in left.columns or column not in right.columns:
            continue
        a = pd.to_numeric(left[column], errors="coerce")
        b = pd.to_numeric(right[column], errors="coerce")
        # ip is in NCAA notation on both sides, so exact comparison is right.
        same = (a == b) | (a.isna() & b.isna())
        per_column[column] = float(same.mean())
        all_agree &= same

    print(f"\n  counting stats identical on {all_agree.mean():.2%} of joined rows")
    print("  per-column agreement (worst 8):")
    for column, rate in sorted(per_column.items(), key=lambda kv: kv[1])[:8]:
        a = pd.to_numeric(left[column], errors="coerce")
        b = pd.to_numeric(right[column], errors="coerce")
        delta = (a - b).dropna()
        bias = delta.mean() if len(delta) else float("nan")
        print(f"    {column:<6} {rate:7.2%}   mean(ours-theirs)={bias:+.3f}")

    rate_agreement = {}
    for column in rate_columns:
        if column not in left.columns or column not in right.columns:
            continue
        a = pd.to_numeric(left[column], errors="coerce")
        b = pd.to_numeric(right[column], errors="coerce")
        b = b.replace([np.inf, -np.inf], np.nan)
        comparable = a.notna() & b.notna()
        if not comparable.any():
            continue
        close = np.isclose(a[comparable], b[comparable], atol=RATE_TOLERANCE, rtol=1e-6)
        rate_agreement[column] = float(close.mean())

    print(f"\n  pure-arithmetic rates within {RATE_TOLERANCE:g}:")
    for column, rate in sorted(rate_agreement.items(), key=lambda kv: kv[1]):
        print(f"    {column:<7} {rate:7.2%}")

    print("\n  league-relative columns (correlation, NOT equality -- see "
          "derive/advanced.py):")
    for column in DERIVED[category]:
        if column not in left.columns or column not in right.columns:
            continue
        a = pd.to_numeric(left[column], errors="coerce")
        b = pd.to_numeric(right[column], errors="coerce").replace([np.inf, -np.inf], np.nan)
        comparable = a.notna() & b.notna()
        if comparable.sum() < 10:
            continue
        r = float(np.corrcoef(a[comparable], b[comparable])[0, 1])
        mae = float((a[comparable] - b[comparable]).abs().mean())
        bias = float((a[comparable] - b[comparable]).mean())
        print(f"    {column:<7} r={r:.4f}  mae={mae:.4f}  mean(ours-theirs)={bias:+.4f}")

    # Per-row diff records for triage.
    mismatch = left[~all_agree]
    for position in mismatch.index:
        differing = []
        for column in counts:
            if column not in left.columns or column not in right.columns:
                continue
            a, b = left.at[position, column], right.at[position, column]
            if pd.notna(a) and pd.notna(b) and float(a) != float(b):
                differing.append(f"{column}:{a}!={b}")
        records.append({
            "bucket": "joined_counts_differ",
            "key": left.at[position, "_key"],
            "name": left.at[position, "name"],
            "team": left.at[position, "team"],
            "year": left.at[position, "year"],
            "differences": ";".join(differing),
        })
    for index in theirs_only:
        records.append({
            "bucket": "fangraphs_unmatched", "key": theirs.at[index, "_key"],
            "name": theirs.at[index, "name"], "team": theirs.at[index, "team"],
            "year": theirs.at[index, "year"], "differences": "",
        })
    for index in ours_only:
        records.append({
            "bucket": "ours_unmatched", "key": ours.at[index, "_key"],
            "name": ours.at[index, "name"], "team": ours.at[index, "team"],
            "year": ours.at[index, "year"], "differences": "",
        })

    summary = {
        "joined": len(pairs),
        "join_rate": len(pairs) / max(len(theirs), 1),
        "counts_identical": float(all_agree.mean()),
        "rate_agreement": rate_agreement,
        "records": records,
    }

    # What is gated, and why it is not the row-level identical rate.
    #
    # A sub-100% counting agreement does NOT imply we are wrong. NCAA revises box
    # scores after FanGraphs takes an export, so scattered +/-1 differences are
    # expected and are the vendor being stale, not us being broken. Spot-checked
    # to the source: Andrew Rubayo (Quinnipiac, 2025) reads Bk=1, WP=12, SV=blank
    # on his NCAA page, exactly what we emit, while FanGraphs records 0/13/1.
    #
    # What WOULD indicate a bug is a *systematic* offset in one column -- a
    # mis-mapped header shifts every row in the same direction. So the gate is on
    # per-column bias, which detects that, plus the join rate, which detects
    # missing players. The row-level rate is reported for triage.
    #
    # Exemptions live in the module-level BIAS_EXEMPT table, each with the
    # measurement that justifies it.
    BIAS_LIMIT = 0.10

    failures = []
    if summary["join_rate"] < 0.97:
        failures.append(f"join rate {summary['join_rate']:.1%} < 97% "
                        f"-- players are genuinely missing, not just misspelled")

    biased = {}
    exempted = {}
    for column in counts:
        if column not in left.columns or column not in right.columns:
            continue
        a = pd.to_numeric(left[column], errors="coerce")
        b = pd.to_numeric(right[column], errors="coerce")
        delta = (a - b).dropna()
        if not len(delta) or abs(delta.mean()) <= BIAS_LIMIT:
            continue
        reason = _bias_exempt(year, category, column)
        if reason:
            exempted[column] = round(float(delta.mean()), 3)
        else:
            biased[column] = round(float(delta.mean()), 3)
    if biased:
        failures.append(f"systematic per-column offset (likely a header-mapping "
                        f"bug): {biased}")
    if exempted:
        print("\n  known-source offsets, exempt with a documented reason:")
        for column, value in sorted(exempted.items()):
            print(f"    {column:<7} {value:+.3f}  "
                  f"{_bias_exempt(year, category, column).split('.')[0]}.")

    # A rate is gated against its own inputs' joint agreement, not a fixed floor.
    # See RATE_INPUTS for why. The ceiling is reported alongside every rate so a
    # low number is attributable rather than mysterious.
    ceilings = _input_ceilings(left, right, category, rate_agreement)
    weak = {}
    for column, agreement in rate_agreement.items():
        ceiling = ceilings.get(column)
        floor = 0.95 if ceiling is None else min(0.95, ceiling - RATE_CEILING_SLACK)
        if agreement < floor:
            weak[column] = (agreement, ceiling)
    if weak:
        failures.append(
            "rate columns below what their inputs support: "
            + ", ".join(
                f"{c}={a:.1%}" + (f" (inputs {ci:.1%})" if ci is not None else "")
                for c, (a, ci) in sorted(weak.items())))

    if ceilings:
        print("\n  rate agreement vs the ceiling its input counts allow:")
        for column in sorted(rate_agreement, key=lambda c: rate_agreement[c]):
            ceiling = ceilings.get(column)
            if ceiling is None:
                continue
            headroom = rate_agreement[column] - ceiling
            print(f"    {column:<7} {rate_agreement[column]:>7.2%}  "
                  f"inputs {ceiling:>7.2%}  ({headroom:+.2%})")

    summary["rate_ceilings"] = ceilings
    summary["biased_columns"] = biased

    if failures:
        print("\n  FAILURES:")
        for failure in failures:
            print(f"    - {failure}")

    return summary, (1 if failures else 0)


def _input_ceilings(left, right, category, rate_agreement):
    """Fraction of joined rows where every input of each rate matches exactly.

    That is the most a pure-arithmetic rate could possibly agree on, so it is the
    right thing to compare the rate against.
    """
    ceilings = {}
    for column, inputs in RATE_INPUTS.get(category, {}).items():
        if column not in rate_agreement:
            continue
        usable = [c for c in inputs
                  if c in left.columns and c in right.columns]
        if len(usable) != len(inputs):
            continue
        match = None
        for name in usable:
            a = pd.to_numeric(left[name], errors="coerce")
            b = pd.to_numeric(right[name], errors="coerce")
            same = (a == b) | (a.isna() & b.isna())
            match = same if match is None else (match & same)
        if match is not None and len(match):
            ceilings[column] = float(match.mean())
    return ceilings


def _locate_output():
    """Prefer the complete CSVs, fall back to the PARTIAL ones a year-by-year
    build produces. Returns (batting, pitching, is_partial)."""
    for suffix, partial in (("", False), (".PARTIAL", True)):
        batting = os.path.join(config.OUT_DIR, f"batting_combined_all{suffix}.csv")
        pitching = os.path.join(config.OUT_DIR, f"pitching_combined_all{suffix}.csv")
        if os.path.exists(batting) and os.path.exists(pitching):
            return batting, pitching, partial
    return None, None, False


def run(year=None):
    if not (os.path.exists(config.FG_BATTING) and os.path.exists(config.FG_PITCHING)):
        print("FanGraphs originals not present -- skipping the comparison.")
        print(f"  looked for {config.FG_BATTING}")
        print("This validator needs FanGraphs leaderboard access; the scrape and")
        print("the acceptance gates do not.")
        return 0

    our_batting, our_pitching, partial = _locate_output()
    if our_batting is None:
        print(f"no scraped output in {config.OUT_DIR}. Run `python run.py` first.")
        return 1
    if partial:
        # Building year by year is the normal workflow, and each year's A/B is the
        # checkpoint that gates the next one. Refusing to read PARTIAL files would
        # mean no year could be validated until all six were done.
        print(f"reading *.PARTIAL.csv -- these hold a subset of years, so league "
              f"constants are fitted to that subset. Fine for a per-year check, "
              f"not for the final numbers.\n")

    status = 0
    all_records = {}

    batting_summary, code = compare(
        pd.read_csv(our_batting), pd.read_csv(config.FG_BATTING),
        category="batting", counts=BATTING_COUNTS, rate_columns=BATTING_RATES,
        tiebreak=["g", "ab", "h"], year=year,
    )
    status |= code
    all_records["batting"] = batting_summary["records"]

    pitching_summary, code = compare(
        pd.read_csv(our_pitching), pd.read_csv(config.FG_PITCHING),
        category="pitching", counts=PITCHING_COUNTS, rate_columns=PITCHING_RATES,
        tiebreak=["ip", "so", "bb"], year=year,
    )
    status |= code
    all_records["pitching"] = pitching_summary["records"]

    os.makedirs(config.REPORT_DIR, exist_ok=True)
    for category, records in all_records.items():
        path = os.path.join(config.REPORT_DIR, f"fg_diff_{category}.csv")
        pd.DataFrame(records).to_csv(path, index=False)
        print(f"\nwrote {path} ({len(records):,} rows)")

    print("\nSort fg_diff_*.csv by `differences` -- a systematic offset in one")
    print("column across many rows is a column-mapping bug; scattered +/-1 is an")
    print("NCAA box-score revision since the FanGraphs export was taken.")
    return status
