"""Acceptance gates.

Each gate prints PASS/FAIL and names the report to look at. The run exits
non-zero if any hard gate fails. Some checks are reported without gating, where
the expected value is genuinely uncertain (per-conference r-squared on ~10 teams,
the FIP offset against FanGraphs).

Row-count gates are **floors, not equalities**: FanGraphs' "noMin" leaderboard
still filters something, so our scrape legitimately returns more players.
"""

import os

import pandas as pd

import config

# Row counts in the existing FanGraphs-derived CSVs, per year.
FG_ROWS = {
    "batting": {2021: 5327, 2022: 5405, 2023: 5328, 2024: 5390, 2025: 5376, 2026: 5329},
    "pitching": {2021: 4956, 2022: 5072, 2023: 5147, 2024: 5318, 2025: 5471, 2026: 5403},
}

# Live discovery counts, verified.
EXPECTED_TEAMS = {2021: 293, 2022: 301, 2023: 305, 2024: 305, 2025: 307, 2026: 308}

# Teams no public source has, so the team-count gate expects them to be absent.
# Naming them individually is the point: a bare tolerance would also swallow a
# real regression that dropped a conference.
KNOWN_MISSING_TEAMS = {
    2022: {"STMN": ("Stonehill, a 2022 D2->D1 transition program, is in neither "
                    "mirror. The other seven of that cohort -- BELL, CBU, MRMK, "
                    "TAR, UCSD, UNA, UTU -- are recovered from the legacy mirror, "
                    "so 2022 reaches 300 of 301.")},
}

# How far below the FanGraphs row count a year may fall.
#
# The original gate was "at least as many rows as FanGraphs", which is right for a
# live scrape of the whole population: the no-minimum export still filters, so we
# should have more. The public mirrors are marginally *short* in some years for
# reasons that are understood and unfixable, so the gate is now a tolerance with
# the measured shortfalls recorded here.
#
# Measured against FanGraphs (2021-2025 build):
#   batting  2021 5,244 vs 5,327  (-1.56%)  Texas Southern's batting grid is in no
#                                           public source, plus ~40 players the
#                                           legacy mirror omits
#            2022 5,396 vs 5,405  (-0.17%)  Stonehill absent from both mirrors
#            2023 5,342 vs 5,328  (+0.26%)
#            2024 5,438 vs 5,390  (+0.89%)
#            2025 5,401 vs 5,376  (+0.46%)
#   pitching 2022 5,061 vs 5,072  (-0.22%)  Stonehill
#            2023 5,146 vs 5,147  (-0.02%)
FG_ROW_SHORTFALL_TOLERANCE = 0.02

MAX_ROW_MULTIPLE = 3.0


class Gates:
    def __init__(self):
        self.failures = []
        self.notes = []

    def check(self, name, ok, detail=""):
        status = "PASS" if ok else "FAIL"
        print(f"  [{status}] {name}" + (f" -- {detail}" if detail else ""))
        if not ok:
            self.failures.append(name)
        return ok

    def note(self, name, detail):
        print(f"  [note] {name} -- {detail}")
        self.notes.append((name, detail))


def run(batting, pitching, constants, coverage, scope="conference"):
    gates = Gates()
    print("\n=== Structural ===")

    # 40 and 39, against FanGraphs' 40 and 39: `mlbamid` dropped, `age` renamed to
    # `class`, `person_id` added. The five pitching decision columns are kept --
    # see the schema block in config.py for why they were briefly dropped.
    for name, frame, columns, expected in (
        ("batting", batting, config.BATTING_COLUMNS, 40),
        ("pitching", pitching, config.PITCHING_COLUMNS, 39),
    ):
        gates.check(
            f"{name} has exactly the {expected} expected columns in order",
            list(frame[columns].columns) == columns and len(columns) == expected,
            f"{len(columns)} columns",
        )

    for name, frame in (("batting", batting), ("pitching", pitching)):
        duplicates = frame.duplicated(subset=["playerid", "year"]).sum()
        gates.check(f"{name}: (playerid, year) is unique", duplicates == 0,
                    f"{duplicates} duplicates")

    for name, frame in (("batting", batting), ("pitching", pitching)):
        bad_class = set(frame["class"].dropna().unique()) - config.VALID_CLASSES
        null_rate = frame["class"].isna().mean()
        gates.check(f"{name}: class values are valid", not bad_class, f"unexpected: {bad_class}")
        gates.note(f"{name}: class null rate", f"{null_rate:.2%}")

    if os.path.exists(config.UNIQUE_TEAMS):
        valid = set(pd.read_csv(config.UNIQUE_TEAMS)["Acronym"])
        for name, frame in (("batting", batting), ("pitching", pitching)):
            unknown = set(frame["team"].unique()) - valid
            gates.check(
                f"{name}: every team is in unique_teams.csv", not unknown,
                f"{len(unknown)} unknown: {sorted(unknown)[:10]}",
            )
    else:
        gates.note("team acronym check", f"skipped, {config.UNIQUE_TEAMS} absent")

    # A two-way player must present identically in both files, because
    # build_2026_combined.py merges them on playerid alone.
    shared = batting.merge(
        pitching[["playerid", "year", "name", "team", "class"]],
        on=["playerid", "year"], suffixes=("_b", "_p"), how="inner",
    )
    mismatched = shared[
        (shared["name_b"] != shared["name_p"]) | (shared["team_b"] != shared["team_p"])
    ]
    gates.check("two-way players agree across the two files", len(mismatched) == 0,
                f"{len(mismatched)} mismatches")

    print("\n=== Coverage ===")
    for year, group in coverage.groupby("year"):
        teams = group["team_id"].nunique()
        expected = EXPECTED_TEAMS.get(year)
        if expected is None:
            gates.note(f"{year}: team count", f"{teams} (no expectation recorded)")
            continue
        known = KNOWN_MISSING_TEAMS.get(year, {})
        present = set(group["mapped_acronym"].dropna())
        unaccounted = {a for a in known if a in present}
        target = expected - len(known)
        detail = f"{teams} vs {expected}"
        if known:
            detail += (f", less {len(known)} acknowledged missing "
                       f"({', '.join(sorted(known))})")
        gates.check(f"{year}: team count equals discovery", teams == target
                    and not unaccounted, detail)

    low, high = config.ROWS_PER_TEAM_BAND
    per_team = batting.groupby(["year", "ncaa_team_id"]).size()
    outliers = per_team[(per_team < low) | (per_team > high)]
    gates.note("team-seasons outside the rows-per-team band",
               f"{len(outliers)} of {len(per_team)} (band {low}-{high})")

    print("\n=== Row counts (band around the FanGraphs baseline) ===")
    for name, frame in (("batting", batting), ("pitching", pitching)):
        for year, count in frame.groupby("year").size().items():
            reference = FG_ROWS[name].get(year)
            if reference is None:
                gates.note(f"{name} {year}", f"{count} rows (no FanGraphs baseline)")
                continue
            floor = int(reference * (1 - FG_ROW_SHORTFALL_TOLERANCE))
            ceiling = int(MAX_ROW_MULTIPLE * reference)
            ok = floor <= count <= ceiling
            gates.check(
                f"{name} {year}: rows in [{floor}, {ceiling}]",
                ok, f"{count} rows ({count / reference - 1:+.2%} vs FanGraphs)",
            )

    print("\n=== League constants ===")
    division_rows = constants[constants["scope"] == "division"]
    for _, row in division_rows.iterrows():
        gates.check(f"{int(row['year'])} division r2 >= 0.94",
                    row["r2"] >= 0.94, f"r2={row['r2']:.4f}")

    violations = constants[
        (constants["w_1b"] > constants["w_2b"])
        | (constants["w_2b"] > constants["w_3b"])
        | (constants["w_3b"] > constants["w_hr"])
    ]
    gates.check("monotone hit weights in every league group", len(violations) == 0,
                f"{len(violations)} violations")

    # Bands differ by scope, and both sets are derived from data rather than
    # guessed. Division: the vendored fit gives woba_scale 1.200-1.242 and
    # cfip_constant 3.99-4.26 for 2021-2026 D1. Conference: recovered from the
    # FanGraphs files themselves, where per-conference scale runs 0.875-1.204
    # (frequently BELOW 1.0), cfip runs 3.69-5.43, lgOBP 0.348-0.407, and
    # lgR/PA 0.134-0.192. The bands below pad those ranges modestly.
    BANDS = {
        "division": {"woba_scale": (1.00, 1.50), "cfip": (3.50, 5.00),
                     "lg_obp": (0.32, 0.42), "lg_r_pa": (0.12, 0.21)},
        "conference": {"woba_scale": (0.80, 1.35), "cfip": (3.00, 6.00),
                       "lg_obp": (0.30, 0.44), "lg_r_pa": (0.10, 0.23)},
    }

    for level, bands in BANDS.items():
        subset = constants[constants["scope"] == level]
        if subset.empty:
            continue
        for column, (low, high) in bands.items():
            values = subset[column].dropna()
            if values.empty:
                continue
            gates.check(
                f"{level}: {column} in [{low}, {high}]",
                values.between(low, high).all(),
                f"range {values.min():.4f}-{values.max():.4f}"
                + (f", {int((~values.between(low, high)).sum())} outside"
                   if not values.between(low, high).all() else ""),
            )

    conference_rows = constants[constants["scope"] == "conference"]
    if len(conference_rows):
        fitted = conference_rows[conference_rows["r2"].notna()]
        if len(fitted):
            gates.note("per-conference r2 (reported, not gated)",
                       f"median {fitted['r2'].median():.4f}, "
                       f"min {fitted['r2'].min():.4f}, n={len(fitted)}")
        else:
            # The expected and correct state: event weights come from the
            # division fit, so there is no per-conference regression to score.
            gates.note(
                f"{len(conference_rows)} conference group(s)",
                "using division event weights with per-conference scalars "
                "(lg_obp, lg_r_pa, cfip, woba_scale) -- a per-conference "
                "regression is degenerate at 10-17 teams, see derive/constants.py",
            )
        # Every conference must still have its own scalars, or the whole point of
        # conference scope is lost.
        for column in ("lg_obp", "lg_r_pa", "cfip", "woba_scale"):
            distinct = conference_rows[column].round(6).nunique()
            gates.check(
                f"conference {column} varies across groups",
                distinct > 1,
                f"{distinct} distinct value(s) over {len(conference_rows)} groups",
            )

    print("\n=== Identities ===")
    level = "conference" if scope == "conference" else "division"
    index = {(r["year"], r["league"]): r
             for _, r in constants[constants["scope"] == level].iterrows()}

    batting = batting.copy()
    batting["_pa_woba"] = batting["ab"] + batting["bb"] + batting["hbp"] + batting["sf"]
    # One league label per row, so the grouping matches the constants table
    # exactly under either scope.
    batting["_league"] = (
        batting["conference"] if scope == "conference" else "ALL"
    )

    worst_woba = 0.0
    worst_wrc = 0.0
    unmatched_groups = []
    borrowed = []
    for (year, league), group in batting.groupby(["year", "_league"]):
        row = index.get((year, league))
        if row is None:
            unmatched_groups.append((year, league))
            continue
        # These two identities are what anchoring a group to its OWN league totals
        # guarantees. A group too small to have its own totals borrows the
        # division's (derive/constants.py::MIN_TEAMS_FOR_OWN_SCALARS), so it is
        # measured against a different league by design and the identity is not
        # expected to hold. Excluded and reported, not silently tolerated -- a
        # widened threshold would have to be 40 wRC+ wide and would hide anything.
        if "division scalars" in str(row.get("shrunk_toward", "")):
            borrowed.append((year, league, int(row.get("n_teams", 0))))
            continue
        weight = group["_pa_woba"]
        if weight.sum() > 0:
            observed = float((group["woba"] * weight).sum() / weight.sum())
            worst_woba = max(worst_woba, abs(observed - row["lg_woba"]))
        if group["pa"].sum() > 0:
            observed = float((group["wrc+"] * group["pa"]).sum() / group["pa"].sum())
            worst_wrc = max(worst_wrc, abs(observed - 100.0))

    # A group with no constants row would make the two gates below pass
    # vacuously, so it is itself a failure.
    gates.check("every league group has a constants row", not unmatched_groups,
                f"missing: {unmatched_groups[:5]}")

    if borrowed:
        gates.note("groups measured against division scalars",
                   f"{len(borrowed)} group(s) too small for their own league "
                   f"totals, excluded from the two identities below: "
                   f"{', '.join(f'{y} {lg} (n={n})' for y, lg, n in borrowed)}")

    gates.check("PA-weighted wOBA equals lg_obp per group (+/-0.0005)",
                worst_woba <= 0.0005, f"worst deviation {worst_woba:.6f}")
    gates.check("PA-weighted wRC+ equals 100 per group (+/-0.5)",
                worst_wrc <= 0.5, f"worst deviation {worst_wrc:.4f}")

    # sum(wraa) is NOT exactly zero, and should not be expected to be. wraa
    # scales by `pa` (which includes SH) while the anchoring identity holds on
    # PA *excluding* SH, so the residual is sum(sh * (woba - lgwOBA)) / scale.
    # FanGraphs' own files carry the same artefact and it grows over time:
    # 0.17% of sum|wraa| in 2021 rising to 1.36% in 2025 at division level,
    # median 1.76% and max 27.7% per conference-year. The band below is set from
    # those measurements, not from theory.
    total_wraa = float(batting["wraa"].sum())
    total_abs = float(batting["wraa"].abs().sum())
    ratio = abs(total_wraa) / total_abs if total_abs else 0.0
    gates.check("sum(wraa) within 3% of sum|wraa|", ratio <= 0.03,
                f"{total_wraa:+.1f} of {total_abs:.0f} = {ratio:.2%}")

    valid = pitching[pitching["era"].notna() & pitching["fip"].notna()]
    if len(valid):
        mean_offset = float((valid["era"] - valid["fip"]).mean())
        gates.note("mean(era - fip) over the population", f"{mean_offset:+.4f}")

    print("\n=== Cross-check against the vendored constants ===")
    reference_path = os.path.join(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))), "vendor", "reference", "batting_weights.csv")
    if os.path.exists(reference_path):
        reference = pd.read_csv(reference_path)
        _compare_reference(gates, division_rows, reference)
    else:
        gates.note("vendored constants", "absent, skipped")

    print()
    if gates.failures:
        print(f"{len(gates.failures)} GATE(S) FAILED:")
        for name in gates.failures:
            print(f"  - {name}")
        return 1
    print("ALL GATES PASSED")
    return 0


def _compare_reference(gates, division_rows, reference):
    """Cross-check the in-folder fit against the packaged one.

    This compares two different *populations*, not two runs of one method: the
    packaged constants are fitted to NCAA official team statistics, ours to the
    player rows we scraped. So exact agreement is not the expectation and a
    modest gap is informative rather than alarming.

    Four weights are gated at 10%: `w_1b`, `w_hr`, `w_bb`, `w_hbp`. Measured
    against the packaged fit for 2025 D1 they land at 0.5%, 0.5%, 1.7% and 7.8%.

    **`w_2b` and `w_3b` are reported, not gated**, because the monotone-hits
    constraint deliberately couples them. An unconstrained fit puts the triple
    below the double often enough that the upstream implementation notes it
    happens in 34 of 55 division-seasons; when it does, inverse-variance isotonic
    regression pools the two into a single value. Ours came out
    w_2b = 1.377 / w_3b = 1.429 against the packaged 1.274 / 1.725 -- 17% on the
    triple, entirely from that pooling. Triples are under 1% of plate
    appearances, so the effect on wOBA is about 0.0015, which is why wOBA still
    correlates with FanGraphs at r = 0.995. Gating this would be testing the
    noise, not the method.

    `w_sb`/`w_cs` are likewise not gated: they are no longer fitted at all but
    taken from published run values (see `derive.constants.RUN_SB`), so a
    difference from the packaged regression is expected and intended.
    """
    candidates = [c for c in reference.columns if c.lower() in ("division", "ncaa_division")]
    if candidates:
        reference = reference[reference[candidates[0]] == 1]

    matched = 0
    worst = 0.0
    worst_label = ""
    running_worst = 0.0
    running_label = ""

    for _, row in division_rows.iterrows():
        match = reference[reference["year"] == row["year"]]
        if match.empty:
            continue
        match = match.iloc[0]
        matched += 1
        for column in ("w_1b", "w_hr", "w_bb", "w_hbp"):
            if column not in match or not match[column]:
                continue
            relative = abs(row[column] - match[column]) / abs(match[column])
            if relative > worst:
                worst, worst_label = relative, f"{int(row['year'])} {column}"
        for column in ("w_2b", "w_3b", "w_sb", "w_cs"):
            if column not in match or not match[column]:
                continue
            relative = abs(row[column] - match[column]) / abs(match[column])
            if relative > running_worst:
                running_worst, running_label = relative, f"{int(row['year'])} {column}"

    if matched == 0:
        gates.note("vendored constants", "no overlapping years")
        return

    gates.check("w_1b/w_hr/w_bb/w_hbp within 10% of the vendored fit",
                worst <= 0.10, f"worst {worst:.2%} at {worst_label}")
    gates.note("w_2b/w_3b/w_sb/w_cs vs the vendored fit (not gated -- see docstring)",
               f"worst {running_worst:.2%} at {running_label}")

    if "cwoba_scale" in reference.columns:
        scales = []
        for _, row in division_rows.iterrows():
            match = reference[reference["year"] == row["year"]]
            if not match.empty:
                scales.append(abs(row["woba_scale"] - match.iloc[0]["cwoba_scale"])
                              / match.iloc[0]["cwoba_scale"])
        if scales:
            gates.check("woba_scale within 5% of the vendored fit", max(scales) <= 0.05,
                        f"worst {max(scales):.2%}")


def run_from_disk(scope="conference"):
    """Re-run the gates against whatever is already in out/."""
    paths = {
        "batting": os.path.join(config.OUT_DIR, "batting_combined_all.csv"),
        "pitching": os.path.join(config.OUT_DIR, "pitching_combined_all.csv"),
    }
    missing = [p for p in paths.values() if not os.path.exists(p)]
    if missing:
        print(f"missing output: {missing}\nRun `python run.py` first.")
        return 1

    batting = pd.read_csv(paths["batting"])
    pitching = pd.read_csv(paths["pitching"])
    constants = pd.read_csv(os.path.join(config.REPORT_DIR, "league_constants.csv"))
    coverage = pd.read_csv(os.path.join(config.REPORT_DIR, "coverage.csv"))

    # The CSVs drop `conference` and `ncaa_team_id`, which the identity gates
    # need. Recover them from the coverage report, keyed on (team, YEAR).
    #
    # Keying on the acronym alone is wrong and was silently so: conference
    # realignment moves teams between leagues, so one row per acronym assigns
    # every season the conference of whichever year happened to sort first. That
    # went unnoticed while runs were single-year; building 2021-2025 together
    # surfaced it as five league groups with no constants row (C-USA, The
    # American and MEAC in 2022/2023) and broke the PA-weighted wOBA and wRC+
    # identities, which are computed per league group.
    lookup = (coverage.dropna(subset=["mapped_acronym"])
              .drop_duplicates(subset=["mapped_acronym", "year"])
              .set_index(["mapped_acronym", "year"]))
    for frame in (batting, pitching):
        keys = list(zip(frame["team"], frame["year"]))
        frame["conference"] = [lookup["conference"].get(k) for k in keys]
        frame["ncaa_team_id"] = [lookup["team_id"].get(k) for k in keys]

    return run(batting, pitching, constants, coverage, scope=scope)
