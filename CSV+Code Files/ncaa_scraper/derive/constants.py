"""League constants, fitted from our own scrape.

Replicates the method in ``ncaa_bbStats/tools/build_league_constants.py``, but
sourced from the player rows this folder scraped rather than from NCAA team
statistics, so the constants describe exactly the population we emit.

**The regression form is load-bearing.** It fits *runs per PA on event rates*
with an intercept, which makes **outs the omitted category** and each coefficient
the marginal runs from turning one out into that event -- the runs-above-out
quantity wOBA needs. Fitting event *counts* with no intercept instead yields
coefficients that predict total runs (beta_1b ~ 0.25 rather than ~0.77), which
silently inflates ``woba_scale`` by about 2x and produces a wOBA that looks
plausible and is wrong. Validated against the packaged constants: this form
reproduces their r-squared to four decimals (0.9732 vs 0.9733 for 2021 D1).

**Why per conference.** FanGraphs' college wOBA uses the *conference* as the
league, not the division. This is not a guess: in the existing FanGraphs CSVs
``(wrc - wraa)/pa`` is constant to 1e-17 within a team and takes exactly 31
distinct values across 2021 D1, and those 31 groups are the conferences
(WCC=0.133536, Southland=0.137083, Big East=0.137275, ...). Their cFIP behaves
the same way and reproduces from conference totals to five decimals.

**The problem with per conference, and the fix.** A conference has ~10 teams
against 8 predictors, so an unshrunk per-conference regression is mostly noise.
Every conference fit is therefore shrunk toward its division-year fit, which is
shrunk toward a division-pooled fit across all years. Shrinkage happens *before*
the monotone-hits constraint, because a shrunk noisy fit can cross.

**Two documented deviations from FanGraphs.** Park factor is fixed at 1.0 (no
public NCAA park-factor series exists, and inventing one would be
unfalsifiable), and the wRC+ denominator uses league R/PA over all hitters
rather than non-pitchers (NCAA two-way players make that split ambiguous). Both
belong in the paper, not just here.
"""

import numpy as np
import pandas as pd

# The events the run-value regression fits, in a fixed order.
EVENTS = ["1b", "2b", "3b", "hr", "bb", "hbp", "sb", "cs"]
# Only these enter wOBA; SB/CS are used for wSB.
WOBA_EVENTS = ["1b", "2b", "3b", "hr", "bb", "hbp"]

# Run value of a stolen base. The accepted constant, and confirmed fixed at this
# value across every season of the FanGraphs data. Not fitted -- see the comment
# in _make_record.
RUN_SB = 0.200

# Event weights are ALWAYS taken from the division fit, never fitted per
# conference. See `_CONFERENCE_FIT_IS_DEGENERATE` below for why.
#
# Set True only to reproduce the broken behaviour for comparison.
FIT_WEIGHTS_PER_CONFERENCE = False

_CONFERENCE_FIT_IS_DEGENERATE = """
Why conference-level event weights are not fitted.

The regression has 9 parameters (8 events + intercept). A conference has 10-17
teams in a season, so the fit has 1-8 residual degrees of freedom. Measured on
2025 D1 with per-conference fitting enabled, the results are not merely noisy,
they are impossible:

    conference   w_bb     w_hbp    w_hr     r2
    MAC          -0.150    2.363    2.909   0.998
    SWAC          1.522   -0.594    1.210   0.992
    NEC           0.291    2.681    1.637   0.997
    division      0.740    0.736    1.984   0.961    <- 292 teams

A walk cannot have negative run value and a hit-by-pitch cannot be worth more
than a home run. The r-squared values above 0.99 are the tell: they are
saturation, not fit quality. Worse, saturation collapses the standard errors,
which drives the empirical-Bayes weight lambda = tau2/(tau2+SE2) toward 1 -- so
the shrinkage that was supposed to rescue these fits does almost nothing exactly
where it is needed. Applied to real players this produced wOBA of 0.659 for a
.305/.465/.476 hitter.

BB and HBP are the worst affected because they are collinear with each other and
with the hit terms; ten observations cannot separate them.

This is not fixable with more data: no NCAA conference has enough teams in a
season, and pooling a conference across 2021-2026 gives only ~60-100
team-seasons, still thin for 9 parameters and no longer season-specific.

What we do instead: take the event weights from the division fit, which has
~300 team-seasons, and compute per (year, conference) only the quantities that
genuinely vary and are estimated from totals rather than a regression --
lg_obp, lg_r_pa, cfip, lg_wsb, and the woba_scale that anchors lgwOBA to lgOBP.

That is also what the FanGraphs data supports. Their per-conference weight
*shapes* are near-constant across 153 conference-years (2B = 1.330 +/- 0.021,
BB = 0.8295 +/- 0.011, HBP = 0.857 +/- 0.009); what varies between the SEC and
the MAC is the run environment, not the run value of a double. The
per-conference scale is preserved, because woba_scale is recomputed per
conference from that conference's own OBP.
"""

# Hit types in strictly increasing order of run value: a home run is a triple
# plus a guaranteed run, so its value cannot be lower, and likewise down the
# chain.
MONOTONE_CHAIN = ("1b", "2b", "3b", "hr")

# Below this many teams, a conference borrows the division's scalars (lg_obp,
# lg_r_pa, woba_scale, cfip) rather than computing its own. Four is the smallest
# real D1 conference in 2021-2025; every group under it is a one-team artefact --
# an independent program, or the Ivy League in the season it did not play.
MIN_TEAMS_FOR_OWN_SCALARS = 4


def _weighted_lstsq(frame):
    """PA-weighted least squares of **runs per PA on event rates**.

    The intercept matters: with events expressed as fractions of PA, **outs are
    the omitted category**, so each coefficient is the marginal runs from turning
    one out into that event -- the runs-above-out quantity wOBA needs. The
    intercept itself is a nuisance term and is not used.

    Fitting counts with no intercept instead gives coefficients that predict
    total runs (beta_1b ~ 0.25) rather than run values above an out
    (beta_1b ~ 0.77), which then inflates ``woba_scale`` by roughly 2x. This
    matches ``ncaa_bbStats/tools/build_league_constants.py``.

    Returns:
        (beta, stderr, r2, rmse, n) with beta/stderr as dicts keyed by event, or
        ``(None, None, nan, nan, n)`` if the system is underdetermined.
    """
    n = len(frame)
    if n < len(EVENTS) + 2:  # +1 for the intercept, +1 for a residual dof
        return None, None, float("nan"), float("nan"), n

    pa = frame["pa"].to_numpy(dtype=float)
    if (pa <= 0).any():
        keep = pa > 0
        frame, pa = frame[keep], pa[keep]
        n = len(frame)
        if n < len(EVENTS) + 2:
            return None, None, float("nan"), float("nan"), n

    target = frame["r"].to_numpy(dtype=float) / pa
    design = np.column_stack(
        [np.ones(n)] + [frame[event].to_numpy(dtype=float) / pa for event in EVENTS]
    )

    weights = pa / pa.mean()
    root = np.sqrt(weights)
    design_w, target_w = design * root[:, None], target * root

    coefficients, *_ = np.linalg.lstsq(design_w, target_w, rcond=None)

    residual = target_w - design_w @ coefficients
    dof = n - design.shape[1]
    try:
        sigma2 = float(residual @ residual) / dof
        covariance = sigma2 * np.linalg.inv(design_w.T @ design_w)
        standard_errors = np.sqrt(np.diag(covariance))
    except np.linalg.LinAlgError:
        standard_errors = np.full(design.shape[1], float("nan"))

    # Report fit quality in runs, which is interpretable, not in runs per PA.
    predicted_runs = (design @ coefficients) * pa
    actual_runs = target * pa
    ss_residual = float(((actual_runs - predicted_runs) ** 2).sum())
    ss_total = float(((actual_runs - actual_runs.mean()) ** 2).sum())
    r2 = 1.0 - ss_residual / ss_total if ss_total else float("nan")
    rmse = float(np.sqrt(ss_residual / n))

    beta = {event: float(coefficients[i + 1]) for i, event in enumerate(EVENTS)}
    stderr = {event: float(standard_errors[i + 1]) for i, event in enumerate(EVENTS)}
    return beta, stderr, r2, rmse, n


def _between_group_variance(fits):
    """Per-event true between-group variance (tau squared) from sibling fits.

    Splits the observed spread of each coefficient across sibling groups into a
    true signal component and the sampling noise implied by the within-group
    standard errors. What survives removing the noise is tau squared, which
    :func:`_shrink` turns into a per-event weight. A negative result means the
    spread sits below the noise floor -- no detectable real variation -- so the
    group collapses fully to its parent.

    With fewer than two siblings there is nothing to estimate from, so tau
    squared is zero and every group inherits its parent. That is the right
    behaviour for a single-year run, where the year fit and the pooled fit are
    the same fit anyway.
    """
    tau2 = {}
    for event in EVENTS:
        estimates = np.array([f[0][event] for f in fits if f[0] is not None])
        variances = np.array([f[1][event] ** 2 for f in fits if f[1] is not None])
        if len(estimates) < 2 or not np.isfinite(variances).all():
            tau2[event] = 0.0
            continue
        tau2[event] = max(0.0, float(estimates.var(ddof=1) - variances.mean()))
    return tau2


def _shrink(beta, stderr, parent, tau2):
    """Empirical-Bayes shrink one fit toward ``parent``."""
    if beta is None:
        return dict(parent), {event: 0.0 for event in EVENTS}

    shrunk = {}
    used = {}
    for event in EVENTS:
        se2 = (stderr or {}).get(event, float("nan")) ** 2
        t2 = tau2.get(event, 0.0)
        if not np.isfinite(se2) or (t2 + se2) <= 0:
            weight = 0.0
        else:
            weight = t2 / (t2 + se2)
        shrunk[event] = weight * beta[event] + (1.0 - weight) * parent[event]
        used[event] = weight
    return shrunk, used


def _enforce_monotone_hits(beta, stderr):
    """Project the hit-type run values onto the order physics requires.

    An unconstrained fit routinely puts the triple above the home run: triples
    occur in well under 1% of plate appearances, so their coefficient is the
    noisiest in the model and absorbs rally context belonging elsewhere. Left
    alone it overrates triples-heavy hitters in every downstream metric.

    Pool-adjacent-violators (isotonic regression) weighted by inverse variance,
    so a violation is resolved by moving the less precisely estimated
    coefficient further. Coefficients already in order are untouched. Applied
    *after* shrinkage, since a shrunk fit can still cross.
    """
    blocks = []
    for event in MONOTONE_CHAIN:
        se = (stderr or {}).get(event, 1.0)
        weight = 1.0 / (se * se) if se and np.isfinite(se) and se > 0 else 1.0
        blocks.append([beta[event], weight, [event]])

    i = 0
    while i < len(blocks) - 1:
        if blocks[i][0] <= blocks[i + 1][0]:
            i += 1
            continue
        value_a, weight_a, events_a = blocks[i]
        value_b, weight_b, events_b = blocks[i + 1]
        pooled = (value_a * weight_a + value_b * weight_b) / (weight_a + weight_b)
        blocks[i:i + 2] = [[pooled, weight_a + weight_b, events_a + events_b]]
        i = max(i - 1, 0)

    out = dict(beta)
    for value, _weight, events in blocks:
        for event in events:
            out[event] = value
    return out


def team_totals(batting: pd.DataFrame) -> pd.DataFrame:
    """Aggregate player rows to one row per (year, conference, team)."""
    columns = EVENTS + ["r", "pa", "ab", "h", "so", "sf", "sh", "gdp"]
    present = [c for c in columns if c in batting.columns]
    return (
        batting.groupby(["year", "conference", "ncaa_team_id"], dropna=False)[present]
        .sum(min_count=1)
        .reset_index()
    )


def _aggregates(batting_group, pitching_group):
    """League totals and the scalars derived from them, for one group."""
    b = batting_group[[c for c in EVENTS + ["r", "ab", "h", "so", "sf", "sh", "pa"]
                       if c in batting_group.columns]].sum()

    pa_woba = b["ab"] + b["bb"] + b["hbp"] + b["sf"]
    pa_total = pa_woba + b["sh"]

    aggregates = {
        "lg_pa_woba": float(pa_woba),
        "lg_pa": float(pa_total),
        "lg_obp": float((b["h"] + b["bb"] + b["hbp"]) / pa_woba) if pa_woba else float("nan"),
        "lg_r_pa": float(b["r"] / pa_total) if pa_total else float("nan"),
    }
    for event in EVENTS:
        aggregates[f"lg_{event}"] = float(b[event])
    aggregates["lg_r"] = float(b["r"])

    # Pitching side: innings must be summed in TRUE innings, never in NCAA
    # notation, or the FIP constant is wrong by several percent.
    ip_true = float(pitching_group["ip_true"].sum())
    er = float(pitching_group["er"].sum())
    aggregates["lg_ip_true"] = ip_true
    aggregates["lg_era"] = 9.0 * er / ip_true if ip_true else float("nan")

    # Runs per out, for the caught-stealing run value. GDP is absent from the
    # 2021 grid, so it is simply omitted there rather than assumed zero -- it
    # shifts outs by well under 1%.
    gdp = float(b["gdp"]) if "gdp" in b.index and b["gdp"] == b["gdp"] else 0.0
    outs = float(b["ab"] - b["h"] + b["sh"] + b["sf"] + b["cs"]) + gdp
    aggregates["lg_outs"] = outs
    aggregates["lg_r_out"] = float(b["r"]) / outs if outs else float("nan")

    if ip_true:
        aggregates["cfip"] = aggregates["lg_era"] - (
            13.0 * float(pitching_group["hr"].sum())
            + 3.0 * (float(pitching_group["bb"].sum()) + float(pitching_group["hbp"].sum()))
            - 2.0 * float(pitching_group["so"].sum())
        ) / ip_true
    else:
        aggregates["cfip"] = float("nan")

    return aggregates


def build(batting: pd.DataFrame, pitching: pd.DataFrame, scope: str = "conference"):
    """Fit league constants.

    Args:
        batting: player-level batting rows with counting stats, ``conference``,
            ``ncaa_team_id``, and ``year``.
        pitching: player-level pitching rows including ``ip_true``.
        scope: ``"conference"`` (matches FanGraphs) or ``"division"``.

    Returns:
        DataFrame, one row per league group, with the scaled weights, the
        anchoring scalars, ``cfip``, and fit diagnostics. The ``division`` rows
        are always included even under conference scope -- gate 8 and gate 14 in
        ``validate/acceptance.py`` check against them, and they are the shrinkage
        parent.
    """
    teams = team_totals(batting)

    # Level 3: pooled across all years of this division.
    pooled = _weighted_lstsq(teams)
    if pooled[0] is None:
        raise ValueError(
            f"only {len(teams)} team-seasons -- not enough to fit a pooled "
            f"regression on {len(EVENTS)} events plus an intercept. Scrape more "
            f"teams (a full season is ~300)."
        )
    pooled_beta = _enforce_monotone_hits(pooled[0], pooled[1])

    # Level 2: one fit per division-year, shrunk toward pooled. Estimating tau2
    # needs the whole set of sibling fits, so fit them all before shrinking any.
    division_fits = {year: _weighted_lstsq(group) for year, group in teams.groupby("year")}
    division_tau2 = _between_group_variance(list(division_fits.values()))

    records = []

    for year, year_teams in teams.groupby("year"):
        beta, stderr, r2, rmse, n = division_fits[year]
        division_beta, division_lambda = _shrink(beta, stderr, pooled_beta, division_tau2)
        division_beta = _enforce_monotone_hits(division_beta, stderr)

        year_batting = batting[batting["year"] == year]
        year_pitching = pitching[pitching["year"] == year]

        records.append(
            _make_record(
                scope_level="division", year=year, group="ALL",
                beta=division_beta, n_teams=len(year_teams),
                r2=r2, rmse=rmse,
                batting_group=year_batting, pitching_group=year_pitching,
                shrunk_to="pooled", lambdas=division_lambda,
            )
        )

        if scope != "conference":
            continue

        # Per conference: reuse the division event weights and recompute only the
        # scalars, which come from league totals rather than a regression.
        # _CONFERENCE_FIT_IS_DEGENERATE explains why the weights are not fitted
        # here; FIT_WEIGHTS_PER_CONFERENCE exists only to reproduce that failure.
        conference_groups = dict(list(year_teams.groupby("conference")))

        conference_fits = {}
        conference_tau2 = {event: 0.0 for event in EVENTS}
        if FIT_WEIGHTS_PER_CONFERENCE:
            conference_fits = {
                conference: _weighted_lstsq(group)
                for conference, group in conference_groups.items()
            }
            fittable = [f for f in conference_fits.values() if f[0] is not None]
            if len(fittable) >= 2:
                conference_tau2 = _between_group_variance(fittable)

        for conference, group in conference_groups.items():
            # A league average estimated from too few teams is not a league
            # average. With one team it is that team's own rate, which makes its
            # wRC+ trivially 100 and every wOBA-relative column meaningless.
            # Measured: DI Independent 2023 (one team) produced lg_r_pa 0.0948
            # against a 0.15-0.19 range across real conferences, with woba_scale
            # 1.244 and cfip 5.667 similarly out of family. Ivy League 2021 is the
            # same shape, because that season was cancelled and only one program
            # has data.
            #
            # Such groups take the division's scalars instead. The team keeps its
            # own counting stats and rates; only the league context it is measured
            # against changes, from a sample of one to the whole division.
            if len(group) < MIN_TEAMS_FOR_OWN_SCALARS:
                batting_slice = year_batting
                pitching_slice = year_pitching
                scalar_note = (f"division scalars (only {len(group)} team(s) in "
                               f"{conference})")
            else:
                batting_slice = year_batting[year_batting["conference"] == conference]
                pitching_slice = year_pitching[year_pitching["conference"] == conference]
                scalar_note = None

            if FIT_WEIGHTS_PER_CONFERENCE:
                beta, stderr, r2, rmse, _n = conference_fits[conference]
                weights, lambdas = _shrink(beta, stderr, division_beta, conference_tau2)
                weights = _enforce_monotone_hits(weights, stderr)
                shrunk_to = "division" if beta is not None else "division (no fit)"
            else:
                weights = dict(division_beta)
                lambdas = {event: 0.0 for event in EVENTS}
                r2 = rmse = float("nan")
                shrunk_to = "division weights (conference fit is degenerate)"

            records.append(
                _make_record(
                    scope_level="conference", year=year, group=conference,
                    beta=weights, n_teams=len(group), r2=r2, rmse=rmse,
                    batting_group=batting_slice,
                    pitching_group=pitching_slice,
                    shrunk_to=(f"{shrunk_to}; {scalar_note}" if scalar_note
                               else shrunk_to),
                    lambdas=lambdas,
                )
            )

    return pd.DataFrame(records)


def _make_record(*, scope_level, year, group, beta, n_teams, r2, rmse,
                 batting_group, pitching_group, shrunk_to, lambdas=None):
    """Anchor one group's weights to its OBP and package the row.

    The anchoring identity is ``lgwOBA == lgOBP`` -- confirmed against the
    FanGraphs data at division level (2021: 0.36445 both sides). Scaling the raw
    regression weights by ``lgOBP / rawLgWOBA`` makes it true by construction,
    which is also what makes gate 9 a real test of the pipeline.
    """
    aggregates = _aggregates(batting_group, pitching_group)
    beta_by_event = dict(beta)

    raw_woba_numerator = sum(
        beta_by_event[event] * aggregates[f"lg_{event}"] for event in WOBA_EVENTS
    )
    raw_lg_woba = raw_woba_numerator / aggregates["lg_pa_woba"] if aggregates["lg_pa_woba"] else float("nan")
    scale = aggregates["lg_obp"] / raw_lg_woba if raw_lg_woba else float("nan")

    weights = {f"w_{event}": scale * beta_by_event[event] for event in EVENTS}

    # Baserunning uses published run values, NOT the regression coefficients, and
    # is NOT multiplied by woba_scale.
    #
    # Two reasons. First, wSB is expressed in runs, while the wOBA weights are
    # rescaled onto the OBP scale -- mixing the two units is simply wrong.
    # Second, a steal is a rare event and its regression coefficient is
    # correspondingly unstable: the fitted value came out ~0.27 against the
    # accepted 0.200, and caught-stealing ~-0.35 against a run-environment value
    # near -0.55. Reverse-engineering the FanGraphs files showed runSB fixed at
    # 0.200 every season with runCS varying by conference, which is exactly the
    # standard formulation rather than anything fitted.
    #
    # runCS = -(2 * R/Out + 0.075) is the conventional expression; R/Out carries
    # the conference's run environment, which is the part that should vary.
    run_sb = RUN_SB
    r_out = aggregates["lg_r_out"]
    run_cs = -(2.0 * r_out + 0.075) if r_out == r_out else float("nan")
    weights["w_sb"] = run_sb
    weights["w_cs"] = run_cs

    on_base = aggregates["lg_1b"] + aggregates["lg_bb"] + aggregates["lg_hbp"]
    lg_wsb = (
        (run_sb * aggregates["lg_sb"] + run_cs * aggregates["lg_cs"]) / on_base
        if on_base else float("nan")
    )

    return {
        "scope": scope_level,
        "year": int(year),
        "league": group,
        "n_teams": int(n_teams),
        **weights,
        "woba_scale": scale,
        "lg_woba": aggregates["lg_obp"],   # identity, by construction
        "lg_obp": aggregates["lg_obp"],
        "lg_r_pa": aggregates["lg_r_pa"],
        # Exposed so runCS = -(2*lg_r_out + 0.075) is checkable from the file.
        "lg_r_out": aggregates["lg_r_out"],
        "lg_outs": aggregates["lg_outs"],
        "lg_wsb": lg_wsb,
        "cfip": aggregates["cfip"],
        "lg_era": aggregates["lg_era"],
        "lg_ip_true": aggregates["lg_ip_true"],
        "lg_pa": aggregates["lg_pa"],
        "lg_pa_woba": aggregates["lg_pa_woba"],
        "r2": r2,
        "rmse_runs": rmse,
        "shrunk_toward": shrunk_to,
        **{f"lambda_{event}": (lambdas or {}).get(event) for event in EVENTS},
    }


def lookup(constants: pd.DataFrame, scope: str):
    """Index the constants table for row-wise application.

    Returns a dict keyed ``(year, league)`` under conference scope or
    ``(year, "ALL")`` under division scope.
    """
    level = "conference" if scope == "conference" else "division"
    subset = constants[constants["scope"] == level]
    return {(row["year"], row["league"]): row for _, row in subset.iterrows()}
