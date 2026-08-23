"""League-relative statistics: wOBA, wRAA, wRC, wRC+, wSB.

=============================================================================
THESE VALUES DO NOT EQUAL FANGRAPHS' AND THE MODEL MUST BE RETRAINED.
=============================================================================

The columns computed here -- plus ``fip``, ``e-f``, and ``lob%`` in
``rates.py`` -- are the nine that change. FanGraphs' linear weights come from a
proprietary Markov model; ours come from a documented team-level runs regression
(``derive/constants.py``). Measured agreement against the existing FanGraphs
CSVs at division scope:

    woba   r = 0.993 - 0.995      wrc+   r = 0.977 - 0.984
    wsb    r = 0.85  - 0.94       spd    r = 0.81

and ``fip`` sat 0.30-0.35 higher at division scope. Conference scope should
narrow that, since it matches what FanGraphs actually does.

So ``xgboostAllWithTeamsV10.ipynb`` cannot be run against these features
unchanged -- ``fip_pitch``, ``woba_bat``, and ``wrc+_bat`` are all in
``PARTA_FEATURES``, and a level shift there is a silent distribution shift.
There is an upside: ``make_public_data.py`` currently drops exactly these nine
columns as ``FG_DERIVED_COLS`` because they were not redistributable, so a model
trained with NCAA-native versions is a strict information gain over the public
model.

Adapted from ``ncaa_bbStats.advanced_stats`` (upstream SHA 24b3050), with the
constants taken from our own scrape rather than the packaged table.
"""


def woba(counts, weights):
    """Weighted on-base average.

    Denominator excludes sacrifice hits -- see ``rates.batting_rates``.
    """
    denominator = counts["ab"] + counts["bb"] + counts["hbp"] + counts["sf"]
    if denominator <= 0:
        return None
    numerator = (
        weights["w_1b"] * counts["1b"]
        + weights["w_2b"] * counts["2b"]
        + weights["w_3b"] * counts["3b"]
        + weights["w_hr"] * counts["hr"]
        + weights["w_bb"] * counts["bb"]
        + weights["w_hbp"] * counts["hbp"]
    )
    return numerator / denominator


def wraa(woba_value, counts, constants):
    """Weighted runs above average."""
    if woba_value is None or counts["pa"] <= 0:
        return None
    scale = constants["woba_scale"]
    if not scale:
        return None
    return (woba_value - constants["lg_woba"]) / scale * counts["pa"]


def wrc(wraa_value, counts, constants):
    """Weighted runs created."""
    if wraa_value is None:
        return None
    return wraa_value + constants["lg_r_pa"] * counts["pa"]


def wrc_plus(wraa_value, counts, constants):
    """wRC+, indexed so the league is 100.

    Park factor is fixed at 1.0, which collapses FanGraphs' general form

        100 * [(wRAA/PA + lgR/PA) + (lgR/PA - PF*lgR/PA)] / (lgwRC/PA_nonpitcher)

    to the expression below. Two deviations, both deliberate and both documented
    in the README and the paper:

    * **PF = 1.0.** There is no public NCAA park-factor series. Consequence:
      hitters at extreme-altitude programs (Air Force, New Mexico, Utah Valley)
      are flattered.
    * **The denominator uses league R/PA over all hitters**, not non-pitchers,
      because NCAA two-way players make that split ambiguous.
    """
    if wraa_value is None or counts["pa"] <= 0:
        return None
    lg_r_pa = constants["lg_r_pa"]
    if not lg_r_pa:
        return None
    return 100.0 * (wraa_value / counts["pa"] + lg_r_pa) / lg_r_pa


def wsb(counts, constants):
    """Weighted stolen base runs.

    ``wsb = w_sb*SB + w_cs*CS - lgwSB*(1B + BB + HBP)``. This form was recovered
    exactly from the FanGraphs data (max error 0.0 across all 153
    conference-years), with runSB constant at 0.200 every year.
    """
    on_base = counts["1b"] + counts["bb"] + counts["hbp"]
    return (
        constants["w_sb"] * counts["sb"]
        + constants["w_cs"] * counts["cs"]
        - constants["lg_wsb"] * on_base
    )


def add_batting_advanced(counts, constants):
    """All five league-relative batting columns for one player-season."""
    woba_value = woba(counts, constants)
    wraa_value = wraa(woba_value, counts, constants)
    return {
        "woba": woba_value,
        "wraa": wraa_value,
        "wrc": wrc(wraa_value, counts, constants),
        "wrc+": wrc_plus(wraa_value, counts, constants),
        "wsb": wsb(counts, constants),
    }
