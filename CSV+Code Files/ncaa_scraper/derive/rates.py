"""Coercion and the pure-arithmetic rate statistics.

Everything here is reproducible from counting stats alone -- no league context,
no fitted weights. These are the columns that should match the FanGraphs export
to floating point, which makes them the correctness proof for the whole scrape
(see ``validate/against_fangraphs.py``).

The formulas were established by fitting against the existing FanGraphs CSVs, so
two of them are not the textbook version:

* **``obp`` and ``woba`` divide by ``ab + bb + hbp + sf``, excluding SH**, while
  ``pa`` includes SH. Fitting wOBA against ``pa`` leaves a residual correlating
  -0.607 with ``sh``; excluding SH makes it exact.
* **Pitching ``k%`` and ``bb%`` divide by ``tbf``**, not by an innings-derived
  estimate, and opponent ``avg`` is ``h / (tbf - bb - hbp)``.

**Blank means zero.** NCAA renders a zero counting stat as an empty cell -- Tre
Jones' 2022 row has ``CS=''`` and ``IBB=''`` and he certainly had zero of each.
Only ``class`` keeps null when blank, since there a blank really is unknown.
"""

import math

import numpy as np

import config


def to_int(value, default=0):
    """Coerce an NCAA cell to int, treating blank/dash as ``default`` (0)."""
    if value is None:
        return default
    text = str(value).strip().replace(",", "")
    if text in ("", "-"):
        return default
    try:
        return int(float(text))
    except ValueError:
        return default


def to_float(value, default=None):
    if value is None:
        return default
    text = str(value).strip().replace(",", "")
    if text in ("", "-"):
        return default
    try:
        return float(text)
    except ValueError:
        return default


def ip_to_float(value):
    """Convert NCAA innings notation to true innings.

    NCAA writes partial innings as tenths: ``97.1`` is 97 innings and one out,
    ``97.2`` is two outs. Treating it as a decimal understates outs and inflates
    every innings-denominated rate -- ERA, WHIP, K/9, FIP, all of them.

    Adapted from ``ncaa_bbStats.advanced_stats.ip_to_float`` (upstream SHA
    24b3050).

    Returns:
        float | None: true innings (``97.667``), or None if unreadable.
    """
    if value is None:
        return None
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    if value != value:  # NaN
        return None
    whole = int(value)
    tenths = round(value - whole, 1)
    if tenths == 0.1:
        return whole + 1.0 / 3.0
    if tenths == 0.2:
        return whole + 2.0 / 3.0
    return float(whole) if tenths == 0.0 else value


def _safe(numerator, denominator):
    """Divide, returning None rather than inf/NaN when the denominator is empty.

    FanGraphs' own export carries `inf` in 2.1-3.4% of pitching rate rows. That
    is a defect worth not reproducing: `inf` propagates silently through a model,
    while None is visibly missing.
    """
    if denominator is None or numerator is None:
        return None
    if denominator == 0:
        return None
    result = numerator / denominator
    if isinstance(result, float) and (math.isinf(result) or math.isnan(result)):
        return None
    return result


# ---------------------------------------------------------------------------
# Batting
# ---------------------------------------------------------------------------

BATTING_COUNTS = ("g", "ab", "h", "2b", "3b", "hr", "r", "rbi", "bb", "so",
                  "hbp", "sf", "sh", "sb", "cs")


def absent_columns(record, year):
    """Columns that must stay None for this row, rather than becoming zero.

    Two independent reasons a value can be missing, and neither is a zero:

    * **Year-level** -- the source never published the column that season.
      ``gdp`` is the original case: it is not on the 2021 NCAA grid, and the 2021
      bulk mirror carries a ``GDP`` column that is 0% nonzero. 2026's source
      generation dropped it too, along with the W/L/SV/CG/SHO decisions.
    * **Row-level** -- the adapter knows this particular row has no source for a
      column even though the season generally does. Batting ``g`` for the seven
      2022 teams patched in from the legacy mirror is the case: that mirror's
      batting ``G`` is *team* games (constant within school for all 300 schools),
      and those teams are absent from the roster file that supplies real
      per-player games.

    ``to_int`` defaults to 0, which is right for a blank cell in an NCAA grid and
    wrong for either of these. Without this, the seven patched teams shipped
    ``g=0`` and dragged the whole year's mean games down by 0.76 -- caught by the
    per-column bias gate, which is exactly what it is for.
    """
    absent = {name for name in ("gdp",) if year in config.GDP_ABSENT_YEARS}
    if year in config.PITCH_DECISIONS_ABSENT_YEARS:
        absent.update(config.PITCH_DECISION_COLUMNS)
    absent.update(record.get("_absent") or ())
    return absent


def batting_counts(record, year):
    """Coerce one parsed batting row to integer counting stats."""
    absent = absent_columns(record, year)
    out = {name: None if name in absent else to_int(record.get(name))
           for name in BATTING_COUNTS}
    out["gdp"] = None if "gdp" in absent else to_int(record.get("gdp"))
    # These two are computed, so they are only as available as their inputs.
    out["1b"] = (None if any(out[c] is None for c in ("h", "2b", "3b", "hr"))
                 else out["h"] - out["2b"] - out["3b"] - out["hr"])
    out["pa"] = (None if any(out[c] is None
                             for c in ("ab", "bb", "hbp", "sf", "sh"))
                 else out["ab"] + out["bb"] + out["hbp"] + out["sf"] + out["sh"])
    return out


def batting_rates(c):
    """The ten pure-arithmetic batting rates. All verified exact vs FanGraphs."""
    # The wOBA/OBP denominator: PA excluding sacrifice hits.
    pa_woba = c["ab"] + c["bb"] + c["hbp"] + c["sf"]

    avg = _safe(c["h"], c["ab"])
    obp = _safe(c["h"] + c["bb"] + c["hbp"], pa_woba)
    slg = _safe(c["1b"] + 2 * c["2b"] + 3 * c["3b"] + 4 * c["hr"], c["ab"])

    return {
        "avg": avg,
        "obp": obp,
        "slg": slg,
        "ops": None if obp is None or slg is None else obp + slg,
        "iso": None if slg is None or avg is None else slg - avg,
        "bb%": _safe(c["bb"], c["pa"]),
        "k%": _safe(c["so"], c["pa"]),
        "bb/k": _safe(c["bb"], c["so"]),
        "babip": _safe(c["h"] - c["hr"], c["ab"] - c["so"] - c["hr"] + c["sf"]),
        "_pa_woba": pa_woba,
    }


def speed_score(c):
    """Bill James speed score.

    Four of the five published factors. The fifth is the GDP-avoidance factor,
    omitted because ``GDP`` is absent from the 2021 NCAA grid -- dropping it
    uniformly keeps ``spd`` comparable across all years rather than silently
    changing definition at the 2021/2022 boundary. ``ncaa_bbStats.cspd`` makes
    the same choice.

    Each factor is clipped to [0, 10] and the result averages the computable
    ones. Note the constants were fitted to MLB, so the NCAA population centres
    near 3.9 rather than James' intended 5.0; agreement with FanGraphs' `spd` is
    r=0.81, the weakest of any column here. All inputs are retained in the output
    so a modeller can prefer them over the composite.
    """
    factors = []

    attempts = c["sb"] + c["cs"]
    on_base = c["1b"] + c["bb"] + c["hbp"]

    factors.append(((c["sb"] + 3) / (attempts + 7) - 0.4) * 20)

    if on_base > 0:
        factors.append(math.sqrt(attempts / on_base) / 0.07)

    triple_opportunities = c["ab"] - c["hr"] - c["so"]
    if triple_opportunities > 0:
        factors.append((c["3b"] / triple_opportunities) / 0.02)

    scoring_chances = c["h"] + c["bb"] + c["hbp"] - c["hr"]
    if scoring_chances > 0:
        factors.append(((c["r"] - c["hr"]) / scoring_chances - 0.1) / 0.04)

    if not factors:
        return None
    return float(np.mean([min(max(f, 0.0), 10.0) for f in factors]))


# ---------------------------------------------------------------------------
# Pitching
# ---------------------------------------------------------------------------

PITCHING_COUNTS = ("g", "gs", "cg", "sho", "sv", "w", "l", "h", "r", "er",
                   "bb", "so", "hr", "hbp", "wp", "bk", "tbf")


def pitching_counts(record, year=None):
    """Coerce one parsed pitching row, keeping ``ip`` in NCAA notation.

    ``ip`` is emitted as reported (``97.2`` = 97 2/3) to match the existing
    schema, and ``ip_true`` carries the real value for every computation.

    ``year`` is optional so the live path's existing call site keeps working; the
    absence rules only apply to sources that declare them.
    """
    absent = absent_columns(record, year) if year is not None else set()
    out = {name: None if name in absent else to_int(record.get(name))
           for name in PITCHING_COUNTS}
    out["ip"] = to_float(record.get("ip"), default=0.0)
    out["ip_true"] = ip_to_float(out["ip"]) or 0.0
    out["p_oab"] = to_int(record.get("p_oab"))
    return out


def _left_on_base(c):
    """LOB% = (H + BB + HBP - R) / (H + BB + HBP - 1.4*HR), capped above at 1.0.

    Capped, not clamped, and the asymmetry is the point.

    **Above 1.0 is an artefact.** A pitcher cannot strand more runners than
    reached base against him, so values over 1 come from the 1.4*HR term in the
    denominator rather than from anything that happened. FanGraphs caps the same
    way: 254 rows in their file sit at exactly 1.0 where the raw expression gives
    1.05 to 1.36.

    **Below 0.0 is real.** A reliever can be charged with more runs than he put
    on base, because inherited runners score -- Jack Lang, 2021: 7 baserunners
    allowed, 8 runs charged, LOB% -0.357. FanGraphs keeps those negative and so
    do we. Flooring at zero would erase a genuine signal about relief usage.
    (NCAA also publishes `Inh Run` and `Inh Run Score`, so the effect is
    measurable directly if it ever matters downstream.)

    A non-positive denominator means effectively nobody reached base, so everyone
    who did was stranded: 1.0, matching FanGraphs, rather than None.
    """
    denominator = c["h"] + c["bb"] + c["hbp"] - 1.4 * c["hr"]
    if denominator <= 0:
        return 1.0
    value = (c["h"] + c["bb"] + c["hbp"] - c["r"]) / denominator
    return min(value, 1.0)


def pitching_rates(c, cfip):
    """The pitching rates. ``cfip`` is the per-league FIP constant.

    Opponent AB is FanGraphs' identity ``tbf - bb - hbp`` rather than NCAA's real
    ``P-OAB`` column, for schema compatibility -- the identity reproduces
    FanGraphs at 99.5%. ``P-OAB`` is carried into the coverage report as a
    diagnostic, since it is the better number and a future schema revision
    should prefer it.
    """
    ip_true = c["ip_true"]
    opp_ab = c["tbf"] - c["bb"] - c["hbp"]

    era = _safe(9 * c["er"], ip_true)
    fip_core = _safe(13 * c["hr"] + 3 * (c["bb"] + c["hbp"]) - 2 * c["so"], ip_true)
    fip = None if fip_core is None or cfip is None else fip_core + cfip

    k_pct = _safe(c["so"], c["tbf"])
    bb_pct = _safe(c["bb"], c["tbf"])

    lob = _left_on_base(c)

    return {
        "era": era,
        "whip": _safe(c["h"] + c["bb"], ip_true),
        "k/9": _safe(9 * c["so"], ip_true),
        "bb/9": _safe(9 * c["bb"], ip_true),
        "hr/9": _safe(9 * c["hr"], ip_true),
        "k/bb": _safe(c["so"], c["bb"]),
        "k%": k_pct,
        "bb%": bb_pct,
        "k-bb%": None if k_pct is None or bb_pct is None else k_pct - bb_pct,
        "avg": _safe(c["h"], opp_ab),
        "babip": _safe(c["h"] - c["hr"], c["tbf"] - c["so"] - c["hr"] - c["bb"] - c["hbp"]),
        "lob%": lob,
        "fip": fip,
        "e-f": None if era is None or fip is None else era - fip,
    }
