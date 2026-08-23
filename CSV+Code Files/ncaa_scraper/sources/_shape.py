"""Row shaping and the invariants worth crashing over.

The adapters all end up doing the same three risky things -- re-encoding innings,
deciding whether a blank is a zero or an absence, and asserting that a column the
mapping table names actually exists -- so they are written once, here, with the
reasoning attached.
"""

import math

import pandas as pd

import config

# ---------------------------------------------------------------------------
# Innings pitched: the highest-severity trap in the whole build.
#
# NCAA reports innings in thirds notation, where 97.2 means 97 and two thirds.
# `derive/rates.py::ip_to_float` expects exactly that and converts it. But the
# two mirror generations disagree:
#
#   rich   (2022-2024)  0.0 / 0.1 / 0.2        -> NCAA notation, pass through
#   lean   (2025-2026)  0.0 / 0.333 / 0.667    -> true innings, must re-encode
#   legacy (2021)       0.0 / 0.333 / 0.667    -> true innings, must re-encode
#
# Feeding true innings to `ip_to_float` does not error -- it reads .667 as 6.67
# thirds and returns a number that is wrong by a factor of two, silently
# corrupting ERA, WHIP, K/9, BB/9, HR/9, LOB%, FIP, E-F and the league `cfip`
# constant that FIP depends on. Nothing downstream would catch it, because every
# derived value would be internally consistent.
#
# So each adapter declares its source format as a constant and asserts it on
# load, and asserts NCAA notation on the way out.
# ---------------------------------------------------------------------------

NCAA_NOTATION = "ncaa_thirds"
TRUE_INNINGS = "true_innings"

# Thirds notation only ever has .0, .1 or .2 after the point.
_NCAA_FRACTIONS = {0, 1, 2}
# True innings land on 0, 1/3 or 2/3. The lean files are pre-summed floats, so
# they carry accumulated error (.332, .666, .668 all appear); allow a tolerance.
_TRUE_FRACTIONS = (0.0, 1.0 / 3.0, 2.0 / 3.0)
_TRUE_TOLERANCE = 0.02


def _fractions(values: pd.Series) -> pd.Series:
    """Fractional parts, with float drift folded back to zero.

    The lean and legacy mirrors store pre-summed thirds as floats, and the drift
    accumulates: 2026 contains innings recorded as 26.991999, 23.994 and 32.993,
    which are 27, 24 and 33. Those floor to the integer below with a fraction just
    under 1.0, which is the same quantity as 0.0 rather than a fourth notation.

    The fold threshold is derived from the same tolerance the classifier uses, so
    the two cannot disagree -- an earlier hardcoded 0.995 was tighter than the
    0.02 tolerance and rejected six rows in 74,209 for being 0.008 off an integer.
    """
    fractions = (values - values.apply(math.floor)).round(3)
    return fractions.where(fractions < 1.0 - _TRUE_TOLERANCE, 0.0)


def classify_ip(series: pd.Series):
    """Guess a series' innings format from its fractional parts."""
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty:
        return None
    fractions = _fractions(values)
    if fractions.map(lambda f: int(round(f * 10)) in _NCAA_FRACTIONS
                     and abs(f * 10 - round(f * 10)) < 1e-6).all():
        return NCAA_NOTATION
    if fractions.map(lambda f: any(abs(f - t) <= _TRUE_TOLERANCE
                                   for t in _TRUE_FRACTIONS)).all():
        return TRUE_INNINGS
    return None


def assert_ip_format(series: pd.Series, expected: str, *, where: str) -> None:
    actual = classify_ip(series)
    if actual != expected:
        values = pd.to_numeric(series, errors="coerce").dropna()
        fractions = sorted(_fractions(values).unique())[:12]
        raise AssertionError(
            f"{where}: innings format is {actual or 'unrecognised'}, expected "
            f"{expected}. Observed fractional parts: {fractions}. Shipping the "
            f"wrong one silently corrupts every pitching rate -- see "
            f"sources/_shape.py.")


def true_innings_to_ncaa(value):
    """Re-encode true innings as NCAA thirds notation, exactly invertibly.

    97.667 -> '97.2'. Rounding happens on *outs*, which is the only integer
    quantity in play, so the round trip through `ip_to_float` is lossless.
    """
    number = pd.to_numeric(value, errors="coerce")
    if number is None or pd.isna(number):
        return None
    outs = int(round(float(number) * 3))
    return f"{outs // 3}.{outs % 3}"


def outs_from_true_innings(value):
    number = pd.to_numeric(value, errors="coerce")
    if number is None or pd.isna(number):
        return None
    return int(round(float(number) * 3))


def ncaa_from_outs(outs):
    if outs is None or pd.isna(outs):
        return None
    outs = int(outs)
    return f"{outs // 3}.{outs % 3}"


# ---------------------------------------------------------------------------
# Blank versus absent
# ---------------------------------------------------------------------------

def blank_to_zero(value):
    """A blank cell in an NCAA stat grid means zero.

    47-80% of counting-stat cells in the rich mirror are blank, and reading them
    as missing rather than zero drops agreement with FanGraphs from ~98% to
    ~50%. `derive/rates.py::to_int` already defaults to 0, so passing the blank
    through unchanged is correct -- this helper exists to make that explicit at
    the call sites where it matters.
    """
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return 0
    if isinstance(value, str) and not value.strip():
        return 0
    return value


def absent(year: int, column: str) -> bool:
    """True when a column has no source for this year and must stay None.

    The distinction from `blank_to_zero` is the whole point: a blank cell is a
    zero we can support, whereas a column the source never published is not.
    Emitting 0 for it would be a fabricated observation.
    """
    if column == "gdp":
        return year in config.GDP_ABSENT_YEARS
    if column in config.PITCH_DECISION_COLUMNS:
        return year in config.PITCH_DECISIONS_ABSENT_YEARS
    return False


def require_columns(frame: pd.DataFrame, columns, *, where: str) -> None:
    """Fail if the source lost a column we map, instead of zeroing it."""
    missing = [c for c in columns if c not in frame.columns]
    if missing:
        raise AssertionError(
            f"{where}: source is missing mapped column(s) {missing}. Upstream "
            f"schema changed; fix the mapping table rather than letting these "
            f"become zeros. Columns present: {sorted(frame.columns)[:40]}")
