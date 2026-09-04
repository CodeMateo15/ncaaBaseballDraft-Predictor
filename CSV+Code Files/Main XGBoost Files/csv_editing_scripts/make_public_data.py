"""Generate the public copy of the combined input matrix.

The originals carry player-level columns sourced from FanGraphs' college
leaderboards, which sit behind a paid membership and whose terms of use do not
permit redistributing the bulk data. That does not make every column off
limits. This script applies the same line the `ncaa_bbStats` package drew in
its own DATA_PROVENANCE.md, which is where the reasoning lives in full:

  Facts about sporting events are not copyrightable in the United States
  (Feist Publications v. Rural Telephone Service, 499 U.S. 340 (1991)). What a
  compiler does own is its original selection, arrangement, and derived
  analytics.

So two categories come out, and nothing else:

  1. FG-derived metrics. wRC+, wOBA, wRAA, wRC, wSB, Spd, FIP, E-F and LOB%
     are computed from FanGraphs' own NCAA linear weights and league
     constants. Those are FanGraphs' analytical product, not facts.

  2. FG identifiers. `playerid` is their internal key (values like
     `sa3028661`); it is replaced in place by an opaque surrogate. `mlbamid`
     is inherited from the FG export and is dropped.

Everything else stays: the raw counting statistics are records of what
happened on the field, and the pure-arithmetic rates built from them (AVG,
OBP, SLG, OPS, ISO, BABIP, BB%, K%, ERA, WHIP, K/9, BB/9, K-BB%) are exactly
reproducible from those counts. No replacement metrics are computed here --
this is a strip, not a substitution.

The honest caveat, same as the one ncaa_bbStats states: the *provenance* of the
retained counting statistics is still a FanGraphs export, even though the
underlying facts are not FanGraphs'. Re-deriving them from stats.ncaa.org
individual-player pages would remove the dependency entirely.

Run from this directory:
    python make_public_data.py
"""

from pathlib import Path
import pandas as pd

HERE = Path(__file__).resolve().parent
DATA_DIR = HERE.parent  # CSV+Code Files/Main XGBoost Files/

# (private_filename, public_filename). The private original stays untouched on
# disk and is gitignored; the public copy is what gets committed and shared.
FILES = [
    ("batting_pitching_combined_with_rpi_2026_eada.csv",
     "../archive/data/batting_pitching_combined_with_rpi_public.csv"),
]

# Category 1: metrics that depend on FanGraphs' linear weights, league
# constants or park factors. Listed by name rather than matched by suffix --
# a `_pitch` rule cannot tell `fip_pitch` from `whip_pitch`, and only one of
# those is FanGraphs' work.
FG_DERIVED_COLS = frozenset({
    "fip_pitch",    # FG linear weights
    "e-f_pitch",    # ERA minus FIP, so inherits FIP
    "lob%_pitch",   # FG league constants
    "wrc_bat",      # FG linear weights
    "wraa_bat",     # FG linear weights
    "woba_bat",     # FG linear weights
    "wrc+_bat",     # FG linear weights + park factors
    "wsb_bat",      # FG stolen-base run values
    "spd_bat",      # FG speed score
})

# Category 2: identifiers carried straight out of the FG export by
# build_2026_combined.py (`PlayerId` -> playerid, `MLBAMID` -> mlbamid).
FG_IDENT_DROP = frozenset({"mlbamid"})

# Replaced in place rather than dropped: the notebook groups on `playerid` to
# count a player's eligibility seasons, so the public file needs *a* stable
# player key -- just not FanGraphs'.
FG_IDENT_SURROGATE = "playerid"

DROP_COLS = FG_DERIVED_COLS | FG_IDENT_DROP


def surrogate_ids(values: pd.Series) -> pd.Series:
    """Map each distinct player key to an opaque `pNNNNN` code.

    Sorting the input first makes the assignment deterministic, so rerunning
    the script produces a byte-identical file. One code per real player, so
    rows for the same player across seasons still group together.
    """
    keys = values.astype(str).str.strip()
    codes = {k: f"p{i:05d}" for i, k in enumerate(sorted(keys.unique()), start=1)}
    return keys.map(codes)


def strip(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    # Fail loudly if a column was renamed upstream. Silently shipping an FG
    # metric because its name drifted is the failure mode worth guarding.
    missing = sorted(DROP_COLS - set(df.columns))
    if missing:
        raise KeyError(
            f"expected FanGraphs columns not found: {missing}. "
            "If these were renamed upstream, update DROP_COLS -- do not just "
            "delete the entry, or the column ships."
        )
    if FG_IDENT_SURROGATE not in df.columns:
        raise KeyError(f"{FG_IDENT_SURROGATE!r} missing; cannot build surrogate ids")

    out = df.drop(columns=sorted(DROP_COLS))
    out[FG_IDENT_SURROGATE] = surrogate_ids(out[FG_IDENT_SURROGATE])
    return out, sorted(DROP_COLS)


def main() -> None:
    for private, public in FILES:
        src = DATA_DIR / private
        if not src.exists():
            print(f"skip: {private} (not present)")
            continue
        df = pd.read_csv(src, low_memory=False)
        stripped, dropped = strip(df)
        dst = DATA_DIR / public
        stripped.to_csv(dst, index=False)
        print(f"wrote {public}: {len(stripped.columns)} cols kept, "
              f"{len(dropped)} FG cols stripped, "
              f"{stripped[FG_IDENT_SURROGATE].nunique()} players re-keyed")


if __name__ == "__main__":
    main()
