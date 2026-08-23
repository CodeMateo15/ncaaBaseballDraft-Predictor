"""NCAA column header -> target column.

**Map by header name, never by index.** Verified header drift across 2021-2026
(one team per year, live):

    batting   2021: 28 columns -- no `Ht`, no `B/T`, no `GDP`
              2022-2026: 31 columns, stable
    pitching  2021: 37 columns -- no `Ht`, no `B/T`; has both `G` and `App`
              2022-2023: 39 columns -- has both `G` and `App`
              2024-2026: 38 columns -- `G` is gone, only `App`

The `G`/`App` split is why 2021-2023 pitching grids list every rostered player
(~36) while 2024+ list only pitchers (~19): in the earlier years the grid was
keyed on games *played*. Since we drop rows with no innings, both shapes give the
same result.

Columns deliberately unmapped are listed in `IGNORED_*` so that an unexpected new
header is distinguishable from one we chose not to use.
"""

# ---------------------------------------------------------------------------
# Batting
# ---------------------------------------------------------------------------

BATTING_MAP = {
    "name": ["Player"],
    "class": ["Yr"],
    "g": ["GP"],
    "ab": ["AB"],
    "h": ["H"],
    "2b": ["2B"],
    "3b": ["3B"],
    "hr": ["HR"],
    "r": ["R"],
    "rbi": ["RBI"],
    "bb": ["BB"],
    "so": ["K"],
    "hbp": ["HBP"],
    "sf": ["SF"],
    "sh": ["SH"],
    "sb": ["SB"],
    "cs": ["CS"],
    # Absent from the 2021 grid. `OPP DP` is the candidate stand-in and is
    # tested by run.py --probe-gdp-2021 against FanGraphs' 2021 `gdp`; until
    # that test passes, 2021 `gdp` is null.
    "gdp": ["GDP"],
}

# Read but not emitted: kept for diagnostics and cross-checks.
BATTING_EXTRA = {
    "pos": ["Pos"],
    "ncaa_ba": ["BA"],          # cross-check for our computed `avg`
    "ncaa_obp": ["OBPct"],      # cross-check for our computed `obp`
    "ncaa_slg": ["SlgPct"],     # cross-check for our computed `slg`
    "tb": ["TB"],               # cross-check for our computed `slg` numerator
    "opp_dp": ["OPP DP"],       # 2021 `gdp` candidate
    "ibb": ["IBB"],             # does NCAA's BB include these? -- see note below
}

IGNORED_BATTING = {"#", "GS", "Picked", "RBI2out", "Ht", "B/T"}

# ---------------------------------------------------------------------------
# Pitching
# ---------------------------------------------------------------------------

PITCHING_MAP = {
    "name": ["Player"],
    "class": ["Yr"],
    # `App` (appearances) is games pitched, which is what FanGraphs' `g` means.
    # `G` exists only in 2021-2023 and counts games *played*, so it must not be
    # used -- see the module docstring.
    "g": ["App"],
    "gs": ["GS"],
    "ip": ["IP"],
    "cg": ["CG"],
    "sho": ["SHO"],
    "sv": ["SV"],
    "w": ["W"],
    "l": ["L"],
    "h": ["H"],
    "r": ["R"],
    "er": ["ER"],
    "bb": ["BB"],
    "so": ["SO"],
    "hr": ["HR-A"],
    "hbp": ["HB"],
    "wp": ["WP"],
    "bk": ["Bk"],
    "tbf": ["BF"],
}

PITCHING_EXTRA = {
    "pos": ["Pos"],
    "ncaa_era": ["ERA"],        # cross-check for our computed `era`
    "p_oab": ["P-OAB"],         # real opponent AB -- better than FanGraphs' identity
    "ibb": ["IBB"],
}

IGNORED_PITCHING = {
    "#", "G", "2B-A", "3B-A", "Inh Run", "Inh Run Score", "SHA", "SFA",
    "Pitches", "GO", "FO", "KL", "pickoffs", "Ht", "B/T",
}

# ---------------------------------------------------------------------------
# Which target columns may legitimately be missing, and when
# ---------------------------------------------------------------------------

# {target_column: set_of_years_where_absence_is_expected}
OPTIONAL = {
    "gdp": {2021},
}

# An open question the FanGraphs diff settles: NCAA reports `BB` and `IBB` as
# separate columns, and it is not documented whether `BB` is the total (as in
# MLB convention) or unintentional-only. If our `bb` comes out systematically
# low against FanGraphs by roughly the IBB count, add IBB. validate/
# against_fangraphs.py reports `bb` mismatches separately for exactly this
# reason. Do not guess -- measure.

CATEGORIES = ("batting", "pitching")


def column_map(category: str):
    return BATTING_MAP if category == "batting" else PITCHING_MAP


def extra_map(category: str):
    return BATTING_EXTRA if category == "batting" else PITCHING_EXTRA


def ignored(category: str):
    return IGNORED_BATTING if category == "batting" else IGNORED_PITCHING


def build_index(headers, category: str):
    """Resolve a page's headers into ``({target: position}, unknown_headers)``.

    Args:
        headers: the ``thead th`` texts, in page order.
        category: ``"batting"`` or ``"pitching"``.

    Returns:
        (index, unknown): ``index`` maps target/extra column names to positions;
        ``unknown`` lists headers that are neither mapped nor deliberately
        ignored -- those are reported, never silently dropped.
    """
    position = {}
    for i, header in enumerate(headers):
        # First occurrence wins. NCAA does not repeat headers, but if it ever
        # does, taking the first keeps behaviour deterministic.
        position.setdefault(header, i)

    index = {}
    for target, candidates in {**column_map(category), **extra_map(category)}.items():
        for candidate in candidates:
            if candidate in position:
                index[target] = position[candidate]
                break

    known = set()
    for candidates in {**column_map(category), **extra_map(category)}.values():
        known.update(candidates)
    known.update(ignored(category))
    unknown = [h for h in headers if h not in known]

    return index, unknown


def missing_required(index, category: str, year: int):
    """Target columns that are absent and whose absence is not expected."""
    return [
        target
        for target in column_map(category)
        if target not in index and year not in OPTIONAL.get(target, set())
    ]
