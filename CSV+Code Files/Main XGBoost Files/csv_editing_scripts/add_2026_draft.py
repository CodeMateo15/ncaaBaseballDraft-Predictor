"""
Fill the draft-outcome columns (Round / Pick / Drafted By / Drafted From / Drafted?)
from the MLB StatsAPI draft JSONs in MLBStatsAPIDraftDataAccess/.

build_2026_combined.py blanked those columns for 2026 because the draft had not been held.
The historical 2021-2025 labels came from ncaa_*/masterDraft.py reading all_drafts.json,
whose school names are long-form ("Arizona State University"); StatsAPI ships short forms
("Arizona State", "USC", "Illinois-Chicago"), so masterDraft's exact-school condition can
almost never fire against it and its rule cannot simply be replayed here.

  1. parse each statsapi_draft_{year}.json into picks (round labels collapse to 1-20)
  2. normalize school names into a comparable key space (inverted TEAM_NAME_MAP + school_key)
  3. match picks to player-seasons in three passes, each guarded on school agreement
  4. write labels ADDITIVELY -- an existing Drafted?=True is never cleared
  5. verify only the 5 draft columns moved, then write atomically

Labels are additive because the two sources have complementary errors: StatsAPI lists some
college players under a high school ("Griff O'Ferrall" / "Farragut HS") or "No School", so
dropping rows StatsAPI cannot confirm would lose genuine positives.

Run:  python csv_editing_scripts/add_2026_draft.py --dry-run
      python csv_editing_scripts/add_2026_draft.py
"""

import argparse
import collections
import json
import os
import re
import sys
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd
from rapidfuzz import fuzz, process

# Reuse the team-name normalization built for the RPI merge.
from add_team_rpi import TEAM_NAME_MAP

# --------------------------------------------------------------------------------------
# Paths
# --------------------------------------------------------------------------------------
HERE = Path(__file__).resolve().parent          # .../Main XGBoost Files/csv_editing_scripts
ROOT = HERE.parent                              # .../Main XGBoost Files
CSVROOT = ROOT.parent                           # .../CSV+Code Files

COMBINED = ROOT / "batting_pitching_combined_with_rpi_2026.csv"
DRAFT_DIR = ROOT / "MLBStatsAPIDraftDataAccess"
REPORT = ROOT / "draft_match_report.csv"
BACKUP = ROOT / "draft_labels_backup.csv"

YEARS = [2021, 2022, 2023, 2024, 2025, 2026]

DRAFT_COLS = ["Round", "Pick", "Drafted By", "Drafted From", "Drafted?"]

# Acceptance thresholds. The gaps between genuine matches and false positives are wide
# (pass 1: 96.8 vs 40; pass 2: 100 vs <=41), so these sit in empty space, not on a knife edge.
EXACT_SCHOOL_MIN = 70    # pass 1: exact name -> loose school guard
FUZZY_NAME_MIN = 85      # pass 2: rapidfuzz.ratio on cleaned names
FUZZY_SCHOOL_MIN = 90    # pass 2/3: strong school guard
FIRST_NAME_MIN = 80      # pass 3: first-name similarity fallback
REVIEW_SCHOOL_MAX = 90   # accepted matches below this get printed for review
NEAR_MISS_MIN = 50       # rejects at/above this get printed for review
SANITY_RANGE = (250, 420)  # per-year matched-pick count guard

# Row columns compared fuzzily against a pick's school. Acronym is deliberately excluded:
# it is only ever compared by exact equality, because token_set_ratio on a 3-4 character
# acronym produces spurious highs ("miami" vs "MIA").
SCHOOL_SURFACES = ["Full Team Name", "team_old", "team_new"]

# StatsAPI reports each club under its CURRENT name, while the historical rows use the name
# the franchise carried at the time of that draft. Without this, filling a 2021 row would
# write "Cleveland Guardians" alongside the "Cleveland Indians" already in the same year.
# 2026 keeps the bare "Athletics" -- that is the club's own current name.
CLUB_BY_YEAR = {
    (2021, "Cleveland Guardians"): "Cleveland Indians",
    (2021, "Athletics"): "Oakland Athletics",
    (2022, "Athletics"): "Oakland Athletics",
    (2023, "Athletics"): "Oakland Athletics",
    (2024, "Athletics"): "Oakland Athletics",
    (2025, "Athletics"): "Sacramento Athletics",
}

# StatsAPI school string -> a name that appears on one of the row surfaces above.
# Only one entry is needed: with school_key() plus the inverted TEAM_NAME_MAP, this is the
# sole genuine match in 2021-2026 that would otherwise score below 90.
SCHOOL_ALIAS = {
    "UNC Wilmington": "UNCW",
}

_SCHOOL_DROP = {"university", "of", "at", "the", "u", "college", "and"}
_SCHOOL_SUB = {"saint": "st", "state": "st"}

NAME_SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}

# Used only inside pass 3, to decide whether two first names can be the same person.
NICKNAME = {
    "joey": "joseph", "joe": "joseph", "mike": "michael", "bob": "robert",
    "bobby": "robert", "rob": "robert", "jake": "jacob", "will": "william",
    "bill": "william", "billy": "william", "nick": "nicholas", "tony": "anthony",
    "dan": "daniel", "danny": "daniel", "jim": "james", "jimmy": "james",
    "tom": "thomas", "tommy": "thomas", "charlie": "charles", "chuck": "charles",
    "gabe": "gabriel", "sam": "samuel", "ben": "benjamin", "josh": "joshua",
    "drew": "andrew", "andy": "andrew", "rick": "richard", "ricky": "richard",
    "steve": "steven", "nate": "nathan", "alex": "alexander", "pat": "patrick",
    "vinny": "vincent", "manny": "manuel", "dom": "dominic", "chris": "christopher",
    "ty": "tyler", "matt": "matthew", "mitch": "mitchell", "zach": "zachary",
}


# --------------------------------------------------------------------------------------
# Normalizers
# --------------------------------------------------------------------------------------
def clean(s):
    """Name/full-name normalizer copied from ncaa_*/masterDraft.py."""
    if pd.isna(s):
        return ""
    s = str(s).lower()
    s = s.replace("&", "and")
    s = re.sub(r"\buniv\.?\b", "university", s)
    s = s.replace("college of", "")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


@lru_cache(maxsize=None)
def school_key(s):
    """Reduce a school name to comparable tokens.

    Splitting on punctuation is what makes "Illinois-Chicago" comparable to "UIC" --
    token_set_ratio splits on whitespace only, so a hyphenated name is one opaque token.
    "state"/"saint" both collapse to "st"; applied to both sides, so the conflation is
    harmless.
    """
    s = re.sub(r"[^a-z0-9]+", " ", clean(s))
    return " ".join(_SCHOOL_SUB.get(t, t) for t in s.split() if t not in _SCHOOL_DROP)


def name_tokens(s):
    """Cleaned name tokens with generational suffixes dropped."""
    toks = re.sub(r"[^a-z0-9 ]", " ", clean(s)).split()
    return [t for t in toks if t not in NAME_SUFFIXES]


def build_school_aliases():
    """StatsAPI school key -> alternate spelling to also try.

    TEAM_NAME_MAP maps NCAA-style names to RPI-style names ("Arizona St." -> "Arizona
    State"); StatsAPI uses the value side, so it is inverted here.
    """
    values = [clean(v) for v in TEAM_NAME_MAP.values()]
    assert len(values) == len(set(values)), "TEAM_NAME_MAP has duplicate values; inversion is lossy"
    aliases = {school_key(v): k for k, v in TEAM_NAME_MAP.items()}
    aliases.update({school_key(k): v for k, v in SCHOOL_ALIAS.items()})
    return aliases


def compatible_first_name(a, b):
    if a == b or NICKNAME.get(a, a) == NICKNAME.get(b, b):
        return True
    if len(a) >= 2 and len(b) >= 2 and (a.startswith(b) or b.startswith(a)):
        return True
    return fuzz.ratio(a, b) >= FIRST_NAME_MIN


# --------------------------------------------------------------------------------------
# Draft JSON
# --------------------------------------------------------------------------------------
@dataclass(frozen=True)
class DraftPick:
    pick: int
    round_num: int
    round_label: str
    name: str
    school: str
    school_class: str
    club: str


def load_picks(path, year):
    """Parse one statsapi_draft_{year}.json.

    Round labels include non-numeric entries (PPI, CB-A, CB-B, SUP-2, 1C, 2C, 4C). The
    historical Round column only holds 1-20, and cross-checking all_drafts.json at identical
    overall pick numbers shows those labels belong to the preceding numeric round (verified
    across 3,465 overlapping picks in 2021-2025 with no mismatch).
    """
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    drafts = data["drafts"]
    assert int(drafts["draftYear"]) == year, f"{path.name} holds draftYear {drafts['draftYear']}"

    picks = []
    carry = None
    for rd in drafts["rounds"]:
        label = str(rd["round"])
        m = re.search(r"\d+", label)
        num = int(m.group()) if m else carry
        assert num is not None, f"{year}: first round label {label!r} has no digits to carry from"
        assert carry is None or num >= carry, f"{year}: round order regressed at {label!r}"
        assert 1 <= num <= 25, f"{year}: implausible round {num} from {label!r}"
        carry = num

        for p in rd["picks"]:
            # Same expression the notebook uses to build its own (year, Pick) merge key, so
            # the CSV and the notebook can never disagree. pickOverall is absent today.
            overall = p.get("pickOverall") or p["displayPickNumber"]
            person = p.get("person") or {}
            school = p.get("school") or {}
            name = person.get("fullName") or ""
            assert name, f"{year}: pick {overall} has no person.fullName"
            picks.append(DraftPick(
                pick=int(overall),
                round_num=num,
                round_label=label,
                name=name,
                school=school.get("name") or "",
                school_class=school.get("schoolClass") or "",
                club=(p.get("team") or {}).get("name") or "",
            ))

    numbers = [p.pick for p in picks]
    assert len(numbers) == len(set(numbers)), f"{year}: duplicate overall pick numbers"
    return picks


# --------------------------------------------------------------------------------------
# Matching
# --------------------------------------------------------------------------------------
@dataclass
class Match:
    row: int
    pick: DraftPick
    name_score: float
    school_score: float
    method: str
    n_candidates: int


class YearIndex:
    """Precomputed lookup structures for one year's player-seasons."""

    def __init__(self, rows):
        self.rows = rows
        self.by_name = collections.defaultdict(list)
        self.by_surname = collections.defaultdict(list)
        self.acronym_key = {}
        self.surface_keys = {}
        self.first_name = {}

        for idx, row in rows.iterrows():
            cleaned = clean(row["nameascii"])
            self.by_name[cleaned].append(idx)
            self.acronym_key[idx] = school_key(row["Acronym"])
            self.surface_keys[idx] = [school_key(row[s]) for s in SCHOOL_SURFACES]
            toks = name_tokens(row["nameascii"])
            if toks:
                self.by_surname[toks[-1]].append(idx)
                self.first_name[idx] = toks[0]

        self.all_names = list(self.by_name)

    def school_score(self, pick_keys, idx):
        if self.acronym_key[idx] in pick_keys:
            return 100.0
        return max(fuzz.token_set_ratio(k, s)
                   for k in pick_keys for s in self.surface_keys[idx])


def pick_school_keys(pick, aliases):
    key = school_key(pick.school)
    keys = {key}
    alias = aliases.get(key)
    if alias:
        keys.add(school_key(alias))
    return keys


def match_year(year, rows, aliases):
    """Match one year's picks to player-seasons. Returns (matches, report_rows)."""
    picks = load_picks(DRAFT_DIR / f"statsapi_draft_{year}.json", year)
    index = YearIndex(rows)

    matches = {}          # row index -> Match
    report_rows = []
    used = set()

    def record(pick, status, best_idx=None, name_score=None, school_score=None, n_cands=0):
        rec = {
            "year": year, "pick": pick.pick, "round": pick.round_num,
            "round_label": pick.round_label, "api_name": pick.name,
            "api_school": pick.school, "api_school_class": pick.school_class,
            "api_club": pick.club, "status": status,
            "name_score": None if name_score is None else round(name_score, 1),
            "school_score": None if school_score is None else round(school_score, 1),
            "n_candidates": n_cands,
        }
        if status.startswith("matched"):
            row = rows.loc[best_idx]
            rec.update({
                "matched_nameascii": row["nameascii"], "matched_playerid": row["playerid"],
                "matched_acronym": row["Acronym"], "matched_full_team_name": row["Full Team Name"],
                "best_rejected_nameascii": None, "best_rejected_school_score": None,
            })
        else:
            rej = None if best_idx is None else rows.loc[best_idx, "nameascii"]
            rec.update({
                "matched_nameascii": None, "matched_playerid": None,
                "matched_acronym": None, "matched_full_team_name": None,
                "best_rejected_nameascii": rej,
                "best_rejected_school_score": rec["school_score"],
            })
        report_rows.append(rec)

    # -- pass 1: exact cleaned name, loose school guard -------------------------------
    remaining = []
    for pick in picks:
        keys = pick_school_keys(pick, aliases)
        cands = [i for i in index.by_name.get(clean(pick.name), []) if i not in used]
        if not cands:
            remaining.append(pick)
            continue
        scored = sorted(((index.school_score(keys, i), -i) for i in cands), reverse=True)
        best_score, neg_idx = scored[0]
        best = -neg_idx
        if best_score >= EXACT_SCHOOL_MIN:
            used.add(best)
            matches[best] = Match(best, pick, 100.0, best_score, "exact_name", len(cands))
            record(pick, "matched_exact_name", best, 100.0, best_score, len(cands))
        else:
            record(pick, "rejected_school_guard", best, 100.0, best_score, len(cands))

    # -- pass 2: fuzzy name, strong school guard --------------------------------------
    still = []
    for pick in remaining:
        keys = pick_school_keys(pick, aliases)
        hits = process.extract(clean(pick.name), index.all_names, scorer=fuzz.ratio,
                               score_cutoff=FUZZY_NAME_MIN, limit=None)
        # Score every hit above the cutoff, not just the best name: otherwise a closer name
        # at the wrong school can shadow a slightly worse name at the right one.
        cands = [(index.school_score(keys, i), score, -i)
                 for name, score, _ in hits for i in index.by_name[name] if i not in used]
        if not cands:
            still.append(pick)
            continue
        best_school, best_name, neg_idx = max(cands)
        best = -neg_idx
        if best_school >= FUZZY_SCHOOL_MIN:
            used.add(best)
            matches[best] = Match(best, pick, best_name, best_school, "fuzzy_name", len(cands))
            record(pick, "matched_fuzzy_name", best, best_name, best_school, len(cands))
        else:
            record(pick, "rejected_fuzzy_school_guard", best, best_name, best_school, len(cands))

    # -- pass 3: same school + exact surname + compatible first name -------------------
    for pick in still:
        keys = pick_school_keys(pick, aliases)
        toks = name_tokens(pick.name)
        if not toks:
            record(pick, "no_candidate")
            continue
        qualified = [
            i for i in index.by_surname.get(toks[-1], [])
            if i not in used
            and compatible_first_name(toks[0], index.first_name[i])
            and index.school_score(keys, i) >= FUZZY_SCHOOL_MIN
        ]
        if len(qualified) == 1:
            best = qualified[0]
            score = index.school_score(keys, best)
            used.add(best)
            matches[best] = Match(best, pick, 0.0, score, "school_surname", 1)
            record(pick, "matched_school_surname", best, 0.0, score, 1)
        elif len(qualified) > 1:
            record(pick, "ambiguous_pass3", qualified[0], 0.0,
                   index.school_score(keys, qualified[0]), len(qualified))
        else:
            record(pick, "no_candidate")

    lo, hi = SANITY_RANGE
    assert lo <= len(matches) <= hi, f"{year}: matched {len(matches)} picks, outside {SANITY_RANGE}"
    return matches, report_rows


# --------------------------------------------------------------------------------------
# Apply
# --------------------------------------------------------------------------------------
def as_bool(series):
    """Drafted? is stored as the literal strings True/False; tolerate either form."""
    if series.dtype == bool:
        return series.fillna(False).astype(bool)
    return series.map(lambda v: str(v).strip().lower() == "true").astype(bool)


def apply_matches(df, matches):
    """Write the 5 draft columns for matched rows.

    Round/Pick always come from StatsAPI -- that is the numbering the notebook merges on.
    Drafted By / Drafted From are only filled where blank, so existing values survive, and
    newly filled clubs go through CLUB_BY_YEAR so a year never mixes franchise names.
    """
    for row, match in matches.items():
        df.at[row, "Round"] = float(match.pick.round_num)
        df.at[row, "Pick"] = float(match.pick.pick)
        df.at[row, "Drafted?"] = True
        if pd.isna(df.at[row, "Drafted By"]) or not str(df.at[row, "Drafted By"]).strip():
            year = int(df.at[row, "year"])
            df.at[row, "Drafted By"] = CLUB_BY_YEAR.get((year, match.pick.club), match.pick.club)
        if pd.isna(df.at[row, "Drafted From"]) or not str(df.at[row, "Drafted From"]).strip():
            df.at[row, "Drafted From"] = clean(df.at[row, "Full Team Name"])


def carried_over(df, before_flag, matches_by_year):
    """Rows labeled drafted before that no pick matched -- kept, but worth eyeballing."""
    matched = {row for matches in matches_by_year.values() for row in matches}
    keep = before_flag & ~df.index.isin(matched)
    return df.index[keep].tolist()


# --------------------------------------------------------------------------------------
# Verification
# --------------------------------------------------------------------------------------
def verify(df, other_before, before_flag, expected_cols, n_rows, matches_by_year, dup_before,
           expect_columns=171):
    assert list(df.columns) == expected_cols, "column set or order changed"
    # The width is asserted, not inferred, so a truncated or double-merged input is
    # caught before labels are written. It is a parameter because the public matrix
    # is a different width (181: no age/mlbamid/decisions, plus class, person_id and
    # the two qualification flags) while being the same shape otherwise.
    assert len(expected_cols) == expect_columns, \
        f"expected the {expect_columns}-column schema, got {len(expected_cols)}"
    start = expected_cols.index("Round")
    assert expected_cols[start:start + 5] == DRAFT_COLS, \
        f"draft columns are no longer contiguous: {expected_cols[start:start + 5]}"
    assert len(df) == n_rows, f"row count changed: {n_rows} -> {len(df)}"

    pd.testing.assert_frame_equal(other_before, df.drop(columns=DRAFT_COLS),
                                  check_exact=True, check_dtype=True)

    flag = df["Drafted?"]
    assert flag.dtype == bool, f"Drafted? must stay bool for the notebook's astype(int), got {flag.dtype}"
    assert flag.notna().all(), "Drafted? has nulls"
    assert (~before_flag | flag).all(), "a row lost its drafted label; labeling must be additive"

    drafted = df[flag]
    rounds = drafted["Round"]
    assert rounds.notna().all(), "drafted rows with no Round"
    assert set(rounds.unique()) <= {float(i) for i in range(1, 21)}, \
        f"Round outside 1-20: {sorted(set(rounds.unique()) - {float(i) for i in range(1, 21)})}"
    picks = drafted["Pick"]
    assert picks.notna().all(), "drafted rows with no Pick"
    assert (picks == picks.round()).all(), "non-integral Pick"
    assert picks.min() >= 1, f"Pick below 1: {picks.min()}"
    assert drafted["Drafted By"].notna().all(), "drafted rows with no Drafted By"
    assert drafted["Drafted From"].notna().all(), "drafted rows with no Drafted From"
    assert df.loc[~flag, DRAFT_COLS[:4]].isna().all().all(), "undrafted rows carry draft values"

    for year, matches in matches_by_year.items():
        rows = {m.row for m in matches.values()}
        nums = {m.pick.pick for m in matches.values()}
        assert len(rows) == len(nums) == len(matches), f"{year}: match is not one-to-one"

    # The 2023 Caleb Hobson pair (one FanGraphs entity split into a Pitcher and a Batter row)
    # already collides today, so this can only be asserted not to get worse.
    dup_after = set(map(tuple, drafted[drafted.duplicated(["year", "Pick"], keep=False)][["year", "Pick"]].values))
    assert dup_after <= dup_before, f"new (year, Pick) collisions: {sorted(dup_after - dup_before)}"
    return dup_after


# --------------------------------------------------------------------------------------
# Report
# --------------------------------------------------------------------------------------
def report(df, before, before_flag, matches_by_year, report_rows, carry_rows, dup_after):
    flag = df["Drafted?"]
    print("\n=== drafted labels by year ===")
    print(f"{'year':>6}  {'before':>7}  {'after':>7}  {'added':>7}")
    for year in sorted(df["year"].unique()):
        m = df["year"] == year
        b, a = int(before_flag[m].sum()), int(flag[m].sum())
        print(f"{int(year):>6}  {b:>7}  {a:>7}  {a - b:>+7}")
    print(f"{'total':>6}  {int(before_flag.sum()):>7}  {int(flag.sum()):>7}  "
          f"{int(flag.sum()) - int(before_flag.sum()):>+7}")

    status = collections.Counter(r["status"] for r in report_rows)
    print("\n=== pick match status (all years) ===")
    for name, count in status.most_common():
        print(f"  {name:<28} {count:>5}")

    print("\n=== matches by method ===")
    method = collections.Counter(m.method for ms in matches_by_year.values() for m in ms.values())
    for name, count in method.most_common():
        print(f"  {name:<28} {count:>5}")

    changed = changed_rows(df, before)
    print(f"\nrows with any draft-column change: {len(changed)} of {len(df)}")

    surname = [r for r in report_rows if r["status"] == "matched_school_surname"]
    print(f"\n--- review: surname-pass matches ({len(surname)}) ---")
    for r in sorted(surname, key=lambda r: (r["year"], r["pick"])):
        print(f"  {r['year']} pk{r['pick']:<4} r{r['round']:<3} {r['api_name']} / {r['api_school']}"
              f"  ->  {r['matched_nameascii']} ({r['matched_acronym']})")

    weak = [r for r in report_rows
            if r["status"].startswith("matched") and (r["school_score"] or 0) < REVIEW_SCHOOL_MAX]
    print(f"\n--- review: matches with school_score < {REVIEW_SCHOOL_MAX} ({len(weak)}, expect 0) ---")
    for r in weak:
        print(f"  {r['year']} pk{r['pick']} school={r['school_score']} {r['api_name']} / "
              f"{r['api_school']} -> {r['matched_nameascii']} ({r['matched_acronym']})")

    near = [r for r in report_rows
            if r["status"].startswith("rejected")
            and NEAR_MISS_MIN <= (r["school_score"] or 0) < EXACT_SCHOOL_MIN]
    print(f"\n--- review: rejects in the {NEAR_MISS_MIN}-{EXACT_SCHOOL_MIN} school band "
          f"({len(near)}, expect 0) ---")
    for r in near:
        print(f"  {r['year']} pk{r['pick']} school={r['school_score']} {r['api_name']} / "
              f"{r['api_school']} -> {r['best_rejected_nameascii']}")

    print(f"\n--- review: labels carried over with no StatsAPI match ({len(carry_rows)}) ---")
    for row in carry_rows:
        r = df.loc[row]
        print(f"  {int(r['year'])} pk{r['Pick']:.0f} r{r['Round']:.0f} {r['nameascii']} "
              f"({r['team']}) -- {r['Drafted By']}")

    if dup_after:
        print(f"\nWARNING pre-existing (year, Pick) collisions retained: "
              f"{sorted((int(y), int(p)) for y, p in dup_after)}")


def changed_rows(df, before):
    changed = pd.Series(False, index=df.index)
    for col in DRAFT_COLS:
        a, b = before[col], df[col]
        if col == "Drafted?":
            diff = as_bool(a) != b
        elif col in ("Round", "Pick"):
            diff = ~((a.isna() & b.isna()) | (a == b))
        else:
            diff = a.fillna("") != b.fillna("")
        changed |= diff
    return df.index[changed]


# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------
def parse_args(argv):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    p.add_argument("--years", type=int, nargs="+", default=YEARS)
    p.add_argument("--csv", type=Path, default=COMBINED)
    p.add_argument("--draft-json-dir", type=Path, default=DRAFT_DIR)
    p.add_argument("--output", type=Path, default=None, help="defaults to --csv (in place)")
    p.add_argument("--report", type=Path, default=REPORT)
    p.add_argument("--backup", type=Path, default=BACKUP)
    p.add_argument("--dry-run", action="store_true", help="report only; write no CSV")
    p.add_argument("--force", action="store_true", help="skip the per-year sanity range")
    p.add_argument("--expect-columns", type=int, default=171,
                   help="asserted column count of the input schema (171 for the "
                        "private matrix, 181 for the public one)")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    global DRAFT_DIR, SANITY_RANGE
    DRAFT_DIR = args.draft_json_dir
    if args.force:
        SANITY_RANGE = (0, 10 ** 9)
    output = args.output or args.csv

    df = pd.read_csv(args.csv, low_memory=False)
    expected_cols = list(df.columns)
    n_rows = len(df)
    df["Drafted?"] = as_bool(df["Drafted?"])

    before = df[DRAFT_COLS].copy()
    before_flag = before["Drafted?"].copy()
    other_before = df.drop(columns=DRAFT_COLS).copy()
    drafted_before = df[before_flag]
    dup_before = set(map(tuple, drafted_before[drafted_before.duplicated(
        ["year", "Pick"], keep=False)][["year", "Pick"]].values))

    aliases = build_school_aliases()
    matches_by_year, report_rows = {}, []
    for year in args.years:
        rows = df[df["year"] == year]
        assert len(rows) > 0, f"no rows for year {year}"
        matches, recs = match_year(year, rows, aliases)
        matches_by_year[year] = matches
        report_rows.extend(recs)
        print(f"{year}: {len(rows)} player-seasons, {len(matches)} picks matched")

    for matches in matches_by_year.values():
        apply_matches(df, matches)

    carry_rows = carried_over(df, before_flag, matches_by_year)
    for row in carry_rows:
        r = df.loc[row]
        report_rows.append({
            "year": int(r["year"]), "pick": r["Pick"], "round": r["Round"],
            "round_label": None, "api_name": None, "api_school": None,
            "api_school_class": None, "api_club": None, "status": "carried_over_existing",
            "name_score": None, "school_score": None, "n_candidates": 0,
            "matched_nameascii": r["nameascii"], "matched_playerid": r["playerid"],
            "matched_acronym": r["Acronym"], "matched_full_team_name": r["Full Team Name"],
            "best_rejected_nameascii": None, "best_rejected_school_score": None,
        })

    dup_after = verify(df, other_before, before_flag, expected_cols, n_rows,
                       matches_by_year, dup_before,
                       expect_columns=args.expect_columns)
    report(df, before, before_flag, matches_by_year, report_rows, carry_rows, dup_after)

    rep = pd.DataFrame(report_rows).sort_values(["year", "pick"], na_position="last")
    rep.to_csv(args.report, index=False)
    print(f"\nwrote {args.report.relative_to(CSVROOT)} ({len(rep)} rows)")

    if args.dry_run:
        print("dry run -- combined CSV not written")
        return 0

    # Write-once: a second run reads back its own output, so overwriting here would replace
    # the pre-run snapshot with the post-run one and destroy the only copy of the originals.
    if args.backup.exists():
        print(f"kept existing {args.backup.relative_to(CSVROOT)} (pre-run labels preserved)")
    else:
        snapshot = df[["year", "playerid", "nameascii", "team"]].join(before.add_prefix("old_"))
        snapshot.to_csv(args.backup, index=False)
        print(f"wrote {args.backup.relative_to(CSVROOT)} (original labels)")

    tmp = output.with_suffix(".csv.tmp")
    df.to_csv(tmp, index=False)
    os.replace(tmp, output)
    print(f"wrote {output.relative_to(CSVROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
