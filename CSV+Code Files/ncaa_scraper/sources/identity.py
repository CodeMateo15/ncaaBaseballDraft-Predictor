"""A cross-season person key, because NCAA's player id is per-season.

NCAA mints a **new** ``player_id`` for every player every season. Verified on the
roster files: consecutive years share *exactly zero* ids (2021->2022 and
2025->2026 both checked), even though the numeric ranges overlap heavily. The
legacy mirror's ``stats_player_seq`` is a third, disjoint id space again.

That breaks anything that counts a player's seasons -- which the draft-eligibility
filter does, and which ``make_public_data.py`` documents its surrogate ids as
supporting. So we mint our own key here.

**This is a record-linkage estimate, not a vendor key.** Every link is earned by
an evidence rule and written to ``out/reports/person_links.csv`` with the rule
that earned it, so the precision is auditable rather than assumed. The project's
rule that *player data* is never joined on a name still holds: names only ever
block candidates here, and a name alone never links anything.

Signal availability is very uneven, and 2021 is the problem year:

    year  hometown  high_school  height  bats/throws  jersey
    2021    100%        100%      100%      100%        0%     <- all null but jersey
    2022     41%         46%        0%       39%        0%
    2023     20%         27%        0%       22%        0%
    2024      5%         14%        0%       14%        0%
    2025      7%         16%        0%       12%        0%
    2026      4%          9%        0%        6%        0%

(percentages are *null* rates). So 2021<->2022 links rest on school + jersey +
class progression alone, and are marked as the weakest tier so a reader can
discount them.
"""

import os

import pandas as pd

import config
from sources import rosters

# Ordered strongest first. The rule that fires is recorded per link.
TIER_HOMETOWN = "hometown+high_school"
TIER_PHYSICAL = "height+bats+throws"
TIER_SCHOOL_JERSEY = "school+jersey"
TIER_SCHOOL_UNIQUE = "school+unique-in-block"

_HAND = {"RIGHT": "R", "LEFT": "L", "BOTH": "B"}


class _Union:
    def __init__(self):
        self.parent = {}

    def find(self, key):
        self.parent.setdefault(key, key)
        while self.parent[key] != key:
            self.parent[key] = self.parent[self.parent[key]]
            key = self.parent[key]
        return key

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return False
        # Order by the natural (year, id) sort so runs are reproducible.
        if rb < ra:
            ra, rb = rb, ra
        self.parent[rb] = ra
        return True


def _clean(value):
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    return text.lower() or None


def _records(roster: pd.DataFrame, year: int):
    out = []
    for row in roster.itertuples():
        first = rosters.fold(getattr(row, "player_first_name", None))
        last = rosters.fold(getattr(row, "player_last_name", None))
        if not last:
            # Fall back to splitting the full name rather than dropping the row.
            parts = rosters.fold(getattr(row, "player_full_name", None)).split()
            if len(parts) >= 2:
                first, last = parts[0], parts[-1]
        out.append({
            "key": (year, int(row.player_id)),
            "year": year,
            "player_id": int(row.player_id),
            "block": (last, first),
            "school_id": getattr(row, "school_id", None),
            "class_ord": config.CLASS_ORDINAL.get(getattr(row, "class", None)),
            "jersey": _clean(getattr(row, "player_jersey_num", None)),
            "height": _clean(getattr(row, "player_height_string", None)),
            "bats": _HAND.get(str(getattr(row, "player_batting_hand", "")).upper()),
            "throws": _HAND.get(str(getattr(row, "player_throwing_hand", "")).upper()),
            "hometown": _clean(getattr(row, "player_hometown", None)),
            "high_school": _clean(getattr(row, "player_high_school", None)),
        })
    return out


def _class_progression_ok(a, b) -> bool:
    """A player's class advances by at most one per season, and never goes back.

    A redshirt or medical year keeps the class the same, so 0 is allowed; two
    classes in one year is not a person, it is two people. When either class is
    unknown the check cannot run, and the caller must rely on stronger evidence.
    """
    gap = b["year"] - a["year"]
    if a["class_ord"] is None or b["class_ord"] is None:
        return True
    delta = b["class_ord"] - a["class_ord"]
    return 0 <= delta <= gap


def _evidence(a, b, block_year_counts):
    """The strongest rule linking two player-seasons, or None."""
    if a["year"] == b["year"]:
        return None
    if not _class_progression_ok(a, b):
        return None

    both = lambda f: a[f] is not None and b[f] is not None and a[f] == b[f]  # noqa: E731

    if both("hometown") and both("high_school"):
        return TIER_HOMETOWN
    if both("height") and both("bats") and both("throws"):
        return TIER_PHYSICAL

    same_school = (a["school_id"] is not None
                   and a["school_id"] == b["school_id"])
    if same_school and both("jersey"):
        return TIER_SCHOOL_JERSEY
    # Last resort, and only when the name is unique in both seasons: two players
    # sharing a name at one school in different years would otherwise merge.
    if same_school and (block_year_counts.get(a["year"]) == 1
                        and block_year_counts.get(b["year"]) == 1):
        return TIER_SCHOOL_UNIQUE
    return None


def mint(rosters_by_year: dict, *, report_path=None):
    """Return {(year, player_id): person_id} plus the per-link evidence rows."""
    records = []
    for year in sorted(rosters_by_year):
        records.extend(_records(rosters_by_year[year], year))

    blocks = {}
    for record in records:
        blocks.setdefault(record["block"], []).append(record)

    union = _Union()
    links = []
    for block, members in blocks.items():
        if len(members) < 2:
            continue
        year_counts = {}
        for member in members:
            year_counts[member["year"]] = year_counts.get(member["year"], 0) + 1
        members = sorted(members, key=lambda r: r["key"])
        for i, a in enumerate(members):
            for b in members[i + 1:]:
                rule = _evidence(a, b, year_counts)
                if rule is None:
                    continue
                if union.union(a["key"], b["key"]):
                    links.append({
                        "last": block[0], "first": block[1],
                        "year_a": a["year"], "player_id_a": a["player_id"],
                        "year_b": b["year"], "player_id_b": b["player_id"],
                        "rule": rule,
                        "school_a": a["school_id"], "school_b": b["school_id"],
                    })

    # Deterministic ids: number the components by their earliest (year, id).
    roots = {}
    for record in sorted(records, key=lambda r: r["key"]):
        root = union.find(record["key"])
        roots.setdefault(root, len(roots) + 1)
    mapping = {record["key"]: f"q{roots[union.find(record['key'])]:06d}"
               for record in records}

    if report_path and links:
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        pd.DataFrame(links).to_csv(report_path, index=False)

    return mapping, links


def load_all(division, *, manifest, shas, offline=False, refresh=False,
             years=None):
    """Load every roster year the mirror has, for cross-season linking.

    Always all six years, even for a single-year build: a person key that changed
    depending on which years you happened to run would be useless for joining.
    """
    years = years or sorted(config.BULK_YEAR_SOURCES)
    out = {}
    for year in years:
        out[year] = rosters.load(year, division, manifest=manifest, shas=shas,
                                 offline=offline, refresh=refresh)
    return out


def summarise(mapping, rosters_by_year):
    """Coverage stats worth printing: how many people, and how many multi-season."""
    seasons = {}
    for (year, _pid), person in mapping.items():
        seasons.setdefault(person, set()).add(year)
    sizes = pd.Series([len(v) for v in seasons.values()])
    return {
        "player_seasons": len(mapping),
        "persons": len(seasons),
        "multi_season_persons": int((sizes > 1).sum()),
        "max_seasons": int(sizes.max()) if len(sizes) else 0,
        "mean_seasons": float(sizes.mean()) if len(sizes) else 0.0,
    }
