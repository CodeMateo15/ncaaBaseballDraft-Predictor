"""Roster loading and identity, from the modern mirror's ``{year}_rosters.csv``.

Rosters do three jobs no other file can:

1. **Identity for the lean generation.** The 2025/2026 season-stat files dropped
   every identity column except the player's name, so class, school, division and
   conference come from here, joined on ``player_id`` (99.94% coverage in 2025).
2. **Games played for 2021.** The legacy mirror's batting ``G`` is *team* games,
   not player games -- constant within school for 288 of 290 schools. The roster's
   ``player_G`` is the real per-player figure, and for 2021 it matches the repo's
   own scraped ``g`` exactly.
3. **The evidence for cross-season person links.** ``player_hometown`` and
   ``player_high_school`` are the only fields stable enough to tie a player's
   seasons together once you know NCAA re-mints ``player_id`` annually.

A caution learned the hard way: ``player_G`` is *not* trustworthy in every year.
For 2021 it equals the true season total, but the 2025 roster is a mid-season
snapshot (matching the real value on ~10% of rows, mean 9.8 games short). So it
is used only where it has been validated, never as a general fallback.
``player_weight`` is 100% null in every year 2021-2026 and is not exposed at all.
"""

import unicodedata

import pandas as pd

import config
from sources import bulk

# Roster class labels carry a trailing period, and an unknown class is spelled
# '---' rather than left blank.
_UNKNOWN_CLASS = {"---", "", "-"}


def _norm_class(value):
    if value is None or pd.isna(value):
        return None
    text = str(value).strip().rstrip(".")
    if text in _UNKNOWN_CLASS:
        return None
    return text if text in config.VALID_CLASSES else None


def fold(text):
    """Accent-fold and lowercase, for name bridging only -- never for identity."""
    if text is None or pd.isna(text):
        return ""
    decomposed = unicodedata.normalize("NFKD", str(text))
    stripped = "".join(c for c in decomposed if not unicodedata.combining(c))
    return " ".join(stripped.lower().split())


def load(year: int, division, *, manifest, shas, offline=False,
         refresh=False) -> pd.DataFrame:
    """One row per player-season for the given year.

    ``division=None`` returns every division, which is what the identity-coverage
    check needs: the season-stat files cover D1, D2 and D3 together, so measuring
    roster coverage against the D1 subset alone would look like a 70% failure when
    it is really just the other two divisions.
    """
    path = bulk.roster_path("modern", year)
    frame = bulk.load_csv("modern", path, manifest=manifest,
                          sha=shas["modern"], offline=offline, refresh=refresh)
    if division is not None:
        frame = frame[frame["ncaa_division"] == division].copy()
    else:
        frame = frame.copy()
    frame["player_id"] = pd.to_numeric(frame["player_id"], errors="coerce")
    frame = frame.dropna(subset=["player_id"])
    frame["player_id"] = frame["player_id"].astype("int64")
    frame["class"] = frame["player_class"].map(_norm_class)
    frame["player_G"] = pd.to_numeric(frame.get("player_G"), errors="coerce")
    return frame


def identity_frame(roster: pd.DataFrame) -> pd.DataFrame:
    """The columns the adapters join onto stat rows, one row per player_id."""
    columns = {
        "player_id": "player_id",
        "player_full_name": "name",
        "class": "class",
        "school_id": "school_id",
        "school_name": "school_name",
        "team_conference_name": "conference",
        "player_positions": "pos",
        "player_G": "roster_g",
    }
    present = {src: dst for src, dst in columns.items() if src in roster.columns}
    out = roster[list(present)].rename(columns=present)
    # A player listed twice in one season (rare, and always a duplicate row rather
    # than a genuine second stint) would fan out the stat join. Collapse first.
    return out.drop_duplicates(subset=["player_id"], keep="first")


def team_dicts(roster: pd.DataFrame):
    """Discovery-shaped team dicts, so ``mapping.acronym.map_teams`` is reused.

    ``map_teams`` wants ``{team_id, ncaa_name, conference}``. We key on
    ``school_id`` rather than the mirror's per-season ``team_id`` because
    ``school_id`` identifies the institution and is stable across seasons, which
    makes the coverage report joinable year to year. The two are 1:1 within a
    season (verified for 2025: 307 <-> 307), so nothing is lost.
    """
    grouped = (roster.dropna(subset=["school_id", "school_name"])
               .drop_duplicates(subset=["school_id"]))
    return [
        {
            "team_id": int(row.school_id),
            "ncaa_name": row.school_name,
            "conference": getattr(row, "team_conference_name", None),
        }
        for row in grouped.itertuples()
    ]
