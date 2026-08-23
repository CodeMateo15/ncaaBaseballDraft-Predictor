"""Offline sources for the player-stat build.

The live scraper in ``ncaa/`` is unusable from a blocked IP. This package
supplies the same rows from pre-scraped public mirrors, shaped to the contract
``run.py::build_batting_frame`` and ``build_pitching_frame`` already expect, so
everything downstream -- ``derive/``, ``validate/``, ``emit()`` -- runs unchanged.
"""

from sources import bulk, manifest  # noqa: F401
