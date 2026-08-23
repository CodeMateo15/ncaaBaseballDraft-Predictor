"""On-disk cache for fetched pages and parsed rows.

Two layers, because they have very different costs:

* **HTML** (~1.9 MB/page, ~700 MB gzipped for six years of D1). Makes a re-run
  cost zero requests and a crash resume at the first missing key.
* **Parsed rows** (~15 MB total). Makes a warm run take under a minute instead
  of the ~25 minutes BeautifulSoup needs to re-chew 3,600 pages. Iterating on
  the derived-stat code should not cost a coffee break.

Every write is ``*.tmp`` then :func:`os.replace`, so a kill -9 mid-write leaves
either the old entry or the new one, never a truncated file. That is what makes
resume safe without a bookkeeping file to corrupt.
"""

import gzip
import json
import os

import config


def _ensure(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)


def _write_atomic(path: str, data: bytes) -> None:
    _ensure(path)
    tmp = path + ".tmp"
    with open(tmp, "wb") as handle:
        handle.write(data)
    os.replace(tmp, path)


def html_path(year: int, division: int, team_id, category: str) -> str:
    return os.path.join(
        config.CACHE_DIR, f"d{division}", str(year), f"{team_id}_{category}.html.gz"
    )


def rows_path(year: int, division: int, team_id, category: str) -> str:
    return os.path.join(
        config.CACHE_DIR, f"d{division}", str(year), f"{team_id}_{category}.rows.json.gz"
    )


def teams_path(year: int, division: int) -> str:
    return os.path.join(config.CACHE_DIR, f"d{division}", str(year), "_teams.json")


def read_html(path: str):
    """Return cached HTML, or None if not cached."""
    if not os.path.exists(path):
        return None
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        return handle.read()


def write_html(path: str, html: str) -> None:
    _write_atomic(path, gzip.compress(html.encode("utf-8")))


def read_json_gz(path: str):
    if not os.path.exists(path):
        return None
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        return json.load(handle)


def write_json_gz(path: str, obj) -> None:
    _write_atomic(path, gzip.compress(json.dumps(obj).encode("utf-8")))


def read_json(path: str):
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: str, obj) -> None:
    _write_atomic(path, json.dumps(obj, indent=2).encode("utf-8"))


def fetch_or_cached(session, url: str, path: str, refresh: bool = False) -> str:
    """Return ``path``'s cached HTML, fetching ``url`` into it if absent.

    Args:
        session: an :class:`ncaa.session.NcaaSession`, or None to require a hit.
        refresh: ignore any existing entry and refetch.

    Raises:
        FileNotFoundError: not cached and no session was supplied.
    """
    if not refresh:
        cached = read_html(path)
        if cached is not None:
            return cached

    if session is None:
        raise FileNotFoundError(
            f"{path} is not cached and no session was given (offline mode)"
        )

    html = session.get(url)
    write_html(path, html)
    return html


def prune_html(division: int = None) -> int:
    """Delete cached HTML, keeping parsed rows. Returns bytes reclaimed.

    Run this only after validation passes -- the parsed rows are enough to
    rebuild the CSVs, but not to re-examine a parsing decision.
    """
    root = config.CACHE_DIR
    if division is not None:
        root = os.path.join(root, f"d{division}")

    reclaimed = 0
    for dirpath, _dirnames, filenames in os.walk(root):
        for name in filenames:
            if name.endswith(".html.gz"):
                full = os.path.join(dirpath, name)
                reclaimed += os.path.getsize(full)
                os.remove(full)
    return reclaimed
