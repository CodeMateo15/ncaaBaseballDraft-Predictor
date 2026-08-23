"""Download the bulk mirror files, once, by pinned commit SHA.

Deliberately boring: no impersonation, no challenge solver, no request budget.
raw.githubusercontent.com serves static files to anyone, which is the whole
reason this path exists -- it works from the blocked IP that made the live
scraper unusable.

Two rules the rest of the package relies on:

* **Pin, then fetch.** The SHA is resolved once per repository per run and
  recorded in the manifest. Every file URL embeds that SHA, so a re-run either
  gets byte-identical data or fails; it cannot silently drift when upstream
  pushes.
* **Cache is authoritative.** A file present locally is never re-downloaded and
  never re-hashed against the network. ``--bulk-refresh`` is the only way to move
  a pin, and ``--verify-bulk`` is the only way to re-check bytes.
"""

import io
import json
import os
import urllib.error
import urllib.parse
import urllib.request

import pandas as pd

import config
from sources import manifest as manifest_mod

_UA = "ncaa-draft-research/1.0 (academic; contact via repository)"


class BulkFetchError(RuntimeError):
    """A bulk file could not be obtained. Never swallowed -- a missing source
    file silently becomes a missing team-season, which is exactly the class of
    error this project cannot afford."""


def _get(url: str, *, timeout=60) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": _UA})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.read()
    except urllib.error.HTTPError as error:
        raise BulkFetchError(f"HTTP {error.code} for {url}") from error
    except urllib.error.URLError as error:
        raise BulkFetchError(f"network error for {url}: {error.reason}") from error


def resolve_sha(alias: str, *, manifest, refresh=False, pin=None) -> str:
    """Return the commit SHA to fetch `alias` at, resolving it at most once."""
    if pin:
        manifest_mod.record_sha(manifest, alias, pin)
        return pin

    existing = manifest["resolved"].get(alias)
    if existing and not refresh:
        return existing["sha"]

    spec = config.BULK_REPOS[alias]
    url = config.GITHUB_COMMIT_API.format(**spec)
    payload = json.loads(_get(url).decode("utf-8"))
    sha = payload["sha"]
    manifest_mod.record_sha(manifest, alias, sha)
    return sha


def local_path(alias: str, path: str) -> str:
    spec = config.BULK_REPOS[alias]
    return os.path.join(config.BULK_CACHE_DIR,
                        f"{spec['owner']}_{spec['repo']}", *path.split("/"))


def fetch(alias: str, path: str, *, manifest, sha, offline=False,
          refresh=False) -> str:
    """Ensure one upstream file is cached locally. Returns its local path."""
    target = local_path(alias, path)
    if os.path.exists(target) and not refresh:
        return target
    if offline:
        raise BulkFetchError(
            f"{alias}:{path} is not cached and --offline forbids downloading it. "
            f"Run once without --offline to populate {config.BULK_CACHE_DIR}.")

    url = config.GITHUB_RAW.format(sha=sha, path=path,
                                   **{k: v for k, v in config.BULK_REPOS[alias].items()
                                      if k in ("owner", "repo")})
    data = _get(url)
    os.makedirs(os.path.dirname(target), exist_ok=True)
    tmp = target + ".tmp"
    with open(tmp, "wb") as handle:
        handle.write(data)
    os.replace(tmp, target)

    frame = pd.read_csv(io.BytesIO(data), low_memory=False)
    manifest_mod.record_file(manifest, alias=alias, path=path, url=url,
                             local_path=target, rows=len(frame),
                             columns=list(frame.columns),
                             extra={"source_last_commit": last_commit(alias, path)})
    return target


def last_commit(alias: str, path: str):
    """When this specific file was last written upstream.

    Not the same as the repository's HEAD, and the difference is what caught the
    2026 problem: HEAD was current, but the 2026 season file had not been touched
    since 9 April -- mid-season -- because the maintainer stopped updating the
    repository entirely (final commit, 12 April 2026, titled "That's all folks").
    A season file written before its season ended holds a partial season, and
    nothing in the data itself says so.
    """
    spec = config.BULK_REPOS[alias]
    url = (f"https://api.github.com/repos/{spec['owner']}/{spec['repo']}"
           f"/commits?path={urllib.parse.quote(path)}&per_page=1")
    try:
        payload = json.loads(_get(url).decode("utf-8"))
    except (BulkFetchError, ValueError):
        return None
    if not payload:
        return None
    return payload[0]["commit"]["committer"]["date"]


def season_path(alias: str, year: int, category: str) -> str:
    return config.BULK_FILES[(alias, "season")].format(year=year, category=category)


def roster_path(alias: str, year: int) -> str:
    return config.BULK_FILES[(alias, "roster")].format(year=year)


def load_csv(alias: str, path: str, *, manifest, sha, offline=False,
             refresh=False) -> pd.DataFrame:
    target = fetch(alias, path, manifest=manifest, sha=sha, offline=offline,
                   refresh=refresh)
    return pd.read_csv(target, low_memory=False)


def required_files(years):
    """Every (alias, path) the given years need, so one pass can prefetch them.

    Kept declarative rather than discovered lazily: it lets `--probe-bulk` and
    the manifest report the full intended footprint before anything downloads,
    and it means a typo in BULK_FILES surfaces immediately.
    """
    wanted = []
    years = sorted(years)
    for year in years:
        sources = config.BULK_YEAR_SOURCES.get(year, ())
        if "rich" in sources or "lean" in sources:
            for category in ("batting", "pitching"):
                wanted.append(("modern", season_path("modern", year, category)))
        # Matches legacy, legacy_patch and legacy_fill -- naming a new variant
        # should not silently skip its downloads.
        if any(source.startswith("legacy") for source in sources):
            for category in ("batting", "pitching"):
                wanted.append(("legacy", season_path("legacy", year, category)))
            wanted.append(("legacy", roster_path("legacy", year)))
        # Rosters carry class, school and (for 2021) games played, for every year.
        wanted.append(("modern", roster_path("modern", year)))
    seen = set()
    return [item for item in wanted if not (item in seen or seen.add(item))]


# College baseball's regular season plus postseason ends in late June. A season
# file last written before this month-day of the following-or-same calendar year
# cannot contain a finished season.
SEASON_COMPLETE_AFTER = (7, 1)


def stale_seasons(manifest, years):
    """Season files written before their season finished, i.e. partial seasons.

    Returns {year: (path, last_commit)}. This is a data-completeness fact that the
    files themselves do not carry, so it has to come from the repository history.
    """
    stale = {}
    for entry in manifest.get("files", {}).values():
        stamp = entry.get("source_last_commit")
        if not stamp:
            continue
        path = entry["path"]
        for year in years:
            if f"{year}_season" not in path and f"/{year}_" not in path:
                continue
            if "season_stats" not in path:
                continue
            cutoff = f"{year}-{SEASON_COMPLETE_AFTER[0]:02d}-" \
                     f"{SEASON_COMPLETE_AFTER[1]:02d}"
            if stamp[:10] < cutoff:
                stale[year] = (path, stamp)
    return stale


def prefetch(years, *, refresh=False, offline=False, pins=None):
    """Download everything the run needs and return (manifest, shas)."""
    manifest = manifest_mod.load()
    pins = pins or {}
    shas = {}
    files = required_files(years)
    aliases = sorted({alias for alias, _ in files})
    for alias in aliases:
        shas[alias] = resolve_sha(alias, manifest=manifest, refresh=refresh,
                                  pin=pins.get(alias))

    for index, (alias, path) in enumerate(files, 1):
        target = local_path(alias, path)
        state = "cached" if os.path.exists(target) and not refresh else "fetching"
        print(f"  [{index}/{len(files)}] {state}: {alias}:{path}", flush=True)
        fetch(alias, path, manifest=manifest, sha=shas[alias], offline=offline,
              refresh=refresh)

    manifest_mod.save(manifest)
    return manifest, shas
