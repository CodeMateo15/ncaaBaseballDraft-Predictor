"""Provenance record for every bulk file we download.

The point of this file is citability. The emitted CSVs are derived from two
GitHub repositories that their owner can rewrite or delete at any time, so
"we downloaded it from armstjc/ncaa_baseball_data" is not a reproducible claim.
A commit SHA plus a sha256 per file is.

``MANIFEST.json`` is written next to the cache it describes and is the thing to
cite. ``verify()`` re-hashes every cached file against it, which turns silent
corruption (a truncated download, an edited CSV) into a loud failure.

Superseded SHAs are retained rather than overwritten, so ``--bulk-refresh``
leaves an audit trail of what the data used to be.
"""

import hashlib
import json
import os
import time

import config


def sha256_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load() -> dict:
    if not os.path.exists(config.BULK_MANIFEST):
        return {"resolved": {}, "files": {}, "superseded": []}
    with open(config.BULK_MANIFEST, "r", encoding="utf-8") as handle:
        return json.load(handle)


def save(manifest: dict) -> None:
    os.makedirs(os.path.dirname(config.BULK_MANIFEST), exist_ok=True)
    tmp = config.BULK_MANIFEST + ".tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")
    os.replace(tmp, config.BULK_MANIFEST)


def record_sha(manifest: dict, alias: str, sha: str) -> dict:
    """Pin a repository to a commit, keeping any previous pin.

    A changed SHA is not an error -- it is what ``--bulk-refresh`` is for -- but
    it must be visible, because every derived number moves with it.
    """
    previous = manifest["resolved"].get(alias)
    if previous and previous["sha"] != sha:
        manifest["superseded"].append({**previous, "alias": alias,
                                       "replaced_at": _now()})
        # Entries pinned to the old SHA are no longer described by this manifest.
        for key in [k for k, v in manifest["files"].items()
                    if v.get("alias") == alias and v.get("commit_sha") != sha]:
            manifest["files"].pop(key)
    manifest["resolved"][alias] = {
        "owner": config.BULK_REPOS[alias]["owner"],
        "repo": config.BULK_REPOS[alias]["repo"],
        "branch": config.BULK_REPOS[alias]["branch"],
        "sha": sha,
        "resolved_at": _now(),
    }
    return manifest


def record_file(manifest: dict, *, alias: str, path: str, url: str,
                local_path: str, rows=None, columns=None, extra=None) -> dict:
    manifest["files"][f"{alias}:{path}"] = {
        "alias": alias,
        "owner": config.BULK_REPOS[alias]["owner"],
        "repo": config.BULK_REPOS[alias]["repo"],
        "commit_sha": manifest["resolved"][alias]["sha"],
        "path": path,
        "url": url,
        "local_path": os.path.relpath(local_path, config.HERE),
        "sha256": sha256_file(local_path),
        "bytes": os.path.getsize(local_path),
        "rows": rows,
        "columns": columns,
        "fetched_at": _now(),
        **(extra or {}),
    }
    return manifest


def verify(manifest=None):
    """Re-hash every cached file. Returns a list of problems, empty if clean."""
    manifest = manifest or load()
    problems = []
    for key, entry in sorted(manifest.get("files", {}).items()):
        local = os.path.join(config.HERE, entry["local_path"])
        if not os.path.exists(local):
            problems.append((key, "missing", entry["local_path"]))
            continue
        actual = sha256_file(local)
        if actual != entry["sha256"]:
            problems.append((key, "sha256 mismatch",
                             f"expected {entry['sha256'][:12]}, got {actual[:12]}"))
    return problems


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")
