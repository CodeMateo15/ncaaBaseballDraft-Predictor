# Vendored files

## Why vendored rather than imported

`ncaa_bbStats` is installed on this machine, but not usably:

1. Anaconda has an editable install (`__editable__.ncaa_bbstats-1.3.0.pth`) whose
   only line points at `/Users/mateobiggs/CollegeBaseballStatsPackage/src` — a
   directory that no longer exists after the repo was renamed to `ncaa_bbStats`.
   `import ncaa_bbStats` fails outright there.
2. `/Library/Frameworks/Python.framework/Versions/3.12/lib/python3.12/site-packages/ncaa_bbStats/`
   holds a **stale v1 copy** with no `_normalize.py`, no `team_registry.py`, and
   no `advanced_stats.py`. It imports *successfully* and silently gives you the
   wrong package. That is the failure mode worth avoiding.
3. Only the anaconda interpreter has `pandas`, `curl_cffi`, and `bs4` together.

Beyond the mechanics: an editable install pointing at a working tree outside
this repo is the same class of defect this folder exists to remove — a number
whose provenance you cannot reconstruct from what you shipped. Vendoring makes
`pip install -r requirements.txt && python run.py` sufficient.

## Manifest

Upstream: `/Users/mateobiggs/ncaa_bbStats` at git SHA `24b305085fab498d6566a35f00af4def7feec605`.

| local path | upstream path | upstream sha256 | change |
|---|---|---|---|
| `mapping/_normalize.py` | `src/ncaa_bbStats/_normalize.py` | `a12a5aee781bf11fe4e96faa5dcd59c91860200dd0c2c9c4173ae07d61ad64cb` | verbatim |
| `mapping/team_registry.py` | `src/ncaa_bbStats/team_registry.py` | `278100655e886435ffea7eb46a91110f6cdcf5f31ba9926f2f5e89e82905fc50` | 2 import lines replaced by a local `data_path()`; local sha256 `c5a11594feae8259ee20a177baa80a68a07b71824e8167698e7a5fda3d145a48` |
| `mapping/data/registry/teams.csv` | `src/data/registry/teams.csv` | `7c659421af59c8274396ae8d542c3e38e5ab97ffc74a00d99b3cc54e66ace19f` | verbatim |
| `mapping/data/registry/team_aliases.csv` | `src/data/registry/team_aliases.csv` | `fff6018fbf1f656f700673164972f48e74b51426cfd49be572590ad68aca44bb` | verbatim |
| `mapping/data/registry/team_seasons.csv` | `src/data/registry/team_seasons.csv` | `01410fd069a012976dc6fa84af6a50e0e0c86f8d41380408ca8d175fb7be7439` | verbatim |
| `vendor/reference/batting_weights.csv` | `src/data/league_constants/batting_weights.csv` | `eb7e4af63cb60455904b6377b361e50f57628b779d52e53ff86fdab26e86e6bb` | verbatim, **cross-check target only** |
| `vendor/reference/pitching_constants.csv` | `src/data/league_constants/pitching_constants.csv` | `ef3dfcbcd9e7963c6a68740b6965738608fc3d5488bd489bfdc1e0c212e1cc7a` | verbatim, **cross-check target only** |

`derive/rates.py::ip_to_float` and the `c*` functions in `derive/advanced.py` are
adapted from `src/ncaa_bbStats/advanced_stats.py` at the same SHA. They are
adaptations rather than copies (the constants come from our own scrape), so they
are attributed in their docstrings rather than hash-tracked here.

## The two reference files are not authoritative here

`vendor/reference/*.csv` are the league constants `ncaa_bbStats` built from NCAA
*team* statistics. This folder regenerates its own constants from its own player
scrape (`derive/constants.py`), so that the numbers describe exactly the
population we emit. The vendored copies exist so `validate/acceptance.py` gate 14
can assert the new fit lands within 5% of them — a disagreement means the
population or the regression changed and needs explaining.

## Re-vendoring

```bash
SRC=/Users/mateobiggs/ncaa_bbStats
cp $SRC/src/ncaa_bbStats/_normalize.py            mapping/_normalize.py
cp $SRC/src/ncaa_bbStats/team_registry.py         mapping/team_registry.py   # then re-apply the data_path patch
cp $SRC/src/data/registry/*.csv                   mapping/data/registry/
cp $SRC/src/data/league_constants/*.csv           vendor/reference/
shasum -a 256 mapping/_normalize.py mapping/team_registry.py \
              mapping/data/registry/*.csv vendor/reference/*.csv
```

Update this table when you do.
