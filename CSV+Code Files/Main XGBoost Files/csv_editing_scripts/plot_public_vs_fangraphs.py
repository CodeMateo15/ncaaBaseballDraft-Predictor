"""Public reconstruction vs FanGraphs for the nine paywalled metrics -- paper figure.

`ncaa_pitchingNoMinCSV/DATA_NOTICE.md` names exactly nine columns the public matrix
cannot redistribute and therefore derives from raw NCAA counting stats:

    batting   wRC+, wOBA, wRAA, wRC, wSB, Spd
    pitching  FIP, E-F (ERA-FIP), LOB%

One figure, `figures/s0_public_vs_fangraphs.{png,pdf}`: nine panels, each the derived
column against its private (paywalled FanGraphs) original with a y = x reference.
Axes are labelled Public / Private to match how the paper names the two builds.
Styled to match the Stage 1-3 figures (NAVY/RED, dotted grid, no suptitle -- the
caption carries it).

Suggested caption:
    Public reconstruction of the nine FanGraphs-derived metrics, 2021-2025
    (n = 25,546 batting and 24,652 pitching player-seasons). Each panel plots the
    value derived from public NCAA counting statistics against the private
    (paywalled FanGraphs) original; the dashed line is y = x, not a fit. Eight of the nine
    reproduce the original to r >= 0.99. Spd is the exception (r = 0.933) and is
    biased low, because Speed Score mixes in components that public play-by-play
    does not expose. Axes are clipped to the 1st-99th percentile so that a small
    number of extreme low-sample seasons do not compress the plots; r is computed
    on all rows, unclipped.

JOIN CAVEAT, stated up front because the repo's standing rule is never to join player
data on name: there is no shared key. FanGraphs mints `sa3043667`-style ids and the
NCAA public feed mints its own numerics, with zero overlap, so (year, name) is the only
join available. The mitigation is to drop every name that is ambiguous within a year on
EITHER side rather than risk a wrong pair -- this is a validation figure, so losing
~1.3k of ~26.8k player-seasons costs nothing and a mispair would be a silent lie.

Usage:
    python csv_editing_scripts/plot_public_vs_fangraphs.py [--outdir figures]
"""

import argparse
import glob
import os
import re
import unicodedata

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)             # Main XGBoost Files/
CSVROOT = os.path.dirname(ROOT)          # CSV+Code Files/

# (FanGraphs column, public column, display label). Labels follow the notebook's
# _pretty() conventions, including E-F -> ERA-FIP.
BATTING = [('wRC+', 'wrc+', 'wRC+'), ('wOBA', 'woba', 'wOBA'), ('wRAA', 'wraa', 'wRAA'),
           ('wRC', 'wrc', 'wRC'), ('wSB', 'wsb', 'wSB'), ('Spd', 'spd', 'Spd')]
PITCHING = [('FIP', 'fip', 'FIP'), ('E-F', 'e-f', 'ERA-FIP'), ('LOB%', 'lob%', 'LOB%')]

NAVY, RED = '#1a2a4a', '#c8102e'         # matches the notebook's figure helpers


def norm_name(s):
    s = unicodedata.normalize('NFKD', str(s)).encode('ascii', 'ignore').decode()
    return re.sub(r'\s+', ' ', re.sub(r'[^a-z ]', '', s.lower())).strip()


def numeric(s):
    """Coerce to float, tolerating a trailing % on either side's rate columns."""
    return pd.to_numeric(pd.Series(s).astype(str).str.replace('%', '', regex=False),
                         errors='coerce')


def load_pair(fg_glob, public_csv, pairs):
    """Return (public, fangraphs) frames aligned row-for-row on player-season."""
    fg_files = sorted(glob.glob(os.path.join(CSVROOT, fg_glob)))
    if not fg_files:
        raise SystemExit(
            f"no FanGraphs files matched {fg_glob!r}.\n"
            "These are gitignored -- see ncaa_pitchingNoMinCSV/DATA_NOTICE.md. "
            "This figure can only be built on a working copy that has them.")
    frames = []
    for path in fg_files:
        year = int(re.search(r'(\d{4})', os.path.basename(path)).group(1))
        d = pd.read_csv(path, encoding='utf-8-sig')
        d.columns = [c.strip() for c in d.columns]
        d['year'] = year
        d['_nc'] = d['NameASCII'].map(norm_name)
        frames.append(d[['_nc', 'year'] + [a for a, _, _ in pairs]])
    fg = pd.concat(frames, ignore_index=True)

    pub = pd.read_csv(os.path.join(CSVROOT, public_csv), low_memory=False)
    pub['_nc'] = pub['nameascii'].map(norm_name)

    fg = fg[~fg.duplicated(['_nc', 'year'], keep=False)]
    pub = pub[~pub.duplicated(['_nc', 'year'], keep=False)]
    merged = fg.merge(pub[['_nc', 'year'] + [b for _, b, _ in pairs]],
                      on=['_nc', 'year'], how='inner')

    F = pd.DataFrame({a: numeric(merged[a]) for a, _, _ in pairs})
    P = pd.DataFrame({a: numeric(merged[b]) for a, b, _ in pairs})
    keep = F.notna().all(axis=1) & P.notna().all(axis=1)
    print(f"  {len(merged):,} joined -> {int(keep.sum()):,} complete on all {len(pairs)}")
    return P[keep].reset_index(drop=True), F[keep].reset_index(drop=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--outdir', default=os.path.join(ROOT, 'figures'))
    args = ap.parse_args()

    print('batting:')
    Pb, Fb = load_pair('ncaa_battingNoMinCSV/batting_advanced_2*.csv',
                       'ncaa_public/batting_combined_all.csv', BATTING)
    print('pitching:')
    Pp, Fp = load_pair('ncaa_pitchingNoMinCSV/pitching_advanced_2*.csv',
                       'ncaa_public/pitching_combined_all.csv', PITCHING)

    panels = ([(lab, Pb[a].values, Fb[a].values) for a, _, lab in BATTING]
              + [(lab, Pp[a].values, Fp[a].values) for a, _, lab in PITCHING])

    print('\nper-metric reconstruction quality:')
    for lab, pub, fgv in panels:
        r = np.corrcoef(pub, fgv)[0, 1]
        print(f'  {lab:<8} r={r:.5f}  R2={r*r:.5f}  MAE={np.abs(pub-fgv).mean():.4g}')
    print(f'\n  n = {len(Pb):,} batting, {len(Pp):,} pitching player-seasons')

    plt.rcParams.update({'font.size': 11, 'axes.labelsize': 12})
    fig, axes = plt.subplots(3, 3, figsize=(9.6, 9.8))
    for k, (ax, (lab, pub, fgv)) in enumerate(zip(axes.flat, panels)):
        # Robust limits: wRC+ reaches 990 and FIP 99.97 on tiny samples, and one such
        # season would squash every real point into a corner.
        both = np.concatenate([pub, fgv])
        lo, hi = np.nanpercentile(both, 1), np.nanpercentile(both, 99)
        pad = (hi - lo) * 0.04
        lo, hi = lo - pad, hi + pad
        m = (pub >= lo) & (pub <= hi) & (fgv >= lo) & (fgv <= hi)
        # rasterized: 25k points x 9 panels as vectors makes an unusably large PDF.
        ax.scatter(fgv[m], pub[m], s=3, alpha=0.12, color=NAVY, edgecolor='none',
                   rasterized=True)
        ax.plot([lo, hi], [lo, hi], 'r--', lw=1.3, color=RED,
                label='y = x' if k == 0 else None)
        ax.set_xlim(lo, hi); ax.set_ylim(lo, hi); ax.set_aspect('equal')
        ax.set_title(lab, fontsize=13)
        ax.text(0.05, 0.95, f'r = {np.corrcoef(pub, fgv)[0, 1]:.3f}',
                transform=ax.transAxes, va='top', ha='left', fontsize=11)
        ax.grid(ls=':', lw=0.5, alpha=0.5)
        # Outer labels only -- nine copies of the same pair is clutter in print.
        if k // 3 == 2:
            ax.set_xlabel('Private')
        if k % 3 == 0:
            ax.set_ylabel('Public')
        if k == 0:
            ax.legend(loc='lower right', fontsize=10, framealpha=0.9)
    plt.tight_layout()

    os.makedirs(args.outdir, exist_ok=True)
    stem = os.path.join(args.outdir, 's0_public_vs_fangraphs')
    fig.savefig(f'{stem}.png', dpi=300, bbox_inches='tight')
    fig.savefig(f'{stem}.pdf', bbox_inches='tight')
    print(f'  saved {stem}.pdf')
    plt.close(fig)


if __name__ == '__main__':
    main()
