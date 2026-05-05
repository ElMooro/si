
# 1) Wire Clusters into canonical pages

- `18:57:40`     ✓ index.html
- `18:57:40`     ✓ desk.html
- `18:57:40`     ❌ brief.html: no_anchor
- `18:57:40`     ❌ calls.html: no_anchor
- `18:57:40`     ❌ performance.html: no_anchor
- `18:57:40`     ❌ sizing.html: no_anchor
- `18:57:40`     ❌ backtest.html: no_anchor
- `18:57:40`     ❌ weights.html: no_anchor
- `18:57:40`     ❌ horizons.html: no_anchor
- `18:57:40`     ❌ themes.html: no_anchor
- `18:57:40`     ❌ nobrainers.html: no_anchor
- `18:57:40`     ✓ insiders.html
- `18:57:40`     ❌ 13f.html: no_anchor
- `18:57:40`     ❌ accuracy.html: no_anchor
- `18:57:40`     ❌ allocator.html: no_anchor
- `18:57:40`     ❌ sectors.html: no_anchor
- `18:57:40`     ❌ momentum.html: no_anchor
- `18:57:40`     ❌ news.html: no_anchor
- `18:57:40`     ❌ research.html: no_anchor
- `18:57:40`     ❌ vol.html: no_anchor
- `18:57:40`     ❌ ticker.html: no_anchor
- `18:57:40`     ❌ today.html: no_anchor
- `18:57:40`     ❌ feedback.html: no_anchor
- `18:57:40`   
- `18:57:40`     patched: 3  skipped: 0  failed: 20

# 2) Verify reachability (after deploy)

- `18:57:40`     insider-clusters.html: 19,177b in repo
- `18:57:40`     insiders.html: 22,476b in repo

# 3) Live curl from inside Action

- `18:57:40`     200       19177b  https://justhodl.ai/insider-clusters.html
- `18:57:40`     200       22428b  https://justhodl.ai/insiders.html
- `18:57:40`     200       43345b  https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/data/insider-clusters.json