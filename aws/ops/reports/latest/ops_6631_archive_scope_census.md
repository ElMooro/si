# ops 6631 - archive scope census: what each folder costs the archive and whether it is rebuilt (read-only)

**Status:** success  
**Duration:** 173.9s  
**Finished:** 2026-10-23T19:08:10+00:00  

## Data

| families | inventory | min30_usd_month | put_usd_month | window_days | writes_day |
|---|---|---|---|---|---|
| 603 | 2026-10-22T01-00Z | 3.34 | 22.05 | 1.0 | 74455 |

## Log
- `19:05:16` inventory 2026-10-22T01-00Z (9 files), window 2026-10-21T01:00 -> 2026-10-22T01:00 (1.00 days)
- `19:05:27` file 1/9: rows 928436, keys 525506, keys written in window 20247
- `19:05:46` file 2/9: rows 2616701, keys 2161389, keys written in window 34996
- `19:06:16` file 3/9: rows 5616701, keys 2708324, keys written in window 50184
- `19:06:25` file 4/9: rows 6616701, keys 2708324, keys written in window 50184
- `19:06:25` file 5/9: rows 6616702, keys 2708325, keys written in window 50184
- `19:06:25` file 6/9: rows 6616966, keys 2708397, keys written in window 50186
- `19:06:33` file 7/9: rows 7461115, keys 2795994, keys written in window 52485
- `19:07:00` file 8/9: rows 10461115, keys 2795994, keys written in window 52485
- `19:07:11` file 9/9: rows 11461115, keys 3420298, keys written in window 54559
## access logs: who writes each family (yesterday, sampled)

- `19:08:10` log objects 262172, read 6000 (x43.7)
## families by monthly archive cost of their writes

- `19:08:10` audit-private/20260909-originals/financial-statement-resea writes/day   15384 (new   6252, rewrites   9132) | PUT $ 4.62 + 30d-min $ 0.46 /mo, + 25.26 GB/mo | holds   21.60 GB, rebuilt 0.038 | [('github-actions-justhodl', 15381)]
- `19:08:10` audit-private/20260909-originals/fx-research/              writes/day    6301 (new   3973, rewrites   2328) | PUT $ 1.89 + 30d-min $ 0.11 /mo, + 15.62 GB/mo | holds    0.65 GB, rebuilt 0.799 | [('justhodl-polygon-fx-regime', 9788)]
- `19:08:10` data/fundgraph/                                            writes/day    4206 (new    650, rewrites   3556) | PUT $ 1.26 + 30d-min $ 0.35 /mo, +  5.18 GB/mo | holds   10.40 GB, rebuilt 0.085 | [('justhodl-fundamental-graphs', 2797)]
- `19:08:10` data/etf-holdings-research/                                writes/day    4537 (new   4531, rewrites      6) | PUT $ 1.36 + 30d-min $ 0.00 /mo, + 21.25 GB/mo | holds   17.37 GB, rebuilt 0.041 | [('justhodl-etf-constituents', 2185)]
- `19:08:10` data/(files)                                               writes/day    2423 (new      0, rewrites   2423) | PUT $ 0.73 + 30d-min $ 0.42 /mo, +  0.00 GB/mo | holds    0.98 GB, rebuilt 0.642 | [('justhodl-intraday-pulse', 350), ('justhodl-options-analytics', 350), ('justhodl-opex-calendar', 131)]
- `19:08:10` audit-private/20260909-originals/tail-research/            writes/day    2522 (new   2522, rewrites      0) | PUT $ 0.76 + 30d-min $ 0.00 /mo, + 10.33 GB/mo | holds    0.47 GB, rebuilt 0.729 | [('justhodl-tail-risk', 1792)]
- `19:08:10` data/evidence/                                             writes/day    2022 (new   2022, rewrites      0) | PUT $ 0.61 + 30d-min $ 0.00 /mo, +  8.02 GB/mo | holds   22.86 GB, rebuilt 0.012 | [('justhodl-etf-true-flows', 1311), ('justhodl-tradingview', 1092)]
- `19:08:10` audit-private/20260909-originals/futures-research/         writes/day    1751 (new   1070, rewrites    681) | PUT $ 0.53 + 30d-min $ 0.03 /mo, +  4.21 GB/mo | holds    0.60 GB, rebuilt 0.251 | [('justhodl-polygon-futures-curves', 830)]
- `19:08:10` data/warm/fred-scoped/                                     writes/day    1507 (new      0, rewrites   1507) | PUT $ 0.45 + 30d-min $ 0.13 /mo, +  0.00 GB/mo | holds   39.99 GB, rebuilt 0.005 | []
- `19:08:10` data/repo-history/                                         writes/day    1658 (new      0, rewrites   1658) | PUT $ 0.50 + 30d-min $ 0.08 /mo, +  0.00 GB/mo | holds    0.23 GB, rebuilt 0.976 | [('justhodl-repo', 2185)]
- `19:08:10` data/providers/gdelt/                                      writes/day    1628 (new      1, rewrites   1627) | PUT $ 0.49 + 30d-min $ 0.08 /mo, +  0.00 GB/mo | holds    0.11 GB, rebuilt 1.0 | [('justhodl-provider-catalog', 2840)]
- `19:08:10` audit-private/20260909-originals/etf-constituent-research/ writes/day    1335 (new   1335, rewrites      0) | PUT $ 0.40 + 30d-min $ 0.00 /mo, +  7.37 GB/mo | holds    6.32 GB, rebuilt 0.037 | [('justhodl-etf-constituents', 2884)]
- `19:08:10` audit-private/20260909-originals/options-research/         writes/day    1372 (new    707, rewrites    665) | PUT $ 0.41 + 30d-min $ 0.03 /mo, +  2.82 GB/mo | holds   13.15 GB, rebuilt 0.007 | []
- `19:08:10` equity-research/(files)                                    writes/day    1141 (new    296, rewrites    845) | PUT $ 0.34 + 30d-min $ 0.05 /mo, +  1.32 GB/mo | holds    2.50 GB, rebuilt 0.042 | [('justhodl-equity-research', 787)]
- `19:08:10` data/providers/fred/                                       writes/day    1129 (new      0, rewrites   1129) | PUT $ 0.34 + 30d-min $ 0.06 /mo, +  0.00 GB/mo | holds    0.07 GB, rebuilt 1.0 | []
- `19:08:10` data/risk-regime-research/                                 writes/day    1088 (new   1088, rewrites      0) | PUT $ 0.33 + 30d-min $ 0.00 /mo, +  5.49 GB/mo | holds    0.41 GB, rebuilt 0.531 | [('justhodl-risk-regime', 743)]
- `19:08:10` data/vintage-research/                                     writes/day     697 (new    327, rewrites    370) | PUT $ 0.21 + 30d-min $ 0.10 /mo, +  7.02 GB/mo | holds    7.22 GB, rebuilt 0.098 | [('justhodl-vintage-fred', 568)]
- `19:08:10` equity-research-history/                                   writes/day    1112 (new    755, rewrites    357) | PUT $ 0.33 + 30d-min $ 0.02 /mo, +  3.36 GB/mo | holds    3.44 GB, rebuilt 0.031 | [('justhodl-equity-research', 787)]
- `19:08:10` data/watchlist-insights/                                   writes/day    1104 (new      0, rewrites   1104) | PUT $ 0.33 + 30d-min $ 0.06 /mo, +  0.00 GB/mo | holds    0.15 GB, rebuilt 0.996 | []
- `19:08:10` data/warm/nyfed-research/                                  writes/day     426 (new      0, rewrites    426) | PUT $ 0.13 + 30d-min $ 0.26 /mo, +  0.00 GB/mo | holds    0.70 GB, rebuilt 0.0 | []
- `19:08:10` data/series-cache/                                         writes/day     618 (new      3, rewrites    615) | PUT $ 0.19 + 30d-min $ 0.17 /mo, +  0.07 GB/mo | holds    9.73 GB, rebuilt 0.009 | [('github-actions-justhodl', 612), ('justhodl-symdir', 44)]
- `19:08:10` data/warm/tv-bars/                                         writes/day     955 (new      0, rewrites    955) | PUT $ 0.29 + 30d-min $ 0.06 /mo, +  0.00 GB/mo | holds    0.09 GB, rebuilt 0.951 | [('github-actions-justhodl', 1355), ('justhodl-tv-bars', 44)]
- `19:08:10` audit-private/20260909-originals/etf-desk-research/        writes/day     960 (new    948, rewrites     12) | PUT $ 0.29 + 30d-min $ 0.00 /mo, +  4.29 GB/mo | holds    2.04 GB, rebuilt 0.071 | []
- `19:08:10` data/option-flow-research/                                 writes/day     701 (new    701, rewrites      0) | PUT $ 0.21 + 30d-min $ 0.00 /mo, + 10.42 GB/mo | holds   46.70 GB, rebuilt 0.007 | []
- `19:08:10` equity-research/industry2/                                 writes/day     583 (new    170, rewrites    413) | PUT $ 0.17 + 30d-min $ 0.09 /mo, +  3.11 GB/mo | holds    9.15 GB, rebuilt 0.035 | [('justhodl-equity-research', 568)]
- `19:08:10` data/warm/blackswan/                                       writes/day     813 (new      0, rewrites    813) | PUT $ 0.24 + 30d-min $ 0.04 /mo, +  0.00 GB/mo | holds    0.19 GB, rebuilt 0.266 | [('justhodl-dxy-predict', 1180)]
- `19:08:10` data/tail-research/                                        writes/day     776 (new    388, rewrites    388) | PUT $ 0.23 + 30d-min $ 0.02 /mo, +  1.53 GB/mo | holds    0.07 GB, rebuilt 0.695 | [('justhodl-tail-risk', 655)]
- `19:08:10` data/implied-research/                                     writes/day     776 (new    388, rewrites    388) | PUT $ 0.23 + 30d-min $ 0.02 /mo, +  1.53 GB/mo | holds    0.06 GB, rebuilt 0.807 | [('justhodl-implied-prob', 437)]
- `19:08:10` data/fx-quote-research/                                    writes/day     764 (new    764, rewrites      0) | PUT $ 0.23 + 30d-min $ 0.00 /mo, +  3.00 GB/mo | holds    0.16 GB, rebuilt 0.616 | [('justhodl-polygon-fx-regime', 2054)]
- `19:08:10` audit-private/20260909-originals/provider-fund-flow-resear writes/day     664 (new    664, rewrites      0) | PUT $ 0.20 + 30d-min $ 0.00 /mo, +  3.05 GB/mo | holds    1.50 GB, rebuilt 0.074 | [('justhodl-etf-fund-flows', 1005)]
- `19:08:10` data/provider-flow-research/                               writes/day     652 (new    642, rewrites     10) | PUT $ 0.20 + 30d-min $ 0.00 /mo, +  2.74 GB/mo | holds    1.24 GB, rebuilt 0.074 | []
- `19:08:10` data/holdings-research/                                    writes/day     141 (new    141, rewrites      0) | PUT $ 0.04 + 30d-min $ 0.00 /mo, + 14.90 GB/mo | holds   81.91 GB, rebuilt 0.006 | []
- `19:08:10` audit-private/20260909-originals/market-internals/         writes/day     254 (new    254, rewrites      0) | PUT $ 0.08 + 30d-min $ 0.00 /mo, +  9.90 GB/mo | holds    6.88 GB, rebuilt 0.048 | []
- `19:08:10` data/warm/gleif/                                           writes/day       2 (new      0, rewrites      2) | PUT $ 0.00 + 30d-min $ 0.20 /mo, +  0.00 GB/mo | holds    0.54 GB, rebuilt 1.0 | []
- `19:08:10` data/statement-research/                                   writes/day     503 (new    503, rewrites      0) | PUT $ 0.15 + 30d-min $ 0.00 /mo, +  3.73 GB/mo | holds    3.34 GB, rebuilt 0.037 | []
- `19:08:10` data/warm/eba-risk-dashboard/                              writes/day     463 (new      0, rewrites    463) | PUT $ 0.14 + 30d-min $ 0.03 /mo, +  0.00 GB/mo | holds    0.07 GB, rebuilt 1.0 | []
- `19:08:10` data/engines/                                              writes/day     492 (new      0, rewrites    492) | PUT $ 0.15 + 30d-min $ 0.02 /mo, +  0.00 GB/mo | holds    0.06 GB, rebuilt 1.0 | []
- `19:08:10` data/etf-desk-research/                                    writes/day     470 (new    470, rewrites      0) | PUT $ 0.14 + 30d-min $ 0.00 /mo, +  2.16 GB/mo | holds    1.01 GB, rebuilt 0.071 | []
- `19:08:10` data/warm/gdelt-full/                                      writes/day     405 (new    393, rewrites     12) | PUT $ 0.12 + 30d-min $ 0.00 /mo, +  1.55 GB/mo | holds  108.39 GB, rebuilt 0.001 | []
- `19:08:10` data/option-population-research/                           writes/day     328 (new    328, rewrites      0) | PUT $ 0.10 + 30d-min $ 0.00 /mo, +  2.40 GB/mo | holds    8.87 GB, rebuilt 0.009 | []
- `19:08:10` audit-private/20260909-originals/offexchange-research/     writes/day     356 (new    137, rewrites    219) | PUT $ 0.11 + 30d-min $ 0.01 /mo, +  0.71 GB/mo | holds    0.45 GB, rebuilt 0.088 | [('justhodl-dark-pool', 481)]
- `19:08:10` data/research-network/                                     writes/day     333 (new    333, rewrites      0) | PUT $ 0.10 + 30d-min $ 0.00 /mo, +  2.26 GB/mo | holds    1.10 GB, rebuilt 0.06 | []
- `19:08:10` audit-private/20260909-originals/geopolitical-risk-researc writes/day     333 (new    333, rewrites      0) | PUT $ 0.10 + 30d-min $ 0.00 /mo, +  2.06 GB/mo | holds    1.18 GB, rebuilt 0.056 | []
- `19:08:10` data/search/                                               writes/day     198 (new      2, rewrites    196) | PUT $ 0.06 + 30d-min $ 0.05 /mo, +  0.04 GB/mo | holds   37.73 GB, rebuilt 0.088 | []
- `19:08:10` data/short-volume-research/                                writes/day     259 (new    259, rewrites      0) | PUT $ 0.08 + 30d-min $ 0.00 /mo, +  2.38 GB/mo | holds    1.19 GB, rebuilt 0.067 | []
- `19:08:10` data/eps-revision-velocity/                                writes/day     202 (new    202, rewrites      0) | PUT $ 0.06 + 30d-min $ 0.00 /mo, +  2.14 GB/mo | holds    0.58 GB, rebuilt 0.151 | []
- `19:08:10` data/carry-research/                                       writes/day     173 (new    173, rewrites      0) | PUT $ 0.05 + 30d-min $ 0.00 /mo, +  2.54 GB/mo | holds    3.61 GB, rebuilt 0.022 | [('justhodl-carry-surface', 262)]
- `19:08:10` data/providers/worldbank/                                  writes/day     242 (new      0, rewrites    242) | PUT $ 0.07 + 30d-min $ 0.01 /mo, +  0.00 GB/mo | holds    0.02 GB, rebuilt 1.0 | []
- `19:08:10` data/warm/census-us/                                       writes/day     195 (new      0, rewrites    195) | PUT $ 0.06 + 30d-min $ 0.02 /mo, +  0.00 GB/mo | holds    0.07 GB, rebuilt 0.537 | [('justhodl-census-us', 175)]
- `19:08:10` data/warm/dol-full/                                        writes/day      77 (new      0, rewrites     77) | PUT $ 0.02 + 30d-min $ 0.06 /mo, +  0.00 GB/mo | holds    0.16 GB, rebuilt 0.0 | []
- `19:08:10` audit-private/20260909-originals/short-volume-research/    writes/day     212 (new     79, rewrites    133) | PUT $ 0.06 + 30d-min $ 0.01 /mo, +  0.34 GB/mo | holds    0.20 GB, rebuilt 0.062 | [('justhodl-short-pressure', 44)]
- `19:08:10` audit-private/20260909-originals/implied-research/         writes/day     195 (new    195, rewrites      0) | PUT $ 0.06 + 30d-min $ 0.00 /mo, +  1.08 GB/mo | holds    0.17 GB, rebuilt 0.684 | [('justhodl-implied-prob', 175)]
- `19:08:10` data/providers/(files)                                     writes/day     188 (new      0, rewrites    188) | PUT $ 0.06 + 30d-min $ 0.01 /mo, +  0.00 GB/mo | holds    0.01 GB, rebuilt 0.989 | [('justhodl-provider-catalog', 44)]
- `19:08:10` data/fifx-vol-research/                                    writes/day      53 (new     53, rewrites      0) | PUT $ 0.02 + 30d-min $ 0.00 /mo, +  3.99 GB/mo | holds    2.42 GB, rebuilt 0.065 | []
- `19:08:10` data/corporate-actions/                                    writes/day     197 (new    195, rewrites      2) | PUT $ 0.06 + 30d-min $ 0.00 /mo, +  0.77 GB/mo | holds    0.42 GB, rebuilt 0.062 | [('justhodl-corporate-actions', 2185)]
- `19:08:10` audit-private/20260909-originals/global-sovereign-research writes/day     188 (new    188, rewrites      0) | PUT $ 0.06 + 30d-min $ 0.00 /mo, +  0.75 GB/mo | holds    0.47 GB, rebuilt 0.054 | [('justhodl-global-sovereign', 175)]
- `19:08:10` audit-private/20260909-originals/signal-board-research/    writes/day     195 (new     98, rewrites     97) | PUT $ 0.06 + 30d-min $ 0.00 /mo, +  0.39 GB/mo | holds    0.98 GB, rebuilt 0.013 | [('justhodl-signal-board', 262)]
- `19:08:10` data/buyback-engine/                                       writes/day     180 (new    180, rewrites      0) | PUT $ 0.05 + 30d-min $ 0.00 /mo, +  1.02 GB/mo | holds    0.42 GB, rebuilt 0.076 | [('justhodl-buyback-engine', 262)]
- `19:08:10` data/warm/nyfed-hhdc/                                      writes/day     169 (new      0, rewrites    169) | PUT $ 0.05 + 30d-min $ 0.01 /mo, +  0.00 GB/mo | holds    0.02 GB, rebuilt 0.0 | []
- `19:08:10` data/warm/fhfa-hpi/                                        writes/day     174 (new      0, rewrites    174) | PUT $ 0.05 + 30d-min $ 0.01 /mo, +  0.00 GB/mo | holds    0.04 GB, rebuilt 0.0 | []
- `19:08:10` ✅ read-only; nothing changed
