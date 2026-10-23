# ops 6632 - archive scope B: exact saving per excluded folder, measured before any change (read-only)

**Status:** success  
**Duration:** 142.7s  
**Finished:** 2026-10-23T19:36:58+00:00  

## Data

| all_ia_first_month | excluded_prefixes | inventory | kept_ia_first_month | kept_it_first_month | problems | save_ia_12m_avg | save_ia_first_month | window_days |
|---|---|---|---|---|---|---|---|---|
| 28.995 | 23 | 2026-10-22T01-00Z | 13.15 | 7.917 | 0 | 24.094 | 15.846 | 1.0 |

## Log
## A. the proposed scope (config/archive-scope.json)

- `19:34:35` 23 excluded prefixes, 37 must_keep sentinels, self-test _health/archive-scope-excluded/
## B. inventory read

- `19:34:35` window 2026-10-21T01:00 -> 2026-10-22T01:00 (1.00 days): newest inventory 2026-10-22T01-00Z, previous 2026-10-21T01-00Z
- `19:35:09` previous inventory: 3381427 keys existed
- `19:36:57` newest inventory: 11461033 versions, 54559 keys written in the window, 36873 of them new
## C. saving per excluded prefix (Standard-IA archive, today's price)

- `19:36:58` audit-private/20260909-originals/financial-statement-research/      15384 writes/d (new  6252) | save IA $ 5.39/mo now (put 4.62 + 30d 0.47 + 24.81 GB/mo growth), 12-mo avg $ 7.10 | holds   163452 obj    0.68 GB (archived copies stay: $0.27/mo) | inside 2: requests/(12135/d), (files)(3249/d)
- `19:36:58` audit-private/20260909-originals/fx-research/                        6301 writes/d (new  3973) | save IA $ 2.20/mo now (put 1.89 + 30d 0.11 + 15.62 GB/mo growth), 12-mo avg $ 3.27 | holds     4971 obj    0.02 GB (archived copies stay: $0.01/mo) | inside 2: requests/(4268/d), (files)(2033/d)
- `19:36:58` data/etf-holdings-research/                                          4537 writes/d (new  4531) | save IA $ 1.63/mo now (put 1.36 + 30d 0.00 + 21.26 GB/mo growth), 12-mo avg $ 3.09 | holds   111612 obj    9.22 GB (archived copies stay: $0.22/mo) | inside 11: snapshots/(1657/d), rows/(1375/d), comparisons/(990/d), memberships/(256/d), directories/(249/d), requests/(7/d)
- `19:36:58` data/option-flow-research/                                            701 writes/d (new   701) | save IA $ 0.34/mo now (put 0.21 + 30d 0.00 + 10.42 GB/mo growth), 12-mo avg $ 1.06 | holds    95102 obj   46.17 GB (archived copies stay: $0.58/mo) | inside 6: rows/(654/d), chains/(44/d), inputs/(1/d), outputs/(1/d), compilers/(0/d), runs/(1/d)
- `19:36:58` data/option-population-research/                                      328 writes/d (new   328) | save IA $ 0.13/mo now (put 0.10 + 30d 0.00 + 2.40 GB/mo growth), 12-mo avg $ 0.29 | holds    36475 obj    8.64 GB (archived copies stay: $0.11/mo) | inside 6: groups/(315/d), chains/(10/d), outputs/(1/d), compilers/(0/d), inputs/(1/d), runs/(1/d)
- `19:36:58` data/tail-research/                                                   776 writes/d (new   388) | save IA $ 0.27/mo now (put 0.23 + 30d 0.02 + 1.53 GB/mo growth), 12-mo avg $ 0.38 | holds      558 obj    0.01 GB (archived copies stay: $0.00/mo) | inside 5: requests/(485/d), outputs/(97/d), inputs/(97/d), runs/(97/d), compilers/(0/d)
- `19:36:58` data/implied-research/                                                776 writes/d (new   388) | save IA $ 0.27/mo now (put 0.23 + 30d 0.02 + 1.53 GB/mo growth), 12-mo avg $ 0.38 | holds      481 obj    0.01 GB (archived copies stay: $0.00/mo) | inside 5: requests/(485/d), outputs/(97/d), inputs/(97/d), runs/(97/d), compilers/(0/d)
- `19:36:58` data/fx-quote-research/                                               764 writes/d (new   764) | save IA $ 0.27/mo now (put 0.23 + 30d 0.00 + 3.00 GB/mo growth), 12-mo avg $ 0.47 | holds     1240 obj    0.09 GB (archived copies stay: $0.00/mo) | inside 5: bars/(473/d), outputs/(97/d), inputs/(97/d), runs/(97/d), compilers/(0/d)
- `19:36:58` data/statement-research/                                              503 writes/d (new   503) | save IA $ 0.20/mo now (put 0.15 + 30d 0.00 + 3.73 GB/mo growth), 12-mo avg $ 0.45 | holds    13598 obj    3.33 GB (archived copies stay: $0.04/mo) | inside 5: records/(500/d), outputs/(1/d), inputs/(1/d), compilers/(0/d), runs/(1/d)
- `19:36:58` data/etf-desk-research/                                               470 writes/d (new   470) | save IA $ 0.17/mo now (put 0.14 + 30d 0.00 + 2.16 GB/mo growth), 12-mo avg $ 0.32 | holds     6604 obj    0.43 GB (archived copies stay: $0.01/mo) | inside 6: profiles/(464/d), outputs/(2/d), inputs/(2/d), runs/(2/d), compilers/(0/d), (files)(0/d)
- `19:36:58` data/short-volume-research/                                           259 writes/d (new     0) | save IA $ 0.11/mo now (put 0.08 + 30d 0.03 + 0.00 GB/mo growth), 12-mo avg $ 0.11 | holds     3893 obj    1.19 GB (archived copies stay: $0.01/mo) | inside 5: records/(256/d), outputs/(1/d), inputs/(1/d), compilers/(0/d), runs/(1/d)
- `19:36:58` data/risk-regime-research/                                           1088 writes/d (new  1088) | save IA $ 0.40/mo now (put 0.33 + 30d 0.00 + 5.49 GB/mo growth), 12-mo avg $ 0.77 | holds     2048 obj    0.27 GB (archived copies stay: $0.01/mo) | inside 6: sources/(794/d), outputs/(97/d), inputs/(97/d), runs/(97/d), settlement-inputs/(3/d), compilers/(0/d) | NAMES TO CHECK: ['sources/']
- `19:36:58` data/provider-flow-research/                                          652 writes/d (new   642) | save IA $ 0.23/mo now (put 0.20 + 30d 0.00 + 2.75 GB/mo growth), 12-mo avg $ 0.42 | holds     8692 obj    0.73 GB (archived copies stay: $0.01/mo) | inside 7: histories/(634/d), requests/(12/d), outputs/(2/d), inputs/(2/d), runs/(2/d), compilers/(0/d) | NAMES TO CHECK: ['histories/']
- `19:36:58` data/holdings-research/                                               141 writes/d (new   141) | save IA $ 0.23/mo now (put 0.04 + 30d 0.00 + 14.90 GB/mo growth), 12-mo avg $ 1.25 | holds    23415 obj   80.83 GB (archived copies stay: $1.02/mo) | inside 11: filings/(74/d), funds/(36/d), canonical/(21/d), probes/(2/d), outputs/(2/d), legacy/(2/d)
- `19:36:58` data/fundgraph/cache/                                                1298 writes/d (new   193) | save IA $ 0.67/mo now (put 0.39 + 30d 0.24 + 2.89 GB/mo growth), 12-mo avg $ 0.86 | holds    14792 obj    7.16 GB (archived copies stay: $0.09/mo) | inside 1: (files)(1298/d)
- `19:36:58` data/fundgraph/hits/                                                 1868 writes/d (new   242) | save IA $ 0.65/mo now (put 0.56 + 30d 0.08 + 0.95 GB/mo growth), 12-mo avg $ 0.72 | holds    17643 obj    0.00 GB (archived copies stay: $0.03/mo) | inside 1: (files)(1868/d)
- `19:36:58` equity-research/                                                     1725 writes/d (new   449) | save IA $ 0.71/mo now (put 0.52 + 30d 0.14 + 4.25 GB/mo growth), 12-mo avg $ 1.01 | holds    32951 obj   11.36 GB (archived copies stay: $0.15/mo) | inside 3: (files)(1141/d), industry2/(583/d), shared/(1/d)
- `19:36:58` data/watchlist-insights/                                             1104 writes/d (new     0) | save IA $ 0.39/mo now (put 0.33 + 30d 0.06 + 0.00 GB/mo growth), 12-mo avg $ 0.39 | holds     1107 obj    0.02 GB (archived copies stay: $0.00/mo) | inside 3: stock/(808/d), etf/(294/d), (files)(2/d)
- `19:36:58` data/series-cache/                                                    618 writes/d (new     3) | save IA $ 0.36/mo now (put 0.18 + 30d 0.17 + 0.05 GB/mo growth), 12-mo avg $ 0.36 | holds    67399 obj    2.02 GB (archived copies stay: $0.12/mo) | inside 262: 18/(8/d), 52/(6/d), 29/(8/d), 97/(6/d), a1/(6/d), f9/(3/d) | NAMES TO CHECK: ['imf-source/', 'cboe-index-source/', 'defillama-tvl-source/', 'census-source/', 'oecd-source/', 'regional-fed-source/']
- `19:36:58` data/search/                                                          198 writes/d (new     2) | save IA $ 0.11/mo now (put 0.06 + 30d 0.02 + 2.25 GB/mo growth), 12-mo avg $ 0.27 | holds     1135 obj   37.71 GB (archived copies stay: $0.47/mo) | inside 3: providers/(194/d), index/(2/d), (files)(2/d)
- `19:36:58` data/engines/                                                         492 writes/d (new     0) | save IA $ 0.17/mo now (put 0.15 + 30d 0.02 + 0.00 GB/mo growth), 12-mo avg $ 0.17 | holds      492 obj    0.00 GB (archived copies stay: $0.00/mo) | inside 1: (files)(492/d)
- `19:36:58` data/providers/gdelt/page-                                           1628 writes/d (new     1) | save IA $ 0.57/mo now (put 0.49 + 30d 0.08 + 0.00 GB/mo growth), 12-mo avg $ 0.57 | holds      814 obj    0.06 GB (archived copies stay: $0.00/mo) | inside 1: (pages)(1628/d)
- `19:36:58` data/providers/fred/page-                                            1128 writes/d (new     0) | save IA $ 0.39/mo now (put 0.34 + 30d 0.06 + 0.00 GB/mo growth), 12-mo avg $ 0.39 | holds      564 obj    0.05 GB (archived copies stay: $0.00/mo) | inside 1: (pages)(1128/d)
## D. data.html catalog pages, every provider slug (data/providers/<slug>/page-NNN.json)

- `19:36:58` data/providers/gdelt/page-                1628 writes/d | IA $0.568/mo | IN SCOPE
- `19:36:58` data/providers/fred/page-                 1128 writes/d | IA $0.394/mo | IN SCOPE
- `19:36:58` data/providers/worldbank/page-             242 writes/d | IA $0.084/mo | not listed
- `19:36:58` data/providers/boj/page-                    68 writes/d | IA $0.024/mo | not listed
- `19:36:58` data/providers/statcan/page-                34 writes/d | IA $0.012/mo | not listed
- `19:36:58` data/providers/eurostat/page-               34 writes/d | IA $0.012/mo | not listed
- `19:36:58` data/providers/nyfed/page-                   8 writes/d | IA $0.003/mo | not listed
- `19:36:58` data/providers/polygon/page-                 8 writes/d | IA $0.003/mo | not listed
- `19:36:58` data/providers/bls/page-                     8 writes/d | IA $0.003/mo | not listed
- `19:36:58` data/providers/oecd/page-                    6 writes/d | IA $0.002/mo | not listed
- `19:36:58` data/providers/worldbank-wgi/page-           6 writes/d | IA $0.002/mo | not listed
- `19:36:58` data/providers/fhfa-hpi/page-                2 writes/d | IA $0.001/mo | not listed
- `19:36:58` data/providers/fragile-states-index/page-     2 writes/d | IA $0.001/mo | not listed
- `19:36:58` data/providers/hk-data/page-                 2 writes/d | IA $0.001/mo | not listed
- `19:36:58` data/providers/imf/page-                     2 writes/d | IA $0.001/mo | not listed
- `19:36:58` data/providers/nyfed-hhdc/page-              2 writes/d | IA $0.001/mo | not listed
- `19:36:58` data/providers/nyfed-research/page-          2 writes/d | IA $0.001/mo | not listed
- `19:36:58` data/providers/ofr-bsrm/page-                2 writes/d | IA $0.001/mo | not listed
- `19:36:58` data/providers/ofr-hfm/page-                 2 writes/d | IA $0.001/mo | not listed
- `19:36:58` data/providers/ofr/page-                     2 writes/d | IA $0.001/mo | not listed
- `19:36:58` data/providers/other/page-                   2 writes/d | IA $0.001/mo | not listed
- `19:36:58` data/providers/risk/page-                    2 writes/d | IA $0.001/mo | not listed
- `19:36:58` data/providers/sec-edgar/page-               2 writes/d | IA $0.001/mo | not listed
- `19:36:58` data/providers/te-mirror/page-               2 writes/d | IA $0.001/mo | not listed
- `19:36:58` data/providers/tradingview/page-             4 writes/d | IA $0.001/mo | not listed
- `19:36:58` data/providers/world-uncertainty-index/page-     2 writes/d | IA $0.001/mo | not listed
- `19:36:58` data/providers/bea/page-                     2 writes/d | IA $0.001/mo | not listed
- `19:36:58` data/providers/census-us/page-               2 writes/d | IA $0.001/mo | not listed
- `19:36:58` data/providers/eba-risk-dashboard/page-      2 writes/d | IA $0.001/mo | not listed
- `19:36:58` data/providers/ecb/page-                     4 writes/d | IA $0.001/mo | not listed
- `19:36:58` data/providers/esma-ratings/page-            2 writes/d | IA $0.001/mo | not listed
## E. the archive before and after (monthly, estimates from the window)

- `19:36:58` whole bucket (today)     74455 writes/d | Standard-IA: $ 29.00 first month, $ 46.79 12-month avg, + 258.8 GB/mo | Intelligent-Tiering: $ 16.44 first month, $ 27.36 12-month avg
- `19:36:58` excluded by scope B      42739 writes/d | Standard-IA: $ 15.85 first month, $ 24.09 12-month avg, + 120.0 GB/mo | Intelligent-Tiering: $  8.53 first month, $ 13.20 12-month avg
- `19:36:58` kept after B             31716 writes/d | Standard-IA: $ 13.15 first month, $ 22.70 12-month avg, + 138.8 GB/mo | Intelligent-Tiering: $  7.92 first month, $ 14.16 12-month avg
- `19:36:58` archive holdings (current copies, IA-billable): 1280.6 GB in all, 253.6 GB of it in excluded prefixes ($3.17/mo, stays unless root deletes it)
## F. biggest folders that stay archived (for review)

- `19:36:58` data/(files)                                                     2423 writes/d (new     0) | IA $ 1.15/mo, + 0.00 GB/mo | IT $ 0.48/mo
- `19:36:58` audit-private/20260909-originals/tail-research/                  2522 writes/d (new  2522) | IA $ 0.89/mo, +10.33 GB/mo | IT $ 0.73/mo
- `19:36:58` data/evidence/                                                   2022 writes/d (new  2022) | IA $ 0.71/mo, + 8.02 GB/mo | IT $ 0.32/mo
- `19:36:58` audit-private/20260909-originals/futures-research/               1751 writes/d (new  1070) | IA $ 0.61/mo, + 4.21 GB/mo | IT $ 0.27/mo
- `19:36:58` data/warm/fred-scoped/                                           1507 writes/d (new     0) | IA $ 0.58/mo, + 0.00 GB/mo | IT $ 0.29/mo
- `19:36:58` data/repo-history/                                               1658 writes/d (new     0) | IA $ 0.58/mo, + 0.00 GB/mo | IT $ 0.26/mo
- `19:36:58` audit-private/20260909-originals/etf-constituent-research/       1335 writes/d (new  1335) | IA $ 0.49/mo, + 7.37 GB/mo | IT $ 0.27/mo
- `19:36:58` audit-private/20260909-originals/options-research/               1372 writes/d (new   707) | IA $ 0.48/mo, + 2.87 GB/mo | IT $ 0.29/mo
- `19:36:58` data/vintage-research/                                            697 writes/d (new   327) | IA $ 0.40/mo, +13.52 GB/mo | IT $ 0.40/mo
- `19:36:58` equity-research-history/                                         1112 writes/d (new   744) | IA $ 0.40/mo, + 3.34 GB/mo | IT $ 0.30/mo
- `19:36:58` data/warm/nyfed-research/                                         426 writes/d (new     0) | IA $ 0.39/mo, + 0.00 GB/mo | IT $ 0.14/mo
- `19:36:58` data/fundgraph/                                                  1040 writes/d (new   187) | IA $ 0.36/mo, + 0.74 GB/mo | IT $ 0.16/mo
- `19:36:58` data/warm/tv-bars/                                                955 writes/d (new     0) | IA $ 0.35/mo, + 0.00 GB/mo | IT $ 0.17/mo
- `19:36:58` audit-private/20260909-originals/etf-desk-research/               960 writes/d (new   948) | IA $ 0.34/mo, + 4.30 GB/mo | IT $ 0.16/mo
- `19:36:58` data/warm/blackswan/                                              813 writes/d (new     0) | IA $ 0.28/mo, + 0.00 GB/mo | IT $ 0.12/mo
- `19:36:58` audit-private/20260909-originals/provider-fund-flow-research/     664 writes/d (new   664) | IA $ 0.24/mo, + 3.05 GB/mo | IT $ 0.12/mo
- `19:36:58` data/warm/gleif/                                                    2 writes/d (new     0) | IA $ 0.20/mo, + 0.00 GB/mo | IT $ 0.05/mo
- `19:36:58` audit-private/20260909-originals/market-internals/                254 writes/d (new   254) | IA $ 0.20/mo, + 9.90 GB/mo | IT $ 0.28/mo
- `19:36:58` data/warm/eba-risk-dashboard/                                     463 writes/d (new     0) | IA $ 0.16/mo, + 0.00 GB/mo | IT $ 0.07/mo
- `19:36:58` data/warm/gdelt-full/                                             405 writes/d (new     0) | IA $ 0.14/mo, + 0.00 GB/mo | IT $ 0.06/mo
- `19:36:58` audit-private/20260909-originals/offexchange-research/            356 writes/d (new   137) | IA $ 0.13/mo, + 0.98 GB/mo | IT $ 0.07/mo
- `19:36:58` data/research-network/                                            333 writes/d (new   333) | IA $ 0.13/mo, + 2.26 GB/mo | IT $ 0.12/mo
- `19:36:58` audit-private/20260909-originals/geopolitical-risk-research/      333 writes/d (new   333) | IA $ 0.13/mo, + 2.06 GB/mo | IT $ 0.08/mo
- `19:36:58` data/eps-revision-velocity/                                       202 writes/d (new   202) | IA $ 0.09/mo, + 2.14 GB/mo | IT $ 0.06/mo
- `19:36:58` data/providers/worldbank/                                         242 writes/d (new     0) | IA $ 0.08/mo, + 0.00 GB/mo | IT $ 0.04/mo
- `19:36:58` data/warm/dol-full/                                                77 writes/d (new     0) | IA $ 0.08/mo, + 0.00 GB/mo | IT $ 0.03/mo
- `19:36:58` data/carry-research/                                              173 writes/d (new   173) | IA $ 0.08/mo, + 2.54 GB/mo | IT $ 0.10/mo
- `19:36:58` audit-private/20260909-originals/short-volume-research/           212 writes/d (new     1) | IA $ 0.07/mo, + 0.00 GB/mo | IT $ 0.03/mo
- `19:36:58` data/warm/census-us/                                              195 writes/d (new     0) | IA $ 0.07/mo, + 0.00 GB/mo | IT $ 0.03/mo
- `19:36:58` audit-private/20260909-originals/implied-research/                195 writes/d (new   195) | IA $ 0.07/mo, + 1.08 GB/mo | IT $ 0.05/mo
- `19:36:58` ✅ scope measured; read-only, nothing changed
