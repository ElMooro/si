# ops 6791 - stablecoin supply history first light 2026-11-12T15:41+00:00

**Status:** success  
**Duration:** 7.0s  
**Finished:** 2026-11-12T15:41:25+00:00  

## Data

| days | fails | jpx_links | latest | supply_history |
|---|---|---|---|---|
| 3271 | [] | 11 | 2026-11-12 total 289503379559.82 usdt 175848249954.02 usdc 69902618129.26 | {"ok": true, "days": 3271, "latest": "2026-11-12", "total": 289503379559.82} |

## Log
- `15:41:18` function: code last modified 2026-11-12T15:37:14.000+0000, sha j969umXuwMw6, timeout 90s
- `15:41:19` before: data/stablecoin-supply-history.json absent (expected on first light)
- `15:41:20` invoke: status 200, function error None, 2s, body {"state": "CONTRACTING", "prev": "CONTRACTING", "delta_30d": -14966693392, "signal": 100, "supply_history": {"ok": true, "days": 3271, "latest": "2026-11-12", "total": 289503379559.82}}
- `15:41:20` feed: generated 2026-11-12T15:41:20.522397+00:00 | 3271 days | first 2017-11-29 | latest {"date": "2026-11-12", "total": 289503379559.82, "minted": 0, "bridged": 0, "usdt": 175848249954.02, "usdc": 69902618129.26} | units USD | definitions ['total', 'minted', 'bridged', 'usdt', 'usdc']
- `15:41:20` last 400 days filled: {'total': 400, 'usdt': 400, 'usdc': 400, 'minted': 1, 'bridged': 1}
- `15:41:20` regime output: LastModified 2026-11-12T14:00+00:00 -> 2026-11-12T15:41+00:00 | version 1.1 | state CONTRACTING | total 286199145799
## Korea / Japan sources from the runner (read-only)

- `15:41:21` JPX investor-type listing: HTTP 200, 34043 bytes, 11 file links: ['/english/markets/statistics-equities/investor-type/tvdivq00000014fy-att/revision_information_e.xls', '/english/markets/statistics-equities/investor-type/vk0khi000002qnwu-att/stock_1_w_20261005_20261009.pdf', '/english/markets/statistics-equities/investor-type/vk0khi000002qnwu-att/stock_1_w_20261005_20261009.xlsx', '/english/markets/statistics-equities/investor-type/vk0khi000002qxwh-att/stock_1_w_20261013_20261016.pdf', '/english/markets/statistics-equities/investor-type/vk0khi000002qxwh-att/stock_1_w_20261013_20261016.xlsx', '/english/markets/statistics-equities/investor-type/vk0khi000002thzf-att/stock_1_w_20261019_20261023.pdf', '/english/markets/statistics-equities/investor-type/vk0khi000002thzf-att/stock_1_w_20261019_20261023.xlsx', '/english/markets/statistics-equities/investor-type/vk0khi000002tvus-att/stock_1_w_20261026_20261030.pdf', '/english/markets/statistics-equities/investor-type/vk0khi000002tvus-att/stock_1_w_20261026_20261030.xlsx', '/english/markets/statistics-equities/investor-type/vk0khi000002u47h-att/stock_1_w_20261102_20261106.pdf', '/english/markets/statistics-equities/investor-type/vk0khi000002u47h-att/stock_1_w_20261102_20261106.xlsx']
- `15:41:21` newest candidate or-type/vk0khi000002qnwu-att/stock_1_w_20261005_20261009.pdf: HTTP 200, 644 KB, application/pdf
- `15:41:21` sub-pages: ['/english/markets/statistics-equities/investor-type/00-01.html', '/english/markets/statistics-equities/investor-type/00-02.html', '/english/markets/statistics-equities/investor-type/02.html', '/english/markets/statistics-equities/investor-type/03.html', '/english/markets/statistics-equities/investor-type/04.html', '/english/markets/statistics-equities/investor-type/07.html', '/english/markets/statistics-equities/investor-type/index.html', '/english/markets/statistics-equities/investor-type/tvdivq00000014fy-att/revision_information_e.xls', '/english/markets/statistics-equities/investor-type/vk0khi000002qnwu-att/stock_1_w_20261005_20261009.pdf', '/english/markets/statistics-equities/investor-type/vk0khi000002qnwu-att/stock_1_w_20261005_20261009.xlsx', '/english/markets/statistics-equities/investor-type/vk0khi000002qxwh-att/stock_1_w_20261013_20261016.pdf', '/english/markets/statistics-equities/investor-type/vk0khi000002qxwh-att/stock_1_w_20261013_20261016.xlsx']
- `15:41:22` Naver KOSPI trend: HTTP 200, 101 bytes | dict keys ['bizdate', 'personalValue', 'foreignValue', 'institutionalValue']
- `15:41:23` Naver KOSPI investor daily (pageSize 60): HTTP 404, 4209 bytes | html, 0 td cells, first []
- `15:41:24` Naver KOSPI investor (alt path): HTTP 404, 4209 bytes | html, 0 td cells, first []
- `15:41:24` Naver KOSDAQ trend: HTTP 200, 99 bytes | dict keys ['bizdate', 'personalValue', 'foreignValue', 'institutionalValue']
- `15:41:25` Naver sise investor daily (classic): HTTP 410, 1590 bytes | html, 0 td cells, first []
- `15:41:25` ✅ stablecoin supply history live: 3271 days, latest 2026-11-12
