# ops 5094 -- CSP policy-as-code: fix the edge header + prove global-cycle renders

**Status:** success  
**Duration:** 282.0s  
**Finished:** 2026-09-01T23:53:52+00:00  

## Data

| connect_src | csp_violations | data_loaded | decisive | engine | failed_req | header_chars | ladder | load_s | map_with_data | n_files | page_errors | script_src | tiles | visit |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 24 |  |  |  |  |  | 1682 |  |  |  | 566 |  | 9 |  | policy |
|  | 0 | True | GLOBAL EXPANSION · 74% of world GDP in expansion+recovery (avg CLI 108 | 2.0 | 0 |  | 10 | 10.2 | 34 |  | 0 |  | 34 | gc-desktop |
|  | 0 | True | GLOBAL EXPANSION · 74% of world GDP in expansion+recovery (avg CLI 108 | 2.0 | 1 |  | 10 | 10.0 | 34 |  | 0 |  | 34 | gc-mobile |
|  | 0 | True | 9/1/2026, 11:17:34 PM | None | 0 |  | None | 9.9 | None |  | 0 |  | None | gc-history |
|  | 0 | None |  | None | 1 |  | None | 10.4 | None |  | 0 |  | None | index |
|  | 0 | None |  | None | 3 |  | None | 12.3 | None |  | 0 |  | None | chart-pro |
|  | 0 | None |  | None | 1 |  | None | 10.3 | None |  | 2 |  | None | why |
|  | 0 | None |  | None | 1 |  | None | 10.7 | None |  | 0 |  | None | fortress |
|  | 0 | True | GLOBAL EXPANSION · 74% of world GDP in expansion+recovery (avg CLI 108 | 2.0 | 1 |  | 10 | 5.0 | 34 |  | 0 |  | 34 | gc-warm1 |
|  | 0 | True | GLOBAL EXPANSION · 74% of world GDP in expansion+recovery (avg CLI 108 | 2.0 | 1 |  | 10 | 9.9 | 34 |  | 0 |  | 34 | gc-warm2 |

## Log
- `23:49:10` started 2026-09-01T23:49:10+00:00
## S1 live header before

- `23:49:10` https://justhodl.ai/global-cycle.html: HTTP 200 csp_len=422 jsdelivr_in_script_src=False
- `23:49:10`   before: default-src 'self' https://justhodl-dashboard-live.s3.amazonaws.com; connect-src 'self' https://justhodl-dashboard-live.s3.amazonaws.com https://s3.amazonaws.com https://justhodl-data-proxy.raafouis.workers.dev https://api.telegram.org; img-src 'self' data: https:; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline' https://cdnjs.cloudflare.com; object-src 'none'; base-uri 'none'; frame-ancestors 'self'
- `23:49:10` https://www.justhodl.ai/global-cycle.html: HTTP 200 csp_len=422 jsdelivr_in_script_src=False
- `23:49:10`   before: default-src 'self' https://justhodl-dashboard-live.s3.amazonaws.com; connect-src 'self' https://justhodl-dashboard-live.s3.amazonaws.com https://s3.amazonaws.com https://justhodl-data-proxy.raafouis.workers.dev https://api.telegram.org; img-src 'self' data: https:; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline' https://cdnjs.cloudflare.com; object-src 'none'; base-uri 'none'; frame-ancestors 'self'
## S2 policy-as-code

- `23:49:11` generated header (1682 chars, 566 files scanned):
- `23:49:11`   default-src 'self'
- `23:49:11`   script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net https://cdn.plot.ly https://cdn.tailwindcss.com https://cdnjs.cloudflare.com https://challenges.cloudflare.com https://justhodl-data-proxy.raafouis.workers.dev https://s3.tradingview.com https://static.cloudflareinsights.com https://unpkg.com
- `23:49:11`   style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net https://cdnjs.cloudflare.com https://fonts.googleapis.com https://unpkg.com
- `23:49:11`   font-src 'self' data: https://cdn.jsdelivr.net https://cdnjs.cloudflare.com https://fonts.gstatic.com https://unpkg.com
- `23:49:11`   img-src 'self' data: blob: https:
- `23:49:11`   media-src 'self' data: https:
- `23:49:11`   connect-src 'self' https://*.execute-api.us-east-1.amazonaws.com https://*.lambda-url.us-east-1.on.aws https://*.raafouis.workers.dev https://*.supabase.co https://api.dexscreener.com https://api.justhodl.ai https://api.stlouisfed.org https://api.telegram.org https://cdn.jsdelivr.net https://cdn.plot.ly https://cdnjs.cloudflare.com https://financialmodelingprep.com https://justhodl-dashboard-live.s3.amazonaws.com https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com https://macro-data-lake.s3.amazonaws.com https://patentsview.org https://s3.amazonaws.com https://s3.us-east-1.amazonaws.com https://static.cloudflareinsights.com https://unpkg.com https://web.archive.org wss://*.execute-api.us-east-1.amazonaws.com wss://*.raafouis.workers.dev wss://*.supabase.co
- `23:49:11`   frame-src 'self' https://*.tradingview-widget.com https://*.tradingview.com https://challenges.cloudflare.com https://justhodl.ai
- `23:49:11`   worker-src 'self' blob:
- `23:49:11`   manifest-src 'self'
- `23:49:11`   object-src 'none'
- `23:49:11`   base-uri 'none'
- `23:49:11`   frame-ancestors 'self'
- `23:49:11`   upgrade-insecure-requests
## S3 Cloudflare upsert

- `23:49:12` zone fb59e2d0…
- `23:49:12` entrypoint ruleset 0c1dee9c… with 1 rule(s); 1 match ref/description
- `23:49:12` PATCH rule 8e753c02…: HTTP 200 success=True errors=[]
- `23:49:12` ✅ rule upserted via PATCH rule 8e753c02…
## S4 edge propagation

- `23:49:33` ✅ edge serves the new header on 2 host(s) after 20s
## S5 Pages deploy (vendored assets + patched page)

- `23:52:15` ✅ Pages deploy live after 162s: {"page": 200, "page_has_vendor": true, "vendor": {"/assets/vendor/d3.v7.9.0.min.js": [200, 279706], "/assets/vendor/topojson-client.v3.1.0.min.js": [200, 7169], "/assets/vendor/world-atlas-2.0.2-countries-110m.json": [200, 107761]}}
## S6 headless render

- `23:52:15` pip installing playwright
- `23:52:18` launched runner Google Chrome (channel=chrome)
## gc-desktop -- https://justhodl.ai/global-cycle.html @ 1440x1000 (10.2s)

- `23:52:29`   DOM title = "Global Business Cycle \u00b7 JustHodl.AI"
- `23:52:29`   DOM decisive = "GLOBAL EXPANSION \u00b7 74% of world GDP in expansion+recovery (avg CLI 108.79). Maximum cyclical risk exposure justified \u2014 overweight equity (especially EM + small caps), commodities, and HY credit. Underweight long duration"
- `23:52:29`   DOM genTime = "9/1/2026, 11:17:32 PM"
- `23:52:29`   DOM ageStr = "updated 0h ago"
- `23:52:29`   DOM globalPhase = "GLOBAL EXPANSION"
- `23:52:29`   DOM globalCli = "avg CLI 108.79"
- `23:52:29`   DOM pctExpansion = "67.7%"
- `23:52:29`   DOM countryCount = "34"
- `23:52:29`   DOM freshCount = "33/34"
- `23:52:29`   DOM freshSub = "1 country >3 months stale"
- `23:52:29`   DOM ladderCells = 10
- `23:52:29`   DOM regions = 6
- `23:52:29`   DOM tiles = 34
- `23:52:29`   DOM physTags = 34
- `23:52:29`   DOM mapPaths = 177
- `23:52:29`   DOM mapWithData = 34
- `23:52:29`   DOM mapNote = null
- `23:52:29`   DOM d3 = "object"
- `23:52:29`   DOM topojson = "object"
- `23:52:29`   DOM dataLoaded = true
- `23:52:29`   DOM worldLoaded = true
- `23:52:29`   DOM engineVersion = "2.0"
- `23:52:29`   DOM swController = true
- `23:52:29`   firstVisibleText = "JUSTHODL\u00b7AI\nSPX 7,632.6 -0.7%\nNDX 26,100 -1.0%\nBTC 77,544 -1.3%\nGOLD 4,381 -0.3%\nUS10Y 4.75%\nVIX 14.9\nDXY 118.7 +0.3%\nCN GDP 21.05%\nUS CPI 2.95%\n19:52:28 ET\nCOMMAND CENTER \u2192\nJustHodl.AI\nDASHBOARD\nINTEL\n\ud83d\udfe2 LCE\n\ud83c\udf0d Cycle (live)\n\ud83d\udcc8 Cycle (history)\n\ud83c\udfdb AUCT
- `23:52:29`   CSP violations (0): []
- `23:52:29`   page errors (0): []
- `23:52:29`   other console errors/warnings (1): [{"type": "error", "text": "Failed to load resource: the server responded with a status of 404 ()"}]
- `23:52:29`   failed requests (0): []
## gc-mobile -- https://justhodl.ai/global-cycle.html @ 390x844 (10.0s)

- `23:52:39`   DOM title = "Global Business Cycle \u00b7 JustHodl.AI"
- `23:52:39`   DOM decisive = "GLOBAL EXPANSION \u00b7 74% of world GDP in expansion+recovery (avg CLI 108.79). Maximum cyclical risk exposure justified \u2014 overweight equity (especially EM + small caps), commodities, and HY credit. Underweight long duration"
- `23:52:39`   DOM genTime = "9/1/2026, 11:17:32 PM"
- `23:52:39`   DOM ageStr = "updated 0h ago"
- `23:52:39`   DOM globalPhase = "GLOBAL EXPANSION"
- `23:52:39`   DOM globalCli = "avg CLI 108.79"
- `23:52:39`   DOM pctExpansion = "67.7%"
- `23:52:39`   DOM countryCount = "34"
- `23:52:39`   DOM freshCount = "33/34"
- `23:52:39`   DOM freshSub = "1 country >3 months stale"
- `23:52:39`   DOM ladderCells = 10
- `23:52:39`   DOM regions = 6
- `23:52:39`   DOM tiles = 34
- `23:52:39`   DOM physTags = 34
- `23:52:39`   DOM mapPaths = 177
- `23:52:39`   DOM mapWithData = 34
- `23:52:39`   DOM mapNote = null
- `23:52:39`   DOM d3 = "object"
- `23:52:39`   DOM topojson = "object"
- `23:52:39`   DOM dataLoaded = true
- `23:52:39`   DOM worldLoaded = true
- `23:52:39`   DOM engineVersion = "2.0"
- `23:52:39`   DOM swController = true
- `23:52:39`   firstVisibleText = "JUSTHODL\u00b7AI\nSPX\n7,632.6 -0.7%\nBTC\n77,544 -1.3%\nJustHodl.AI\nDASHBOARD\nINTEL\n\ud83d\udfe2 LCE\n\ud83c\udf0d Cycle (live)\n\ud83d\udcc8 Cycle (history)\n\ud83c\udfdb AUCTIONS\n\ud83d\udcc8 BONDS\n\ud83c\udf2a STRESS\nRISK\nMACRO\n\ud83c\udf0d GLOBAL BUSINESS CYCLE\nSynthetic Composite Leading Indicator acros
- `23:52:39`   CSP violations (0): []
- `23:52:39`   page errors (0): []
- `23:52:39`   other console errors/warnings (2): [{"type": "error", "text": "Access to fetch at 'https://nu4umjskc25osscrbmqh3o2gte0utlkx.lambda-url.us-east-1.on.aws/?diag=1&page=%2Fglobal-cycle.html&favs=0&sw=none&v=3276' from origin 'https://justhodl.ai' has been blocked by CORS policy: The 'Access-Control-Allow-Origin' header contains multiple values '*, *', but only one is allowed. Have the server send the header with a valid value."}, {"type": "error", "text": "Failed to load resource: net::ERR_FAILED"}]
- `23:52:39`   failed requests (1): [{"url": "https://nu4umjskc25osscrbmqh3o2gte0utlkx.lambda-url.us-east-1.on.aws/?diag=1&page=%2Fglobal-cycle.html&favs=0&sw=none&v=3276", "failure": "net::ERR_FAILED"}]
## gc-history -- https://justhodl.ai/global-cycle/ @ 1440x1000 (9.9s)

- `23:52:49`   DOM title = "Global Business Cycle \u00b7 History \u00b7 JustHodl.AI"
- `23:52:49`   DOM svgs = 41
- `23:52:49`   DOM paths = 85
- `23:52:49`   DOM genTime = "9/1/2026, 11:17:34 PM"
- `23:52:49`   DOM dataLoaded = true
- `23:52:49`   DOM d3 = "object"
- `23:52:49`   firstVisibleText = "JustHodl.AI\nHome\nMacro\n\ud83d\udfe2 LCE\n\ud83c\udf0d Cycle (live)\n\ud83d\udcc8 Cycle (history)\nMorning brief\nScreener\nGlobal Business Cycle \u00b7 History\n5 years of weekly synthetic CLI per country \u2014 phase trajectory, breadth evolution, and country sparklines\n9/1/2026, 11:17:34 PM\n35m ago\nCURRENT EXP
- `23:52:49`   CSP violations (0): []
- `23:52:49`   page errors (0): []
- `23:52:49`   other console errors/warnings (0): []
- `23:52:49`   failed requests (0): []
## index -- https://justhodl.ai/ @ 1440x1000 (10.4s)

- `23:53:00`   DOM title = "JustHodl.AI \u00b7 Operator Console"
- `23:53:00`   DOM bodyChars = 38324
- `23:53:00`   DOM scripts = 19
- `23:53:00`   DOM fontsLoaded = 78
- `23:53:00`   firstVisibleText = "JUSTHODL\u00b7AI\nSPX 7,632.6 -0.7%\nNDX 26,100 -1.0%\nBTC 77,544 -1.3%\nGOLD 4,381 -0.3%\nUS10Y 4.75%\nVIX 14.9\nDXY 118.7 +0.3%\nCN GDP 21.05%\nUS CPI 2.95%\n19:52:59 ET\nCOMMAND CENTER \u2192\nNEW\n\u26a1 Institutional Intell"
- `23:53:00`   CSP violations (0): []
- `23:53:00`   page errors (0): []
- `23:53:00`   other console errors/warnings (2): [{"type": "error", "text": "Access to fetch at 'https://nu4umjskc25osscrbmqh3o2gte0utlkx.lambda-url.us-east-1.on.aws/?diag=1&page=%2F&favs=0&sw=none&v=3276' from origin 'https://justhodl.ai' has been blocked by CORS policy: The 'Access-Control-Allow-Origin' header contains multiple values '*, *', but only one is allowed. Have the server send the header with a valid value."}, {"type": "error", "text": "Failed to load resource: net::ERR_FAILED"}]
- `23:53:00`   failed requests (1): [{"url": "https://nu4umjskc25osscrbmqh3o2gte0utlkx.lambda-url.us-east-1.on.aws/?diag=1&page=%2F&favs=0&sw=none&v=3276", "failure": "net::ERR_FAILED"}]
## chart-pro -- https://justhodl.ai/chart-pro.html @ 1440x1000 (12.3s)

- `23:53:12`   DOM title = "Chart Pro \u00b7 JustHodl.AI \u00b7 Trading Terminal"
- `23:53:12`   DOM bodyChars = 25209
- `23:53:12`   DOM scripts = 15
- `23:53:12`   DOM fontsLoaded = 8
- `23:53:12`   firstVisibleText = "JUSTHODL\u00b7AI\nSPX 7,632.6 -0.7%\nNDX 26,100 -1.0%\nBTC 77,544 -1.3%\nGOLD 4,381 -0.3%\nUS10Y 4.75%\nVIX 14.9\nDXY 118.7 +0.3%\nCN GDP 21.05%\nUS CPI 2.95%\n19:53:12 ET\nCOMMAND CENTER \u2192\nJustHodl\n\u00b7 CHART PRO\nHome\n"
- `23:53:12`   CSP violations (0): []
- `23:53:12`   page errors (0): []
- `23:53:12`   other console errors/warnings (50): [{"type": "error", "text": "Failed to load resource: the server responded with a status of 400 ()"}, {"type": "error", "text": "Failed to load resource: the server responded with a status of 400 ()"}, {"type": "error", "text": "Failed to load resource: the server responded with a status of 400 ()"}, {"type": "error", "text": "Failed to load resource: the server responded with a status of 400 ()"}, {"type": "error", "text": "Failed to load resource: the server responded with a status of 400 ()"}, {"type": "error", "text": "Failed to load resource: the server responded with a status of 400 ()"}, {"type": "error", "text": "Failed to load resource: the server responded with a status of 400 ()"}, {"type": "error", "text": "Failed to load resource: the server responded with a status of 400 ()"}]
- `23:53:12`   failed requests (3): [{"url": "https://nu4umjskc25osscrbmqh3o2gte0utlkx.lambda-url.us-east-1.on.aws/?diag=1&page=%2Fchart-pro.html&favs=0&sw=none&v=3276", "failure": "net::ERR_FAILED"}, {"url": "https://nu4umjskc25osscrbmqh3o2gte0utlkx.lambda-url.us-east-1.on.aws/?diag=1&page=chartpro&lists=492&err=&sw=none&drawer=c3efc5d3&favs=0&lwc=true", "failure": "net::ERR_FAILED"}, {"url": "https://widget-sheriff.tradingview-widget.com/sheriff/api/v1/rules/search?origin=https%3A%2F%2Fjusthodl.ai", "failure": "net::ERR_ABORTED"}]
## why -- https://justhodl.ai/why.html @ 1440x1000 (10.3s)

- `23:53:23`   DOM title = "Research Desk \u00b7 JustHodl.AI"
- `23:53:23`   DOM bodyChars = 14939
- `23:53:23`   DOM scripts = 8
- `23:53:23`   DOM fontsLoaded = 8
- `23:53:23`   firstVisibleText = "JUSTHODL\u00b7AI\nSPX 7,632.6 -0.7%\nNDX 26,100 -1.0%\nBTC 77,544 -1.3%\nGOLD 4,381 -0.3%\nUS10Y 4.75%\nVIX 14.9\nDXY 118.7 +0.3%\nCN GDP 21.05%\nUS CPI 2.95%\n19:53:22 ET\nCOMMAND CENTER \u2192\n\ud83c\udfdb Research Desk instituti"
- `23:53:23`   CSP violations (0): []
- `23:53:23`   page errors (2): ["Unexpected identifier 'font'", "Unexpected identifier 'font'"]
- `23:53:23`   other console errors/warnings (7): [{"type": "error", "text": "Failed to load resource: the server responded with a status of 403 ()"}, {"type": "error", "text": "Access to fetch at 'https://nu4umjskc25osscrbmqh3o2gte0utlkx.lambda-url.us-east-1.on.aws/?diag=1&page=%2Fwhy.html&favs=0&sw=none&v=3276' from origin 'https://justhodl.ai' has been blocked by CORS policy: The 'Access-Control-Allow-Origin' header contains multiple values '*, *', but only one is allowed. Have the server send the header with a valid value."}, {"type": "error", "text": "Failed to load resource: net::ERR_FAILED"}, {"type": "error", "text": "Failed to load resource: the server responded with a status of 403 ()"}, {"type": "error", "text": "Failed to load resource: the server responded with a status of 403 ()"}, {"type": "error", "text": "Failed to load resource: the server responded with a status of 403 ()"}, {"type": "error", "text": "Failed to load resource: the server responded with a status of 403 (Forbidden)"}]
- `23:53:23`   failed requests (1): [{"url": "https://nu4umjskc25osscrbmqh3o2gte0utlkx.lambda-url.us-east-1.on.aws/?diag=1&page=%2Fwhy.html&favs=0&sw=none&v=3276", "failure": "net::ERR_FAILED"}]
## fortress -- https://justhodl.ai/fortress.html @ 1440x1000 (10.7s)

- `23:53:36`   DOM title = "Fortress Coil \u00b7 Dump-Resilient Accumulation Radar \u00b7 JustHodl.AI"
- `23:53:36`   DOM bodyChars = 166646
- `23:53:36`   DOM scripts = 6
- `23:53:36`   DOM fontsLoaded = 8
- `23:53:36`   firstVisibleText = "JUSTHODL\u00b7AI\nSPX 7,632.6 -0.7%\nNDX 26,100 -1.0%\nBTC 77,544 -1.3%\nGOLD 4,381 -0.3%\nUS10Y 4.75%\nVIX 14.9\nDXY 118.7 +0.3%\nCN GDP 21.05%\nUS CPI 2.95%\n19:53:33 ET\nCOMMAND CENTER \u2192\nFortress Coildump-resilien"
- `23:53:36`   CSP violations (0): []
- `23:53:36`   page errors (0): []
- `23:53:36`   other console errors/warnings (2): [{"type": "error", "text": "Access to fetch at 'https://nu4umjskc25osscrbmqh3o2gte0utlkx.lambda-url.us-east-1.on.aws/?diag=1&page=%2Ffortress.html&favs=0&sw=none&v=3276' from origin 'https://justhodl.ai' has been blocked by CORS policy: The 'Access-Control-Allow-Origin' header contains multiple values '*, *', but only one is allowed. Have the server send the header with a valid value."}, {"type": "error", "text": "Failed to load resource: net::ERR_FAILED"}]
- `23:53:36`   failed requests (1): [{"url": "https://nu4umjskc25osscrbmqh3o2gte0utlkx.lambda-url.us-east-1.on.aws/?diag=1&page=%2Ffortress.html&favs=0&sw=none&v=3276", "failure": "net::ERR_FAILED"}]
## gc-warm1 -- https://justhodl.ai/global-cycle.html @ 1440x1000 (5.0s)

- `23:53:42`   DOM title = "Global Business Cycle \u00b7 JustHodl.AI"
- `23:53:42`   DOM decisive = "GLOBAL EXPANSION \u00b7 74% of world GDP in expansion+recovery (avg CLI 108.79). Maximum cyclical risk exposure justified \u2014 overweight equity (especially EM + small caps), commodities, and HY credit. Underweight long duration"
- `23:53:42`   DOM genTime = "9/1/2026, 11:17:32 PM"
- `23:53:42`   DOM ageStr = "updated 0h ago"
- `23:53:42`   DOM globalPhase = "GLOBAL EXPANSION"
- `23:53:42`   DOM globalCli = "avg CLI 108.79"
- `23:53:42`   DOM pctExpansion = "67.7%"
- `23:53:42`   DOM countryCount = "34"
- `23:53:42`   DOM freshCount = "33/34"
- `23:53:42`   DOM freshSub = "1 country >3 months stale"
- `23:53:42`   DOM ladderCells = 10
- `23:53:42`   DOM regions = 6
- `23:53:42`   DOM tiles = 34
- `23:53:42`   DOM physTags = 34
- `23:53:42`   DOM mapPaths = 177
- `23:53:42`   DOM mapWithData = 34
- `23:53:42`   DOM mapNote = null
- `23:53:42`   DOM d3 = "object"
- `23:53:42`   DOM topojson = "object"
- `23:53:42`   DOM dataLoaded = true
- `23:53:42`   DOM worldLoaded = true
- `23:53:42`   DOM engineVersion = "2.0"
- `23:53:42`   DOM swController = false
- `23:53:42`   firstVisibleText = "JUSTHODL\u00b7AI\nSPX 7,632.6 -0.7%\nNDX 26,100 -1.0%\nBTC 77,544 -1.3%\nGOLD 4,381 -0.3%\nUS10Y 4.75%\nVIX 14.9\nDXY 118.7 +0.3%\nCN GDP 21.05%\nUS CPI 2.95%\n19:53:41 ET\nCOMMAND CENTER \u2192\nJustHodl.AI\nDASHBOARD\nINTEL\n\ud83d\udfe2 LCE\n\ud83c\udf0d Cycle (live)\n\ud83d\udcc8 Cycle (history)\n\ud83c\udfdb AUCT
- `23:53:42`   CSP violations (0): []
- `23:53:42`   page errors (0): []
- `23:53:42`   other console errors/warnings (2): [{"type": "error", "text": "Access to fetch at 'https://nu4umjskc25osscrbmqh3o2gte0utlkx.lambda-url.us-east-1.on.aws/?diag=1&page=%2Fglobal-cycle.html&favs=0&sw=none&v=3276' from origin 'https://justhodl.ai' has been blocked by CORS policy: The 'Access-Control-Allow-Origin' header contains multiple values '*, *', but only one is allowed. Have the server send the header with a valid value."}, {"type": "error", "text": "Failed to load resource: net::ERR_FAILED"}]
- `23:53:42`   failed requests (1): [{"url": "https://nu4umjskc25osscrbmqh3o2gte0utlkx.lambda-url.us-east-1.on.aws/?diag=1&page=%2Fglobal-cycle.html&favs=0&sw=none&v=3276", "failure": "net::ERR_FAILED"}]
## gc-warm2 -- https://justhodl.ai/global-cycle.html @ 1440x1000 (9.9s)

- `23:53:52`   DOM title = "Global Business Cycle \u00b7 JustHodl.AI"
- `23:53:52`   DOM decisive = "GLOBAL EXPANSION \u00b7 74% of world GDP in expansion+recovery (avg CLI 108.79). Maximum cyclical risk exposure justified \u2014 overweight equity (especially EM + small caps), commodities, and HY credit. Underweight long duration"
- `23:53:52`   DOM genTime = "9/1/2026, 11:17:32 PM"
- `23:53:52`   DOM ageStr = "updated 0h ago"
- `23:53:52`   DOM globalPhase = "GLOBAL EXPANSION"
- `23:53:52`   DOM globalCli = "avg CLI 108.79"
- `23:53:52`   DOM pctExpansion = "67.7%"
- `23:53:52`   DOM countryCount = "34"
- `23:53:52`   DOM freshCount = "33/34"
- `23:53:52`   DOM freshSub = "1 country >3 months stale"
- `23:53:52`   DOM ladderCells = 10
- `23:53:52`   DOM regions = 6
- `23:53:52`   DOM tiles = 34
- `23:53:52`   DOM physTags = 34
- `23:53:52`   DOM mapPaths = 177
- `23:53:52`   DOM mapWithData = 34
- `23:53:52`   DOM mapNote = null
- `23:53:52`   DOM d3 = "object"
- `23:53:52`   DOM topojson = "object"
- `23:53:52`   DOM dataLoaded = true
- `23:53:52`   DOM worldLoaded = true
- `23:53:52`   DOM engineVersion = "2.0"
- `23:53:52`   DOM swController = true
- `23:53:52`   firstVisibleText = "JUSTHODL\u00b7AI\nSPX 7,632.6 -0.7%\nNDX 26,100 -1.0%\nBTC 77,544 -1.3%\nGOLD 4,381 -0.3%\nUS10Y 4.75%\nVIX 14.9\nDXY 118.7 +0.3%\nCN GDP 21.05%\nUS CPI 2.95%\n19:53:51 ET\nCOMMAND CENTER \u2192\nJustHodl.AI\nDASHBOARD\nINTEL\n\ud83d\udfe2 LCE\n\ud83c\udf0d Cycle (live)\n\ud83d\udcc8 Cycle (history)\n\ud83c\udfdb AUCT
- `23:53:52`   CSP violations (0): []
- `23:53:52`   page errors (0): []
- `23:53:52`   other console errors/warnings (2): [{"type": "error", "text": "Access to fetch at 'https://nu4umjskc25osscrbmqh3o2gte0utlkx.lambda-url.us-east-1.on.aws/?diag=1&page=%2Fglobal-cycle.html&favs=0&sw=yes&v=3276' from origin 'https://justhodl.ai' has been blocked by CORS policy: The 'Access-Control-Allow-Origin' header contains multiple values '*, *', but only one is allowed. Have the server send the header with a valid value."}, {"type": "error", "text": "Failed to load resource: net::ERR_FAILED"}]
- `23:53:52`   failed requests (1): [{"url": "https://nu4umjskc25osscrbmqh3o2gte0utlkx.lambda-url.us-east-1.on.aws/?diag=1&page=%2Fglobal-cycle.html&favs=0&sw=yes&v=3276", "failure": "net::ERR_FAILED"}]
## verdict

- `23:53:52` ✅ VERDICT: GREEN -- CSP derived from code is live at the edge; global-cycle.html, /global-cycle/, index, chart-pro, why and fortress render with zero CSP violations
