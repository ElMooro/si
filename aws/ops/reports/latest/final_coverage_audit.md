# FINAL Coverage Audit — Definitive Answer

**Status:** success  
**Duration:** 1.2s  
**Finished:** 2026-04-26T01:30:10+00:00  

## Log
## A. Site inventory

- `01:30:09`   HTML pages in repo:         50
- `01:30:09`   Launcher tiles:             31
- `01:30:09`   Topbar nav items:           12
## B. Lambda inventory

- `01:30:10`   Total Lambdas:              106
- `01:30:10`   justhodl-*:                 48
- `01:30:10`   other agent Lambdas:        58
## C. Coverage classification

- `01:30:10` 
  Coverage breakdown for 80 mapped Lambdas:
- `01:30:10`     GAP                             14
- `01:30:10`     surfaced                        50
- `01:30:10`     surfaced-partial                 2
- `01:30:10`     utility                          8
- `01:30:10`     utility-deprecated               2
- `01:30:10`     utility-loop1                    3
- `01:30:10`     utility-loop3                    1
- `01:30:10` 
  Unmapped Lambdas (26):
- `01:30:10`     FinancialIntelligence-Backend
- `01:30:10`     MLPredictor
- `01:30:10`     OpenBBS3DataProxy
- `01:30:10`     aiapi-market-analyzer
- `01:30:10`     autonomous-ai-processor
- `01:30:10`     chatgpt-agent-api
- `01:30:10`     createEnhancedIndex
- `01:30:10`     createUniversalIndex
- `01:30:10`     ecb
- `01:30:10`     economyapi
- `01:30:10`     fedliquidityapi-test
- `01:30:10`     global-liquidity-agent-TEST
- `01:30:10`     macro-financial-report-viewer
- `01:30:10`     macro-report-api
- `01:30:10`     multi-agent-orchestrator
- `01:30:10`     nyfed-financial-stability-fetcher
- `01:30:10`     nyfed-primary-dealer-fetcher
- `01:30:10`     nyfedapi-isolated
- `01:30:10`     openbb-system2-api
- `01:30:10`     openbb-websocket-broadcast
- `01:30:10`     openbb-websocket-handler
- `01:30:10`     permanent-market-intelligence
- `01:30:10`     scrapeMacroData
- `01:30:10`     testEnhancedScraper
- `01:30:10`     ultimate-multi-agent
- `01:30:10`     universal-agent-gateway
## D. SURFACED Lambdas (have a page or are part of a surfaced feature)

- `01:30:10`   ✅ alphavantage-market-agent                  → /stock/ (Alpha Vantage data)
- `01:30:10`   ✅ alphavantage-technical-analysis            → /stock/ technical tab
- `01:30:10`   ✅ benzinga-news-agent                        → /benzinga.html
- `01:30:10`   ✅ bls-employment-api-v2                      → /bls.html (v2)
- `01:30:10`   ✅ bls-labor-agent                            → /bls.html
- `01:30:10`   ✅ census-economic-agent                      → /census.html
- `01:30:10`   ✅ cftc-futures-positioning-agent             → /positioning/
- `01:30:10`   ✅ coinmarketcap-agent                        → /crypto/ (CMC source)
- `01:30:10`   ✅ daily-liquidity-report                     → /liquidity.html (daily)
- `01:30:10`   ✅ ecb-auto-updater                           → /ecb.html backend (weekly)
- `01:30:10`   ✅ ecb-data-daily-updater                     → /ecb.html backend
- `01:30:10`   ✅ eia-energy-agent                           → /eia.html
- `01:30:10`   ✅ fedliquidityapi                            → /liquidity.html backend
- `01:30:10`   ✅ fmp-fundamentals-agent                     → /fmp.html + /screener/
- `01:30:10`   ✅ fred-ice-bofa-api                          → /fred.html (via fred proxy)
- `01:30:10`   ✅ global-liquidity-agent-v2                  → /liquidity.html
- `01:30:10`   ✅ justhodl-advanced-charts                   → /charts.html backend
- `01:30:10`   ✅ justhodl-ai-chat                           → AI chat (multiple pages)
- `01:30:10`   ✅ justhodl-asymmetric-scorer                 → /desk.html setups
- `01:30:10`   ✅ justhodl-bond-regime-detector              → /desk.html regime cell + /
- `01:30:10`   ✅ justhodl-charts-agent                      → /charts.html backend
- `01:30:10`   ✅ justhodl-cot-extremes-scanner              → /positioning/
- `01:30:10`   ✅ justhodl-crypto-intel                      → /crypto/ + /
- `01:30:10`   ✅ justhodl-daily-report-v3                   → /intelligence.html + /
- `01:30:10`   ✅ justhodl-dex-scanner                       → /dex.html
- `01:30:10`   ✅ justhodl-divergence-scanner                → /desk.html divergences
- `01:30:10`   ✅ justhodl-ecb-proxy                         → /ecb.html (via proxy)
- `01:30:10`   ✅ justhodl-edge-engine                       → /edge.html + /
- `01:30:10`   ✅ justhodl-fred-proxy                        → /fred.html (via proxy)
- `01:30:10`   ✅ justhodl-health-monitor                    → /system.html (NEW)
- `01:30:10`   ✅ justhodl-intelligence                      → /intelligence.html
- `01:30:10`   ✅ justhodl-investor-agents                   → /investor.html (NEW)
- `01:30:10`   ✅ justhodl-khalid-metrics                    → /khalid/
- `01:30:10`   ✅ justhodl-liquidity-agent                   → /liquidity.html
- `01:30:10`   ✅ justhodl-ml-predictions                    → /ml-predictions.html
- `01:30:10`   ✅ justhodl-options-flow                      → /flow.html + /
- `01:30:10`   ✅ justhodl-pnl-tracker                       → /desk.html PnL card
- `01:30:10`   ✅ justhodl-reports-builder                   → /reports.html
- `01:30:10`   ✅ justhodl-risk-sizer                        → /risk.html
- `01:30:10`   ✅ justhodl-stock-ai-research                 → /stock/ AI tab
- `01:30:10`   ✅ justhodl-stock-analyzer                    → /stock/ (on-demand)
- `01:30:10`   ✅ justhodl-stock-screener                    → /screener/
- `01:30:10`   ✅ justhodl-treasury-proxy                    → /treasury-auctions.html (via proxy)
- `01:30:10`   ✅ justhodl-valuations-agent                  → /valuations.html
- `01:30:10`   ✅ justhodl-watchlist-debate                  → /investor.html (cached results)
- `01:30:10`   ✅ nasdaq-datalink-agent                      → /nasdaq-datalink.html
- `01:30:10`   ✅ ofrapi                                     → /ofr.html
- `01:30:10`   ✅ treasury-api                               → /treasury-auctions.html
- `01:30:10`   ✅ treasury-auto-updater                      → /treasury-auctions.html backend
- `01:30:10`   ✅ xccy-basis-agent                           → /carry.html (xccy basis)
- `01:30:10`   ✅ enhanced-repo-agent                        → feeds /repo.html (which is stub)
- `01:30:10`   ✅ justhodl-morning-intelligence              → Telegram alerts
## E. UTILITY Lambdas (no UI needed by design)

- `01:30:10` 
  utility:
- `01:30:10`     ⚙ justhodl-bloomberg-v8                      → feeds /charts.html (every 5min)
- `01:30:10`     ⚙ justhodl-cache-layer                       → backend cache (no UI)
- `01:30:10`     ⚙ justhodl-chat-api                          → likely older alias of ai-chat
- `01:30:10`     ⚙ justhodl-crypto-enricher                   → feeds /crypto/ via crypto-intel.json
- `01:30:10`     ⚙ justhodl-data-collector                    → backend collector (no UI)
- `01:30:10`     ⚙ justhodl-email-reports                     → Telegram daily 13:00 (8AM ET)
- `01:30:10`     ⚙ justhodl-email-reports-v2                  → Telegram daily 12:00
- `01:30:10`     ⚙ justhodl-telegram-bot                      → Telegram alerts (no web UI needed)
- `01:30:10` 
  utility-loop1:
- `01:30:10`     ⚙ justhodl-calibrator                        → surfaced via /reports.html scorecard
- `01:30:10`     ⚙ justhodl-outcome-checker                   → backend Loop 1 (DDB justhodl-outcomes)
- `01:30:10`     ⚙ justhodl-signal-logger                     → backend Loop 1 (DDB justhodl-signals)
- `01:30:10` 
  utility-loop3:
- `01:30:10`     ⚙ justhodl-prompt-iterator                   → backend Loop 3 (weekly self-improvement)
- `01:30:10` 
  utility-deprecated:
- `01:30:10`     ⚙ justhodl-ultimate-orchestrator             → old orchestrator (Sep 2025)
- `01:30:10`     ⚙ justhodl-ultimate-trading                  → old (Sep 2025)
## F. TRUE GAPS — Lambdas producing data with no UI

- `01:30:10` 
  14 Lambdas with no UI surface:
- `01:30:10`     🔍 bea-economic-agent                         → BEA data agent, no /bea.html page
- `01:30:10`         data: BEA economic
- `01:30:10`     🔍 bond-indices-agent                         → hourly, no /bonds.html page
- `01:30:10`         data: bond indices
- `01:30:10`     🔍 dollar-strength-agent                      → DXY data, no dedicated page
- `01:30:10`         data: USD strength
- `01:30:10`     🔍 fmp-stock-picks-agent                      → runs hourly weekdays, no UI
- `01:30:10`         data: stock picks data
- `01:30:10`     🔍 google-trends-agent                        → Google Trends data, no UI
- `01:30:10`         data: search trends
- `01:30:10`     🔍 justhodl-daily-macro-report                → daily 12:00 report, no UI
- `01:30:10`         data: unknown — NO LOGS
- `01:30:10`     🔍 justhodl-financial-secretary               → runs every 4h, no UI
- `01:30:10`         data: secretary/findings.json (file missing!)
- `01:30:10`     🔍 justhodl-news-sentiment                    → PRODUCES sentiment data NOT SURFACED
- `01:30:10`         data: sentiment/news.json (file missing!)
- `01:30:10`     🔍 justhodl-repo-monitor                      → /repo.html is 451B stub
- `01:30:10`         data: repo-data.json
- `01:30:10`     🔍 macro-financial-intelligence               → daily macro report, no UI
- `01:30:10`         data: macro intel
- `01:30:10`     🔍 manufacturing-global-agent                 → manufacturing PMI data, no UI
- `01:30:10`         data: global PMI
- `01:30:10`     🔍 news-sentiment-agent                       → sentiment data, no UI
- `01:30:10`         data: news sentiment
- `01:30:10`     🔍 securities-banking-agent                   → banking sector data, no UI
- `01:30:10`         data: banking
- `01:30:10`     🔍 volatility-monitor-agent                   → VIX/MOVE/etc, no /volatility.html
- `01:30:10`         data: vol surfaces
## G. FINAL ANSWER

- `01:30:10` 
  Lambdas surfaced as features:    52/80  (65%)
- `01:30:10`   Lambdas as utility (no UI need): 14/80  (18%)
- `01:30:10`   TRUE coverage gaps:              14/80  (18%)
- `01:30:10`   Effective coverage (surfaced + utility): 82%
## H. Page health summary

- `01:30:10` 
  Working pages linked from launcher: 31
- `01:30:10`   Pages built this session:
- `01:30:10`     /system.html   (78-component health monitor)
- `01:30:10`     /investor.html (Legendary Investor Panel — 6 personas)
- `01:30:10`   Pages fixed this session:
- `01:30:10`     intelligence.html, positioning/index.html, crypto/index.html
- `01:30:10`     (sed-replaced 6 dead http://...s3-website... URLs)
- `01:30:10` 
  Stub/dead pages remaining (untouched, awaiting cleanup approval):
- `01:30:10`     Reports.html (252B), ml.html (288B), repo.html (451B), stocks.html (249B)
- `01:30:10`     pro.html (59 days stale), exponential-search-dashboard.html (dead OpenBB),
- `01:30:10`     macroeconomic-platform.html (dead OpenBB)
- `01:30:10` Done
