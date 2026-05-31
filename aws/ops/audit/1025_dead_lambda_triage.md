# Dead Lambda Triage

Generated: 2026-05-31T12:59:13.291641+00:00
Total dead Lambdas: **367**

## Classification Summary

- **CANDIDATE_DELETE_DEPRECATED**: 25
- **CANDIDATE_DELETE_STALE**: 0
- **REVIEW_6M_STALE**: 0
- **REVIEW_3M_STALE**: 0
- **VERSIONED_REVIEW**: 0
- **ACTIVE_DEVELOPMENT**: 274
- **KEEP_NON_CRON_TRIGGER**: 68


### CANDIDATE_DELETE_DEPRECATED (25)

| Name | Last Modified | Size | Description |
|------|---------------|------|-------------|
| `justhodl-vix-backwardation-trigger` | 2026-05-30 | 7.3KB | Institutional once-per-cycle capitulation buy signal. State machine NULL->WARM->ARMED->FIRED->COOLDO |
| `justhodl-coffee-can` | 2026-05-30 | 5.8KB | Coffee-Can Tracker — multibagger holding-discipline + thesis-break detection |
| `justhodl-firm-stress` | 2026-05-30 | 5.3KB | Firm Stress Desk - re-prices the firm book through 15 scenarios (historical replays + macro shocks)  |
| `justhodl-metals-miners` | 2026-05-30 | 7.3KB | Metals & Miners screen - scores gold/silver/copper/uranium/lithium miners as leveraged calls on thei |
| `justhodl-global-macro` | 2026-05-30 | 4.7KB | Per-country economic regime aggregator. 15 countries × 5 dimensions (unemp, PMI, IP YoY, equity ETF, |
| `justhodl-magic-formula` | 2026-05-30 | 4.4KB | Greenblatt Magic Formula Screener: rank S&P 500 (ex-fin/utils/REITs) by Earnings Yield + ROIC. GuruF |
| `justhodl-forward-returns` | 2026-05-30 | 9.7KB | Capital Compass — forward 10y expected returns per asset class (Damodaran/GMO/AQR methodology). Earn |
| `justhodl-smart-money-holdings` | 2026-05-30 | 7.6KB | Builds inverse mapping {symbol: top funds holding it} for screener integration |
| `justhodl-breadth-thrust` | 2026-05-30 | 8.5KB | Zweig Breadth Thrust + Whaley January Barometer + Coppock Curve. 11-of-11 historical 12m positive re |
| `justhodl-smart-money-tracker` | 2026-05-30 | 3.9KB | Top hedge funds / institutional holders activity (13F-based) |
| `justhodl-portfolio-snapshot` | 2026-05-30 | 5.2KB | Portfolio enrichment (#9). Joins positions+watchlist with alpha/confluence/regime/sentiment + latest |
| `justhodl-tax-plan` | 2026-05-30 | 9.8KB | Tax-Aware Portfolio Engine. Per-position LT/ST classification + tax-if-sold-today. After-tax forward |
| `justhodl-ab-test` | 2026-05-30 | 4.7KB | A/B test of competing prompt strategies |
| `justhodl-history-api` | 2026-05-30 | 2.8KB | Read-only API for justhodl-history DDB. Function URL exposes /index, /snapshot, /latest, /timestamps |
| `justhodl-capital-return` | 2026-05-30 | 3.9KB | Capital-return cannibal screen - companies shrinking their share count via FCF-funded net buybacks a |
| `justhodl-gdelt-sentiment` | 2026-05-30 | 4.3KB | GDELT 2.0 global news + geopolitical sentiment. Pulls latest 15-min GKG batch, filters financial the |
| `justhodl-carry-surface` | 2026-05-30 | 9.9KB | UNIVERSAL CARRY SURFACE — institutional cross-asset carry engine. Answers: 'which asset is the marke |
| `justhodl-public-api-demo` | 2026-05-30 | 6.8KB | Reference public API endpoint demonstrating api_auth.py. |
| `justhodl-opex-calendar` | 2026-05-30 | 8.6KB | OPEX/0DTE gamma pinning calendar (Edge #8). Classifies trading day into OPEX regime (PRE/OPEX/POST/Q |
| `justhodl-tic-flows` | 2026-05-30 | 4.1KB | TIC Flows — foreign Treasury holdings + de-dollarization tracker |
| `justhodl-post-earnings-mean-rev` | 2026-05-30 | 5.8KB | Post-earnings drift exhaustion / mean-rev scanner. 4-factor: earnings 5-15td ago + RSI<=25 or >=75 + |
| `justhodl-gold-equity-rotation` | 2026-05-30 | 4.6KB | Gold-equity rotation. 5-factor: SPY/GLD ratio z-score, 20d momentum + MA50/MA200, 5d persistence, GD |
| `justhodl-backtest-harness` | 2026-05-30 | 5.3KB | Daily snapshot of all signal types + forward-return tracker. DDB table justhodl-backtest. |
| `justhodl-vol-target-unwind` | 2026-05-30 | 8.9KB | Vol-target unwind trigger (Edge #4). RV21 vs 16/20/25 thresholds; estimates AUM mechanically rebalan |
| `justhodl-calls-backtest` | 2026-05-30 | 4.1KB | Replays decisive-call ledger as SPY-exposure backtest. Daily. |

### ACTIVE_DEVELOPMENT (274)

| Name | Last Modified | Size | Description |
|------|---------------|------|-------------|
| `treasury-auto-updater` | 2026-05-22 | 1.5KB |  |
| `permanent-market-intelligence` | 2026-05-22 | 4.2KB | Complete market intelligence with all metrics |
| `global-liquidity-agent-v2` | 2026-05-22 | 0.7KB |  |
| `ofrapi` | 2026-05-22 | 21.4KB |  |
| `ecb-data-daily-updater` | 2026-05-22 | 2.5KB |  |
| `daily-liquidity-report` | 2026-05-22 | 4.6KB |  |
| `bls-employment-api-v2` | 2026-05-22 | 5.6KB |  |
| `fmp-stock-picks-agent` | 2026-05-22 | 6.5KB |  |
| `openbb-system2-api` | 2026-05-22 | 3.4KB |  |
| `justhodl-market-interpreter` | 2026-05-28 | 3.7KB | Arch #4 — AI-driven market interpretation engine. For each of 7 macro contexts (yield-curve/vix-curv |
| `justhodl-feed-catalog` | 2026-05-28 | 3.0KB | Arch #5 — Feed Catalog + JSON Schemas. Generates data/feed-catalog.json daily: every feed with size, |
| `justhodl-dep-graph` | 2026-05-28 | 3.1KB | Arch #8 — Platform Dependency Graph. Daily-eve: scans 374 Lambdas + 200+ pages, builds Lambda->S3->p |
| `justhodl-synthetic-monitor` | 2026-05-30 | 3.0KB | Arch #7 — end-to-end synthetic monitor. Every 15min: HTTPS-checks 13 critical pages + 4 critical fee |
| `justhodl-merger-arb` | 2026-05-30 | 6.5KB | Merger-arbitrage spread desk - parses SEC S-4 deal terms on announced M&A, prices gross spread + ann |
| `justhodl-data-collector` | 2026-05-30 | 1.2KB | Collects historical data - safe to delete |
| `justhodl-leading-markets` | 2026-05-30 | 5.5KB | Leading Markets engine — canary markets that lead macro tops/bottoms |
| `justhodl-stablecoin-flow` | 2026-05-30 | 7.1KB | Stablecoin mint/flow tracker (Edge #7). Aggregates 15+ USD stables via DefiLlama; classifies regime; |
| `justhodl-asymmetric-scorer` | 2026-05-30 | 8.6KB | Phase 2B — Asymmetric reward/risk equity scorer |
| `justhodl-fedwatch-rate-probability` | 2026-05-30 | 5.4KB | FedWatch Rate Probability (R5). CME FedWatch-equivalent rate-path tracker from Yahoo ZQ futures + FR |
| `justhodl-cds-proxy` | 2026-05-30 | 4.1KB | CDS Proxy — sovereign + corp credit risk from FRED spreads |
| `justhodl-esi` | 2026-05-30 | 4.0KB | Economic Surprise Index — time-decayed composite of macro beats/misses |
| `justhodl-momentum-scanner` | 2026-05-30 | 3.5KB |  |
| `justhodl-divergence-interpreter` | 2026-05-30 | 5.8KB | Regime-conditional divergence interpreter — Claude analyzes 70 cross-asset divergences in context of |
| `justhodl-pnl-tracker` | 2026-05-30 | 5.6KB | Loop 2 — hypothetical PnL tracker (B&H vs khalid_strategy) |
| `justhodl-cro-escalation` | 2026-05-30 | 9.6KB | Firm intraday risk tripwire - watches the live cross-asset tape (SPY/QQQ/HYG/TLT/IWM/VIX) and stack  |
| `justhodl-spinoff-desk` | 2026-05-30 | 7.9KB | Spin-Off and Special-Situations Desk - sources Form 10-12B spin-off registrations from SEC EDGAR, pr |
| `justhodl-volatility-squeeze-hunter` | 2026-05-30 | 16.0KB |  |
| `justhodl-smart-beta` | 2026-05-30 | 6.3KB | Refinitiv/MSCI Smart Beta Composite: 4-factor (Value+Quality+Momentum+LowVol) equal-weight percentil |
| `justhodl-hedge-planner` | 2026-05-30 | 9.0KB | Firm Hedge Execution Planner - turns the Tail Hedge Overlay sleeve into a worked order ticket. Track |
| `justhodl-bond-trace` | 2026-05-30 | 4.0KB | Bond TRACE proxy — HY/IG ETF stress + HY OAS velocity |
| `justhodl-theme-tier-classifier` | 2026-05-30 | 20.6KB |  |
| `justhodl-multi-tf-convergence` | 2026-05-30 | 5.7KB | tier2-retail-edges/multi-tf-convergence |
| `justhodl-liquidity-flow` | 2026-05-30 | 4.0KB | TGA + RRP + WALCL daily delta tracker. Net liquidity regime classification. |
| `justhodl-causality-scanner` | 2026-05-30 | 6.0KB | Exponential Idea #3 — Auto-Causality Discovery. Granger causality across all platform time series in |
| `justhodl-best-ideas` | 2026-05-30 | 5.8KB | Best Ideas board - cross-engine factor-confluence master screen. Fuses 12 single-stock opportunity e |
| `justhodl-catalyst-calendar` | 2026-05-30 | 4.8KB | Forward catalyst calendar — FOMC + Treasury auctions + earnings + witching + index rebalance. |
| `justhodl-market-extremes` | 2026-05-30 | 5.1KB | Market Cycle Extremes Radar - scores top-risk (euphoria signs: stretched valuations, narrowing bread |
| `justhodl-divcut-warning` | 2026-05-30 | 5.4KB | tier2-retail-edges/divcut-warning |
| `justhodl-fed-speak` | 2026-05-30 | 4.9KB | Claude-powered Fed speech sentiment tracker. Pulls Fed RSS, classifies HAWKISH/NEUTRAL/DOVISH on -10 |
| `justhodl-event-study` | 2026-05-30 | 4.7KB | Event Study Automation — algorithmic detection + forward returns |
| `justhodl-divergence-scanner` | 2026-05-30 | 14.1KB | Phase 1B — Cross-asset divergence scanner (12 macro pairs) |
| `justhodl-trade-logger` | 2026-05-30 | 4.2KB | Trade Journal logger (#16). Hourly scan of all signal sidecars (alpha/confluence/regime/debate/optio |
| `justhodl-sellside-views` | 2026-05-30 | 3.6KB | Sell-side strategist views — SPX target consensus + macro forecasts |
| `justhodl-ecb-detail` | 2026-05-30 | 5.3KB | ECB / Eurosystem Liquidity Detail Engine - granular ECB capital-injection data from the ECB Data Por |
| `justhodl-desk-allocator` | 2026-05-30 | 10.0KB | Multi-strategy capital allocator - sizes the seven strategy desks by tail-aware inverse-vol risk par |
| `justhodl-construction-housing` | 2026-05-30 | 3.2KB | Housing & Construction Cycle engine — FRED housing series fused into a cycle regime (permits/starts/ |
| `justhodl-market-internals` | 2026-05-30 | 5.4KB | Market Internals — A/D line + McClellan + % above MAs + 52w H/L |
| `justhodl-crypto-narratives` | 2026-05-30 | 2.3KB | Crypto Narrative / Sector Rotation — CMC categories ranked by 24h momentum + narrative breadth + Fea |
| `justhodl-live-pulse` | 2026-05-30 | 4.7KB | Live Pulse -- fast intraday stress layer. Polls SPY/VIX/DXY/MOVE live quotes every 15 min during US  |
| `justhodl-ma-tracker` | 2026-05-30 | 3.5KB | Mergers and Acquisitions activity tracker |

*… and 224 more (see JSON for full list)*

### KEEP_NON_CRON_TRIGGER (68)

| Name | Last Modified | Size | Description |
|------|---------------|------|-------------|
| `openbb-websocket-broadcast` | 2026-05-22 | 3.5KB |  |
| `bls-labor-agent` | 2026-05-22 | 2.1KB |  |
| `volatility-monitor-agent` | 2026-05-22 | 2.7KB |  |
| `bond-indices-agent` | 2026-05-22 | 2.6KB |  |
| `google-trends-agent` | 2026-05-22 | 2.2KB |  |
| `fmp-fundamentals-agent` | 2026-05-22 | 1.4KB |  |
| `eia-energy-agent` | 2026-05-22 | 2.0KB |  |
| `census-economic-agent` | 2026-05-22 | 1.6KB |  |
| `dollar-strength-agent` | 2026-05-22 | 2.9KB |  |
| `autonomous-ai-processor` | 2026-05-22 | 1.0KB |  |
| `ultimate-multi-agent` | 2026-05-22 | 1.9KB |  |
| `fred-ice-bofa-api` | 2026-05-22 | 0.5KB |  |
| `MLPredictor` | 2026-05-22 | 0.8KB |  |
| `xccy-basis-agent` | 2026-05-22 | 2.5KB |  |
| `coinmarketcap-agent` | 2026-05-22 | 8.2KB | Advanced CoinMarketCap Agent with AI Intelligence |
| `multi-agent-orchestrator` | 2026-05-22 | 1.5KB |  |
| `treasury-api` | 2026-05-22 | 51.9KB |  |
| `benzinga-news-agent` | 2026-05-22 | 1.3KB |  |
| `openbb-websocket-handler` | 2026-05-22 | 2.2KB |  |
| `securities-banking-agent` | 2026-05-22 | 2.5KB |  |
| `enhanced-repo-agent` | 2026-05-22 | 1.9KB |  |
| `manufacturing-global-agent` | 2026-05-22 | 2.9KB |  |
| `bea-economic-agent` | 2026-05-22 | 7.1KB |  |
| `alphavantage-technical-analysis` | 2026-05-22 | 0.8KB |  |
| `news-sentiment-agent` | 2026-05-22 | 0.9KB |  |
| `nasdaq-datalink-agent` | 2026-05-22 | 7.5KB |  |
| `economyapi` | 2026-05-22 | 12.0KB |  |
| `aiapi-market-analyzer` | 2026-05-22 | 2.4KB |  |
| `scrapeMacroData` | 2026-05-22 | 1.1KB |  |
| `fedliquidityapi` | 2026-05-22 | 6.0KB |  |
| `macro-financial-intelligence` | 2026-05-22 | 327.1KB |  |
| `chatgpt-agent-api` | 2026-05-22 | 2.8KB |  |
| `justhodl-ultimate-orchestrator` | 2026-05-30 | 2.4KB | Complete orchestrator with all agents and full data capabilities |
| `justhodl-crypto-enricher` | 2026-05-30 | 3.9KB |  |
| `justhodl-stock-analyzer` | 2026-05-30 | 12.0KB |  |
| `justhodl-stock-screener` | 2026-05-30 | 23.6KB |  |
| `justhodl-portfolio-admin` | 2026-05-30 | 3.9KB | Portfolio CRUD (#9). Invoked manually with action+payload to manage POSITION/WATCHLIST/STOPLOSS item |
| `justhodl-nyfed-dealer-survey` | 2026-05-30 | 2.5KB | NY Fed Survey of Primary Dealers — quarterly market expectations from the 24 banks moving the most f |
| `justhodl-stress-simulator` | 2026-05-30 | 6.6KB |  |
| `justhodl-watchlist` | 2026-05-30 | 4.2KB | Personal watchlist API — GET (public read) + POST add/remove/replace (admin token). |
| `justhodl-oecd-cli` | 2026-05-30 | 3.5KB | OECD Composite Leading Indicators across 16 major economies. Predicts business cycle turning points  |
| `justhodl-sec-8k` | 2026-05-30 | 3.2KB | SEC 8-K material event filings. Atom feed every 30 min, classifies by Item code, flags red flags (4. |
| `justhodl-sec-13f` | 2026-05-30 | 3.2KB | SEC 13F-HR institutional position tracker. Watches 18 major funds for new quarterly filings (Berkshi |
| `justhodl-investor-agents` | 2026-05-30 | 12.9KB |  |
| `justhodl-dex-scanner` | 2026-05-30 | 0.7KB |  |
| `justhodl-treasury-proxy` | 2026-05-30 | 7.6KB |  |
| `justhodl-chart-data` | 2026-05-30 | 7.8KB | Universal historical chart data API — FRED, ECB, OFR, Polygon, Internal. |
| `justhodl-insider-trades` | 2026-05-30 | 8.7KB | SEC EDGAR Form 4 insider-trades pipeline (every 30 min). |
| `justhodl-aaii-sentiment` | 2026-05-30 | 4.0KB | AAII Investor Sentiment Survey weekly retail bull/bear/neutral. Released Thursdays. Daily check for  |
| `justhodl-options-flow` | 2026-05-30 | 14.7KB |  |

*… and 18 more (see JSON for full list)*