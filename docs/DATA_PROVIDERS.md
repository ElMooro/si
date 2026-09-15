# JustHodl data providers — inventory (2026-09-15)

One rule for every keyed provider: the key lives in SSM (`/justhodl/...` SecureString) and is read through
`aws/shared/managed_secret.py`; a Lambda env var for that key must equal the SSM value or not exist at all
(ops 5576 deletes stale env vars so SSM wins). A rejected key is never silent: the FMP clients expose
`key_status=unauthorized` in the harvest.

| Provider | Used for | Key | Where the key lives | Engines (count of source refs) | Plan / entitlement notes |
|---|---|---|---|---|---|
| FMP (financialmodelingprep.com /stable) | company financials, ratios, earnings, profiles, calendars, ETF/analyst data | yes | `/justhodl/fmp/api-key` (env `FMP_KEY`/`FMP_API_KEY` must match or be absent) | 354 refs; stock-buying, buyback, earnings-sentiment, coffee-can, magic-formula, gf-value, etf/fi-census, merger-arb, ipo-pipeline, ignition, index-inclusion, hiring-velocity, investor-agents, stock-ai-research, benzinga-news-agent shim, fmp.html, equity_enrich consumers, chart Valuation/Financials | **FMP Ultimate — paid; the source for all financials/ratios** |
| FRED (api.stlouisfed.org) | macro series | yes | `/justhodl/fred/api-key` (legacy `/justhodl/fred-api-key`) | 209 refs | free |
| Polygon / Massive (api.polygon.io, api.massive.com) | delayed stock tape, options snapshot (Greeks/IV/OI), futures aggregates (10-min delay), ETF Global flows | yes | `/justhodl/polygon/api-key` (legacy `/justhodl/massive-api-key`) | 193 refs; polygon-full, tv-bars lanes, chart tape, options/futures desks, etf-global-desk | Options $29, Futures $29, ETF Global ~$297; **Stocks Financials/ratios NOT entitled — never call them** |
| Telegram (api.telegram.org) | alerts | bot token | `/justhodl/telegram/bot_token`, `/justhodl/telegram/chat_id` | 192 refs | — |
| Anthropic (api.anthropic.com) | LLM voice (credits dead since 2026-09-10) | yes | `/justhodl/anthropic/api_key` | 66 refs via llm_router | paid; not in the learning path |
| Z.ai GLM | second LLM voice via llm_router | yes | `/justhodl/zai-api-key` | router | paid; chat voice only, never grader/lesson |
| xAI (api.x.ai) | tier=grok voice (key not placed) | yes | `/justhodl/xai/api-key` | router | paid; empty → deterministic fallback |
| Perplexity | research voice | yes | `/justhodl/perplexity/api-key` | 6 refs | paid |
| CoinMarketCap (pro-api.coinmarketcap.com) | crypto listings | yes | `/justhodl/cmc/api-key` | 13 refs | — |
| Alpha Vantage | quotes fallback | yes | `/justhodl/alphavantage/api-key` | 8 refs | free tier |
| NewsAPI (newsapi.org) | headlines | yes | `/justhodl/newsapi/api-key` | 8 refs | — |
| Nasdaq Data Link | datasets | yes | `/justhodl/nasdaq-datalink/api-key` | 1 ref | — |
| OpenFIGI | identifiers | yes | `/justhodl/openfigi/api-key` | 8 refs | free |
| Trading Economics (api.tradingeconomics.com) | macro calendar | yes | `/justhodl/te_api` | 10 refs | — |
| CryptoQuant | on-chain | yes | `/justhodl/cryptoquant_api` | 7 refs | — |
| Quiver Quant (api.quiverquant.com) | congress/insider alt-data | check | not in managed_secret; reads env — reconcile lists it | 8 refs | — |
| EIA (api.eia.gov) | energy | check | reconcile lists any env | 7 refs | free |
| Census (api.census.gov) | trade/economic census | check | reconcile lists any env | 7 refs | free |
| Benzinga | news calendar (sunsets Oct 10; FMP calendar shim) | yes | `/justhodl/benzinga/api-key` if present | benzinga-news-agent | sunsetting |
| TradingView (tv-vault, tvnotes) | bars/notes ingestion | session/ingest token | `/justhodl/tradingview/sessionid`, `/justhodl/tvnotes/ingest-token` | 6 refs | — |
| SEC EDGAR (sec.gov, data.sec.gov, efts.sec.gov) | filings, 13F, full-text search | no key (UA required) | — | 108 refs | free |
| NY Fed (markets.newyorkfed.org) | SOFR, RRP, repo, primary dealers | no | — | 44 refs | free |
| ECB / Eurostat (data-api.ecb.europa.eu, ec.europa.eu SDMX) | euro-area series | no | — | 71 refs | free |
| Treasury FiscalData / TreasuryDirect | auctions, debt, buybacks | no | — | 21 refs | free |
| OFR (financialresearch.gov) | funding monitors | no | — | 24 refs | free |
| CFTC (publicreporting.cftc.gov) | COT positioning | no | — | 14 refs | free |
| FINRA (api.finra.org) | short volume / margin | no key placed (see /areas/finra) | — | 13 refs | closed at Khalid's word |
| BIS, IMF, World Bank, OECD SDMX, DBnomics | global macro | no | — | 47 refs | free |
| Cboe (cdn.cboe.com) | VIX/options data | no | — | 13 refs | free |
| Coinbase Exchange, CoinGecko, Deribit | crypto tape / DVOL / official BTC prints (wall) | no | — | 46 refs | free |
| Yahoo Finance (query1.finance.yahoo.com) | fundamentals/quotes fallback on the chart | no | — | 41 refs | unofficial; chart labels it as fallback only |
| Stooq, multpl, worldgovernmentbonds, stoxx, TWSE, JPX/MOF, PBoC, BoJ, BoE, Fed, Nasdaq/NYSE pages, GDELT, Google Trends/News, Wikipedia, HuggingFace/GitHub (reading receipts) | scrapes / public feeds | no | — | see host counts in ops 5576 | public; cited, never trained on |

Live key facts (hash prefixes only) are recorded per run at `s3://justhodl-dashboard-live/data/ops/provider-keys-<date>.json`.
