# Bank fusion map — 2026-10-26

896 engines scanned; 768 match at least one newly banked dataset family; 227 cap their universe for API cost (S&P-only / hard caps / curated lists) and can scan the whole market from the factor book.

Modes: U universe · F factors · H history · E events/feeds.  Scores are keyword evidence from the engine's name, description and docstring (×3) plus source identifiers; they rank the queue, a human (or the wave review) decides the integration.

## Datasets → engines

| dataset | bank | mode | fields | engines |
|---|---|---|---|---|
| `fmp:scores-bulk` | fmp-bank | F | altman_z, piotroski, working_capital, retained_earnings, ebit, revenue, market_cap | 69: stock-valuations, screen-builder, risk-radar, quality-on-sale, magic-formula, investor-lenses, gf-value, edgar-authority, earnings-quality, earnings-confluence, dividend-growth, dislocation-detector, deep-value-screener, deep-value-overlap … |
| `fmp:rating-bulk` | fmp-bank | F | rating, overall score, DCF/ROE/ROA/DE/PE/PB component scores | 14: rating-change-cluster, starmine, stock-ai-research, best-ideas, analyst-consensus, beneish, fmp-stock-picks-agent, universe-builder, screen-builder, predictability, buyback-yield-ranking, stock-analyzer, eva-spread, attention-confluence |
| `fmp:dcf-bulk` | fmp-bank | F | dcf, price, dcf_upside_pct | 27: stock-valuations, mean-reversion, investor-lenses, gf-value, opportunity-engine, fundamentals-engine, equity-research, best-ideas, fmp-bank, debate-engine, alpha-score, opportunity-screener, bagger-engine, risk-radar … |
| `fmp:price-target-summary-bulk` | fmp-bank | F | pt last month/quarter/year avg-high-low, n analysts, pt_upside_pct | 59: starmine, sellside-views, sector-earnings-diffusion, rating-change-cluster, opportunity-screener, estimate-revisions, equity-research, eps-revision-velocity, analyst-consensus, fundamentals-engine, boom-radar, analyst-actions, research-backtest, opportunity-engine … |
| `fmp:upgrades-downgrades-consensus-bulk` | fmp-bank | F | strongBuy/buy/hold/sell/strongSell, consensus | 30: starmine, sellside-views, rating-change-cluster, analyst-consensus, resilience, fundamentals-engine, esma-ratings, attention-signals, alpha-alerts, equity-research, finnhub-signals, edgar-full-index, opportunity-engine, attention-confluence … |
| `fmp:key-metrics-ttm-bulk` | fmp-bank | F | ~60 TTM metrics: EV/EBITDA, ROIC, FCF yield, earnings yield, net debt/EBITDA, interest coverage, ... | 93: upside-thesis, theme-tier-classifier, stock-valuations, stock-buying, smart-money-holdings, smart-beta, screen-builder, quality-on-sale, peer-comparison, opportunity-engine, magic-formula, investor-lenses, fundamental-graphs, dislocation-detector … |
| `fmp:ratios-ttm-bulk` | fmp-bank | F | ~60 TTM ratios: PE, PB, PS, P/FCF, margins, turnovers, payout, dividend yield, leverage | 73: stock-valuations, screen-builder, peer-comparison, dividend-growth, divcut-warning, dislocation-detector, capital-return, opportunity-engine, sp500, equity-research, deep-value-screener, buyback-yield-ranking, theme-tier-classifier, stock-buying … |
| `fmp:statements-bulk` | fmp-bank | H | income / balance / cash-flow statements + growth per fiscal year x annual/Q1-Q4 since 1985 (whole market) | 117: stock-valuations, stock-buying, revenue-acceleration, peer-comparison, gf-value, fundamental-graphs, fundamental-census, eva-spread, equity-research, earnings-quality, divcut-warning, deep-value-screener, deep-value-overlap, capital-return … |
| `fmp:earnings-surprises-bulk` | fmp-bank | F | per-year EPS actual vs estimate (beat rate, avg surprise %) | 80: starmine, resilience, post-earnings-mean-rev, portfolio-catalysts, pead-detector, opportunity-screener, eps-revision-velocity, earnings-whisper, earnings-tracker, earnings-quality, earnings-pead, earnings-iv-crush, earnings-confluence, earnings-cascade … |
| `fmp:peers-bulk` | fmp-bank | F | peer list per symbol | 35: sympathetic-momentum, peer-comparison, pair-trades, insider-industry-cluster, dislocation-detector, pump-radar-brief, pairs-arb, supply-chain-linkage, supply-chain-graph, readthrough, dislocation-ai, ai-rerating-radar, catalyst-chain, bottleneck-boom … |
| `fmp:etf-holder-bulk` | fmp-bank | F | which ETFs hold the symbol and at what weight (passive ownership intensity) | 36: index-recon, etf-true-flows, etf-census, rebalance-radar, macro-confluence, russell-recon-frontrun, index-inclusion, hedge-planner, desk-allocator, catalyst-calendar, theme-cascade, fmp-bank, buyback-yield-ranking, spx-beaters … |
| `fmp:profile-bulk` | fmp-bank | U | every listed symbol: name, exchange, sector, industry, country, market cap, beta, ipo date, isEtf, isActivelyTrading | 118: universe-discovery, universe-builder, theme-tier-classifier, stock-buying, sector-heatmap, screen-builder, market-map, industry-boom, import-canary, impact-graph, deep-value-screener, deal-scanner, data-census, ai-infra-stack … |
| `fmp:eod-bulk` | fmp-bank | H | every listed symbol's close per session since 2000 (global) | 224: wave-signal-logger, theme-cascade-backtest, spx-ma, spx-history, positioning-analog, portfolio-analytics, momentum-leaders, bond-desk, auction-crisis-ai, global-stress, failed-pattern-reversal, 52wk-quality-breakout, signal-backtest, seasonality … |
| `fmp:shares-float-all` | fmp-walker | F | free float shares, outstanding, float % | 101: theme-second-wave, risk-radar, pump-mechanics, pair-trades, options-flow-scanner, microcap-float-squeeze, flow-confluence, ai-infra-stack, squeeze-pretrigger, signal-portfolio, short-interest, repo-lending, pre-pump-detector, lockup-expiration … |
| `fmp:insider-feeds` | fmp-walker | E | insider-trading latest/search/statistics/transaction-type (whole feed, not a watchlist) | 53: sequence-alpha-detector, rating-change-cluster, insider-trades, insider-sell-cluster, insider-industry-cluster, insider-cluster-scanner, insider-buys-enriched, edgar-insiders, political-stocks, fast-filings, sec-filings-intel, insider-radar, ignition, distribution-composite … |
| `fmp:institutional-13f` | fmp-walker | E | institutional-ownership latest/extracts per filer/holder performance/positions summary/industry breakdown | 171: smart-money-holdings, smart-money-tracker, fmp-walker, capital-flow, watchlist, sec-13f, industry-boom, activist-13d, 13f-positions, smart-money-13f, institutional-footprint, fast-filings, accum-composite, 13f-price-divergence … |
| `fmp:political-trades` | fmp-walker | E | senate/house trades, latest, by id | 16: political-trades, political-stocks, house-ptr-extract, political-intel, fmp-walker, political-ai-investigation, executive-intel, congress-direct, congress-alpha, worldbank-wgi, upside-thesis, spx-beaters, screener-alerts, prediction-markets … |
| `fmp:estimates-targets-grades` | fmp-walker | F | analyst-estimates annual/quarter, price-target + consensus + news, grades + historical + news | 107: starmine, sellside-views, sector-earnings-diffusion, rating-change-cluster, opportunity-screener, fundamentals-engine, estimate-revisions, equity-research, eps-revision-velocity, earnings-whisper, analyst-consensus, analyst-actions, trade-journal, peer-comparison … |
| `fmp:transcripts` | fmp-walker | E | earning-call-transcript per quarter + dates + latest | 30: transcript-indexer, pump-earnings-nlp, earnings-transcripts, earnings-sentiment, earnings-nlp, cb-stance, pump-radar-brief, powell-pivot, earnings-tone-velocity, earnings, dislocation-ai, talent-migration, fed-nlp, earnings-cascade … |
| `fmp:calendars` | fmp-walker | E | earnings / dividends / splits / IPO calendars (+ disclosure, prospectus), economic calendar | 54: precatalyst-vol-expansion, portfolio-catalysts, macro-calendar, lockup-expiration, ipo-pipeline, ipo-calendar, econ-calendar, catalyst-skew-premove, catalyst-chain, catalyst-calendar, asia-leads, desk-allocator, catalyst-clusters, benzinga-earnings … |
| `fmp:sec-filings-feeds` | fmp-walker | E | sec-filings-8k / financials / search-form-type / search-symbol | 70: xbrl-fundamentals, talent-migration, structural-pre-signals, sequence-alpha-detector, sec-filings-intel, sec-filing-diff, sec-8k-enrich, sec-8k, sec-13f, sec-10kq, ipo-calendar, insider-trades, fast-filings, edgar-insiders … |
| `fmp:ma-and-capital-feeds` | fmp-walker | E | mergers-acquisitions-latest, crowdfunding, fundraising, symbol-change, delisted | 34: spac-floor-warrant, merger-arb-risk, merger-arb, ma-target-predictor, deal-scanner, ma-tracker, desk-allocator, spinoff-desk, sec-filings-intel, corporate-actions, patent-velocity, industry-boom, contract-gate, catalyst … |
| `fmp:news-feeds` | fmp-walker | E | news stock/general/press-releases/crypto/forex (+ latest), fmp-articles, price-target & grades news | 80: news-wire, hot-stocks-digest, gdelt-sentiment, news-sentiment, narrative-vs-tape, attention-signals, gdelt-buzz, future-intelligence, tiingo-news, structural-pre-signals, sellside-views, sec-8k-enrich, resilience, regime-composite … |
| `fmp:macro-rates` | fmp-walker | F | treasury-rates history, market-risk-premium by country, economic-calendar | 72: fed-pivot-factor-router, bond-warroom, bond-desk, allocator, global-macro, eurodollar-plumbing, wave-signal-logger, stress-index, regime-conditional-router, master-allocator, leading-markets, event-study, ecb-detail, canary-macro … |
| `fmp:cot` | fmp-walker | H | commitment-of-traders report + analysis per symbol | 20: cta-trend-exhaust, crypto-cot, cot-tracker, cftc-deep-view, positioning-analog, fx-intelligence, cftc-full-datasets, yen-carry, cot-extremes-scanner, cftc-futures-positioning-agent, signal-portfolio, institutional-footprint, cot-feed, cds-desk … |
| `fmp:sector-industry-pe-perf` | fmp-walker | F | sector / industry PE snapshots + history, performance snapshots + history | 91: theme-rotation-engine, theme-rotation, sympathetic-momentum, sector-heatmap, sector-earnings-diffusion, rotation-dashboard, macro-confluence, industry-rotation, industry-boom, flow-anomaly-detector, crypto-narratives, breadth-divergence, allocator, spx-beaters … |
| `fmp:market-cap-history` | fmp-walker | H | historical-market-capitalization per symbol | 83: sector-heatmap, concentration-liquidity, insider-buyback-confluence, supply-chain-linkage, risk-monitor, repo-lending, pump-mechanics, peer-comparison, ndx-spx-spread, earnings-blackout, buyback-yield-ranking, bottleneck-boom, regime-map, insider-industry-cluster … |
| `fmp:revenue-segmentation` | fmp-walker | F | revenue by product and by geography (annual/quarter) | 35: theme-second-wave, supply-chain-linkage, readthrough, bottleneck-boom, fundamentals-engine, ai-infra-stack, bottleneck-research, supply-chain-graph, scarcity-radar, ppi-acceleration, phase-detector, chokepoint, taiwan-moea, universe-builder … |
| `fmp:employees` | fmp-walker | F | employee-count + historical | 9: talent-migration, labor-leading, hiring-velocity, consumer-pulse, fundamental-graphs, structural-pre-signals, redflag-alerter, divergence-engine-v2, census-us |
| `fmp:esg` | fmp-walker | F | esg-disclosures / ratings / benchmark | 0:  |
| `fmp:etf-holdings-weightings` | fmp-walker | F | etf-holdings, sector/country weightings, asset exposure, info, portfolio dates | 53: flow-anomaly-detector, etf-true-flows, etf-constituents, etf-census, crypto-etf-flows, crypto-etf-arb, firm-book, etf-issuer-holdings, cef-discount, transcript-indexer, smart-money-cluster, fmp-walker, deep-value-overlap, ark-holdings … |
| `fmp:intraday-1min` | fmp-walker | H | historical-chart-1min per session for the chart universe | 26: streaming-fanout, live-pulse, intraday-pulse, gap-metrics, gap-fill-confirm, cro-escalation, crypto-etf-arb, sector-heatmap, scheduler, fmp-walker, accum-composite, smart-wake, opex-calendar, price-redundancy … |
| `fmp:movers-snapshots` | fmp-walker | E | biggest-gainers / losers / most-actives dated daily | 92: theme-classifier, momentum-leaders, momentum-breakout, crypto-opportunities, attention-signals, theme-cascade, early-movers, catalyst-classifier, theme-rotation-engine, opportunity-screener, master-ranker, hot-stocks-digest, hiring-velocity, future-intelligence … |
| `fmp:dividends-splits` | fmp-walker | H | dividends + splits history per symbol | 40: dividend-growth, divcut-warning, buyback-yield-ranking, tax-plan, cef-discount, ecb-deep, corporate-actions, euro-fragmentation, desk-returns, capital-return, reit-nav-discount, portfolio-snapshot, foreign-flows, canary-warroom … |
| `fmp:governance-execs` | fmp-walker | F | key-executives, executive compensation (+ benchmark) | 24: insider-sell-cluster, insider-industry-cluster, insider-cluster-scanner, insider-buys-enriched, earnings-whisper, sec-filings-intel, worldbank-wgi, talent-migration, political-intel, executive-intel, auction-crisis-ai, edgar-insiders, rv-iv-scanner, llm-cost-dashboard … |
| `fmp:fail-to-deliver` | fmp-walker | H | fail-to-deliver per symbol | 21: plumbing-panel, plumbing-composite, eurodollar-plumbing, equity-ftd, repo, ignition, short-book, settlement-fails, chart-data, treasury-rehypo, plumbing-aggregator, trade-nowcast, ticker-360, khalid-risk … |
| `fmp:social-sentiment` | fmp-walker | F | historical-social-sentiment, trending, change | 18: stocktwits, hot-stocks-digest, crypto-opportunities, attention-signals, future-intelligence, buzz-velocity, attention-confluence, gdelt-buzz, lobbying-intel, digest-trends-ai, narrative-vs-tape, convergence-radar, retail-sentiment, bottom … |
| `fmp:funds-disclosure` | fmp-walker | E | mutual-fund / ETF disclosures per holder (N-PORT) | 13: smart-money-holdings, crypto-etf-flows, ici-flows, etf-true-flows, fmp-walker, etf-fund-flows, tic-flows, 13f-price-divergence, alpha-daily-brief, flows-ai-analysis, etf-flows, capital-flow-radar, ark-holdings |
| `fmp:company-notes` | fmp-walker | F | company-notes (debt instruments outstanding) | 17: systemic-stress, sovereign-stress, credit-equity-divergence, credit-before-equity, cds-proxy, cds-monitor, cds-desk, fi-census, beneish, risk, global-sovereign, global-expansion, risk-regime, crisis-composite … |
| `polygon:short-interest-volume` | polygon-bank | F | short interest (settlement dates, days to cover), daily short volume ratio | 105: risk-radar, pump-mechanics, pair-trades, options-flow-scanner, microcap-float-squeeze, flow-confluence, crypto-funding, theme-second-wave, signal-portfolio, pump-radar-brief, pump-positioning, prepump-summary, institutional-footprint, ai-infra-stack … |
| `polygon:ticker-events-ipos` | polygon-bank | E | ticker events (name/ticker changes), IPOs | 9: universe-discovery, spac-floor-warrant, lockup-expiration, ipo-pipeline, ipo-calendar, corporate-actions, magic-formula, fmp-walker, carry-surface |
| `polygon:options-contracts` | polygon-bank | F | options contracts (incl. expired) for the chart universe | 109: skew-tail-hedging, pump-mechanics, options-flow-scanner, options-confluence, hedge-pnl, hedge-planner, flow-confluence, cro-digest, cboe-options-chain, polygon-options-flow, institutional-footprint, earnings-whisper, trade-logger, stealth-accumulation … |
| `polygon:minute-bars` | polygon-bank | H | minute bars (730 days) for the chart universe | 52: streaming-fanout, market-tape, live-pulse, intraday-pulse, gap-fill-confirm, polygon-bank, cro-escalation, crypto-etf-arb, fmp-walker, trade-ticket-monitor, sector-heatmap, pnl-tracker, insider-buys-enriched, history-snapshotter … |
| `polygon:etf-global` | polygon-bank | F | ETF Global fund flows, constituents, profiles for every US ETF | 61: rebalance-radar, impact-graph, global-flow-desk, flows-ai-analysis, flow-anomaly-detector, etf-true-flows, etf-flows, etf-census, crypto-etf-flows, crypto-etf-arb, capital-flow, wave-signal-logger, macro-confluence, crypto-exchange-flows … |
| `polygon:grouped-crypto-fx` | polygon-bank | H | grouped daily crypto and FX sessions since 2018 | 111: fx-intelligence, fx-decomposition, dxy-equity-divergence, crypto-basis, cross-asset-regime, carry-surface, boj-detail, yen-carry, macro-regime, divergence-engine-v2, cross-asset-confirm, crisis-knowledge-base, global-flow-desk, correlation-breaks … |
| `polygon:eod-per-ticker` | polygon-eod | H | every US ticker's daily OHLCV/VWAP/trades since 2021-08-26 in one file per ticker | 273: volatility-squeeze-hunter, trend-reversal, trend-engine, theme-classifier, theme-cascade-backtest, sympathetic-momentum, spx-ma, seasonality, rv-iv-scanner, post-earnings-mean-rev, portfolio-analytics, ndx-spx-spread, multi-tf-convergence, momentum-leaders … |
| `polygon:market-plumbing` | polygon-bank | E | market status/holidays, exchanges, conditions, ticker types, whole-market snapshot + gainers/losers | 22: forced-selling-bounce, breadth-thrust, breadth-divergence, polygon-bank, market-tape, spx-ma, self-critique, polygon-full, polygon-eod, tape-reader, reversal-radar, allocator, smart-money-tracker, domain-barometers … |
| `eia:full-catalog` | eia-bank | H | whole EIA v2 tree: petroleum stocks/prices/production/imports/refinery runs, natural gas storage/prices, electricity RTO, coal, STEO, nuclear outages | 75: inventory-drawdown, commodity-curves, repo, macro-regime, canary-grid, refining-stress, volatility-squeeze-hunter, trend-engine, supply-chain-graph, structural-pre-signals, singapore-nodx, rotation-chain, risk-ratios, real-economy-collector … |
| `cmc:crypto-market` | provider-bank | F | CMC categories, global metrics history (dominance, total cap), fear & greed history, exchange map, listings | 100: openbb-websocket-broadcast, watchlist, tv-notes-ingest, trade-journal, stablecoin-flow, rotation-radar, risk, regime-map, rebalance-radar, quantum-desk, price-redundancy, position-monitor, portfolio-admin, polygon-bank … |
| `cq:onchain-hourly` | provider-bank | H | CryptoQuant hourly metrics per exchange / miner (flows, reserves) | 72: cryptoquant, stablecoin-flow, rotation-radar, risk, regime-map, rebalance-radar, quantum-desk, onchain-ratios, metals-miners, katlin, hyperliquid-perps, google-trends, global-flow-desk, gap-metrics … |
| `te:macro-markets` | provider-bank | F | Trading Economics calendar, country indicators, markets (bond/commodity/currency/index/crypto), forecasts, ratings (once the 401 is cleared) | 131: systemic-stress, sovereign-stress, sellside-views, prediction-markets, nowcast-desk, macro-surprise, macro-regime, macro-attribution, leading-markets, global-stress, global-markets, global-macro, euro-fragmentation, esma-ratings … |

## Engines → datasets

| engine | providers now | universe | datasets (mode) |
|---|---|---|---|
| `justhodl-alpha-score` | — | hard cap on names | fmp:key-metrics-ttm-bulk (F), fmp:scores-bulk (F), polygon:short-interest-volume (F), polygon:options-contracts (F), fmp:shares-float-all (F), fmp:ratios-ttm-bulk (F), fmp:profile-bulk (U), fmp:institutional-13f (E) |
| `justhodl-analyst-consensus` | fmp | hard cap on names | fmp:upgrades-downgrades-consensus-bulk (F), fmp:price-target-summary-bulk (F), fmp:estimates-targets-grades (F), fmp:earnings-surprises-bulk (F), fmp:movers-snapshots (E), fmp:rating-bulk (F), fmp:news-feeds (E), polygon:options-contracts (F) |
| `justhodl-anomaly-detector` | fred polygon | hard cap on names | polygon:options-contracts (F), polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F), eia:full-catalog (H), fmp:institutional-13f (E), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-attention-confluence` | — | hard cap on names | polygon:options-contracts (F), fmp:insider-feeds (E), fmp:social-sentiment (F), fmp:institutional-13f (E), fmp:upgrades-downgrades-consensus-bulk (F), fmp:price-target-summary-bulk (F), fmp:political-trades (E), fmp:news-feeds (E) |
| `justhodl-best-ideas` | — | hard cap on names | fmp:key-metrics-ttm-bulk (F), fmp:statements-bulk (H), fmp:insider-feeds (E), fmp:estimates-targets-grades (F), fmp:institutional-13f (E), fmp:scores-bulk (F), fmp:dcf-bulk (F), fmp:rating-bulk (F) |
| `justhodl-boom-radar` | — | hard cap on names | fmp:earnings-surprises-bulk (F), fmp:price-target-summary-bulk (F), polygon:short-interest-volume (F), fmp:shares-float-all (F), fmp:calendars (E), fmp:estimates-targets-grades (F), polygon:options-contracts (F), fmp:upgrades-downgrades-consensus-bulk (F) |
| `justhodl-buyback-engine` | fmp | hard cap on names | fmp:statements-bulk (H), fmp:calendars (E), fmp:key-metrics-ttm-bulk (F), polygon:etf-global (F), fmp:movers-snapshots (E), polygon:short-interest-volume (F), fmp:shares-float-all (F), fmp:sec-filings-feeds (E) |
| `justhodl-catalyst-classifier` | — | hard cap on names | fmp:movers-snapshots (E), fmp:calendars (E), fmp:estimates-targets-grades (F), polygon:short-interest-volume (F), fmp:shares-float-all (F), eia:full-catalog (H), fmp:earnings-surprises-bulk (F), fmp:ma-and-capital-feeds (E) |
| `justhodl-compound-aggregator` | — | hard cap on names | fmp:scores-bulk (F), fmp:insider-feeds (E), fmp:key-metrics-ttm-bulk (F), fmp:institutional-13f (E), fmp:estimates-targets-grades (F), fmp:earnings-surprises-bulk (F), fmp:statements-bulk (H), fmp:profile-bulk (U) |
| `justhodl-deep-value-overlap` | — | hard cap on names | fmp:statements-bulk (H), fmp:scores-bulk (F), fmp:key-metrics-ttm-bulk (F), fmp:insider-feeds (E), fmp:calendars (E), polygon:etf-global (F), fmp:etf-holdings-weightings (F), fmp:ratios-ttm-bulk (F) |
| `justhodl-eps-revision-velocity` | fmp | S&P 500 only; hard cap on names; unified universe (data/universe.json) | fmp:price-target-summary-bulk (F), fmp:estimates-targets-grades (F), fmp:earnings-surprises-bulk (F), fmp:movers-snapshots (E), fmp:key-metrics-ttm-bulk (F), fmp:statements-bulk (H), fmp:scores-bulk (F), fmp:profile-bulk (U) |
| `justhodl-equity-research` | polygon sec | S&P 500 only; hard cap on names; Finviz universe | fmp:statements-bulk (H), fmp:price-target-summary-bulk (F), fmp:estimates-targets-grades (F), fmp:ratios-ttm-bulk (F), fmp:key-metrics-ttm-bulk (F), fmp:scores-bulk (F), fmp:dcf-bulk (F), polygon:eod-per-ticker (H) |
| `justhodl-fmp-walker` | — | hard cap on names; curated / watchlist universe; FMP screener ladder (search endpoint, not banked) | fmp:institutional-13f (E), fmp:political-trades (E), polygon:minute-bars (H), fmp:etf-holdings-weightings (F), fmp:news-feeds (E), fmp:sec-filings-feeds (E), fmp:funds-disclosure (E), fmp:intraday-1min (H) |
| `justhodl-fortress` | — | S&P 500 only; hard cap on names; Finviz universe | fmp:scores-bulk (F), polygon:short-interest-volume (F), fmp:shares-float-all (F), fmp:profile-bulk (U), fmp:price-target-summary-bulk (F), polygon:eod-per-ticker (H), fmp:key-metrics-ttm-bulk (F), fmp:eod-bulk (H) |
| `justhodl-forward-returns` | fmp fred | S&P 500 only; hard cap on names | polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F), fmp:sec-filings-feeds (E), fmp:profile-bulk (U), fmp:institutional-13f (E), polygon:eod-per-ticker (H), fmp:key-metrics-ttm-bulk (F) |
| `justhodl-fundamental-graphs` | — | hard cap on names; FMP screener ladder (search endpoint, not banked) | fmp:statements-bulk (H), fmp:key-metrics-ttm-bulk (F), fmp:scores-bulk (F), fmp:price-target-summary-bulk (F), fmp:institutional-13f (E), fmp:employees (F), fmp:ratios-ttm-bulk (F), fmp:dividends-splits (H) |
| `justhodl-katlin` | fmp polygon | hard cap on names; curated / watchlist universe; Finviz universe | polygon:eod-per-ticker (H), polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F), polygon:options-contracts (F), fmp:statements-bulk (H), fmp:institutional-13f (E), fmp:calendars (E) |
| `justhodl-macro-confluence` | — | hard cap on names | fmp:sector-industry-pe-perf (F), polygon:etf-global (F), fmp:etf-holder-bulk (F), polygon:options-contracts (F), polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F), fmp:etf-holdings-weightings (F) |
| `justhodl-master-ranker` | — | hard cap on names; curated / watchlist universe; unified universe (data/universe.json); Finviz universe | polygon:eod-per-ticker (H), fmp:movers-snapshots (E), fmp:earnings-surprises-bulk (F), polygon:options-contracts (F), polygon:short-interest-volume (F), fmp:shares-float-all (F), fmp:estimates-targets-grades (F), fmp:scores-bulk (F) |
| `justhodl-opportunity-engine` | — | hard cap on names; unified universe (data/universe.json) | fmp:key-metrics-ttm-bulk (F), fmp:ratios-ttm-bulk (F), fmp:dcf-bulk (F), fmp:price-target-summary-bulk (F), fmp:institutional-13f (E), fmp:estimates-targets-grades (F), fmp:upgrades-downgrades-consensus-bulk (F), eia:full-catalog (H) |
| `justhodl-opportunity-screener` | — | hard cap on names | fmp:price-target-summary-bulk (F), fmp:estimates-targets-grades (F), fmp:earnings-surprises-bulk (F), fmp:statements-bulk (H), fmp:movers-snapshots (E), polygon:eod-per-ticker (H), fmp:ratios-ttm-bulk (F), fmp:profile-bulk (U) |
| `justhodl-pump-positioning` | fmp | hard cap on names | polygon:short-interest-volume (F), fmp:shares-float-all (F), fmp:movers-snapshots (E), fmp:earnings-surprises-bulk (F), fmp:calendars (E), polygon:options-contracts (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-pump-radar-brief` | — | hard cap on names | polygon:short-interest-volume (F), fmp:transcripts (E), fmp:shares-float-all (F), fmp:peers-bulk (F), fmp:movers-snapshots (E), fmp:institutional-13f (E), polygon:options-contracts (F), fmp:calendars (E) |
| `justhodl-regime-composite` | — | hard cap on names | polygon:options-contracts (F), fmp:insider-feeds (E), fmp:news-feeds (E), fmp:institutional-13f (E), fmp:movers-snapshots (E), te:macro-markets (F), polygon:grouped-crypto-fx (H), cq:onchain-hourly (H) |
| `justhodl-risk-radar` | — | — | polygon:short-interest-volume (F), fmp:shares-float-all (F), fmp:scores-bulk (F), fmp:price-target-summary-bulk (F), fmp:key-metrics-ttm-bulk (F), fmp:statements-bulk (H), fmp:sec-filings-feeds (E), fmp:earnings-surprises-bulk (F) |
| `justhodl-spx-beaters` | polygon | S&P 500 only; hard cap on names; unified universe (data/universe.json) | fmp:sector-industry-pe-perf (F), fmp:profile-bulk (U), polygon:etf-global (F), fmp:etf-holdings-weightings (F), fmp:etf-holder-bulk (F), fmp:political-trades (E), fmp:institutional-13f (E), fmp:key-metrics-ttm-bulk (F) |
| `justhodl-stock-valuations` | fmp | S&P 500 only; hard cap on names; FMP screener ladder (search endpoint, not banked); Finviz universe | fmp:statements-bulk (H), fmp:scores-bulk (F), fmp:ratios-ttm-bulk (F), fmp:key-metrics-ttm-bulk (F), fmp:dcf-bulk (F), fmp:insider-feeds (E), fmp:profile-bulk (U), fmp:etf-holdings-weightings (F) |
| `justhodl-theme-second-wave` | fmp | hard cap on names; unified universe (data/universe.json) | fmp:shares-float-all (F), fmp:revenue-segmentation (F), polygon:short-interest-volume (F), fmp:profile-bulk (U), polygon:options-contracts (F), fmp:institutional-13f (E), polygon:eod-per-ticker (H), fmp:movers-snapshots (E) |
| `justhodl-upside-thesis` | — | hard cap on names | fmp:key-metrics-ttm-bulk (F), polygon:eod-per-ticker (H), fmp:institutional-13f (E), fmp:calendars (E), fmp:profile-bulk (U), fmp:insider-feeds (E), polygon:short-interest-volume (F), fmp:shares-float-all (F) |
| `justhodl-ai-infra-stack` | fmp | hard cap on names; curated / watchlist universe; unified universe (data/universe.json) | fmp:shares-float-all (F), fmp:profile-bulk (U), polygon:short-interest-volume (F), fmp:revenue-segmentation (F), eia:full-catalog (H), polygon:options-contracts (F), polygon:eod-per-ticker (H), fmp:movers-snapshots (E) |
| `justhodl-engine-signal-map` | — | hard cap on names; curated / watchlist universe | fmp:profile-bulk (U), fmp:key-metrics-ttm-bulk (F), polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F), fmp:scores-bulk (F), fmp:sector-industry-pe-perf (F), fmp:ratios-ttm-bulk (F) |
| `justhodl-forward-orders` | fmp newsapi sec | hard cap on names; FMP screener ladder (search endpoint, not banked) | fmp:sec-filings-feeds (E), fmp:eod-bulk (H), fmp:market-cap-history (H), fmp:institutional-13f (E), fmp:earnings-surprises-bulk (F), polygon:short-interest-volume (F), fmp:shares-float-all (F), fmp:movers-snapshots (E) |
| `justhodl-global-flow-desk` | — | hard cap on names | polygon:etf-global (F), polygon:grouped-crypto-fx (H), fmp:statements-bulk (H), cq:onchain-hourly (H), cmc:crypto-market (F), fmp:institutional-13f (E), fmp:etf-holdings-weightings (F), te:macro-markets (F) |
| `justhodl-institutional-footprint` | — | hard cap on names | polygon:short-interest-volume (F), polygon:options-contracts (F), fmp:shares-float-all (F), fmp:institutional-13f (E), fmp:cot (H), fmp:calendars (E), polygon:etf-global (F), fmp:market-cap-history (H) |
| `justhodl-metals-miners` | — | curated / watchlist universe | fmp:profile-bulk (U), fmp:key-metrics-ttm-bulk (F), fmp:ratios-ttm-bulk (F), eia:full-catalog (H), cq:onchain-hourly (H), cmc:crypto-market (F), fmp:scores-bulk (F), fmp:statements-bulk (H) |
| `justhodl-quantum-desk` | — | hard cap on names | polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F), polygon:etf-global (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H), fmp:institutional-13f (E), fmp:etf-holdings-weightings (F) |
| `justhodl-sp500` | fred | S&P 500 only; hard cap on names | fmp:statements-bulk (H), fmp:ratios-ttm-bulk (F), fmp:profile-bulk (U), polygon:eod-per-ticker (H), fmp:movers-snapshots (E), fmp:price-target-summary-bulk (F), fmp:key-metrics-ttm-bulk (F), fmp:upgrades-downgrades-consensus-bulk (F) |
| `justhodl-universe-builder` | fmp | hard cap on names; curated / watchlist universe; FMP screener ladder (search endpoint, not banked); unified universe (data/universe.json) | fmp:profile-bulk (U), fmp:ratios-ttm-bulk (F), fmp:key-metrics-ttm-bulk (F), fmp:revenue-segmentation (F), fmp:peers-bulk (F), polygon:short-interest-volume (F), fmp:shares-float-all (F), fmp:institutional-13f (E) |
| `justhodl-ai-brief` | — | hard cap on names | fmp:earnings-surprises-bulk (F), fmp:macro-rates (F), fmp:sector-industry-pe-perf (F), fmp:insider-feeds (E), fmp:news-feeds (E), polygon:eod-per-ticker (H), fmp:governance-execs (F), fmp:eod-bulk (H) |
| `justhodl-ai-brief-router` | — | hard cap on names | fmp:macro-rates (F), fmp:institutional-13f (E), polygon:eod-per-ticker (H), fmp:eod-bulk (H), fmp:price-target-summary-bulk (F), te:macro-markets (F), polygon:grouped-crypto-fx (H), fmp:estimates-targets-grades (F) |
| `justhodl-allocator` | — | hard cap on names | fmp:sector-industry-pe-perf (F), fmp:macro-rates (F), polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F), fmp:profile-bulk (U), polygon:market-plumbing (E), fmp:earnings-surprises-bulk (F) |
| `justhodl-alpha-research` | — | hard cap on names | fmp:sector-industry-pe-perf (F), fmp:institutional-13f (E), fmp:key-metrics-ttm-bulk (F), fmp:scores-bulk (F), fmp:estimates-targets-grades (F), fmp:statements-bulk (H), fmp:price-target-summary-bulk (F), fmp:earnings-surprises-bulk (F) |
| `justhodl-altseason` | cmc coingecko | hard cap on names | fmp:sector-industry-pe-perf (F), polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F), polygon:eod-per-ticker (H), fmp:movers-snapshots (E), fmp:eod-bulk (H), polygon:short-interest-volume (F) |
| `justhodl-attention-signals` | — | hard cap on names | fmp:social-sentiment (F), fmp:movers-snapshots (E), fmp:upgrades-downgrades-consensus-bulk (F), fmp:news-feeds (E), fmp:shares-float-all (F), fmp:insider-feeds (E), polygon:short-interest-volume (F), fmp:price-target-summary-bulk (F) |
| `justhodl-buyback-yield-ranking` | fmp | hard cap on names | fmp:key-metrics-ttm-bulk (F), fmp:statements-bulk (H), fmp:ratios-ttm-bulk (F), fmp:dividends-splits (H), fmp:market-cap-history (H), fmp:etf-holder-bulk (F), fmp:scores-bulk (F), fmp:profile-bulk (U) |
| `justhodl-crypto-opportunities` | cmc coingecko | hard cap on names; curated / watchlist universe | fmp:social-sentiment (F), fmp:movers-snapshots (E), polygon:short-interest-volume (F), polygon:grouped-crypto-fx (H), fmp:shares-float-all (F), cq:onchain-hourly (H), cmc:crypto-market (F), polygon:minute-bars (H) |
| `justhodl-cta-trend-exhaust` | fmp | hard cap on names | fmp:cot (H), te:macro-markets (F), fmp:movers-snapshots (E), fmp:macro-rates (F), fmp:institutional-13f (E), polygon:short-interest-volume (F), polygon:eod-per-ticker (H), fmp:shares-float-all (F) |
| `justhodl-deal-scanner` | fmp polygon sec | hard cap on names; curated / watchlist universe; unified universe (data/universe.json) | fmp:profile-bulk (U), fmp:ma-and-capital-feeds (E), fmp:sec-filings-feeds (E), fmp:news-feeds (E), fmp:institutional-13f (E), fmp:statements-bulk (H), fmp:market-cap-history (H), fmp:eod-bulk (H) |
| `justhodl-flows-ai-analysis` | — | hard cap on names; curated / watchlist universe | polygon:etf-global (F), fmp:sec-filings-feeds (E), fmp:institutional-13f (E), fmp:insider-feeds (E), fmp:estimates-targets-grades (F), fmp:sector-industry-pe-perf (F), fmp:news-feeds (E), fmp:etf-holdings-weightings (F) |
| `justhodl-gap-metrics` | cboe fred polygon yahoo | hard cap on names | fmp:intraday-1min (H), cq:onchain-hourly (H), cmc:crypto-market (F), fmp:price-target-summary-bulk (F), fmp:estimates-targets-grades (F), polygon:grouped-crypto-fx (H), fmp:news-feeds (E), fmp:earnings-surprises-bulk (F) |
| `justhodl-ignition` | fmp polygon sec | hard cap on names; unified universe (data/universe.json) | fmp:sec-filings-feeds (E), fmp:insider-feeds (E), polygon:short-interest-volume (F), fmp:shares-float-all (F), fmp:movers-snapshots (E), fmp:institutional-13f (E), fmp:fail-to-deliver (H), fmp:estimates-targets-grades (F) |
| `justhodl-impact-graph` | fmp polygon | hard cap on names | polygon:etf-global (F), fmp:profile-bulk (U), fmp:key-metrics-ttm-bulk (F), fmp:etf-holdings-weightings (F), fmp:etf-holder-bulk (F), polygon:eod-per-ticker (H), eia:full-catalog (H), fmp:sector-industry-pe-perf (F) |
| `justhodl-industry-boom` | — | hard cap on names; unified universe (data/universe.json); Finviz universe | fmp:sector-industry-pe-perf (F), fmp:profile-bulk (U), fmp:institutional-13f (E), fmp:statements-bulk (H), fmp:ma-and-capital-feeds (E), fmp:estimates-targets-grades (F), fmp:movers-snapshots (E), fmp:insider-feeds (E) |
| `justhodl-insider-buys-enriched` | — | — | fmp:insider-feeds (E), fmp:governance-execs (F), polygon:options-contracts (F), fmp:profile-bulk (U), fmp:news-feeds (E), fmp:institutional-13f (E), polygon:minute-bars (H), fmp:shares-float-all (F) |
| `justhodl-magic-formula` | fmp | S&P 500 only; hard cap on names | fmp:scores-bulk (F), fmp:key-metrics-ttm-bulk (F), fmp:profile-bulk (U), polygon:eod-per-ticker (H), fmp:eod-bulk (H), fmp:ratios-ttm-bulk (F), fmp:statements-bulk (H), fmp:dcf-bulk (F) |
| `justhodl-options-flow-scanner` | fmp polygon | hard cap on names; unified universe (data/universe.json) | polygon:short-interest-volume (F), polygon:options-contracts (F), fmp:shares-float-all (F), polygon:eod-per-ticker (H), polygon:grouped-crypto-fx (H), fmp:news-feeds (E), fmp:movers-snapshots (E), fmp:institutional-13f (E) |
| `justhodl-polygon-bank` | — | hard cap on names | polygon:minute-bars (H), polygon:grouped-crypto-fx (H), fmp:movers-snapshots (E), polygon:options-contracts (F), polygon:market-plumbing (E), cmc:crypto-market (F), polygon:short-interest-volume (F), fmp:shares-float-all (F) |
| `justhodl-sec-filings-intel` | sec | hard cap on names | fmp:sec-filings-feeds (E), fmp:insider-feeds (E), fmp:governance-execs (F), fmp:ma-and-capital-feeds (E), fmp:institutional-13f (E), polygon:eod-per-ticker (H), fmp:eod-bulk (H), fmp:statements-bulk (H) |
| `justhodl-stock-buying` | — | S&P 500 only; hard cap on names; Finviz universe | fmp:statements-bulk (H), fmp:profile-bulk (U), fmp:key-metrics-ttm-bulk (F), fmp:ratios-ttm-bulk (F), fmp:calendars (E), fmp:earnings-surprises-bulk (F), polygon:eod-per-ticker (H), fmp:institutional-13f (E) |
| `justhodl-apex-fusion` | — | hard cap on names | polygon:short-interest-volume (F), fmp:shares-float-all (F), fmp:movers-snapshots (E), polygon:options-contracts (F), polygon:eod-per-ticker (H), fmp:insider-feeds (E), fmp:estimates-targets-grades (F), fmp:profile-bulk (U) |
| `justhodl-asset-compass` | coingecko fmp fred polygon | hard cap on names | polygon:etf-global (F), fmp:statements-bulk (H), polygon:options-contracts (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H), te:macro-markets (F), fmp:scores-bulk (F), fmp:macro-rates (F) |
| `justhodl-bond-desk` | fmp fred | hard cap on names | fmp:macro-rates (F), fmp:eod-bulk (H), polygon:grouped-crypto-fx (H), polygon:eod-per-ticker (H), cq:onchain-hourly (H), cmc:crypto-market (F), te:macro-markets (F), polygon:options-contracts (F) |
| `justhodl-bottleneck-boom` | fmp fred nyfed sec | hard cap on names; unified universe (data/universe.json) | fmp:statements-bulk (H), fmp:revenue-segmentation (F), fmp:market-cap-history (H), fmp:peers-bulk (F), fmp:profile-bulk (U), fmp:ratios-ttm-bulk (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-bottleneck-research` | — | hard cap on names | fmp:revenue-segmentation (F), fmp:ratios-ttm-bulk (F), fmp:sector-industry-pe-perf (F), fmp:peers-bulk (F), polygon:short-interest-volume (F), fmp:statements-bulk (H), fmp:shares-float-all (F), fmp:estimates-targets-grades (F) |
| `justhodl-buyback-scanner` | sec | hard cap on names | fmp:sec-filings-feeds (E), fmp:statements-bulk (H), fmp:insider-feeds (E), polygon:minute-bars (H), fmp:institutional-13f (E), fmp:estimates-targets-grades (F), polygon:eod-per-ticker (H), fmp:scores-bulk (F) |
| `justhodl-capital-flow` | — | hard cap on names | polygon:etf-global (F), fmp:institutional-13f (E), fmp:profile-bulk (U), fmp:key-metrics-ttm-bulk (F), fmp:ratios-ttm-bulk (F), polygon:options-contracts (F), fmp:etf-holdings-weightings (F), fmp:etf-holder-bulk (F) |
| `justhodl-carry-surface` | fmp fred | hard cap on names | polygon:grouped-crypto-fx (H), fmp:key-metrics-ttm-bulk (F), fmp:dividends-splits (H), fmp:macro-rates (F), polygon:ticker-events-ipos (E), polygon:eod-per-ticker (H), fmp:ma-and-capital-feeds (E), fmp:eod-bulk (H) |
| `justhodl-cds-monitor` | fmp fred | hard cap on names | fmp:company-notes (F), te:macro-markets (F), fmp:estimates-targets-grades (F), fmp:scores-bulk (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H), fmp:statements-bulk (H), fmp:institutional-13f (E) |
| `justhodl-crypto-confluence` | — | hard cap on names | polygon:grouped-crypto-fx (H), fmp:sector-industry-pe-perf (F), cq:onchain-hourly (H), cmc:crypto-market (F), polygon:options-contracts (F), fmp:shares-float-all (F), polygon:short-interest-volume (F), polygon:eod-per-ticker (H) |
| `justhodl-fmp-bank` | — | hard cap on names | fmp:etf-holder-bulk (F), fmp:dcf-bulk (F), fmp:statements-bulk (H), fmp:price-target-summary-bulk (F), fmp:estimates-targets-grades (F), fmp:key-metrics-ttm-bulk (F), fmp:sec-filings-feeds (E), fmp:ratios-ttm-bulk (F) |
| `justhodl-fundamentals-engine` | fmp | hard cap on names | fmp:estimates-targets-grades (F), fmp:upgrades-downgrades-consensus-bulk (F), fmp:scores-bulk (F), fmp:revenue-segmentation (F), fmp:price-target-summary-bulk (F), fmp:dcf-bulk (F), fmp:market-cap-history (H), fmp:institutional-13f (E) |
| `justhodl-gf-value` | fmp | S&P 500 only; hard cap on names | fmp:statements-bulk (H), fmp:scores-bulk (F), fmp:dcf-bulk (F), fmp:estimates-targets-grades (F), fmp:ratios-ttm-bulk (F), fmp:key-metrics-ttm-bulk (F), polygon:etf-global (F), fmp:institutional-13f (E) |
| `justhodl-insider-buyback-confluence` | fmp | hard cap on names | fmp:statements-bulk (H), fmp:market-cap-history (H), fmp:insider-feeds (E), fmp:profile-bulk (U), polygon:short-interest-volume (F), fmp:shares-float-all (F), fmp:key-metrics-ttm-bulk (F), eia:full-catalog (H) |
| `justhodl-nobrainer-rationale` | — | hard cap on names; curated / watchlist universe | fmp:calendars (E), cmc:crypto-market (F), fmp:estimates-targets-grades (F), polygon:minute-bars (H), fmp:upgrades-downgrades-consensus-bulk (F), fmp:price-target-summary-bulk (F), fmp:key-metrics-ttm-bulk (F), fmp:intraday-1min (H) |
| `justhodl-opportunities-research` | — | — | fmp:key-metrics-ttm-bulk (F), fmp:ratios-ttm-bulk (F), fmp:profile-bulk (U), fmp:peers-bulk (F), fmp:news-feeds (E), polygon:short-interest-volume (F), fmp:shares-float-all (F), fmp:scores-bulk (F) |
| `justhodl-plumbing-aggregator` | ecb fred nyfed | hard cap on names | fmp:macro-rates (F), fmp:statements-bulk (H), fmp:movers-snapshots (E), fmp:fail-to-deliver (H), polygon:minute-bars (H), fmp:shares-float-all (F), fmp:intraday-1min (H), fmp:market-cap-history (H) |
| `justhodl-precatalyst-vol-expansion` | fmp | hard cap on names | fmp:calendars (E), polygon:options-contracts (F), polygon:eod-per-ticker (H), fmp:earnings-surprises-bulk (F), fmp:shares-float-all (F), fmp:eod-bulk (H), polygon:short-interest-volume (F), fmp:macro-rates (F) |
| `justhodl-predictability` | — | hard cap on names | fmp:statements-bulk (H), polygon:eod-per-ticker (H), fmp:eod-bulk (H), fmp:news-feeds (E), fmp:key-metrics-ttm-bulk (F), fmp:rating-bulk (F), fmp:estimates-targets-grades (F), fmp:scores-bulk (F) |
| `justhodl-pump-mechanics` | fmp | hard cap on names | polygon:short-interest-volume (F), polygon:options-contracts (F), fmp:shares-float-all (F), fmp:movers-snapshots (E), fmp:sector-industry-pe-perf (F), fmp:market-cap-history (H), fmp:institutional-13f (E), fmp:sec-filings-feeds (E) |
| `justhodl-rebalance-radar` | fmp | hard cap on names | polygon:etf-global (F), fmp:etf-holder-bulk (F), polygon:grouped-crypto-fx (H), fmp:sector-industry-pe-perf (F), cq:onchain-hourly (H), cmc:crypto-market (F), fmp:institutional-13f (E), polygon:eod-per-ticker (H) |
| `justhodl-rotation-dashboard` | polygon | hard cap on names | fmp:sector-industry-pe-perf (F), polygon:etf-global (F), polygon:grouped-crypto-fx (H), polygon:eod-per-ticker (H), fmp:statements-bulk (H), te:macro-markets (F), fmp:etf-holdings-weightings (F), fmp:etf-holder-bulk (F) |
| `justhodl-sec-filing-diff` | sec | hard cap on names | fmp:sec-filings-feeds (E), fmp:institutional-13f (E), cmc:crypto-market (F), fmp:transcripts (E), fmp:revenue-segmentation (F), fmp:peers-bulk (F), polygon:grouped-crypto-fx (H), fmp:market-cap-history (H) |
| `justhodl-starmine` | — | S&P 500 only; hard cap on names | fmp:upgrades-downgrades-consensus-bulk (F), fmp:price-target-summary-bulk (F), fmp:estimates-targets-grades (F), fmp:earnings-surprises-bulk (F), fmp:rating-bulk (F), fmp:market-cap-history (H), fmp:profile-bulk (U), polygon:eod-per-ticker (H) |
| `justhodl-system-signal-logger` | fmp | — | polygon:eod-per-ticker (H), fmp:eod-bulk (H), fmp:scores-bulk (F), fmp:key-metrics-ttm-bulk (F), fmp:insider-feeds (E), fmp:statements-bulk (H), fmp:movers-snapshots (E), fmp:institutional-13f (E) |
| `justhodl-theme-cascade` | — | hard cap on names | fmp:movers-snapshots (E), fmp:etf-holdings-weightings (F), fmp:etf-holder-bulk (F), polygon:short-interest-volume (F), fmp:shares-float-all (F), polygon:etf-global (F), polygon:eod-per-ticker (H), fmp:sector-industry-pe-perf (F) |
| `justhodl-theme-detector` | — | hard cap on names; curated / watchlist universe | polygon:eod-per-ticker (H), fmp:profile-bulk (U), fmp:etf-holdings-weightings (F), eia:full-catalog (H), polygon:grouped-crypto-fx (H), fmp:sector-industry-pe-perf (F), cq:onchain-hourly (H), cmc:crypto-market (F) |
| `justhodl-theme-rotation-engine` | fmp | hard cap on names; curated / watchlist universe | fmp:sector-industry-pe-perf (F), fmp:movers-snapshots (E), fmp:institutional-13f (E), polygon:eod-per-ticker (H), polygon:short-interest-volume (F), fmp:shares-float-all (F), fmp:etf-holdings-weightings (F), fmp:eod-bulk (H) |
| `justhodl-wave-signal-logger` | fmp polygon | hard cap on names | fmp:eod-bulk (H), polygon:etf-global (F), fmp:macro-rates (F), fmp:earnings-surprises-bulk (F), polygon:short-interest-volume (F), fmp:shares-float-all (F), polygon:eod-per-ticker (H), fmp:sector-industry-pe-perf (F) |
| `justhodl-52wk-quality-breakout` | fmp | hard cap on names; curated / watchlist universe | polygon:eod-per-ticker (H), fmp:earnings-surprises-bulk (F), fmp:eod-bulk (H), fmp:insider-feeds (E), fmp:sector-industry-pe-perf (F), fmp:movers-snapshots (E), polygon:minute-bars (H), fmp:intraday-1min (H) |
| `justhodl-alert-router` | — | hard cap on names | polygon:etf-global (F), polygon:short-interest-volume (F), fmp:shares-float-all (F), fmp:sector-industry-pe-perf (F), fmp:macro-rates (F), fmp:earnings-surprises-bulk (F), fmp:cot (H), polygon:eod-per-ticker (H) |
| `justhodl-best-setups` | — | hard cap on names; curated / watchlist universe; unified universe (data/universe.json); Finviz universe | fmp:insider-feeds (E), fmp:institutional-13f (E), fmp:market-cap-history (H), polygon:options-contracts (F), polygon:eod-per-ticker (H), fmp:price-target-summary-bulk (F), fmp:governance-execs (F), fmp:estimates-targets-grades (F) |
| `justhodl-bond-warroom` | ecb tradingview treasury | hard cap on names | te:macro-markets (F), fmp:macro-rates (F), polygon:short-interest-volume (F), fmp:shares-float-all (F), fmp:movers-snapshots (E), polygon:eod-per-ticker (H), fmp:profile-bulk (U), fmp:eod-bulk (H) |
| `justhodl-bottom` | polygon | hard cap on names; Finviz universe | polygon:grouped-crypto-fx (H), fmp:social-sentiment (F), cq:onchain-hourly (H), cmc:crypto-market (F), polygon:etf-global (F), polygon:eod-per-ticker (H), fmp:etf-holdings-weightings (F), eia:full-catalog (H) |
| `justhodl-capital-return` | — | hard cap on names | fmp:statements-bulk (H), fmp:ratios-ttm-bulk (F), fmp:key-metrics-ttm-bulk (F), fmp:scores-bulk (F), fmp:dividends-splits (H), fmp:profile-bulk (U), fmp:dcf-bulk (F), fmp:price-target-summary-bulk (F) |
| `justhodl-convergence-radar` | — | hard cap on names; curated / watchlist universe | fmp:movers-snapshots (E), polygon:short-interest-volume (F), fmp:shares-float-all (F), polygon:eod-per-ticker (H), fmp:social-sentiment (F), fmp:news-feeds (E), fmp:insider-feeds (E), fmp:earnings-surprises-bulk (F) |
| `justhodl-cross-asset-regime` | fmp | hard cap on names | polygon:grouped-crypto-fx (H), eia:full-catalog (H), cq:onchain-hourly (H), cmc:crypto-market (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H), polygon:options-contracts (F), fmp:institutional-13f (E) |
| `justhodl-crypto-cot` | — | hard cap on names | fmp:cot (H), polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F), polygon:options-contracts (F), fmp:institutional-13f (E), fmp:social-sentiment (F), fmp:dividends-splits (H) |
| `justhodl-debate-engine` | — | hard cap on names | fmp:key-metrics-ttm-bulk (F), fmp:dcf-bulk (F), fmp:price-target-summary-bulk (F), polygon:eod-per-ticker (H), fmp:statements-bulk (H), fmp:estimates-targets-grades (F), fmp:earnings-surprises-bulk (F), fmp:profile-bulk (U) |
| `justhodl-deep-value-screener` | fmp | S&P 500 only; hard cap on names; unified universe (data/universe.json) | fmp:statements-bulk (H), fmp:scores-bulk (F), fmp:profile-bulk (U), fmp:key-metrics-ttm-bulk (F), fmp:ratios-ttm-bulk (F), fmp:market-cap-history (H), polygon:short-interest-volume (F), fmp:shares-float-all (F) |
| `justhodl-divcut-warning` | fmp | hard cap on names | fmp:statements-bulk (H), fmp:ratios-ttm-bulk (F), fmp:dividends-splits (H), fmp:estimates-targets-grades (F), fmp:earnings-surprises-bulk (F), fmp:price-target-summary-bulk (F), fmp:movers-snapshots (E), fmp:etf-holder-bulk (F) |
| `justhodl-divergence-engine-v2` | fmp fred | — | polygon:grouped-crypto-fx (H), fmp:macro-rates (F), cq:onchain-hourly (H), cmc:crypto-market (F), eia:full-catalog (H), te:macro-markets (F), fmp:statements-bulk (H), fmp:employees (F) |
| `justhodl-early-movers` | — | hard cap on names | fmp:movers-snapshots (E), polygon:short-interest-volume (F), fmp:shares-float-all (F), polygon:eod-per-ticker (H), fmp:scores-bulk (F), fmp:estimates-targets-grades (F), fmp:price-target-summary-bulk (F), fmp:earnings-surprises-bulk (F) |
| `justhodl-earnings-pead` | fmp | hard cap on names; unified universe (data/universe.json) | fmp:earnings-surprises-bulk (F), fmp:price-target-summary-bulk (F), fmp:estimates-targets-grades (F), fmp:profile-bulk (U), polygon:short-interest-volume (F), fmp:shares-float-all (F), fmp:market-cap-history (H), fmp:institutional-13f (E) |
| `justhodl-etf-true-flows` | sec | S&P 500 only; hard cap on names | polygon:etf-global (F), fmp:etf-holdings-weightings (F), fmp:etf-holder-bulk (F), fmp:funds-disclosure (E), fmp:ratios-ttm-bulk (F), fmp:profile-bulk (U), fmp:key-metrics-ttm-bulk (F), fmp:sec-filings-feeds (E) |
| `justhodl-failure-library` | — | hard cap on names; curated / watchlist universe | fmp:statements-bulk (H), polygon:short-interest-volume (F), fmp:shares-float-all (F), fmp:scores-bulk (F), fmp:insider-feeds (E), fmp:dividends-splits (H), fmp:market-cap-history (H), eia:full-catalog (H) |
| `justhodl-industry-rotation` | fmp polygon | hard cap on names; curated / watchlist universe; Finviz universe | fmp:sector-industry-pe-perf (F), fmp:institutional-13f (E), fmp:etf-holder-bulk (F), polygon:etf-global (F), polygon:eod-per-ticker (H), fmp:ratios-ttm-bulk (F), fmp:profile-bulk (U), fmp:etf-holdings-weightings (F) |
| `justhodl-insider-industry-cluster` | — | hard cap on names; unified universe (data/universe.json) | fmp:peers-bulk (F), fmp:insider-feeds (E), fmp:governance-execs (F), fmp:market-cap-history (H), polygon:options-contracts (F), fmp:sec-filings-feeds (E), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-master-allocator` | yahoo | hard cap on names | fmp:macro-rates (F), fmp:institutional-13f (E), te:macro-markets (F), polygon:grouped-crypto-fx (H), fmp:statements-bulk (H), cq:onchain-hourly (H), cmc:crypto-market (F), eia:full-catalog (H) |
| `justhodl-momentum-breakout` | fmp | hard cap on names; unified universe (data/universe.json) | polygon:eod-per-ticker (H), fmp:movers-snapshots (E), fmp:eod-bulk (H), fmp:shares-float-all (F), polygon:short-interest-volume (F), fmp:earnings-surprises-bulk (F), fmp:sector-industry-pe-perf (F), fmp:institutional-13f (E) |
| `justhodl-peer-comparison` | — | hard cap on names | fmp:statements-bulk (H), fmp:ratios-ttm-bulk (F), fmp:peers-bulk (F), fmp:key-metrics-ttm-bulk (F), fmp:estimates-targets-grades (F), fmp:news-feeds (E), fmp:market-cap-history (H), fmp:institutional-13f (E) |
| `justhodl-rating-change-cluster` | fmp | hard cap on names; curated / watchlist universe | fmp:upgrades-downgrades-consensus-bulk (F), fmp:rating-bulk (F), fmp:price-target-summary-bulk (F), fmp:insider-feeds (E), fmp:estimates-targets-grades (F), fmp:movers-snapshots (E), fmp:earnings-surprises-bulk (F), fmp:institutional-13f (E) |
| `justhodl-reit-nav-discount` | fmp | hard cap on names; curated / watchlist universe | polygon:eod-per-ticker (H), fmp:key-metrics-ttm-bulk (F), fmp:scores-bulk (F), fmp:dividends-splits (H), fmp:statements-bulk (H), fmp:institutional-13f (E), fmp:etf-holdings-weightings (F), fmp:ma-and-capital-feeds (E) |
| `justhodl-russell-recon-frontrun` | fmp | hard cap on names; FMP screener ladder (search endpoint, not banked); Finviz universe | fmp:etf-holder-bulk (F), polygon:eod-per-ticker (H), fmp:statements-bulk (H), fmp:scores-bulk (F), fmp:market-cap-history (H), polygon:minute-bars (H), fmp:institutional-13f (E), fmp:profile-bulk (U) |
| `justhodl-sector-earnings-diffusion` | fmp | hard cap on names; unified universe (data/universe.json) | fmp:sector-industry-pe-perf (F), fmp:price-target-summary-bulk (F), fmp:estimates-targets-grades (F), fmp:movers-snapshots (E), fmp:earnings-surprises-bulk (F), fmp:upgrades-downgrades-consensus-bulk (F), fmp:institutional-13f (E), eia:full-catalog (H) |
| `justhodl-short-book` | — | hard cap on names; unified universe (data/universe.json) | polygon:eod-per-ticker (H), polygon:etf-global (F), fmp:statements-bulk (H), fmp:scores-bulk (F), polygon:short-interest-volume (F), fmp:fail-to-deliver (H), fmp:shares-float-all (F), fmp:profile-bulk (U) |
| `justhodl-symdir` | bls ecb fred nyfed polygon treasury | S&P 500 only; hard cap on names; curated / watchlist universe; Finviz universe | fmp:profile-bulk (U), te:macro-markets (F), polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F), polygon:eod-per-ticker (H), fmp:macro-rates (F), fmp:eod-bulk (H) |
| `justhodl-13f-price-divergence` | fmp | hard cap on names | fmp:institutional-13f (E), polygon:eod-per-ticker (H), fmp:eod-bulk (H), fmp:sec-filings-feeds (E), fmp:funds-disclosure (E), fmp:etf-holdings-weightings (F), fmp:market-cap-history (H) |
| `justhodl-accum-composite` | — | hard cap on names | polygon:short-interest-volume (F), fmp:institutional-13f (E), fmp:insider-feeds (E), fmp:shares-float-all (F), polygon:minute-bars (H), fmp:intraday-1min (H), fmp:peers-bulk (F) |
| `justhodl-ai-rerating-radar` | fmp | hard cap on names; FMP screener ladder (search endpoint, not banked) | fmp:peers-bulk (F), fmp:institutional-13f (E), fmp:statements-bulk (H), fmp:ratios-ttm-bulk (F), fmp:profile-bulk (U), fmp:key-metrics-ttm-bulk (F), fmp:scores-bulk (F) |
| `justhodl-alpha-daily-brief` | — | hard cap on names; curated / watchlist universe | fmp:estimates-targets-grades (F), fmp:news-feeds (E), fmp:institutional-13f (E), te:macro-markets (F), fmp:funds-disclosure (E), polygon:grouped-crypto-fx (H), fmp:price-target-summary-bulk (F) |
| `justhodl-beneish` | — | S&P 500 only; hard cap on names | fmp:statements-bulk (H), fmp:scores-bulk (F), fmp:company-notes (F), fmp:upgrades-downgrades-consensus-bulk (F), fmp:rating-bulk (F), fmp:price-target-summary-bulk (F), fmp:estimates-targets-grades (F) |
| `justhodl-boom-stage` | yahoo | hard cap on names | te:macro-markets (F), eia:full-catalog (H), fmp:statements-bulk (H), fmp:revenue-segmentation (F), fmp:profile-bulk (U), fmp:shares-float-all (F), fmp:peers-bulk (F) |
| `justhodl-buzz-velocity` | fmp newsapi | hard cap on names; FMP screener ladder (search endpoint, not banked) | fmp:social-sentiment (F), fmp:movers-snapshots (E), polygon:short-interest-volume (F), fmp:shares-float-all (F), fmp:news-feeds (E), polygon:options-contracts (F), fmp:institutional-13f (E) |
| `justhodl-catalyst-clusters` | — | hard cap on names | fmp:calendars (E), fmp:estimates-targets-grades (F), fmp:earnings-surprises-bulk (F), te:macro-markets (F), fmp:institutional-13f (E), eia:full-catalog (H), fmp:movers-snapshots (E) |
| `justhodl-chart-patterns` | — | S&P 500 only; hard cap on names | polygon:eod-per-ticker (H), fmp:ratios-ttm-bulk (F), fmp:profile-bulk (U), fmp:key-metrics-ttm-bulk (F), fmp:eod-bulk (H), polygon:etf-global (F), fmp:etf-holdings-weightings (F) |
| `justhodl-chokepoint` | fmp | S&P 500 only; hard cap on names; curated / watchlist universe | fmp:revenue-segmentation (F), fmp:peers-bulk (F), fmp:key-metrics-ttm-bulk (F), fmp:etf-holdings-weightings (F), eia:full-catalog (H), fmp:statements-bulk (H), fmp:profile-bulk (U) |
| `justhodl-comeback-screener` | — | hard cap on names; Finviz universe | fmp:statements-bulk (H), fmp:ratios-ttm-bulk (F), fmp:profile-bulk (U), fmp:key-metrics-ttm-bulk (F), fmp:institutional-13f (E), polygon:etf-global (F), polygon:eod-per-ticker (H) |
| `justhodl-crisis-composite` | — | hard cap on names | te:macro-markets (F), fmp:macro-rates (F), polygon:market-plumbing (E), fmp:shares-float-all (F), fmp:company-notes (F), polygon:short-interest-volume (F), fmp:scores-bulk (F) |
| `justhodl-crypto-etf-arb` | cmc fmp | hard cap on names | polygon:etf-global (F), fmp:etf-holdings-weightings (F), polygon:minute-bars (H), fmp:intraday-1min (H), polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F) |
| `justhodl-crypto-etf-flows` | — | hard cap on names | polygon:etf-global (F), fmp:funds-disclosure (E), fmp:etf-holdings-weightings (F), polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F), fmp:dividends-splits (H) |
| `justhodl-crypto-gex` | — | hard cap on names | polygon:grouped-crypto-fx (H), polygon:options-contracts (F), cq:onchain-hourly (H), cmc:crypto-market (F), polygon:minute-bars (H), fmp:intraday-1min (H), fmp:dividends-splits (H) |
| `justhodl-dislocation-detector` | polygon | hard cap on names; unified universe (data/universe.json) | fmp:scores-bulk (F), fmp:ratios-ttm-bulk (F), fmp:peers-bulk (F), fmp:key-metrics-ttm-bulk (F), fmp:statements-bulk (H), fmp:profile-bulk (U), fmp:insider-feeds (E) |
| `justhodl-dividend-growth` | — | — | fmp:scores-bulk (F), fmp:ratios-ttm-bulk (F), fmp:dividends-splits (H), fmp:statements-bulk (H), fmp:key-metrics-ttm-bulk (F), fmp:profile-bulk (U), fmp:news-feeds (E) |
| `justhodl-domain-barometers` | — | hard cap on names | te:macro-markets (F), polygon:market-plumbing (E), fmp:sector-industry-pe-perf (F), polygon:grouped-crypto-fx (H), polygon:eod-per-ticker (H), fmp:shares-float-all (F), fmp:eod-bulk (H) |
| `justhodl-earnings-iv-crush` | fmp | hard cap on names | fmp:earnings-surprises-bulk (F), polygon:options-contracts (F), fmp:calendars (E), polygon:eod-per-ticker (H), fmp:eod-bulk (H), fmp:etf-holder-bulk (F), polygon:minute-bars (H) |
| `justhodl-earnings-whisper` | — | hard cap on names | fmp:governance-execs (F), fmp:estimates-targets-grades (F), fmp:earnings-surprises-bulk (F), polygon:options-contracts (F), fmp:price-target-summary-bulk (F), fmp:movers-snapshots (E), fmp:insider-feeds (E) |
| `justhodl-edgar-authority` | — | hard cap on names; Finviz universe | fmp:scores-bulk (F), fmp:sec-filings-feeds (E), fmp:key-metrics-ttm-bulk (F), fmp:statements-bulk (H), fmp:estimates-targets-grades (F), fmp:market-cap-history (H), fmp:dcf-bulk (F) |
| `justhodl-fi-census` | fred | hard cap on names | fmp:profile-bulk (U), polygon:eod-per-ticker (H), fmp:company-notes (F), te:macro-markets (F), fmp:macro-rates (F), fmp:eod-bulk (H), fmp:scores-bulk (F) |
| `justhodl-floor-audit` | polygon sec | hard cap on names; curated / watchlist universe | fmp:statements-bulk (H), polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F), fmp:news-feeds (E), fmp:sec-filings-feeds (E), fmp:market-cap-history (H) |
| `justhodl-flow-confluence` | — | hard cap on names | polygon:short-interest-volume (F), polygon:options-contracts (F), fmp:shares-float-all (F), fmp:institutional-13f (E), polygon:etf-global (F), fmp:sector-industry-pe-perf (F), fmp:insider-feeds (E) |
| `justhodl-gap-fill-confirm` | fmp | hard cap on names | polygon:minute-bars (H), polygon:eod-per-ticker (H), fmp:intraday-1min (H), fmp:earnings-surprises-bulk (F), fmp:sector-industry-pe-perf (F), fmp:news-feeds (E), fmp:eod-bulk (H) |
| `justhodl-leading-markets` | fmp | hard cap on names | te:macro-markets (F), fmp:macro-rates (F), polygon:eod-per-ticker (H), fmp:sector-industry-pe-perf (F), fmp:peers-bulk (F), fmp:revenue-segmentation (F), fmp:eod-bulk (H) |
| `justhodl-ma-target-predictor` | — | hard cap on names; curated / watchlist universe | fmp:ma-and-capital-feeds (E), fmp:institutional-13f (E), fmp:key-metrics-ttm-bulk (F), fmp:market-cap-history (H), fmp:insider-feeds (E), fmp:statements-bulk (H), fmp:sec-filings-feeds (E) |
| `justhodl-market-tape` | fmp fred | — | polygon:minute-bars (H), polygon:grouped-crypto-fx (H), polygon:market-plumbing (E), cq:onchain-hourly (H), cmc:crypto-market (F), fmp:intraday-1min (H), te:macro-markets (F) |
| `justhodl-ndx-spx-spread` | fmp | hard cap on names | polygon:eod-per-ticker (H), fmp:market-cap-history (H), fmp:etf-holder-bulk (F), fmp:sector-industry-pe-perf (F), fmp:profile-bulk (U), fmp:news-feeds (E), fmp:eod-bulk (H) |
| `justhodl-pair-trades` | fmp | hard cap on names | polygon:short-interest-volume (F), fmp:shares-float-all (F), fmp:peers-bulk (F), fmp:movers-snapshots (E), polygon:eod-per-ticker (H), fmp:eod-bulk (H), fmp:institutional-13f (E) |
| `justhodl-pairs-arb` | — | hard cap on names | fmp:peers-bulk (F), polygon:eod-per-ticker (H), fmp:ma-and-capital-feeds (E), fmp:key-metrics-ttm-bulk (F), fmp:eod-bulk (H), fmp:ratios-ttm-bulk (F), fmp:profile-bulk (U) |
| `justhodl-political-stocks` | sec | hard cap on names; curated / watchlist universe | fmp:political-trades (E), fmp:insider-feeds (E), fmp:sec-filings-feeds (E), fmp:sector-industry-pe-perf (F), fmp:news-feeds (E), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-post-earnings-mean-rev` | fmp | hard cap on names | polygon:eod-per-ticker (H), fmp:earnings-surprises-bulk (F), fmp:calendars (E), polygon:options-contracts (F), fmp:eod-bulk (H), fmp:sector-industry-pe-perf (F), eia:full-catalog (H) |
| `justhodl-provider-bank` | — | hard cap on names | te:macro-markets (F), fmp:news-feeds (E), cq:onchain-hourly (H), fmp:sector-industry-pe-perf (F), fmp:revenue-segmentation (F), cmc:crypto-market (F), eia:full-catalog (H) |
| `justhodl-rv-iv-scanner` | fmp fred | hard cap on names | polygon:eod-per-ticker (H), polygon:options-contracts (F), fmp:institutional-13f (E), fmp:governance-execs (F), fmp:macro-rates (F), fmp:eod-bulk (H), fmp:estimates-targets-grades (F) |
| `justhodl-screen-builder` | — | hard cap on names | fmp:scores-bulk (F), fmp:ratios-ttm-bulk (F), fmp:profile-bulk (U), fmp:key-metrics-ttm-bulk (F), fmp:statements-bulk (H), fmp:institutional-13f (E), fmp:rating-bulk (F) |
| `justhodl-sector-heatmap` | fmp | — | fmp:sector-industry-pe-perf (F), fmp:profile-bulk (U), fmp:market-cap-history (H), polygon:minute-bars (H), fmp:intraday-1min (H), fmp:ratios-ttm-bulk (F), fmp:key-metrics-ttm-bulk (F) |
| `justhodl-smart-money-13f` | sec | hard cap on names | fmp:sec-filings-feeds (E), fmp:institutional-13f (E), fmp:revenue-segmentation (F), polygon:options-contracts (F), eia:full-catalog (H), fmp:peers-bulk (F), fmp:market-cap-history (H) |
| `justhodl-smart-wake` | — | hard cap on names; curated / watchlist universe | te:macro-markets (F), polygon:short-interest-volume (F), polygon:options-contracts (F), polygon:minute-bars (H), fmp:shares-float-all (F), fmp:macro-rates (F), fmp:intraday-1min (H) |
| `justhodl-spac-floor-warrant` | fmp | hard cap on names; FMP screener ladder (search endpoint, not banked) | fmp:ma-and-capital-feeds (E), polygon:ticker-events-ipos (E), fmp:shares-float-all (F), fmp:key-metrics-ttm-bulk (F), fmp:ratios-ttm-bulk (F), fmp:profile-bulk (U), fmp:calendars (E) |
| `justhodl-spinoff-desk` | sec | hard cap on names | fmp:sec-filings-feeds (E), fmp:price-target-summary-bulk (F), fmp:ma-and-capital-feeds (E), fmp:estimates-targets-grades (F), fmp:upgrades-downgrades-consensus-bulk (F), fmp:transcripts (E), fmp:dcf-bulk (F) |
| `justhodl-theme-tier-classifier` | — | hard cap on names | fmp:profile-bulk (U), fmp:key-metrics-ttm-bulk (F), fmp:ratios-ttm-bulk (F), fmp:etf-holdings-weightings (F), polygon:etf-global (F), fmp:market-cap-history (H), fmp:dcf-bulk (F) |
| `justhodl-tv-bars` | polygon tradingview yahoo | S&P 500 only; hard cap on names | cmc:crypto-market (F), te:macro-markets (F), polygon:minute-bars (H), polygon:eod-per-ticker (H), fmp:eod-bulk (H), polygon:market-plumbing (E), polygon:grouped-crypto-fx (H) |
| `justhodl-whats-changed` | — | hard cap on names | polygon:short-interest-volume (F), fmp:shares-float-all (F), fmp:earnings-surprises-bulk (F), fmp:eod-bulk (H), fmp:macro-rates (F), polygon:eod-per-ticker (H), fmp:institutional-13f (E) |
| `benzinga-news-agent` | — | hard cap on names | fmp:price-target-summary-bulk (F), fmp:news-feeds (E), fmp:calendars (E), fmp:upgrades-downgrades-consensus-bulk (F), fmp:estimates-targets-grades (F), fmp:dividends-splits (H) |
| `justhodl-alpha-calibrator` | polygon | hard cap on names | polygon:options-contracts (F), fmp:institutional-13f (E), polygon:eod-per-ticker (H), fmp:eod-bulk (H), fmp:estimates-targets-grades (F), fmp:key-metrics-ttm-bulk (F) |
| `justhodl-auction-crisis-detector` | treasury | hard cap on names | polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F), polygon:options-contracts (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-auction-desk` | treasury | hard cap on names | fmp:statements-bulk (H), polygon:options-contracts (F), fmp:estimates-targets-grades (F), fmp:institutional-13f (E), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-bagger-engine` | fmp | hard cap on names; unified universe (data/universe.json) | fmp:key-metrics-ttm-bulk (F), fmp:statements-bulk (H), fmp:dcf-bulk (F), fmp:profile-bulk (U), fmp:eod-bulk (H), fmp:movers-snapshots (E) |
| `justhodl-beta-laggard` | — | hard cap on names | polygon:eod-per-ticker (H), fmp:sector-industry-pe-perf (F), fmp:scores-bulk (F), fmp:profile-bulk (U), fmp:key-metrics-ttm-bulk (F), fmp:ratios-ttm-bulk (F) |
| `justhodl-boj-detail` | fred | hard cap on names | te:macro-markets (F), polygon:grouped-crypto-fx (H), fmp:macro-rates (F), fmp:statements-bulk (H), cq:onchain-hourly (H), cmc:crypto-market (F) |
| `justhodl-breadth-divergence` | fmp | hard cap on names | polygon:eod-per-ticker (H), fmp:sector-industry-pe-perf (F), polygon:market-plumbing (E), fmp:market-cap-history (H), fmp:profile-bulk (U), fmp:eod-bulk (H) |
| `justhodl-calibration-fleet` | — | hard cap on names | fmp:dividends-splits (H), fmp:institutional-13f (E), fmp:estimates-targets-grades (F), polygon:minute-bars (H), te:macro-markets (F), polygon:options-contracts (F) |
| `justhodl-correlation-breaks` | fred | S&P 500 only | polygon:grouped-crypto-fx (H), fmp:sector-industry-pe-perf (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H), fmp:company-notes (F), eia:full-catalog (H) |
| `justhodl-crisis-knowledge-base` | fred | curated / watchlist universe | polygon:grouped-crypto-fx (H), fmp:macro-rates (F), fmp:eod-bulk (H), cq:onchain-hourly (H), cmc:crypto-market (F), te:macro-markets (F) |
| `justhodl-crypto-cycle-risk` | fred | hard cap on names | polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F), polygon:eod-per-ticker (H), fmp:statements-bulk (H), fmp:market-cap-history (H) |
| `justhodl-crypto-liquidity` | coingecko | hard cap on names | polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F), fmp:market-cap-history (H), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-crypto-miners` | — | hard cap on names | polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F), fmp:statements-bulk (H), fmp:scores-bulk (F), fmp:key-metrics-ttm-bulk (F) |
| `justhodl-crypto-narratives` | cmc | hard cap on names | fmp:sector-industry-pe-perf (F), polygon:grouped-crypto-fx (H), fmp:news-feeds (E), cq:onchain-hourly (H), cmc:crypto-market (F), fmp:market-cap-history (H) |
| `justhodl-crypto-options` | — | hard cap on names | polygon:options-contracts (F), polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-desk-allocator` | — | — | fmp:calendars (E), fmp:ma-and-capital-feeds (E), fmp:etf-holder-bulk (F), polygon:options-contracts (F), fmp:statements-bulk (H), polygon:eod-per-ticker (H) |
| `justhodl-earnings-blackout` | fmp | S&P 500 only; hard cap on names; Finviz universe | fmp:calendars (E), fmp:statements-bulk (H), fmp:market-cap-history (H), polygon:etf-global (F), fmp:institutional-13f (E), fmp:etf-holdings-weightings (F) |
| `justhodl-earnings-confluence` | — | hard cap on names | fmp:scores-bulk (F), fmp:earnings-surprises-bulk (F), fmp:movers-snapshots (E), fmp:statements-bulk (H), fmp:transcripts (E), polygon:options-contracts (F) |
| `justhodl-earnings-sentiment` | — | hard cap on names | fmp:transcripts (E), fmp:ratios-ttm-bulk (F), fmp:profile-bulk (U), fmp:key-metrics-ttm-bulk (F), fmp:earnings-surprises-bulk (F), fmp:calendars (E) |
| `justhodl-ecb-derived` | ecb fred | S&P 500 only; hard cap on names | te:macro-markets (F), fmp:macro-rates (F), eia:full-catalog (H), fmp:statements-bulk (H), fmp:news-feeds (E), fmp:eod-bulk (H) |
| `justhodl-edgar-insiders` | sec | hard cap on names | fmp:sec-filings-feeds (E), fmp:insider-feeds (E), fmp:governance-execs (F), polygon:options-contracts (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-etf-census` | — | hard cap on names; curated / watchlist universe | polygon:etf-global (F), fmp:etf-holdings-weightings (F), fmp:etf-holder-bulk (F), fmp:profile-bulk (U), polygon:eod-per-ticker (H), fmp:statements-bulk (H) |
| `justhodl-eurodollar-plumbing` | ecb fmp fred | hard cap on names | fmp:fail-to-deliver (H), fmp:macro-rates (F), te:macro-markets (F), polygon:short-interest-volume (F), polygon:grouped-crypto-fx (H), fmp:shares-float-all (F) |
| `justhodl-eva-spread` | fred | hard cap on names | fmp:statements-bulk (H), fmp:key-metrics-ttm-bulk (F), fmp:institutional-13f (E), fmp:ratios-ttm-bulk (F), fmp:rating-bulk (F), fmp:estimates-targets-grades (F) |
| `justhodl-fmp-ratios` | — | S&P 500 only; hard cap on names | fmp:statements-bulk (H), fmp:ratios-ttm-bulk (F), fmp:key-metrics-ttm-bulk (F), polygon:etf-global (F), fmp:revenue-segmentation (F), fmp:etf-holdings-weightings (F) |
| `justhodl-fx-intelligence` | fred | hard cap on names | polygon:grouped-crypto-fx (H), fmp:cot (H), fmp:institutional-13f (E), te:macro-markets (F), fmp:macro-rates (F), fmp:news-feeds (E) |
| `justhodl-gdelt-sentiment` | — | S&P 500 only; hard cap on names | fmp:news-feeds (E), polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F), te:macro-markets (F), eia:full-catalog (H) |
| `justhodl-global-macro` | fmp fred | hard cap on names | te:macro-markets (F), fmp:macro-rates (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H), fmp:sector-industry-pe-perf (F), polygon:grouped-crypto-fx (H) |
| `justhodl-hiring-velocity` | fmp | hard cap on names; unified universe (data/universe.json) | fmp:movers-snapshots (E), fmp:profile-bulk (U), fmp:employees (F), fmp:shares-float-all (F), polygon:short-interest-volume (F), fmp:statements-bulk (H) |
| `justhodl-hot-stocks-digest` | — | hard cap on names | fmp:social-sentiment (F), fmp:news-feeds (E), fmp:movers-snapshots (E), fmp:price-target-summary-bulk (F), fmp:institutional-13f (E), fmp:dividends-splits (H) |
| `justhodl-import-canary` | — | hard cap on names | fmp:profile-bulk (U), polygon:eod-per-ticker (H), fmp:revenue-segmentation (F), fmp:eod-bulk (H), fmp:market-cap-history (H), eia:full-catalog (H) |
| `justhodl-index-recon` | — | S&P 500 only; hard cap on names; FMP screener ladder (search endpoint, not banked); Finviz universe | fmp:etf-holder-bulk (F), fmp:profile-bulk (U), polygon:etf-global (F), fmp:market-cap-history (H), fmp:shares-float-all (F), fmp:etf-holdings-weightings (F) |
| `justhodl-investor-lenses` | fred | — | fmp:scores-bulk (F), fmp:key-metrics-ttm-bulk (F), fmp:dcf-bulk (F), eia:full-catalog (H), fmp:statements-bulk (H), fmp:dividends-splits (H) |
| `justhodl-lockup-expiration` | fmp | hard cap on names | fmp:calendars (E), fmp:shares-float-all (F), polygon:ticker-events-ipos (E), polygon:options-contracts (F), fmp:profile-bulk (U), polygon:minute-bars (H) |
| `justhodl-magnitude-distributions` | — | hard cap on names | fmp:institutional-13f (E), polygon:eod-per-ticker (H), fmp:eod-bulk (H), polygon:short-interest-volume (F), fmp:shares-float-all (F), fmp:movers-snapshots (E) |
| `justhodl-margin-lending` | fred | S&P 500 only; hard cap on names | polygon:short-interest-volume (F), fmp:shares-float-all (F), fmp:market-cap-history (H), fmp:news-feeds (E), fmp:statements-bulk (H), fmp:scores-bulk (F) |
| `justhodl-mean-reversion` | fmp | hard cap on names | fmp:dcf-bulk (F), polygon:eod-per-ticker (H), fmp:key-metrics-ttm-bulk (F), fmp:profile-bulk (U), fmp:ratios-ttm-bulk (F), fmp:statements-bulk (H) |
| `justhodl-microcap-float-squeeze` | fmp | hard cap on names; unified universe (data/universe.json) | polygon:short-interest-volume (F), fmp:shares-float-all (F), fmp:profile-bulk (U), fmp:movers-snapshots (E), fmp:statements-bulk (H), fmp:market-cap-history (H) |
| `justhodl-momentum-leaders` | fmp | hard cap on names; curated / watchlist universe | polygon:eod-per-ticker (H), fmp:movers-snapshots (E), fmp:eod-bulk (H), polygon:short-interest-volume (F), fmp:shares-float-all (F), fmp:sector-industry-pe-perf (F) |
| `justhodl-opex-calendar` | fmp polygon | — | polygon:options-contracts (F), polygon:minute-bars (H), fmp:calendars (E), fmp:intraday-1min (H), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-physical-econ` | — | hard cap on names | eia:full-catalog (H), polygon:short-interest-volume (F), fmp:shares-float-all (F), fmp:institutional-13f (E), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-portfolio-analytics` | fmp | hard cap on names | polygon:eod-per-ticker (H), fmp:eod-bulk (H), polygon:short-interest-volume (F), fmp:shares-float-all (F), fmp:movers-snapshots (E), polygon:etf-global (F) |
| `justhodl-prepump-alerts-router` | — | hard cap on names | polygon:short-interest-volume (F), fmp:shares-float-all (F), fmp:movers-snapshots (E), polygon:eod-per-ticker (H), polygon:options-contracts (F), fmp:eod-bulk (H) |
| `justhodl-prepump-summary` | — | hard cap on names | polygon:short-interest-volume (F), fmp:shares-float-all (F), fmp:movers-snapshots (E), fmp:estimates-targets-grades (F), fmp:calendars (E), fmp:governance-execs (F) |
| `justhodl-price-redundancy` | polygon yahoo | Finviz universe | polygon:grouped-crypto-fx (H), cmc:crypto-market (F), cq:onchain-hourly (H), fmp:upgrades-downgrades-consensus-bulk (F), polygon:minute-bars (H), fmp:intraday-1min (H) |
| `justhodl-readthrough` | fmp polygon | hard cap on names; curated / watchlist universe; unified universe (data/universe.json) | fmp:revenue-segmentation (F), fmp:calendars (E), fmp:sector-industry-pe-perf (F), fmp:peers-bulk (F), fmp:statements-bulk (H), fmp:eod-bulk (H) |
| `justhodl-regime-map` | polygon | hard cap on names | polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F), fmp:market-cap-history (H), fmp:institutional-13f (E), fmp:dividends-splits (H) |
| `justhodl-revenue-acceleration` | fmp | hard cap on names; unified universe (data/universe.json) | fmp:statements-bulk (H), fmp:earnings-surprises-bulk (F), polygon:options-contracts (F), fmp:price-target-summary-bulk (F), fmp:estimates-targets-grades (F), fmp:institutional-13f (E) |
| `justhodl-risk-regime` | fred | — | polygon:options-contracts (F), polygon:eod-per-ticker (H), polygon:grouped-crypto-fx (H), fmp:eod-bulk (H), fmp:scores-bulk (F), fmp:company-notes (F) |
| `justhodl-screener-alerts` | — | hard cap on names | fmp:profile-bulk (U), fmp:ratios-ttm-bulk (F), fmp:key-metrics-ttm-bulk (F), fmp:political-trades (E), fmp:scores-bulk (F), fmp:insider-feeds (E) |
| `justhodl-sector-emergence` | polygon | — | fmp:estimates-targets-grades (F), polygon:eod-per-ticker (H), fmp:sector-industry-pe-perf (F), fmp:price-target-summary-bulk (F), fmp:earnings-surprises-bulk (F), polygon:etf-global (F) |
| `justhodl-self-critique` | — | — | polygon:market-plumbing (E), polygon:eod-per-ticker (H), fmp:sector-industry-pe-perf (F), polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F) |
| `justhodl-sequence-alpha-detector` | — | hard cap on names; curated / watchlist universe | fmp:sec-filings-feeds (E), fmp:insider-feeds (E), fmp:earnings-surprises-bulk (F), fmp:institutional-13f (E), fmp:governance-execs (F), fmp:key-metrics-ttm-bulk (F) |
| `justhodl-skew-tail-hedging` | fmp fred yahoo | hard cap on names | polygon:options-contracts (F), fmp:institutional-13f (E), fmp:sector-industry-pe-perf (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H), fmp:macro-rates (F) |
| `justhodl-smart-beta` | — | hard cap on names | fmp:key-metrics-ttm-bulk (F), fmp:sector-industry-pe-perf (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H), fmp:ratios-ttm-bulk (F), fmp:etf-holder-bulk (F) |
| `justhodl-smart-money-cluster` | fmp | hard cap on names | fmp:institutional-13f (E), fmp:etf-holdings-weightings (F), fmp:scores-bulk (F), fmp:key-metrics-ttm-bulk (F), fmp:statements-bulk (H), fmp:sec-filings-feeds (E) |
| `justhodl-smart-money-tracker` | — | hard cap on names; curated / watchlist universe | fmp:institutional-13f (E), fmp:movers-snapshots (E), polygon:market-plumbing (E), fmp:profile-bulk (U), fmp:key-metrics-ttm-bulk (F), fmp:ratios-ttm-bulk (F) |
| `justhodl-sovereign-stress` | ecb fred | S&P 500 only; hard cap on names | te:macro-markets (F), fmp:company-notes (F), fmp:news-feeds (E), fmp:macro-rates (F), fmp:scores-bulk (F), fmp:fail-to-deliver (H) |
| `justhodl-spx-ma` | fmp polygon | S&P 500 only; hard cap on names | polygon:eod-per-ticker (H), fmp:eod-bulk (H), polygon:market-plumbing (E), fmp:sector-industry-pe-perf (F), fmp:profile-bulk (U), polygon:minute-bars (H) |
| `justhodl-stablecoin-flow` | — | hard cap on names | polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F), polygon:minute-bars (H), polygon:eod-per-ticker (H), fmp:institutional-13f (E) |
| `justhodl-stealth-accumulation` | — | hard cap on names | polygon:options-contracts (F), fmp:insider-feeds (E), fmp:institutional-13f (E), polygon:short-interest-volume (F), polygon:minute-bars (H), fmp:sec-filings-feeds (E) |
| `justhodl-stock-xray` | — | hard cap on names; Finviz universe | fmp:institutional-13f (E), fmp:peers-bulk (F), fmp:insider-feeds (E), fmp:estimates-targets-grades (F), fmp:revenue-segmentation (F), fmp:shares-float-all (F) |
| `justhodl-stocktwits` | — | hard cap on names | fmp:social-sentiment (F), fmp:profile-bulk (U), fmp:movers-snapshots (E), fmp:shares-float-all (F), polygon:short-interest-volume (F), fmp:news-feeds (E) |
| `justhodl-stress-simulator` | — | — | fmp:institutional-13f (E), fmp:eod-bulk (H), fmp:estimates-targets-grades (F), polygon:eod-per-ticker (H), polygon:grouped-crypto-fx (H), eia:full-catalog (H) |
| `justhodl-structural-pre-signals` | fmp sec | hard cap on names; FMP screener ladder (search endpoint, not banked) | fmp:sec-filings-feeds (E), fmp:news-feeds (E), eia:full-catalog (H), fmp:transcripts (E), fmp:statements-bulk (H), fmp:employees (F) |
| `justhodl-sympathetic-momentum` | fmp | hard cap on names | polygon:eod-per-ticker (H), fmp:sector-industry-pe-perf (F), fmp:peers-bulk (F), fmp:eod-bulk (H), fmp:market-cap-history (H), fmp:profile-bulk (U) |
| `justhodl-theme-cascade-backtest` | — | hard cap on names | polygon:eod-per-ticker (H), fmp:eod-bulk (H), fmp:movers-snapshots (E), fmp:etf-holdings-weightings (F), fmp:sector-industry-pe-perf (F), fmp:etf-holder-bulk (F) |
| `justhodl-theme-classifier` | fmp | hard cap on names | polygon:eod-per-ticker (H), fmp:movers-snapshots (E), fmp:profile-bulk (U), fmp:ma-and-capital-feeds (E), eia:full-catalog (H), fmp:eod-bulk (H) |
| `justhodl-thesis-engine` | — | hard cap on names; curated / watchlist universe | polygon:grouped-crypto-fx (H), fmp:etf-holdings-weightings (F), eia:full-catalog (H), te:macro-markets (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-universe-discovery` | fmp sec | hard cap on names; FMP screener ladder (search endpoint, not banked) | fmp:profile-bulk (U), fmp:calendars (E), polygon:ticker-events-ipos (E), fmp:ratios-ttm-bulk (F), fmp:key-metrics-ttm-bulk (F), fmp:sec-filings-feeds (E) |
| `justhodl-activist-13d` | fmp sec | hard cap on names; curated / watchlist universe | fmp:sec-filings-feeds (E), fmp:institutional-13f (E), polygon:eod-per-ticker (H), fmp:eod-bulk (H), polygon:minute-bars (H) |
| `justhodl-alert-sentinel` | — | hard cap on names | fmp:sector-industry-pe-perf (F), fmp:insider-feeds (E), fmp:profile-bulk (U), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-alpha-alerts` | — | hard cap on names; curated / watchlist universe | fmp:upgrades-downgrades-consensus-bulk (F), fmp:ratios-ttm-bulk (F), fmp:profile-bulk (U), fmp:key-metrics-ttm-bulk (F), cmc:crypto-market (F) |
| `justhodl-alpha-compass` | fmp | hard cap on names; curated / watchlist universe | polygon:eod-per-ticker (H), fmp:eod-bulk (H), fmp:institutional-13f (E), polygon:grouped-crypto-fx (H), fmp:estimates-targets-grades (F) |
| `justhodl-apac-flows` | fmp | hard cap on names | te:macro-markets (F), fmp:institutional-13f (E), polygon:etf-global (F), fmp:transcripts (E), fmp:news-feeds (E) |
| `justhodl-asia-leads` | fmp fred newsapi | hard cap on names | te:macro-markets (F), fmp:calendars (E), fmp:news-feeds (E), fmp:statements-bulk (H), fmp:social-sentiment (F) |
| `justhodl-asset-discovery` | — | hard cap on names; Finviz universe | polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F), polygon:etf-global (F), fmp:governance-execs (F) |
| `justhodl-auction-interpreter` | — | hard cap on names | fmp:macro-rates (F), fmp:institutional-13f (E), fmp:news-feeds (E), fmp:eod-bulk (H), polygon:grouped-crypto-fx (H) |
| `justhodl-backlog` | fmp sec | hard cap on names; curated / watchlist universe; unified universe (data/universe.json) | fmp:statements-bulk (H), fmp:revenue-segmentation (F), fmp:news-feeds (E), eia:full-catalog (H), fmp:sec-filings-feeds (E) |
| `justhodl-bloomberg-v8` | cmc fred newsapi polygon | hard cap on names | fmp:sector-industry-pe-perf (F), fmp:macro-rates (F), fmp:estimates-targets-grades (F), te:macro-markets (F), fmp:market-cap-history (H) |
| `justhodl-canary-warroom` | — | hard cap on names | eia:full-catalog (H), fmp:sector-industry-pe-perf (F), fmp:dividends-splits (H), te:macro-markets (F), fmp:macro-rates (F) |
| `justhodl-capex-pulse` | fmp fred | hard cap on names | fmp:statements-bulk (H), fmp:market-cap-history (H), fmp:sec-filings-feeds (E), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-capital-inflows` | fred | — | polygon:etf-global (F), polygon:short-interest-volume (F), fmp:shares-float-all (F), fmp:news-feeds (E), fmp:dividends-splits (H) |
| `justhodl-catalyst` | — | hard cap on names | fmp:statements-bulk (H), fmp:calendars (E), fmp:profile-bulk (U), fmp:ma-and-capital-feeds (E), fmp:news-feeds (E) |
| `justhodl-catalyst-chain` | — | hard cap on names | fmp:calendars (E), fmp:estimates-targets-grades (F), fmp:sec-filings-feeds (E), fmp:revenue-segmentation (F), fmp:peers-bulk (F) |
| `justhodl-cef-discount` | fmp | hard cap on names; curated / watchlist universe | polygon:eod-per-ticker (H), fmp:etf-holdings-weightings (F), fmp:dividends-splits (H), fmp:ratios-ttm-bulk (F), fmp:sector-industry-pe-perf (F) |
| `justhodl-chart-data` | ecb fred polygon | hard cap on names | fmp:fail-to-deliver (H), te:macro-markets (F), polygon:short-interest-volume (F), polygon:options-contracts (F), fmp:macro-rates (F) |
| `justhodl-convexity-scorer` | — | hard cap on names; curated / watchlist universe | polygon:options-contracts (F), fmp:calendars (E), fmp:earnings-surprises-bulk (F), fmp:market-cap-history (H), fmp:ma-and-capital-feeds (E) |
| `justhodl-correlation-surface` | polygon | hard cap on names | polygon:eod-per-ticker (H), fmp:eod-bulk (H), polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F) |
| `justhodl-credit-before-equity` | — | hard cap on names; unified universe (data/universe.json) | fmp:company-notes (F), fmp:statements-bulk (H), te:macro-markets (F), fmp:news-feeds (E), fmp:macro-rates (F) |
| `justhodl-cross-asset-rv` | fmp fred | hard cap on names | polygon:eod-per-ticker (H), eia:full-catalog (H), fmp:institutional-13f (E), fmp:macro-rates (F), fmp:eod-bulk (H) |
| `justhodl-crypto-funding` | — | hard cap on names | polygon:short-interest-volume (F), fmp:shares-float-all (F), polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F) |
| `justhodl-crypto-ma200` | polygon | hard cap on names | polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F), fmp:institutional-13f (E), fmp:estimates-targets-grades (F) |
| `justhodl-crypto-scorecard` | — | hard cap on names | polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F), fmp:earnings-surprises-bulk (F), fmp:estimates-targets-grades (F) |
| `justhodl-cyclical-bagger` | fmp polygon | hard cap on names | fmp:statements-bulk (H), polygon:short-interest-volume (F), fmp:shares-float-all (F), fmp:calendars (E), fmp:movers-snapshots (E) |
| `justhodl-dislocation-ai` | — | hard cap on names | fmp:price-target-summary-bulk (F), fmp:transcripts (E), fmp:peers-bulk (F), eia:full-catalog (H), fmp:estimates-targets-grades (F) |
| `justhodl-distribution-composite` | — | hard cap on names | fmp:insider-feeds (E), polygon:short-interest-volume (F), fmp:upgrades-downgrades-consensus-bulk (F), fmp:price-target-summary-bulk (F), fmp:estimates-targets-grades (F) |
| `justhodl-earnings-quality` | fmp | hard cap on names | fmp:statements-bulk (H), fmp:scores-bulk (F), fmp:earnings-surprises-bulk (F), fmp:ratios-ttm-bulk (F), fmp:market-cap-history (H) |
| `justhodl-factor-decomposition` | — | hard cap on names | fmp:institutional-13f (E), fmp:key-metrics-ttm-bulk (F), fmp:profile-bulk (U), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-finnhub-signals` | — | hard cap on names | fmp:earnings-surprises-bulk (F), fmp:price-target-summary-bulk (F), fmp:insider-feeds (E), fmp:institutional-13f (E), fmp:upgrades-downgrades-consensus-bulk (F) |
| `justhodl-flow-anomaly-detector` | — | hard cap on names | polygon:etf-global (F), fmp:sector-industry-pe-perf (F), fmp:etf-holdings-weightings (F), fmp:movers-snapshots (E), fmp:institutional-13f (E) |
| `justhodl-fundamental-census` | — | hard cap on names; Finviz universe | fmp:statements-bulk (H), fmp:profile-bulk (U), fmp:scores-bulk (F), fmp:institutional-13f (E), fmp:key-metrics-ttm-bulk (F) |
| `justhodl-future-intelligence` | — | hard cap on names | fmp:social-sentiment (F), fmp:movers-snapshots (E), fmp:news-feeds (E), fmp:eod-bulk (H), fmp:sector-industry-pe-perf (F) |
| `justhodl-geopolitical-risk` | — | hard cap on names; curated / watchlist universe | fmp:news-feeds (E), fmp:movers-snapshots (E), te:macro-markets (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-hot-money` | — | hard cap on names | polygon:etf-global (F), fmp:dividends-splits (H), polygon:eod-per-ticker (H), fmp:institutional-13f (E), fmp:eod-bulk (H) |
| `justhodl-industry-case` | — | hard cap on names; unified universe (data/universe.json) | fmp:peers-bulk (F), fmp:news-feeds (E), fmp:revenue-segmentation (F), fmp:profile-bulk (U), fmp:earnings-surprises-bulk (F) |
| `justhodl-insider-cluster-scanner` | sec | hard cap on names | fmp:insider-feeds (E), fmp:governance-execs (F), fmp:sec-filings-feeds (E), fmp:movers-snapshots (E), fmp:estimates-targets-grades (F) |
| `justhodl-liquidity-capacity` | — | hard cap on names; FMP screener ladder (search endpoint, not banked) | fmp:market-cap-history (H), fmp:profile-bulk (U), fmp:ratios-ttm-bulk (F), fmp:key-metrics-ttm-bulk (F), fmp:shares-float-all (F) |
| `justhodl-liquidity-credit-engine` | fred | hard cap on names | fmp:market-cap-history (H), polygon:eod-per-ticker (H), fmp:ma-and-capital-feeds (E), fmp:eod-bulk (H), fmp:shares-float-all (F) |
| `justhodl-liquidity-inflection` | fred polygon treasury | hard cap on names | polygon:eod-per-ticker (H), fmp:eod-bulk (H), fmp:shares-float-all (F), te:macro-markets (F), fmp:market-cap-history (H) |
| `justhodl-live-pulse` | — | — | polygon:minute-bars (H), fmp:intraday-1min (H), polygon:grouped-crypto-fx (H), te:macro-markets (F), fmp:institutional-13f (E) |
| `justhodl-lobbying-intel` | — | hard cap on names | fmp:social-sentiment (F), fmp:political-trades (E), fmp:calendars (E), fmp:peers-bulk (F), fmp:sec-filings-feeds (E) |
| `justhodl-macro-regime` | — | hard cap on names | te:macro-markets (F), polygon:grouped-crypto-fx (H), eia:full-catalog (H), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-market-machine` | fred | S&P 500 only; hard cap on names | eia:full-catalog (H), polygon:eod-per-ticker (H), fmp:estimates-targets-grades (F), fmp:price-target-summary-bulk (F), fmp:sector-industry-pe-perf (F) |
| `justhodl-merger-arb` | sec | hard cap on names | fmp:ma-and-capital-feeds (E), fmp:price-target-summary-bulk (F), fmp:key-metrics-ttm-bulk (F), fmp:ratios-ttm-bulk (F), fmp:profile-bulk (U) |
| `justhodl-meta-improver` | — | hard cap on names | fmp:ratios-ttm-bulk (F), fmp:profile-bulk (U), fmp:key-metrics-ttm-bulk (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-miss-detector` | polygon | hard cap on names; curated / watchlist universe; unified universe (data/universe.json) | polygon:eod-per-ticker (H), fmp:earnings-surprises-bulk (F), fmp:eod-bulk (H), fmp:price-target-summary-bulk (F), polygon:market-plumbing (E) |
| `justhodl-morning-brief-tg` | — | hard cap on names | fmp:macro-rates (F), fmp:earnings-surprises-bulk (F), te:macro-markets (F), polygon:eod-per-ticker (H), fmp:calendars (E) |
| `justhodl-multi-tf-convergence` | fmp | hard cap on names; curated / watchlist universe | polygon:eod-per-ticker (H), fmp:eod-bulk (H), fmp:movers-snapshots (E), polygon:minute-bars (H), fmp:intraday-1min (H) |
| `justhodl-narrative-vs-tape` | — | hard cap on names | fmp:news-feeds (E), fmp:institutional-13f (E), fmp:social-sentiment (F), polygon:etf-global (F), fmp:movers-snapshots (E) |
| `justhodl-news-sentiment` | — | hard cap on names | fmp:news-feeds (E), fmp:profile-bulk (U), fmp:ratios-ttm-bulk (F), fmp:key-metrics-ttm-bulk (F), polygon:minute-bars (H) |
| `justhodl-onchain-ratios` | — | hard cap on names | polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F), fmp:market-cap-history (H), fmp:dcf-bulk (F) |
| `justhodl-options-confluence` | — | hard cap on names | polygon:options-contracts (F), polygon:short-interest-volume (F), fmp:shares-float-all (F), fmp:calendars (E), polygon:eod-per-ticker (H) |
| `justhodl-pairs-scanner` | polygon | curated / watchlist universe | polygon:eod-per-ticker (H), eia:full-catalog (H), fmp:market-cap-history (H), fmp:sector-industry-pe-perf (F), fmp:eod-bulk (H) |
| `justhodl-portfolio-catalysts` | — | hard cap on names; curated / watchlist universe | fmp:earnings-surprises-bulk (F), fmp:calendars (E), fmp:etf-holder-bulk (F), fmp:price-target-summary-bulk (F), fmp:sec-filings-feeds (E) |
| `justhodl-positioning-analog` | fmp fred | hard cap on names | fmp:eod-bulk (H), fmp:cot (H), polygon:grouped-crypto-fx (H), fmp:institutional-13f (E), polygon:eod-per-ticker (H) |
| `justhodl-powell-pivot` | — | hard cap on names | fmp:transcripts (E), fmp:sector-industry-pe-perf (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H), fmp:dividends-splits (H) |
| `justhodl-pre-pump-detector` | fmp | hard cap on names; unified universe (data/universe.json) | fmp:shares-float-all (F), polygon:short-interest-volume (F), fmp:movers-snapshots (E), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-pump-earnings-nlp` | fmp | hard cap on names | fmp:transcripts (E), polygon:short-interest-volume (F), fmp:shares-float-all (F), fmp:movers-snapshots (E), fmp:price-target-summary-bulk (F) |
| `justhodl-put-call-extreme` | fred | hard cap on names | polygon:options-contracts (F), fmp:earnings-surprises-bulk (F), fmp:news-feeds (E), polygon:eod-per-ticker (H), fmp:sec-filings-feeds (E) |
| `justhodl-radar-backtest` | fmp | hard cap on names | polygon:eod-per-ticker (H), fmp:eod-bulk (H), polygon:short-interest-volume (F), fmp:shares-float-all (F), fmp:movers-snapshots (E) |
| `justhodl-regime-engine` | fred | S&P 500 only; hard cap on names | eia:full-catalog (H), polygon:options-contracts (F), te:macro-markets (F), fmp:institutional-13f (E), fmp:eod-bulk (H) |
| `justhodl-regime-playbook` | — | hard cap on names | polygon:eod-per-ticker (H), fmp:eod-bulk (H), polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F) |
| `justhodl-research-backtest` | — | hard cap on names | fmp:price-target-summary-bulk (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H), fmp:estimates-targets-grades (F), fmp:institutional-13f (E) |
| `justhodl-resilience` | polygon | hard cap on names | fmp:earnings-surprises-bulk (F), fmp:upgrades-downgrades-consensus-bulk (F), fmp:news-feeds (E), fmp:price-target-summary-bulk (F), fmp:calendars (E) |
| `justhodl-risk` | — | S&P 500 only; hard cap on names | te:macro-markets (F), polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F), fmp:company-notes (F) |
| `justhodl-rotation-chain` | fmp | hard cap on names; curated / watchlist universe | fmp:sector-industry-pe-perf (F), eia:full-catalog (H), polygon:eod-per-ticker (H), fmp:eod-bulk (H), fmp:institutional-13f (E) |
| `justhodl-scarcity-radar` | — | hard cap on names | fmp:revenue-segmentation (F), fmp:ratios-ttm-bulk (F), eia:full-catalog (H), fmp:peers-bulk (F), fmp:news-feeds (E) |
| `justhodl-sec-10kq` | sec | hard cap on names | fmp:sec-filings-feeds (E), fmp:statements-bulk (H), fmp:ratios-ttm-bulk (F), fmp:profile-bulk (U), fmp:key-metrics-ttm-bulk (F) |
| `justhodl-sellside-views` | fmp | hard cap on names; curated / watchlist universe | te:macro-markets (F), fmp:upgrades-downgrades-consensus-bulk (F), fmp:price-target-summary-bulk (F), fmp:estimates-targets-grades (F), fmp:news-feeds (E) |
| `justhodl-signal-harvester` | fmp polygon | hard cap on names | polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F), polygon:etf-global (F), fmp:etf-holdings-weightings (F) |
| `justhodl-signal-portfolio` | polygon | hard cap on names | polygon:short-interest-volume (F), fmp:earnings-surprises-bulk (F), fmp:shares-float-all (F), fmp:cot (H), fmp:institutional-13f (E) |
| `justhodl-smart-money-holdings` | — | hard cap on names; curated / watchlist universe | fmp:key-metrics-ttm-bulk (F), fmp:institutional-13f (E), fmp:funds-disclosure (E), fmp:profile-bulk (U), fmp:ratios-ttm-bulk (F) |
| `justhodl-snb-detail` | fred | hard cap on names | te:macro-markets (F), polygon:grouped-crypto-fx (H), fmp:shares-float-all (F), polygon:short-interest-volume (F), fmp:macro-rates (F) |
| `justhodl-squeeze-fuel` | — | — | polygon:short-interest-volume (F), fmp:shares-float-all (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H), fmp:fail-to-deliver (H) |
| `justhodl-streaming-fanout` | — | hard cap on names | polygon:minute-bars (H), fmp:intraday-1min (H), te:macro-markets (F), fmp:institutional-13f (E), fmp:news-feeds (E) |
| `justhodl-stress-index` | fred | S&P 500 only; hard cap on names | fmp:macro-rates (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H), te:macro-markets (F), polygon:grouped-crypto-fx (H) |
| `justhodl-supply-chain-linkage` | — | hard cap on names | fmp:revenue-segmentation (F), fmp:peers-bulk (F), fmp:market-cap-history (H), fmp:news-feeds (E), fmp:institutional-13f (E) |
| `justhodl-symbol-dictionary` | fred polygon | curated / watchlist universe | polygon:eod-per-ticker (H), fmp:eod-bulk (H), te:macro-markets (F), polygon:grouped-crypto-fx (H), eia:full-catalog (H) |
| `justhodl-talent-migration` | fmp sec | hard cap on names; FMP screener ladder (search endpoint, not banked) | fmp:sec-filings-feeds (E), fmp:governance-execs (F), fmp:employees (F), fmp:transcripts (E), fmp:peers-bulk (F) |
| `justhodl-tic-flows` | fred | hard cap on names | polygon:etf-global (F), fmp:funds-disclosure (E), fmp:dividends-splits (H), fmp:market-cap-history (H), fmp:institutional-13f (E) |
| `justhodl-ticker-deep-research` | — | hard cap on names | polygon:short-interest-volume (F), fmp:shares-float-all (F), fmp:movers-snapshots (E), fmp:news-feeds (E), fmp:eod-bulk (H) |
| `justhodl-vol-target-unwind` | fmp | curated / watchlist universe | fmp:institutional-13f (E), polygon:minute-bars (H), polygon:grouped-crypto-fx (H), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-volatility-squeeze-hunter` | fmp | hard cap on names; unified universe (data/universe.json) | polygon:eod-per-ticker (H), polygon:short-interest-volume (F), fmp:shares-float-all (F), eia:full-catalog (H), fmp:eod-bulk (H) |
| `justhodl-13f-clone-alpha` | fmp sec | hard cap on names; curated / watchlist universe | fmp:sec-filings-feeds (E), fmp:institutional-13f (E), polygon:options-contracts (F), fmp:estimates-targets-grades (F) |
| `justhodl-accumulation-radar` | polygon | hard cap on names; curated / watchlist universe | polygon:eod-per-ticker (H), fmp:institutional-13f (E), fmp:dividends-splits (H), fmp:insider-feeds (E) |
| `justhodl-ai-website-synthesis` | — | — | polygon:eod-per-ticker (H), fmp:ma-and-capital-feeds (E), fmp:eod-bulk (H), te:macro-markets (F) |
| `justhodl-alert-backtester` | fred polygon yahoo | hard cap on names | fmp:eod-bulk (H), polygon:eod-per-ticker (H), te:macro-markets (F), fmp:institutional-13f (E) |
| `justhodl-analyst-actions` | — | hard cap on names | fmp:estimates-targets-grades (F), fmp:price-target-summary-bulk (F), fmp:news-feeds (E), fmp:upgrades-downgrades-consensus-bulk (F) |
| `justhodl-apac-leadlag` | fmp | hard cap on names | te:macro-markets (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H), fmp:etf-holdings-weightings (F) |
| `justhodl-ark-holdings` | — | hard cap on names | fmp:etf-holdings-weightings (F), fmp:statements-bulk (H), cmc:crypto-market (F), fmp:funds-disclosure (E) |
| `justhodl-asymmetric-scorer` | — | hard cap on names | fmp:key-metrics-ttm-bulk (F), fmp:ratios-ttm-bulk (F), fmp:profile-bulk (U), fmp:scores-bulk (F) |
| `justhodl-backtest-engine` | — | hard cap on names | polygon:eod-per-ticker (H), fmp:eod-bulk (H), fmp:market-cap-history (H), fmp:etf-holder-bulk (F) |
| `justhodl-base-rates` | — | S&P 500 only; unified universe (data/universe.json) | fmp:earnings-surprises-bulk (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H), fmp:etf-holdings-weightings (F) |
| `justhodl-bond-trace` | fred polygon | hard cap on names | polygon:options-contracts (F), fmp:ma-and-capital-feeds (E), fmp:movers-snapshots (E), fmp:company-notes (F) |
| `justhodl-bottom-signals` | coingecko | S&P 500 only; hard cap on names; curated / watchlist universe | polygon:eod-per-ticker (H), fmp:market-cap-history (H), fmp:institutional-13f (E), polygon:grouped-crypto-fx (H) |
| `justhodl-canary-grid` | ecb fred | hard cap on names | eia:full-catalog (H), fmp:macro-rates (F), te:macro-markets (F), fmp:movers-snapshots (E) |
| `justhodl-capital-flow-radar` | — | — | polygon:etf-global (F), fmp:funds-disclosure (E), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-catalyst-calendar` | fmp | hard cap on names | fmp:earnings-surprises-bulk (F), fmp:calendars (E), fmp:etf-holder-bulk (F), polygon:options-contracts (F) |
| `justhodl-catalyst-skew-premove` | — | hard cap on names | fmp:calendars (E), polygon:options-contracts (F), fmp:earnings-surprises-bulk (F), polygon:short-interest-volume (F) |
| `justhodl-cds-proxy` | fred | hard cap on names | fmp:company-notes (F), te:macro-markets (F), fmp:scores-bulk (F), eia:full-catalog (H) |
| `justhodl-coinbase-premium` | — | hard cap on names | polygon:grouped-crypto-fx (H), fmp:institutional-13f (E), cq:onchain-hourly (H), cmc:crypto-market (F) |
| `justhodl-consensus-bottom` | — | hard cap on names | fmp:key-metrics-ttm-bulk (F), fmp:statements-bulk (H), fmp:institutional-13f (E), fmp:estimates-targets-grades (F) |
| `justhodl-crypto-basis` | — | hard cap on names | polygon:grouped-crypto-fx (H), polygon:options-contracts (F), cq:onchain-hourly (H), cmc:crypto-market (F) |
| `justhodl-crypto-dvol` | — | hard cap on names | polygon:options-contracts (F), polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F) |
| `justhodl-crypto-exchange-flows` | — | hard cap on names | polygon:etf-global (F), polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F) |
| `justhodl-crypto-options-surface` | — | hard cap on names | polygon:grouped-crypto-fx (H), polygon:options-contracts (F), cq:onchain-hourly (H), cmc:crypto-market (F) |
| `justhodl-cryptoquant` | cq | hard cap on names; curated / watchlist universe | cq:onchain-hourly (H), polygon:grouped-crypto-fx (H), cmc:crypto-market (F), fmp:institutional-13f (E) |
| `justhodl-desk-returns` | — | — | fmp:dividends-splits (H), fmp:etf-holder-bulk (F), fmp:news-feeds (E), polygon:eod-per-ticker (H) |
| `justhodl-dix` | — | hard cap on names | polygon:options-contracts (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H), fmp:institutional-13f (E) |
| `justhodl-dxy-equity-divergence` | fmp | hard cap on names | polygon:grouped-crypto-fx (H), fmp:macro-rates (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-dxy-predict` | coingecko | S&P 500 only; hard cap on names; curated / watchlist universe | polygon:grouped-crypto-fx (H), polygon:eod-per-ticker (H), fmp:market-cap-history (H), fmp:institutional-13f (E) |
| `justhodl-earnings-tone-velocity` | — | hard cap on names | fmp:transcripts (E), fmp:movers-snapshots (E), fmp:earnings-surprises-bulk (F), fmp:statements-bulk (H) |
| `justhodl-ecb-detail` | ecb fred | hard cap on names | te:macro-markets (F), fmp:macro-rates (F), fmp:statements-bulk (H), fmp:shares-float-all (F) |
| `justhodl-econ-calendar` | fmp | hard cap on names | te:macro-markets (F), fmp:calendars (E), fmp:earnings-surprises-bulk (F), fmp:sector-industry-pe-perf (F) |
| `justhodl-equity-confluence` | — | hard cap on names | polygon:short-interest-volume (F), fmp:shares-float-all (F), polygon:options-contracts (F), fmp:statements-bulk (H) |
| `justhodl-equity-ftd` | fmp sec | hard cap on names | fmp:fail-to-deliver (H), fmp:shares-float-all (F), polygon:short-interest-volume (F), fmp:movers-snapshots (E) |
| `justhodl-etf-fund-flows` | — | — | fmp:funds-disclosure (E), polygon:etf-global (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-euro-fragmentation` | ecb fred | hard cap on names | te:macro-markets (F), fmp:dividends-splits (H), fmp:macro-rates (F), fmp:scores-bulk (F) |
| `justhodl-event-coordinator` | — | hard cap on names | fmp:earnings-surprises-bulk (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H), fmp:calendars (E) |
| `justhodl-fast-filings` | sec | hard cap on names | fmp:sec-filings-feeds (E), fmp:insider-feeds (E), polygon:etf-global (F), fmp:institutional-13f (E) |
| `justhodl-fed-pivot-factor-router` | — | hard cap on names | fmp:macro-rates (F), fmp:sector-industry-pe-perf (F), fmp:transcripts (E), fmp:institutional-13f (E) |
| `justhodl-feed-catalog` | — | hard cap on names | polygon:etf-global (F), fmp:ratios-ttm-bulk (F), fmp:profile-bulk (U), fmp:key-metrics-ttm-bulk (F) |
| `justhodl-finra-full` | — | hard cap on names | cmc:crypto-market (F), polygon:short-interest-volume (F), fmp:shares-float-all (F), polygon:minute-bars (H) |
| `justhodl-finviz-signals` | — | S&P 500 only; hard cap on names; Finviz universe | fmp:insider-feeds (E), polygon:eod-per-ticker (H), fmp:shares-float-all (F), fmp:movers-snapshots (E) |
| `justhodl-finviz-universe` | — | S&P 500 only; hard cap on names; Finviz universe | polygon:short-interest-volume (F), fmp:shares-float-all (F), polygon:market-plumbing (E), fmp:profile-bulk (U) |
| `justhodl-firm-book` | — | hard cap on names | fmp:etf-holdings-weightings (F), fmp:market-cap-history (H), fmp:dividends-splits (H), fmp:etf-holder-bulk (F) |
| `justhodl-forced-selling-bounce` | — | hard cap on names; curated / watchlist universe | polygon:market-plumbing (E), polygon:eod-per-ticker (H), polygon:options-contracts (F), fmp:institutional-13f (E) |
| `justhodl-forensic-screen` | — | — | polygon:eod-per-ticker (H), fmp:eod-bulk (H), fmp:scores-bulk (F), fmp:estimates-targets-grades (F) |
| `justhodl-fx-decomposition` | fred | hard cap on names | polygon:grouped-crypto-fx (H), polygon:eod-per-ticker (H), fmp:eod-bulk (H), fmp:institutional-13f (E) |
| `justhodl-gdelt-buzz` | — | — | fmp:news-feeds (E), fmp:social-sentiment (F), eia:full-catalog (H), fmp:revenue-segmentation (F) |
| `justhodl-global-business-cycle` | fred yahoo | hard cap on names | polygon:eod-per-ticker (H), te:macro-markets (F), fmp:eod-bulk (H), fmp:ma-and-capital-feeds (E) |
| `justhodl-global-expansion` | sec | hard cap on names | te:macro-markets (F), eia:full-catalog (H), cmc:crypto-market (F), fmp:company-notes (F) |
| `justhodl-global-liquidity` | fred | hard cap on names | fmp:statements-bulk (H), te:macro-markets (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-global-markets` | fmp | hard cap on names | te:macro-markets (F), fmp:sector-industry-pe-perf (F), fmp:profile-bulk (U), polygon:eod-per-ticker (H) |
| `justhodl-global-stress` | fred | hard cap on names | te:macro-markets (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H), fmp:macro-rates (F) |
| `justhodl-grid-queue` | eia | hard cap on names | fmp:statements-bulk (H), eia:full-catalog (H), fmp:sec-filings-feeds (E), fmp:movers-snapshots (E) |
| `justhodl-history-snapshotter` | — | hard cap on names; unified universe (data/universe.json) | polygon:minute-bars (H), fmp:estimates-targets-grades (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-ici-flows` | — | hard cap on names | fmp:funds-disclosure (E), polygon:eod-per-ticker (H), fmp:eod-bulk (H), fmp:news-feeds (E) |
| `justhodl-insider-aggregate` | fmp | — | fmp:insider-feeds (E), polygon:eod-per-ticker (H), fmp:sec-filings-feeds (E), fmp:eod-bulk (H) |
| `justhodl-intraday-pulse` | polygon | hard cap on names; curated / watchlist universe | polygon:minute-bars (H), fmp:intraday-1min (H), fmp:movers-snapshots (E), fmp:upgrades-downgrades-consensus-bulk (F) |
| `justhodl-invest` | — | — | fmp:earnings-surprises-bulk (F), polygon:etf-global (F), eia:full-catalog (H), fmp:sector-industry-pe-perf (F) |
| `justhodl-ipo-pipeline` | fmp | hard cap on names | fmp:calendars (E), polygon:ticker-events-ipos (E), fmp:sector-industry-pe-perf (F), fmp:dcf-bulk (F) |
| `justhodl-khalid-adaptive` | — | hard cap on names | polygon:eod-per-ticker (H), fmp:institutional-13f (E), fmp:eod-bulk (H), fmp:cot (H) |
| `justhodl-liq-indicators` | coingecko | S&P 500 only; hard cap on names; curated / watchlist universe | polygon:eod-per-ticker (H), fmp:market-cap-history (H), fmp:institutional-13f (E), polygon:grouped-crypto-fx (H) |
| `justhodl-liquidity-reversal` | coingecko | S&P 500 only; hard cap on names; curated / watchlist universe | polygon:eod-per-ticker (H), fmp:market-cap-history (H), fmp:institutional-13f (E), polygon:grouped-crypto-fx (H) |
| `justhodl-ma-reversion` | polygon | hard cap on names | polygon:eod-per-ticker (H), fmp:social-sentiment (F), fmp:eod-bulk (H), fmp:dividends-splits (H) |
| `justhodl-ma-tracker` | — | hard cap on names | fmp:ma-and-capital-feeds (E), fmp:profile-bulk (U), fmp:ratios-ttm-bulk (F), fmp:key-metrics-ttm-bulk (F) |
| `justhodl-macro-leads` | fmp fred yahoo | hard cap on names | fmp:sector-industry-pe-perf (F), te:macro-markets (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-macro-predict` | coingecko | S&P 500 only; hard cap on names; curated / watchlist universe | polygon:eod-per-ticker (H), fmp:market-cap-history (H), fmp:institutional-13f (E), polygon:grouped-crypto-fx (H) |
| `justhodl-market-interpreter` | — | hard cap on names | fmp:macro-rates (F), te:macro-markets (F), fmp:eod-bulk (H), polygon:grouped-crypto-fx (H) |
| `justhodl-my-brief` | — | hard cap on names; curated / watchlist universe | polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F), fmp:calendars (E) |
| `justhodl-news-velocity` | — | hard cap on names | fmp:movers-snapshots (E), fmp:news-feeds (E), fmp:institutional-13f (E), fmp:transcripts (E) |
| `justhodl-nobrainer-tracker` | — | hard cap on names | polygon:eod-per-ticker (H), fmp:eod-bulk (H), eia:full-catalog (H), fmp:key-metrics-ttm-bulk (F) |
| `justhodl-patent-velocity` | — | hard cap on names; curated / watchlist universe | fmp:movers-snapshots (E), fmp:ma-and-capital-feeds (E), fmp:sector-industry-pe-perf (F), fmp:institutional-13f (E) |
| `justhodl-pjm-grid` | — | — | eia:full-catalog (H), cq:onchain-hourly (H), cmc:crypto-market (F), polygon:minute-bars (H) |
| `justhodl-pm-decision` | — | hard cap on names | fmp:news-feeds (E), fmp:market-cap-history (H), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-political-intel` | fmp | S&P 500 only; hard cap on names; curated / watchlist universe | fmp:political-trades (E), fmp:governance-execs (F), fmp:institutional-13f (E), fmp:sec-filings-feeds (E) |
| `justhodl-quality-on-sale` | — | hard cap on names; curated / watchlist universe | fmp:scores-bulk (F), fmp:key-metrics-ttm-bulk (F), fmp:statements-bulk (H), fmp:institutional-13f (E) |
| `justhodl-real-economy-collector` | eia fred treasury | hard cap on names | eia:full-catalog (H), fmp:news-feeds (E), fmp:institutional-13f (E), fmp:earnings-surprises-bulk (F) |
| `justhodl-repo` | ecb fred nyfed te | hard cap on names | eia:full-catalog (H), polygon:short-interest-volume (F), fmp:fail-to-deliver (H), te:macro-markets (F) |
| `justhodl-repo-lending` | fred | S&P 500 only; hard cap on names | polygon:short-interest-volume (F), fmp:shares-float-all (F), fmp:market-cap-history (H), fmp:macro-rates (F) |
| `justhodl-risk-gate` | fred | S&P 500 only; hard cap on names | polygon:eod-per-ticker (H), fmp:eod-bulk (H), te:macro-markets (F), fmp:statements-bulk (H) |
| `justhodl-rotation-radar` | coingecko polygon | hard cap on names | fmp:sector-industry-pe-perf (F), polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F) |
| `justhodl-sec-8k` | sec | hard cap on names | fmp:sec-filings-feeds (E), fmp:sector-industry-pe-perf (F), fmp:ma-and-capital-feeds (E), fmp:insider-feeds (E) |
| `justhodl-settlement-fails` | nyfed yahoo | hard cap on names | fmp:fail-to-deliver (H), polygon:short-interest-volume (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-share-flows` | — | — | polygon:etf-global (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H), fmp:ma-and-capital-feeds (E) |
| `justhodl-short-interest` | — | — | polygon:short-interest-volume (F), fmp:shares-float-all (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-squeeze-pretrigger` | fmp | hard cap on names | polygon:short-interest-volume (F), fmp:shares-float-all (F), fmp:calendars (E), fmp:movers-snapshots (E) |
| `justhodl-stock-screener` | — | S&P 500 only; hard cap on names | fmp:ratios-ttm-bulk (F), fmp:profile-bulk (U), fmp:key-metrics-ttm-bulk (F), fmp:scores-bulk (F) |
| `justhodl-strategy-portfolio` | polygon | hard cap on names | fmp:institutional-13f (E), fmp:estimates-targets-grades (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-stress-scenarios` | — | hard cap on names; curated / watchlist universe | fmp:macro-rates (F), fmp:movers-snapshots (E), fmp:eod-bulk (H), polygon:grouped-crypto-fx (H) |
| `justhodl-systemic-stress` | ecb | hard cap on names | te:macro-markets (F), fmp:company-notes (F), fmp:scores-bulk (F), fmp:macro-rates (F) |
| `justhodl-tape-reader` | polygon | S&P 500 only; hard cap on names; unified universe (data/universe.json) | polygon:minute-bars (H), polygon:market-plumbing (E), fmp:institutional-13f (E), fmp:intraday-1min (H) |
| `justhodl-theme-rotation` | fmp polygon | hard cap on names | fmp:sector-industry-pe-perf (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H), polygon:etf-global (F) |
| `justhodl-ticker-360` | — | hard cap on names | polygon:options-contracts (F), polygon:short-interest-volume (F), fmp:sector-industry-pe-perf (F), fmp:fail-to-deliver (H) |
| `justhodl-top-signals` | coingecko | S&P 500 only; hard cap on names; curated / watchlist universe | polygon:eod-per-ticker (H), fmp:market-cap-history (H), fmp:institutional-13f (E), polygon:grouped-crypto-fx (H) |
| `justhodl-trade-journal` | polygon | hard cap on names; curated / watchlist universe | fmp:estimates-targets-grades (F), fmp:institutional-13f (E), cmc:crypto-market (F), fmp:earnings-surprises-bulk (F) |
| `justhodl-transcript-indexer` | — | — | fmp:transcripts (E), fmp:institutional-13f (E), fmp:etf-holdings-weightings (F), cmc:crypto-market (F) |
| `justhodl-trend-engine` | — | — | polygon:eod-per-ticker (H), eia:full-catalog (H), fmp:eod-bulk (H), polygon:grouped-crypto-fx (H) |
| `justhodl-us-cycle` | fmp fred polygon | hard cap on names | fmp:macro-rates (F), te:macro-markets (F), fmp:sector-industry-pe-perf (F), polygon:grouped-crypto-fx (H) |
| `justhodl-velocity-acceleration` | fmp | hard cap on names; curated / watchlist universe | fmp:movers-snapshots (E), fmp:news-feeds (E), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-vix-backwardation-trigger` | — | curated / watchlist universe | fmp:institutional-13f (E), polygon:minute-bars (H), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-vvix-vov-regime` | fmp | hard cap on names | polygon:options-contracts (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H), fmp:macro-rates (F) |
| `justhodl-wl-engines` | — | hard cap on names; curated / watchlist universe | polygon:eod-per-ticker (H), fmp:eod-bulk (H), polygon:grouped-crypto-fx (H), fmp:estimates-targets-grades (F) |
| `justhodl-worldbank-wgi` | — | — | fmp:governance-execs (F), fmp:political-trades (E), eia:full-catalog (H), te:macro-markets (F) |
| `justhodl-13f-positions` | fmp sec | hard cap on names; curated / watchlist universe | fmp:sec-filings-feeds (E), fmp:institutional-13f (E), fmp:scores-bulk (F) |
| `justhodl-activist-filings-scanner` | sec | hard cap on names; unified universe (data/universe.json) | fmp:sec-filings-feeds (E), fmp:institutional-13f (E), polygon:minute-bars (H) |
| `justhodl-activity-nowcast` | — | — | polygon:eod-per-ticker (H), fmp:eod-bulk (H), fmp:etf-holdings-weightings (F) |
| `justhodl-alpha-decay` | — | hard cap on names | polygon:eod-per-ticker (H), fmp:eod-bulk (H), cmc:crypto-market (F) |
| `justhodl-auction-crisis-ai` | — | hard cap on names | fmp:eod-bulk (H), fmp:governance-execs (F), fmp:news-feeds (E) |
| `justhodl-auction-grader` | — | hard cap on names | fmp:estimates-targets-grades (F), polygon:options-contracts (F), cmc:crypto-market (F) |
| `justhodl-backlog-miner` | sec | hard cap on names | fmp:sec-filings-feeds (E), cq:onchain-hourly (H), cmc:crypto-market (F) |
| `justhodl-benzinga-earnings` | polygon | hard cap on names | fmp:earnings-surprises-bulk (F), fmp:calendars (E), fmp:institutional-13f (E) |
| `justhodl-bls-full` | — | hard cap on names; curated / watchlist universe | fmp:profile-bulk (U), te:macro-markets (F), fmp:etf-holdings-weightings (F) |
| `justhodl-boe-full` | — | hard cap on names | te:macro-markets (F), fmp:macro-rates (F), fmp:dividends-splits (H) |
| `justhodl-breadth-thrust` | fred polygon | S&P 500 only; curated / watchlist universe | polygon:market-plumbing (E), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-calls-backtest` | polygon | — | polygon:eod-per-ticker (H), fmp:eod-bulk (H), fmp:news-feeds (E) |
| `justhodl-cannibals` | — | hard cap on names; unified universe (data/universe.json) | fmp:statements-bulk (H), fmp:insider-feeds (E), fmp:scores-bulk (F) |
| `justhodl-cascade-validator` | fmp | hard cap on names | fmp:earnings-surprises-bulk (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-cb-stance` | — | hard cap on names | fmp:transcripts (E), te:macro-markets (F), fmp:statements-bulk (H) |
| `justhodl-cboe-options-chain` | cboe | — | polygon:options-contracts (F), polygon:short-interest-volume (F), fmp:fail-to-deliver (H) |
| `justhodl-cds-desk` | ecb | — | te:macro-markets (F), fmp:cot (H), fmp:company-notes (F) |
| `justhodl-cftc-deep-view` | — | hard cap on names | fmp:cot (H), fmp:institutional-13f (E), fmp:estimates-targets-grades (F) |
| `justhodl-china-liquidity` | fred | hard cap on names | te:macro-markets (F), polygon:etf-global (F), fmp:social-sentiment (F) |
| `justhodl-ciss-stress` | — | — | te:macro-markets (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-coffee-can` | fmp | hard cap on names | fmp:scores-bulk (F), fmp:key-metrics-ttm-bulk (F), fmp:statements-bulk (H) |
| `justhodl-congress-alpha` | — | hard cap on names | fmp:political-trades (E), fmp:institutional-13f (E), fmp:estimates-targets-grades (F) |
| `justhodl-consumer-pulse` | fred | hard cap on names | fmp:employees (F), fmp:profile-bulk (U), te:macro-markets (F) |
| `justhodl-corporate-actions` | polygon yahoo | hard cap on names; Finviz universe | fmp:ma-and-capital-feeds (E), fmp:dividends-splits (H), polygon:ticker-events-ipos (E) |
| `justhodl-correlation-break-trade-router` | — | S&P 500 only | polygon:grouped-crypto-fx (H), fmp:institutional-13f (E), fmp:macro-rates (F) |
| `justhodl-cot-tracker` | — | hard cap on names | fmp:cot (H), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-credit-equity-divergence` | fmp | hard cap on names | fmp:company-notes (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-crisis-canaries` | fred nyfed polygon | hard cap on names | polygon:etf-global (F), fmp:revenue-segmentation (F), fmp:fail-to-deliver (H) |
| `justhodl-cro-escalation` | fmp | hard cap on names | polygon:minute-bars (H), fmp:intraday-1min (H), polygon:options-contracts (F) |
| `justhodl-crypto-emergence` | polygon | — | polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F) |
| `justhodl-crypto-enricher` | — | hard cap on names | polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F) |
| `justhodl-crypto-intel` | cmc coingecko | hard cap on names; Finviz universe | polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F) |
| `justhodl-crypto-stablecoin-peg` | coingecko | hard cap on names | polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F) |
| `justhodl-cycle-clock` | — | — | polygon:eod-per-ticker (H), fmp:etf-holdings-weightings (F), fmp:eod-bulk (H) |
| `justhodl-daily-report-v3` | coingecko ecb fred newsapi polygon | S&P 500 only; hard cap on names | polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F) |
| `justhodl-digest-trends-ai` | — | hard cap on names | fmp:social-sentiment (F), fmp:news-feeds (E), fmp:movers-snapshots (E) |
| `justhodl-divergence-interpreter` | — | hard cap on names | fmp:news-feeds (E), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-earnings-cascade` | — | — | fmp:earnings-surprises-bulk (F), fmp:institutional-13f (E), fmp:transcripts (E) |
| `justhodl-earnings-nlp` | fmp | hard cap on names; curated / watchlist universe | fmp:transcripts (E), fmp:governance-execs (F), fmp:market-cap-history (H) |
| `justhodl-eba-risk-dashboard` | — | hard cap on names | te:macro-markets (F), fmp:news-feeds (E), eia:full-catalog (H) |
| `justhodl-ecb-deep` | ecb | — | te:macro-markets (F), fmp:dividends-splits (H), fmp:sector-industry-pe-perf (F) |
| `justhodl-equity-prewarm` | — | hard cap on names | fmp:market-cap-history (H), fmp:profile-bulk (U), fmp:institutional-13f (E) |
| `justhodl-estimate-revisions` | fmp polygon | hard cap on names | fmp:price-target-summary-bulk (F), fmp:estimates-targets-grades (F), fmp:calendars (E) |
| `justhodl-etf-flows` | polygon | hard cap on names | polygon:etf-global (F), polygon:options-contracts (F), fmp:funds-disclosure (E) |
| `justhodl-etf-issuer-holdings` | — | — | fmp:etf-holdings-weightings (F), polygon:short-interest-volume (F), fmp:fail-to-deliver (H) |
| `justhodl-eurodollar-stress` | — | — | fmp:macro-rates (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-event-study` | fred | S&P 500 only | fmp:macro-rates (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-exchange-flows` | — | hard cap on names | polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F) |
| `justhodl-executive-intel` | — | hard cap on names; curated / watchlist universe | fmp:political-trades (E), fmp:governance-execs (F), fmp:eod-bulk (H) |
| `justhodl-factor-returns` | — | hard cap on names; Finviz universe | fmp:sector-industry-pe-perf (F), fmp:ratios-ttm-bulk (F), fmp:key-metrics-ttm-bulk (F) |
| `justhodl-factor-risk` | polygon | hard cap on names | fmp:profile-bulk (U), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-failed-pattern-reversal` | fmp | hard cap on names | polygon:eod-per-ticker (H), fmp:eod-bulk (H), fmp:etf-holder-bulk (F) |
| `justhodl-financial-secretary` | cmc fred newsapi polygon | S&P 500 only; hard cap on names | fmp:cot (H), fmp:price-target-summary-bulk (F), fmp:market-cap-history (H) |
| `justhodl-finra-short` | — | — | polygon:short-interest-volume (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-fred-tag-crawler` | fred | hard cap on names | fmp:profile-bulk (U), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-global-tide` | fred | S&P 500 only; hard cap on names | te:macro-markets (F), fmp:macro-rates (F), polygon:grouped-crypto-fx (H) |
| `justhodl-gold-equity-rotation` | — | — | fmp:sector-industry-pe-perf (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-google-trends` | — | hard cap on names; curated / watchlist universe | polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F) |
| `justhodl-gsi-horizons` | — | — | te:macro-markets (F), polygon:options-contracts (F), fmp:market-cap-history (H) |
| `justhodl-guardrail-notify` | — | hard cap on names | cmc:crypto-market (F), polygon:short-interest-volume (F), fmp:fail-to-deliver (H) |
| `justhodl-hedge-pnl` | — | hard cap on names | polygon:options-contracts (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-hkma-monitor` | fmp fred | — | te:macro-markets (F), fmp:revenue-segmentation (F), fmp:macro-rates (F) |
| `justhodl-hyperliquid-perps` | — | hard cap on names | polygon:grouped-crypto-fx (H), cq:onchain-hourly (H), cmc:crypto-market (F) |
| `justhodl-implied-prob` | — | — | polygon:eod-per-ticker (H), fmp:macro-rates (F), fmp:eod-bulk (H) |
| `justhodl-index-inclusion` | fmp | S&P 500 only; hard cap on names; FMP screener ladder (search endpoint, not banked); Finviz universe | fmp:etf-holder-bulk (F), fmp:ratios-ttm-bulk (F), fmp:key-metrics-ttm-bulk (F) |
| `justhodl-insider-radar` | fmp polygon | hard cap on names; Finviz universe | fmp:insider-feeds (E), fmp:governance-execs (F), fmp:estimates-targets-grades (F) |
| `justhodl-insider-sell-cluster` | fmp | hard cap on names | fmp:insider-feeds (E), fmp:governance-execs (F), fmp:market-cap-history (H) |
| `justhodl-ipo-calendar` | sec | hard cap on names | fmp:sec-filings-feeds (E), fmp:calendars (E), polygon:ticker-events-ipos (E) |
| `justhodl-macro-calendar` | fmp | hard cap on names | fmp:calendars (E), te:macro-markets (F), fmp:profile-bulk (U) |
| `justhodl-macro-surprise` | fred | — | te:macro-markets (F), fmp:earnings-surprises-bulk (F), polygon:eod-per-ticker (H) |
| `justhodl-market-map` | fmp polygon | S&P 500 only; hard cap on names; curated / watchlist universe | fmp:profile-bulk (U), fmp:sector-industry-pe-perf (F), fmp:market-cap-history (H) |
| `justhodl-miss-calibrator` | — | hard cap on names; unified universe (data/universe.json) | fmp:earnings-surprises-bulk (F), polygon:options-contracts (F), fmp:movers-snapshots (E) |
| `justhodl-momentum-scanner` | polygon | hard cap on names | fmp:key-metrics-ttm-bulk (F), fmp:profile-bulk (U), fmp:ratios-ttm-bulk (F) |
| `justhodl-narrative-density-tracker` | polygon | hard cap on names | fmp:news-feeds (E), fmp:revenue-segmentation (F), fmp:peers-bulk (F) |
| `justhodl-nowcast-desk` | fred | hard cap on names | te:macro-markets (F), fmp:statements-bulk (H), fmp:estimates-targets-grades (F) |
| `justhodl-nyfed-pd` | nyfed yahoo | hard cap on names | polygon:options-contracts (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-nyfed-repo-deep` | nyfed | hard cap on names | polygon:minute-bars (H), fmp:intraday-1min (H), fmp:etf-holdings-weightings (F) |
| `justhodl-official-pulse` | fred | hard cap on names | polygon:eod-per-ticker (H), fmp:sector-industry-pe-perf (F), fmp:eod-bulk (H) |
| `justhodl-options-analytics` | — | hard cap on names | polygon:options-contracts (F), polygon:short-interest-volume (F), fmp:shares-float-all (F) |
| `justhodl-options-gamma` | — | — | polygon:options-contracts (F), polygon:minute-bars (H), fmp:intraday-1min (H) |
| `justhodl-pead-detector` | fmp | hard cap on names; unified universe (data/universe.json) | fmp:earnings-surprises-bulk (F), polygon:eod-per-ticker (H), fmp:calendars (E) |
| `justhodl-plumbing-panel` | nyfed treasury | hard cap on names | fmp:fail-to-deliver (H), polygon:short-interest-volume (F), fmp:shares-float-all (F) |
| `justhodl-political-ai-investigation` | — | hard cap on names; curated / watchlist universe | fmp:political-trades (E), polygon:options-contracts (F), fmp:calendars (E) |
| `justhodl-political-trades` | — | hard cap on names | fmp:political-trades (E), fmp:sec-filings-feeds (E), fmp:market-cap-history (H) |
| `justhodl-polygon-fx-regime` | — | — | polygon:grouped-crypto-fx (H), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-polygon-options-flow` | — | — | polygon:options-contracts (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-portfolio-risk` | — | — | fmp:market-cap-history (H), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-position-monitor` | — | hard cap on names | cmc:crypto-market (F), fmp:price-target-summary-bulk (F), fmp:estimates-targets-grades (F) |
| `justhodl-ppi-acceleration` | fred | hard cap on names | fmp:revenue-segmentation (F), fmp:peers-bulk (F), fmp:transcripts (E) |
| `justhodl-prediction-markets` | — | — | te:macro-markets (F), polygon:options-contracts (F), fmp:political-trades (E) |
| `justhodl-public-archive-index` | — | — | polygon:eod-per-ticker (H), fmp:ratios-ttm-bulk (F), fmp:key-metrics-ttm-bulk (F) |
| `justhodl-research-critique` | — | hard cap on names | fmp:price-target-summary-bulk (F), fmp:estimates-targets-grades (F), fmp:news-feeds (E) |
| `justhodl-retail-sentiment` | — | — | fmp:social-sentiment (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-reversal-radar` | — | hard cap on names | polygon:market-plumbing (E), polygon:eod-per-ticker (H), fmp:macro-rates (F) |
| `justhodl-risk-ratios` | fred polygon yahoo | hard cap on names | eia:full-catalog (H), te:macro-markets (F), fmp:estimates-targets-grades (F) |
| `justhodl-search-attention` | — | hard cap on names | fmp:movers-snapshots (E), polygon:options-contracts (F), fmp:institutional-13f (E) |
| `justhodl-self-improvement` | fmp | hard cap on names | fmp:movers-snapshots (E), polygon:short-interest-volume (F), fmp:shares-float-all (F) |
| `justhodl-shadow-lab` | — | hard cap on names | polygon:eod-per-ticker (H), fmp:estimates-targets-grades (F), fmp:eod-bulk (H) |
| `justhodl-signal-board` | — | — | fmp:ma-and-capital-feeds (E), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-signal-genealogy` | — | — | polygon:eod-per-ticker (H), fmp:eod-bulk (H), fmp:ma-and-capital-feeds (E) |
| `justhodl-signal-orthogonality` | — | hard cap on names; curated / watchlist universe | fmp:revenue-segmentation (F), fmp:peers-bulk (F), eia:full-catalog (H) |
| `justhodl-signal-scorecard` | fmp polygon | hard cap on names | polygon:eod-per-ticker (H), fmp:sec-filings-feeds (E), fmp:eod-bulk (H) |
| `justhodl-spx-history` | — | hard cap on names | fmp:eod-bulk (H), te:macro-markets (F), polygon:eod-per-ticker (H) |
| `justhodl-stress-loadings` | polygon | — | polygon:eod-per-ticker (H), fmp:eod-bulk (H), fmp:macro-rates (F) |
| `justhodl-supply-chain-graph` | polygon | hard cap on names; curated / watchlist universe; Finviz universe | fmp:revenue-segmentation (F), fmp:peers-bulk (F), eia:full-catalog (H) |
| `justhodl-synthetic-monitor` | — | hard cap on names | eia:full-catalog (H), polygon:minute-bars (H), fmp:intraday-1min (H) |
| `justhodl-tape-truth` | cboe polygon | hard cap on names | polygon:minute-bars (H), fmp:intraday-1min (H), polygon:options-contracts (F) |
| `justhodl-tax-plan` | — | — | fmp:dividends-splits (H), fmp:institutional-13f (E), fmp:ratios-ttm-bulk (F) |
| `justhodl-technical-overlays` | — | — | polygon:short-interest-volume (F), fmp:shares-float-all (F), polygon:eod-per-ticker (H) |
| `justhodl-term-premium` | — | — | fmp:macro-rates (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-trade-nowcast` | fred te | hard cap on names | te:macro-markets (F), polygon:short-interest-volume (F), fmp:fail-to-deliver (H) |
| `justhodl-treasury-rehypo` | fred nyfed | hard cap on names | polygon:short-interest-volume (F), fmp:movers-snapshots (E), fmp:fail-to-deliver (H) |
| `justhodl-trend-reversal` | — | S&P 500 only; hard cap on names | polygon:eod-per-ticker (H), fmp:eod-bulk (H), fmp:profile-bulk (U) |
| `justhodl-tv-watchlist-tracker` | polygon | hard cap on names; curated / watchlist universe | polygon:eod-per-ticker (H), fmp:eod-bulk (H), fmp:estimates-targets-grades (F) |
| `justhodl-upside-radar` | fmp polygon | hard cap on names | fmp:statements-bulk (H), fmp:estimates-targets-grades (F), fmp:market-cap-history (H) |
| `justhodl-us10y-sentinel` | — | — | polygon:eod-per-ticker (H), fmp:eod-bulk (H), fmp:movers-snapshots (E) |
| `justhodl-uspto-patents` | — | hard cap on names | fmp:sec-filings-feeds (E), fmp:transcripts (E), cmc:crypto-market (F) |
| `justhodl-vol-radar` | — | hard cap on names | polygon:options-contracts (F), polygon:eod-per-ticker (H), fmp:macro-rates (F) |
| `justhodl-vol-regime` | fred polygon | hard cap on names; curated / watchlist universe | polygon:options-contracts (F), fmp:etf-holder-bulk (F), eia:full-catalog (H) |
| `justhodl-watchlist` | — | hard cap on names; curated / watchlist universe | fmp:institutional-13f (E), fmp:estimates-targets-grades (F), cmc:crypto-market (F) |
| `justhodl-wealth-plan` | — | — | te:macro-markets (F), fmp:institutional-13f (E), eia:full-catalog (H) |
| `justhodl-weekly-ai-review` | — | hard cap on names | fmp:news-feeds (E), fmp:institutional-13f (E), polygon:options-contracts (F) |
| `justhodl-whales` | — | hard cap on names | polygon:etf-global (F), fmp:institutional-13f (E), fmp:sec-filings-feeds (E) |
| `justhodl-yield-curve` | — | — | fmp:macro-rates (F), polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-aaii-sentiment` | — | — | polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-activist-radar` | sec | hard cap on names | fmp:sec-filings-feeds (E), fmp:institutional-13f (E) |
| `justhodl-ai` | — | hard cap on names | cmc:crypto-market (F), fmp:upgrades-downgrades-consensus-bulk (F) |
| `justhodl-alpha-confluence` | — | hard cap on names; curated / watchlist universe | fmp:key-metrics-ttm-bulk (F), fmp:scores-bulk (F) |
| `justhodl-analytics-snapshot` | — | hard cap on names | fmp:sec-filings-feeds (E), fmp:dcf-bulk (F) |
| `justhodl-asia-trade-full` | — | hard cap on names | polygon:short-interest-volume (F), fmp:fail-to-deliver (H) |
| `justhodl-ask` | — | hard cap on names | fmp:transcripts (E), polygon:etf-global (F) |
| `justhodl-asymmetric-hunter` | — | hard cap on names; curated / watchlist universe | fmp:calendars (E), fmp:ratios-ttm-bulk (F) |
| `justhodl-backfill-orchestrator` | — | — | fmp:ma-and-capital-feeds (E), fmp:dividends-splits (H) |
| `justhodl-backtest-harness` | fmp | hard cap on names | polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-bank-stress` | fred | hard cap on names | te:macro-markets (F), fmp:statements-bulk (H) |
| `justhodl-basket-var` | — | hard cap on names | polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-beaters-grader` | — | hard cap on names | fmp:estimates-targets-grades (F), fmp:earnings-surprises-bulk (F) |
| `justhodl-bis-gleif` | — | hard cap on names | fmp:profile-bulk (U), te:macro-markets (F) |
| `justhodl-brain-compiler` | — | hard cap on names | fmp:estimates-targets-grades (F), fmp:shares-float-all (F) |
| `justhodl-calibrator` | — | hard cap on names | polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-calls-research-audit` | — | hard cap on names | polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-canary-macro` | bls | hard cap on names | fmp:macro-rates (F), te:macro-markets (F) |
| `justhodl-census-us` | — | hard cap on names | fmp:profile-bulk (U), fmp:employees (F) |
| `justhodl-chain-resumer` | — | hard cap on names | fmp:statements-bulk (H), fmp:profile-bulk (U) |
| `justhodl-chart-vision` | — | hard cap on names | polygon:eod-per-ticker (H), fmp:earnings-surprises-bulk (F) |
| `justhodl-ciss-ai` | — | — | polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-commodity-curves` | fmp fred | hard cap on names | eia:full-catalog (H), fmp:sector-industry-pe-perf (F) |
| `justhodl-confluence-meta` | — | hard cap on names | fmp:eod-bulk (H), polygon:eod-per-ticker (H) |
| `justhodl-congress-direct` | — | hard cap on names | fmp:political-trades (E), fmp:sec-filings-feeds (E) |
| `justhodl-corp-bonds` | ecb | — | te:macro-markets (F), fmp:company-notes (F) |
| `justhodl-coverage-gap-report` | — | — | polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-credit-stress` | — | — | polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-cross-asset-confirm` | fred | hard cap on names | polygon:grouped-crypto-fx (H), polygon:market-plumbing (E) |
| `justhodl-cusip-map-rebuild` | sec | hard cap on names | fmp:sec-filings-feeds (E), fmp:institutional-13f (E) |
| `justhodl-dark-pool` | — | — | polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-data-census` | sec | hard cap on names | fmp:profile-bulk (U), te:macro-markets (F) |
| `justhodl-dealer-gex` | — | — | polygon:options-contracts (F), fmp:ma-and-capital-feeds (E) |
| `justhodl-dollar-radar` | — | — | polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-dr-snapshot` | — | hard cap on names | fmp:institutional-13f (E), fmp:estimates-targets-grades (F) |
| `justhodl-earnings` | — | hard cap on names; unified universe (data/universe.json) | fmp:transcripts (E), fmp:earnings-surprises-bulk (F) |
| `justhodl-earnings-tracker` | polygon | hard cap on names; curated / watchlist universe | fmp:earnings-surprises-bulk (F), fmp:calendars (E) |
| `justhodl-edgar-full-index` | sec | hard cap on names | fmp:sec-filings-feeds (E), fmp:upgrades-downgrades-consensus-bulk (F) |
| `justhodl-edge-discovery` | polygon | hard cap on names; curated / watchlist universe | polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-eia-bank` | eia | hard cap on names | eia:full-catalog (H), cmc:crypto-market (F) |
| `justhodl-engine-contribution` | — | — | polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-episode-reference` | fred | — | fmp:eod-bulk (H), fmp:institutional-13f (E) |
| `justhodl-epu` | — | — | te:macro-markets (F), fmp:news-feeds (E) |
| `justhodl-esi` | — | hard cap on names | fmp:earnings-surprises-bulk (F), te:macro-markets (F) |
| `justhodl-esma-ratings` | — | hard cap on names | te:macro-markets (F), fmp:upgrades-downgrades-consensus-bulk (F) |
| `justhodl-event-flow-monitor` | — | hard cap on names | fmp:institutional-13f (E), fmp:estimates-targets-grades (F) |
| `justhodl-fabrication-weekly` | — | hard cap on names | polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-fdic-bankfind` | — | hard cap on names | fmp:statements-bulk (H), fmp:market-cap-history (H) |
| `justhodl-fed-collateral` | nyfed | hard cap on names | polygon:short-interest-volume (F), fmp:shares-float-all (F) |
| `justhodl-fedwatch-rate-probability` | — | — | polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-fifx-vol-migration` | — | — | polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-finviz-groups` | — | hard cap on names; Finviz universe | fmp:sector-industry-pe-perf (F), fmp:market-cap-history (H) |
| `justhodl-finviz-news` | — | hard cap on names; Finviz universe | fmp:news-feeds (E), fmp:movers-snapshots (E) |
| `justhodl-fleet-integrity` | — | hard cap on names | fmp:statements-bulk (H), fmp:profile-bulk (U) |
| `justhodl-fomc-reaction` | fmp | — | fmp:macro-rates (F), fmp:etf-holdings-weightings (F) |
| `justhodl-funding-plumbing` | fred | hard cap on names | fmp:statements-bulk (H), fmp:sec-filings-feeds (E) |
| `justhodl-global-flows` | — | hard cap on names | fmp:news-feeds (E), cmc:crypto-market (F) |
| `justhodl-global-sovereign` | — | — | te:macro-markets (F), fmp:company-notes (F) |
| `justhodl-gsi-calibrator` | — | — | te:macro-markets (F), fmp:market-cap-history (H) |
| `justhodl-hedge-planner` | fmp | hard cap on names | polygon:options-contracts (F), fmp:etf-holder-bulk (F) |
| `justhodl-hist-banker` | sec | hard cap on names | fmp:sec-filings-feeds (E), cmc:crypto-market (F) |
| `justhodl-historical-analogs` | fred polygon yahoo | S&P 500 only; hard cap on names | fmp:eod-bulk (H), polygon:eod-per-ticker (H) |
| `justhodl-house-ptr-extract` | — | hard cap on names | fmp:political-trades (E), fmp:sec-filings-feeds (E) |
| `justhodl-insider-trades` | sec | hard cap on names; unified universe (data/universe.json) | fmp:sec-filings-feeds (E), fmp:insider-feeds (E) |
| `justhodl-inventory-drawdown` | fmp fred | hard cap on names | eia:full-catalog (H), fmp:revenue-segmentation (F) |
| `justhodl-journal-grader` | fmp | hard cap on names | fmp:estimates-targets-grades (F), fmp:earnings-surprises-bulk (F) |
| `justhodl-khalid` | — | hard cap on names | fmp:macro-rates (F), polygon:eod-per-ticker (H) |
| `justhodl-khalid-risk` | — | hard cap on names | fmp:fail-to-deliver (H), polygon:short-interest-volume (F) |
| `justhodl-leadlag-graph` | fmp polygon | hard cap on names | polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-liquidity-agent` | — | — | polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-liquidity-profile` | polygon | hard cap on names | polygon:minute-bars (H), fmp:intraday-1min (H) |
| `justhodl-liquidity-pulse` | — | — | polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-macro-attribution` | fred | hard cap on names; curated / watchlist universe | te:macro-markets (F), fmp:profile-bulk (U) |
| `justhodl-market-internals` | — | — | polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-naaim` | — | hard cap on names | polygon:options-contracts (F), fmp:institutional-13f (E) |
| `justhodl-nyfed-cmdi` | nyfed | — | fmp:scores-bulk (F), fmp:estimates-targets-grades (F) |
| `justhodl-nyfed-dealer-survey` | nyfed | — | polygon:options-contracts (F), fmp:statements-bulk (H) |
| `justhodl-oecd-cli` | — | hard cap on names | te:macro-markets (F), fmp:scores-bulk (F) |
| `justhodl-ofr-stfm` | — | hard cap on names | polygon:options-contracts (F), fmp:institutional-13f (E) |
| `justhodl-outcome-checker` | coingecko fmp polygon yahoo | S&P 500 only; hard cap on names | polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-paper-book` | polygon | — | fmp:etf-holder-bulk (F), fmp:sec-filings-feeds (E) |
| `justhodl-playbook-engine` | — | hard cap on names | fmp:macro-rates (F), polygon:options-contracts (F) |
| `justhodl-plumbing-composite` | — | hard cap on names | fmp:fail-to-deliver (H), polygon:short-interest-volume (F) |
| `justhodl-pnl-tracker` | — | hard cap on names | polygon:minute-bars (H), fmp:movers-snapshots (E) |
| `justhodl-portfolio-sizer` | polygon | hard cap on names | polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-portfolio-snapshot` | — | curated / watchlist universe | fmp:dividends-splits (H), polygon:eod-per-ticker (H) |
| `justhodl-prediction-snapshotter` | — | hard cap on names | polygon:options-contracts (F), fmp:movers-snapshots (E) |
| `justhodl-proven-portfolio` | yahoo | hard cap on names | fmp:etf-holder-bulk (F), fmp:estimates-targets-grades (F) |
| `justhodl-redflag-alerter` | — | hard cap on names | fmp:sec-filings-feeds (E), fmp:employees (F) |
| `justhodl-refining-stress` | fmp fred | hard cap on names | eia:full-catalog (H), fmp:institutional-13f (E) |
| `justhodl-regime-anomaly` | — | — | polygon:options-contracts (F), polygon:grouped-crypto-fx (H) |
| `justhodl-regime-conditional-router` | — | hard cap on names | fmp:macro-rates (F), fmp:institutional-13f (E) |
| `justhodl-regime-conditional-trust` | polygon | hard cap on names | polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-repo-market` | fred nyfed | hard cap on names | polygon:short-interest-volume (F), fmp:shares-float-all (F) |
| `justhodl-research-papers` | fmp | S&P 500 only; hard cap on names | fmp:key-metrics-ttm-bulk (F), fmp:dcf-bulk (F) |
| `justhodl-risk-monitor` | — | — | fmp:market-cap-history (H), fmp:institutional-13f (E) |
| `justhodl-scheduler` | — | hard cap on names | polygon:minute-bars (H), fmp:intraday-1min (H) |
| `justhodl-sdmx-walker` | ecb | hard cap on names | te:macro-markets (F), fmp:etf-holdings-weightings (F) |
| `justhodl-seasonality` | polygon | hard cap on names | polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-sec-13f` | sec | hard cap on names; curated / watchlist universe | fmp:sec-filings-feeds (E), fmp:institutional-13f (E) |
| `justhodl-sec-8k-enrich` | sec | hard cap on names | fmp:sec-filings-feeds (E), fmp:news-feeds (E) |
| `justhodl-sector-tilt` | — | — | polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-signal-backtest` | — | hard cap on names | polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-signal-halflife` | — | hard cap on names | polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-sovereign-fiscal` | fred treasury | hard cap on names | te:macro-markets (F), cmc:crypto-market (F) |
| `justhodl-student-rsi` | — | — | polygon:eod-per-ticker (H), fmp:ma-and-capital-feeds (E) |
| `justhodl-symbol-resolver` | fred | hard cap on names; curated / watchlist universe | fmp:cot (H), te:macro-markets (F) |
| `justhodl-symbology-master` | sec | hard cap on names | fmp:profile-bulk (U), fmp:sec-filings-feeds (E) |
| `justhodl-taiwan-moea` | — | hard cap on names | fmp:revenue-segmentation (F), fmp:peers-bulk (F) |
| `justhodl-tenor-signal-interpreter` | fred treasury | — | polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-tiingo-news` | — | hard cap on names | fmp:news-feeds (E), fmp:movers-snapshots (E) |
| `justhodl-track-record` | — | — | polygon:options-contracts (F), fmp:earnings-surprises-bulk (F) |
| `justhodl-trade-evaluator` | polygon | hard cap on names | polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-tv-notes-ingest` | tradingview | hard cap on names; curated / watchlist universe | cmc:crypto-market (F), polygon:options-contracts (F) |
| `justhodl-usd-funding` | fred nyfed | hard cap on names | fmp:macro-rates (F), eia:full-catalog (H) |
| `justhodl-vintage-fred` | fred | — | polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-vix-curve` | cboe | hard cap on names | fmp:profile-bulk (U), fmp:estimates-targets-grades (F) |
| `justhodl-vol-surface` | — | — | polygon:eod-per-ticker (H), fmp:eod-bulk (H) |
| `justhodl-world-uncertainty-index` | — | — | te:macro-markets (F), polygon:eod-per-ticker (H) |
| `justhodl-xbrl-fundamentals` | sec | hard cap on names; Finviz universe | fmp:sec-filings-feeds (E), fmp:statements-bulk (H) |
| `justhodl-yen-carry` | fred | hard cap on names | polygon:grouped-crypto-fx (H), fmp:cot (H) |
| `scrapeMacroData` | — | — | te:macro-markets (F), polygon:minute-bars (H) |
| `cftc-futures-positioning-agent` | polygon | hard cap on names | fmp:cot (H) |
| `ecb` | — | hard cap on names | te:macro-markets (F) |
| `ecb-auto-updater` | ecb | — | te:macro-markets (F) |
| `ecb-data-daily-updater` | — | — | te:macro-markets (F) |
| `eia-energy-agent` | eia fred | hard cap on names | eia:full-catalog (H) |
| `fmp-fundamentals-agent` | — | curated / watchlist universe | polygon:market-plumbing (E) |
| `fmp-stock-picks-agent` | ecb fred | — | fmp:rating-bulk (F) |
| `justhodl-ab-test` | — | — | fmp:eod-bulk (H) |
| `justhodl-auction-tail` | — | hard cap on names | fmp:estimates-targets-grades (F) |
| `justhodl-bis-crossborder` | — | hard cap on names | fmp:macro-rates (F) |
| `justhodl-blackswan-watch` | — | S&P 500 only; hard cap on names; curated / watchlist universe | fmp:institutional-13f (E) |
| `justhodl-boj-full` | — | hard cap on names | te:macro-markets (F) |
| `justhodl-bond-regime-detector` | — | — | fmp:institutional-13f (E) |
| `justhodl-calibration-snapshotter` | — | — | polygon:eod-per-ticker (H) |
| `justhodl-cascade-recalibrator` | — | hard cap on names | fmp:movers-snapshots (E) |
| `justhodl-cb-injection` | ecb fred | hard cap on names | te:macro-markets (F) |
| `justhodl-cftc-full-datasets` | — | hard cap on names | fmp:cot (H) |
| `justhodl-concentration-liquidity` | fmp | hard cap on names; curated / watchlist universe | fmp:market-cap-history (H) |
| `justhodl-contract-gate` | — | hard cap on names | fmp:ma-and-capital-feeds (E) |
| `justhodl-cot-extremes-scanner` | — | hard cap on names; curated / watchlist universe | fmp:cot (H) |
| `justhodl-cot-feed` | — | hard cap on names; curated / watchlist universe | fmp:cot (H) |
| `justhodl-cq-feed` | cq | hard cap on names | cmc:crypto-market (F) |
| `justhodl-credit-composite` | — | — | polygon:options-contracts (F) |
| `justhodl-cro-digest` | — | hard cap on names | polygon:options-contracts (F) |
| `justhodl-cycle-features` | — | hard cap on names | te:macro-markets (F) |
| `justhodl-earnings-transcripts` | — | hard cap on names | fmp:transcripts (E) |
| `justhodl-ecb-history` | ecb | hard cap on names | te:macro-markets (F) |
| `justhodl-engine-conflicts` | — | hard cap on names | fmp:news-feeds (E) |
| `justhodl-engine-leaderboard` | — | hard cap on names | fmp:estimates-targets-grades (F) |
| `justhodl-etf-constituents` | — | — | fmp:etf-holdings-weightings (F) |
| `justhodl-eurostat-history` | — | hard cap on names | te:macro-markets (F) |
| `justhodl-eurostat-oecd` | — | hard cap on names | te:macro-markets (F) |
| `justhodl-families-feed` | — | — | te:macro-markets (F) |
| `justhodl-fao-food-price-index` | — | — | cmc:crypto-market (F) |
| `justhodl-fed-nlp` | — | hard cap on names | fmp:transcripts (E) |
| `justhodl-feedback` | — | hard cap on names | cmc:crypto-market (F) |
| `justhodl-fhfa-hpi` | — | — | fmp:profile-bulk (U) |
| `justhodl-fiat-peg-monitor` | fred polygon | hard cap on names | te:macro-markets (F) |
| `justhodl-firm-stress` | — | — | fmp:institutional-13f (E) |
| `justhodl-fiscaldata-full` | treasury | hard cap on names; curated / watchlist universe | fmp:sec-filings-feeds (E) |
| `justhodl-fleet-freshness-monitor` | — | hard cap on names | polygon:options-contracts (F) |
| `justhodl-flow-lookthrough` | — | — | fmp:etf-holdings-weightings (F) |
| `justhodl-foreign-flows` | fred treasury | hard cap on names | fmp:dividends-splits (H) |
| `justhodl-fragile-states-index` | — | — | eia:full-catalog (H) |
| `justhodl-freight-pulse` | eia fred | hard cap on names | te:macro-markets (F) |
| `justhodl-gdelt-full` | — | hard cap on names | polygon:minute-bars (H) |
| `justhodl-gov-sources` | ecb fred treasury | hard cap on names | fmp:institutional-13f (E) |
| `justhodl-import-sentinel` | — | hard cap on names | fmp:sector-industry-pe-perf (F) |
| `justhodl-interpretation-grader` | polygon | hard cap on names | fmp:estimates-targets-grades (F) |
| `justhodl-jsi-calibrator` | fred | S&P 500 only | fmp:market-cap-history (H) |
| `justhodl-labor-leading` | fred | — | fmp:employees (F) |
| `justhodl-llm-cost-dashboard` | — | hard cap on names | fmp:governance-execs (F) |
| `justhodl-ma200-reclaim` | polygon | hard cap on names | fmp:estimates-targets-grades (F) |
| `justhodl-macro-nowcast` | — | — | te:macro-markets (F) |
| `justhodl-massive-signals` | — | — | polygon:options-contracts (F) |
| `justhodl-merger-arb-risk` | — | — | fmp:ma-and-capital-feeds (E) |
| `justhodl-methodology-scout` | coingecko | hard cap on names; curated / watchlist universe | fmp:estimates-targets-grades (F) |
| `justhodl-near-miss-monitor` | — | hard cap on names | fmp:earnings-surprises-bulk (F) |
| `justhodl-news-wire` | fmp newsapi | hard cap on names | fmp:news-feeds (E) |
| `justhodl-notes-intel` | — | hard cap on names | polygon:grouped-crypto-fx (H) |
| `justhodl-nyfed-markets-full` | nyfed | hard cap on names | polygon:options-contracts (F) |
| `justhodl-options-flow` | fred polygon | hard cap on names | polygon:options-contracts (F) |
| `justhodl-phase-detector` | polygon | hard cap on names | fmp:revenue-segmentation (F) |
| `justhodl-pnl-attribution` | polygon | — | polygon:eod-per-ticker (H) |
| `justhodl-polygon-eod` | — | hard cap on names | polygon:market-plumbing (E) |
| `justhodl-polygon-full` | polygon | hard cap on names | polygon:market-plumbing (E) |
| `justhodl-polygon-futures-curves` | — | — | polygon:minute-bars (H) |
| `justhodl-port-cargo` | — | hard cap on names | fmp:revenue-segmentation (F) |
| `justhodl-portfolio-admin` | — | curated / watchlist universe | cmc:crypto-market (F) |
| `justhodl-portwatch` | — | hard cap on names | fmp:revenue-segmentation (F) |
| `justhodl-proven-alpha` | — | hard cap on names | fmp:estimates-targets-grades (F) |
| `justhodl-push-api` | — | hard cap on names | cmc:crypto-market (F) |
| `justhodl-regsho` | — | — | fmp:profile-bulk (U) |
| `justhodl-sec-bulk` | sec | hard cap on names | fmp:sec-filings-feeds (E) |
| `justhodl-sector-rotation` | — | — | fmp:sector-industry-pe-perf (F) |
| `justhodl-sentiment-surveys` | — | hard cap on names | cmc:crypto-market (F) |
| `justhodl-short-pressure` | — | — | polygon:short-interest-volume (F) |
| `justhodl-singapore-nodx` | — | hard cap on names | eia:full-catalog (H) |
| `justhodl-sizing-engine` | polygon | hard cap on names | fmp:institutional-13f (E) |
| `justhodl-source-map` | — | — | fmp:profile-bulk (U) |
| `justhodl-stealth-flow` | — | hard cap on names | polygon:etf-global (F) |
| `justhodl-stock-ai-research` | — | hard cap on names | fmp:rating-bulk (F) |
| `justhodl-stock-analyzer` | — | hard cap on names | fmp:rating-bulk (F) |
| `justhodl-strategist` | — | hard cap on names | fmp:news-feeds (E) |
| `justhodl-supply-inflection-scanner` | fred | hard cap on names | fmp:sector-industry-pe-perf (F) |
| `justhodl-switzerland` | fred yahoo | hard cap on names | te:macro-markets (F) |
| `justhodl-tail-hedge` | — | — | polygon:options-contracts (F) |
| `justhodl-tail-risk` | — | — | polygon:options-contracts (F) |
| `justhodl-te-feed` | te | hard cap on names | te:macro-markets (F) |
| `justhodl-te-fred-mirror` | fred te | hard cap on names | te:macro-markets (F) |
| `justhodl-tic-full` | treasury | hard cap on names | fmp:macro-rates (F) |
| `justhodl-ticker-trends` | fmp | hard cap on names | fmp:movers-snapshots (E) |
| `justhodl-ticket-ai-rationale` | — | hard cap on names | polygon:options-contracts (F) |
| `justhodl-trade-logger` | polygon | hard cap on names | polygon:options-contracts (F) |
| `justhodl-trade-ticket-monitor` | polygon | hard cap on names | polygon:minute-bars (H) |
| `justhodl-trade-tickets` | polygon | hard cap on names | polygon:minute-bars (H) |
| `justhodl-tradingview` | ecb fmp fred treasury | hard cap on names; curated / watchlist universe; Finviz universe | te:macro-markets (F) |
| `justhodl-transcript-query` | — | hard cap on names | fmp:transcripts (E) |
| `justhodl-treasury-noise` | fred | hard cap on names | eia:full-catalog (H) |
| `justhodl-tv-notes-crawler` | tradingview | hard cap on names; curated / watchlist universe | cmc:crypto-market (F) |
| `justhodl-usgov-direct` | bea bls | hard cap on names | te:macro-markets (F) |
| `justhodl-valuations-agent` | — | — | fmp:dcf-bulk (F) |
| `justhodl-vrp` | — | — | polygon:options-contracts (F) |
| `justhodl-warm-bridge` | bea bls nyfed treasury | hard cap on names | te:macro-markets (F) |
| `openbb-websocket-broadcast` | — | — | cmc:crypto-market (F) |
