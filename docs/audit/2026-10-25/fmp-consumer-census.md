# FMP consumer census — 2026-10-25

229 engines call FMP directly. Families: inline-urls 52, many-helpers 64, one-helper 110, shared-client 3.

## Conversion plan

1. shared-client (done): fmp_stable / equity_enrich -> fmp_book.try_harvest -> fmp_bank (bulk bank, then walker bank) before any live call.
2. one-helper: the engine has exactly one function that builds FMP URLs - wrap it: try fmp_bank.try_bank(path, qs) first, fall through to the existing code. One patch pattern, one test per engine, deploy in batches of ~20 behind the deploy gate.
3. many-helpers: same wrap per helper; review each engine's helpers (list above) before patching.
4. inline-urls: URLs built inline at call sites - route through a new module-level helper first, then the wrap. Review per engine.
5. legacy api/v3-v4 callers: switch to the /stable path while wrapping (legacy answers 403 since 2025-08-31).

## Top endpoints (engines calling)

| endpoint | engines |
|---|---|
| `stable/quote` | 50 |
| `stable/historical-price-eod/full` | 45 |
| `stable/{..}` | 38 |
| `stable/historical-price-eod/light` | 22 |
| `stable/profile` | 21 |
| `stable/income-statement` | 9 |
| `stable/cash-flow-statement` | 5 |
| `stable/earnings-calendar` | 5 |
| `stable/analyst-estimates` | 5 |
| `stable/historical-price-` | 4 |
| `stable/historical-price-eod` | 4 |
| `stable/company-screener` | 4 |
| `stable/key-metrics` | 4 |
| `stable/balance-sheet-statement` | 4 |
| `stable/batch-quote` | 3 |
| `stable/ratios-ttm` | 3 |
| `stable/key-metrics-ttm` | 3 |
| `stable/ratios` | 3 |
| `stable/sp500-constituent` | 3 |
| `stable/earnings` | 3 |
| `stable/insider-trading/latest` | 3 |
| `stable/stock-price-change` | 2 |
| `stable/earning-call-transcript` | 2 |
| `stable/economic-calendar` | 2 |
| `stable/etf/holdings` | 2 |
| `stable/cusip` | 1 |
| `stable/news/general-latest` | 1 |
| `stable/dividends` | 1 |
| `stable/profile-bulk` | 1 |
| `stable/earning-call-transcript-dates` | 1 |
| `stable/grades` | 1 |
| `stable/insider-trading/search` | 1 |
| `stable/enterprise-values` | 1 |
| `stable/ipos-calendar` | 1 |
| `stable/stock-peers` | 1 |
| `stable/stock-list` | 1 |
| `stable/financial-growth` | 1 |
| `stable/news/stock` | 1 |
| `stable/etf-holdings` | 1 |

## Engines by family

### shared-client (3)

- `justhodl-etf-constituents` — 0 endpoints; helpers: -
- `justhodl-industry-rotation` — 1 endpoints; helpers: fmp_holdings(tkr), fmp_scores(tkr), fmp_growth(tkr), lambda_handler(event,context), fmp_estimates(tkr), fmp_pe_ttm(tkr), fmp_quotes(tickers)
- `justhodl-retail-sentiment` — 0 endpoints; helpers: -

### one-helper (110)

- `justhodl-13f-clone-alpha` — 1 endpoints; no tests dir; helpers: hist_closes(sym,frm)
- `justhodl-52wk-quality-breakout` — 1 endpoints; no tests dir; helpers: fmp_get(path,params)
- `justhodl-activist-13d` — 1 endpoints; no tests dir; helpers: fetch_fmp_quote(ticker)
- `justhodl-ai-infra-stack` — 1 endpoints; helpers: _fmp(path)
- `justhodl-ai-rerating-radar` — 1 endpoints; helpers: _fmp(path,tries)
- `justhodl-alpha-compass` — 1 endpoints; helpers: fmp_eod(ticker,dfrom,dto)
- `justhodl-analyst-consensus` — 1 endpoints; no tests dir; helpers: fmp_get(path,params,retries,timeout)
- `justhodl-apac-flows` — 2 endpoints; helpers: us_returns(symbols)
- `justhodl-apac-leadlag` — 2 endpoints; no tests dir; helpers: us_prices(symbols,key,start)
- `justhodl-backtest-harness` — 1 endpoints; helpers: mode_b(uni_full,spy,last_date)
- `justhodl-bagger-engine` — 1 endpoints; no tests dir; helpers: fmp(path,symbol,limit)
- `justhodl-bond-desk` — 1 endpoints; helpers: _fmp_hist(sym,frm)
- `justhodl-bottleneck-boom` — 1 endpoints; helpers: fmp(path,params)
- `justhodl-breadth-divergence` — 1 endpoints; no tests dir; helpers: fmp_history(symbol,days)
- `justhodl-breadth-thrust` — 0 endpoints; no tests dir; helpers: _fmp_get(path,params)
- `justhodl-buyback-scanner` — 0 endpoints; no tests dir; helpers: fmp_json(path,params,timeout)
- `justhodl-capital-flow` — 0 endpoints; helpers: legacy_lambda_handler(event,context)
- `justhodl-cascade-validator` — 1 endpoints; no tests dir; helpers: fetch_fmp_history(ticker,days)
- `justhodl-catalyst-calendar` — 1 endpoints; no tests dir; helpers: fda_events(window_days)
- `justhodl-cds-monitor` — 1 endpoints; no tests dir; helpers: fmp(path,params)
- `justhodl-cef-discount` — 1 endpoints; no tests dir; helpers: fmp_get(path,params)
- `justhodl-chart-vision` — 0 endpoints; no tests dir; helpers: fmp_get(path,params)
- `justhodl-coffee-can` — 1 endpoints; no tests dir; helpers: fmp(path,symbol,limit)
- `justhodl-concentration-liquidity` — 1 endpoints; no tests dir; helpers: fetch_adv_for_symbol(symbol)
- `justhodl-credit-equity-divergence` — 1 endpoints; no tests dir; helpers: fmp_history(symbol,days)
- `justhodl-cro-escalation` — 1 endpoints; helpers: fmp_quote(symbol)
- `justhodl-cross-asset-regime` — 1 endpoints; no tests dir; helpers: fetch_history(symbol,days)
- `justhodl-cross-asset-rv` — 1 endpoints; no tests dir; helpers: fetch_fmp(symbol)
- `justhodl-crypto-etf-arb` — 1 endpoints; no tests dir; helpers: fmp_quotes(symbols)
- `justhodl-cta-trend-exhaust` — 1 endpoints; no tests dir; helpers: fmp_history(symbol,days)
- `justhodl-deal-scanner` — 1 endpoints; helpers: _fmp(path,retries)
- `justhodl-divcut-warning` — 1 endpoints; no tests dir; helpers: fmp_get(path,params)
- `justhodl-divergence-engine-v2` — 1 endpoints; no tests dir; helpers: fetch_etf(symbol,days)
- `justhodl-dxy-equity-divergence` — 1 endpoints; no tests dir; helpers: fmp_history(symbol,days)
- `justhodl-earnings-blackout` — 2 endpoints; no tests dir; helpers: lambda_handler(event,context)
- `justhodl-econ-calendar` — 1 endpoints; no tests dir; helpers: fmp_calendar(d_from,d_to)
- `justhodl-edgar-authority` — 0 endpoints; no tests dir; helpers: _fmp(ep,params)
- `justhodl-equity-ftd` — 1 endpoints; no tests dir; helpers: fmp_vol_price(sym,key)
- `justhodl-etf-census` — 0 endpoints; helpers: fmp(qs)
- `justhodl-eurodollar-plumbing` — 1 endpoints; helpers: fmp_fx(symbol)
- `justhodl-fi-census` — 0 endpoints; no tests dir; helpers: fmp(qs)
- `justhodl-fleet-monitor` — 1 endpoints; helpers: probe_dependencies()
- `justhodl-fomc-reaction` — 1 endpoints; helpers: fmp_closes(symbol,frm)
- `justhodl-forward-orders` — 1 endpoints; no tests dir; helpers: fmp_get(path)
- `justhodl-fundamentals-engine` — 1 endpoints; no tests dir; helpers: fmp_get(path,params,retries,timeout)
- `justhodl-gf-value` — 1 endpoints; no tests dir; helpers: fmp(path)
- `justhodl-global-macro` — 1 endpoints; no tests dir; helpers: fetch_fmp_history(ticker,n_days)
- `justhodl-hedge-planner` — 1 endpoints; helpers: fmp_quote(symbol,fmp_key)
- `justhodl-hedge-pnl` — 0 endpoints; helpers: fmp_history(symbol)
- `justhodl-hkma-monitor` — 1 endpoints; no tests dir; helpers: fmp_spot(symbol)
- `justhodl-ignition` — 1 endpoints; helpers: fmp(path,params)
- `justhodl-impact-graph` — 1 endpoints; helpers: build_graph(gaps)
- `justhodl-index-inclusion` — 1 endpoints; no tests dir; helpers: fmp(path,params)
- `justhodl-insider-aggregate` — 2 endpoints; helpers: fetch_insider_page(page)
- `justhodl-insider-buyback-confluence` — 1 endpoints; no tests dir; helpers: fmp_quote(symbol)
- `justhodl-insider-cluster-scanner` — 0 endpoints; no tests dir; helpers: fmp_get(path,params,timeout)
- `justhodl-ipo-pipeline` — 1 endpoints; no tests dir; helpers: fmp(path)
- `justhodl-journal-grader` — 1 endpoints; helpers: price_now(ticker)
- `justhodl-katlin` — 1 endpoints; helpers: fmp_shares_yoy(sym,today)
- `justhodl-leading-markets` — 1 endpoints; no tests dir; helpers: fetch_history(sym,days)
- `justhodl-leadlag-graph` — 2 endpoints; no tests dir; helpers: hist(sym)
- `justhodl-macro-leads` — 1 endpoints; helpers: fmp_hist(symbol,days)
- `justhodl-magic-formula` — 1 endpoints; no tests dir; helpers: fmp(path)
- `justhodl-market-tape` — 1 endpoints; helpers: fmp_quote(symbol,now)
- `justhodl-mean-reversion` — 1 endpoints; no tests dir; helpers: fmp_ratios(symbol)
- `justhodl-multi-tf-convergence` — 1 endpoints; no tests dir; helpers: fmp_get(path,params)
- `justhodl-news-wire` — 1 endpoints; no tests dir; helpers: fetch_fmp_news()
- `justhodl-outcome-checker` — 1 endpoints; helpers: get_current_price_fmp(ticker)
- `justhodl-political-intel` — 1 endpoints; no tests dir; helpers: fetch_congress_trades()
- `justhodl-positioning-analog` — 1 endpoints; no tests dir; helpers: spx_closes()
- `justhodl-pre-pump-detector` — 1 endpoints; no tests dir; helpers: fetch_history(symbol,days)
- `justhodl-precatalyst-vol-expansion` — 1 endpoints; no tests dir; helpers: fmp_get(path,params)
- `justhodl-pump-earnings-nlp` — 1 endpoints; no tests dir; helpers: fetch_transcript(ticker,year,quarter)
- `justhodl-radar-backtest` — 1 endpoints; no tests dir; helpers: fetch_price_history(ticker)
- `justhodl-rating-change-cluster` — 1 endpoints; no tests dir; helpers: fmp_get(path,params)
- `justhodl-rebalance-radar` — 1 endpoints; no tests dir; helpers: _hist(sym,days)
- `justhodl-refining-stress` — 1 endpoints; no tests dir; helpers: fmp_hist(symbol,days)
- `justhodl-reit-nav-discount` — 1 endpoints; no tests dir; helpers: fmp_get(path,params)
- `justhodl-research-papers` — 1 endpoints; no tests dir; helpers: gather(t,slim_row,sv,sec_rank,macro_line)
- `justhodl-rotation-chain` — 1 endpoints; no tests dir; helpers: fmp_history(ticker,lookback_days)
- `justhodl-rv-iv-scanner` — 1 endpoints; no tests dir; helpers: fmp_ohlc(ticker,n)
- `justhodl-sector-earnings-diffusion` — 1 endpoints; no tests dir; helpers: fetch_estimates(symbol)
- `justhodl-self-improvement` — 1 endpoints; no tests dir; helpers: fetch_fmp_history(ticker)
- `justhodl-sellside-views` — 1 endpoints; no tests dir; helpers: fmp_get(path,params)
- `justhodl-shadow-lab` — 0 endpoints; no tests dir; helpers: ohlc(sym,n)
- `justhodl-signal-backtest` — 0 endpoints; no tests dir; helpers: batch_quotes(tickers)
- `justhodl-signal-harvester` — 1 endpoints; helpers: get_price(sym,identity)
- `justhodl-signal-logger` — 1 endpoints; helpers: _fmp_stable(ticker)
- `justhodl-signal-scorecard` — 1 endpoints; helpers: _spy_history()
- `justhodl-smart-money-cluster` — 1 endpoints; helpers: fetch_fmp_quote(ticker,key)
- `justhodl-spac-floor-warrant` — 1 endpoints; no tests dir; helpers: fmp_get(path,params)
- `justhodl-spx-ma` — 2 endpoints; no tests dir; helpers: breadth_quotes(tks)
- `justhodl-stock-buying` — 0 endpoints; helpers: fmp(path,qs)
- `justhodl-structural-pre-signals` — 1 endpoints; no tests dir; helpers: _fmp(path,tries)
- `justhodl-sympathetic-momentum` — 1 endpoints; no tests dir; helpers: fmp_history(symbol,days)
- `justhodl-system-signal-logger` — 1 endpoints; no tests dir; helpers: _quote_price(symbol)
- `justhodl-talent-migration` — 1 endpoints; no tests dir; helpers: _fmp(path,tries)
- `justhodl-telegram-bot` — 1 endpoints; helpers: cmd_stock(chat_id,ticker)
- `justhodl-theme-rotation` — 2 endpoints; helpers: hist(sym)
- `justhodl-theme-second-wave` — 1 endpoints; helpers: fmp_1m(symbol)
- `justhodl-ticker-trends` — 1 endpoints; no tests dir; helpers: get_recent_price_perf(ticker,days)
- `justhodl-tradingview` — 2 endpoints; helpers: source_url(provider,symbol,kind,request)
- `justhodl-universe-discovery` — 1 endpoints; no tests dir; helpers: _fmp(path,tries)
- `justhodl-upside-radar` — 2 endpoints; no tests dir; helpers: fmp_anatomy(t)
- `justhodl-vix-backwardation-trigger` — 0 endpoints; no tests dir; helpers: _fmp_get(path,params)
- `justhodl-vrp` — 0 endpoints; helpers: _fmp_eod(endpoint,d_from)
- `justhodl-warroom-weights` — 0 endpoints; no tests dir; helpers: fmp_monthly(sym,start)
- `justhodl-watchlist-debate` — 2 endpoints; no tests dir; helpers: get_ticker_metrics(ticker)
- `justhodl-wave-signal-logger` — 1 endpoints; helpers: _fmp_quote(ticker)
- `justhodl-whales` — 0 endpoints; helpers: fetch_positions(cik,year,quarter)

### many-helpers (64)

- `justhodl-13f-positions` — 6 endpoints; helpers: cusip_to_ticker_via_fmp(cusip,name), _hist_closes(sym,frm), _batch_quotes(symbols), _purge_collisions(m,fmp_budget), legacy_lambda_handler(event,context), _enrich(tk)
- `justhodl-13f-price-divergence` — 2 endpoints; no tests dir; helpers: fmp_quote(symbol), fmp_history_returns(symbol)
- `justhodl-asia-leads` — 1 endpoints; helpers: identity(request), korea_flash_tape()
- `justhodl-asset-compass` — 1 endpoints; no tests dir; helpers: lambda_handler(event,context), _cl1y(sym)
- `justhodl-backlog` — 1 endpoints; helpers: fetch_fmp(sym,ep,fetch), endpoint(url), replay(packet,read), acquire(self,url,t)
- `justhodl-buyback-engine` — 1 endpoints; helpers: fmp(path,retries,capture), endpoint(url), replay(packet,read), acquire(self,url,t), logical(records,path)
- `justhodl-buyback-yield-ranking` — 2 endpoints; no tests dir; helpers: fmp_quote(symbol), fmp_cash_flow_ttm(symbol)
- `justhodl-buzz-velocity` — 2 endpoints; no tests dir; helpers: get_universe(), get_recent_price_perf(ticker,days)
- `justhodl-capex-pulse` — 2 endpoints; helpers: _usd_per(ccy), _fmp_cf(sym)
- `justhodl-carry-surface` — 4 endpoints; helpers: fmp_url(symbol,path), fmp_quote_with_history(symbol,days), _dy_from_ratios_ttm(symbol), _dy_from_dividends(symbol,price), fmp_buyback_yield(symbol), acquire(client,bucket,url,key,key_name,deadline)
- `justhodl-chokepoint` — 5 endpoints; helpers: fetch_bulk_universe(), evaluate(sym,centrality,rerate_lookup,bulk)
- `justhodl-commodity-curves` — 2 endpoints; no tests dir; helpers: fetch_quote(sym), fetch_history_etf(sym,days)
- `justhodl-cyclical-bagger` — 3 endpoints; no tests dir; helpers: profile(sym), survival(sym), discriminators(sym)
- `justhodl-deep-value-screener` — 5 endpoints; no tests dir; helpers: fetch_profile(symbol), fetch_balance(symbol), fetch_income(symbol), fetch_cashflow(symbol), fetch_quote(symbol)
- `justhodl-earnings-iv-crush` — 3 endpoints; no tests dir; helpers: fmp_earnings_calendar(days_ahead), fmp_history(symbol,days), fmp_profile(symbol)
- `justhodl-earnings-nlp` — 2 endpoints; no tests dir; helpers: get_latest_transcript_meta(ticker), fetch_transcript_content(ticker,year,quarter)
- `justhodl-earnings-pead` — 2 endpoints; helpers: fetch_earnings_surprises(symbol,limit), fetch_recent_history(symbol,days), _pead_fetch(ticker,endpoint), lambda_handler(event,context)
- `justhodl-earnings-quality` — 5 endpoints; helpers: fmp_quote(symbol), fmp_financials(symbol), _accounting_acquire(symbol)
- `justhodl-eps-revision-velocity` — 3 endpoints; helpers: fetch_quote(symbol), fetch_estimates(symbol), fetch_ratings_history(symbol), _eps_fetch(ticker,endpoint), lambda_handler(event,context)
- `justhodl-estimate-revisions` — 1 endpoints; helpers: fmp_estimate_profile(ticker), _estimate_fetch(symbol), lambda_handler(event,context)
- `justhodl-etf-true-flows` — 0 endpoints; helpers: collect(client,bucket,key,read,deadline), vendor_context(ticker,originals,read,at)
- `justhodl-failed-pattern-reversal` — 2 endpoints; no tests dir; helpers: fmp_history(symbol,days), fmp_profile(symbol)
- `justhodl-forward-returns` — 3 endpoints; no tests dir; helpers: fmp_quote(symbol), fmp_profile(symbol), fmp_history(symbol,days)
- `justhodl-gap-fill-confirm` — 3 endpoints; no tests dir; helpers: fmp_history_full(symbol,days), fmp_quote(symbol), fmp_earnings_calendar(symbol)
- `justhodl-gap-metrics` — 0 endpoints; no tests dir; helpers: _stock_breadth(), m_miner_margin()
- `justhodl-global-markets` — 2 endpoints; no tests dir; helpers: fetch_quote(sym), fetch_history(sym,days)
- `justhodl-global-stress` — 0 endpoints; helpers: safe_url(v,path), collect(client,bucket,fred_key,fmp_key,transport)
- `justhodl-hiring-velocity` — 1 endpoints; helpers: fmp(path,symbol,limit,extra), _hiring_fetch(symbol,endpoint,limit), lambda_handler(event,context)
- `justhodl-insider-radar` — 3 endpoints; no tests dir; helpers: fetch_fmp(), ps_ttm_map(tickers,cap)
- `justhodl-insider-sell-cluster` — 2 endpoints; no tests dir; helpers: fetch_insider_sells(from_date,page_limit), fmp_quote(symbol)
- `justhodl-inventory-drawdown` — 1 endpoints; helpers: dio_trend(tk), lambda_handler(event,context), acquire(symbol)
- `justhodl-lockup-expiration` — 3 endpoints; no tests dir; helpers: fmp_ipos(from_date,to_date), fmp_quote(symbol), fmp_profile(symbol)
- `justhodl-macro-calendar` — 2 endpoints; no tests dir; helpers: get_economic_calendar(days_horizon), get_market_context()
- `justhodl-market-map` — 2 endpoints; no tests dir; helpers: constituents(), mcap_state(tickers)
- `justhodl-microcap-float-squeeze` — 3 endpoints; helpers: fetch_quote(symbol), fetch_profile(symbol), fetch_history(symbol,days), _float_fetch(ticker,endpoint,sources)
- `justhodl-momentum-breakout` — 1 endpoints; helpers: fetch_history(symbol,days), lambda_handler(event,context), endpoint(ticker)
- `justhodl-momentum-leaders` — 1 endpoints; helpers: fetch_price_rows(ticker), lambda_handler(event,context), endpoint(ticker)
- `justhodl-ndx-spx-spread` — 2 endpoints; no tests dir; helpers: fmp_history(symbol,days), fmp_quote(symbol)
- `justhodl-opex-calendar` — 2 endpoints; no tests dir; helpers: fetch_spy_price(), fetch_spy_history()
- `justhodl-opportunity-engine` — 0 endpoints; helpers: fetch_forward_growth(universe,max_n), fetch_capex_buyback(universe,max_n), extract_backlog(ticker,fmp_key), fetch_tail_metrics(tail)
- `justhodl-options-flow-scanner` — 1 endpoints; helpers: get_spot_price(ticker), _option_fetch(url,sources,remaining,kind), _option_company(member,sources,remaining,checked,days_back), lambda_handler(event,context)
- `justhodl-pair-trades` — 2 endpoints; no tests dir; helpers: fetch_peers(ticker), fetch_price_rows(ticker)
- `justhodl-pead-detector` — 3 endpoints; no tests dir; helpers: fetch_earnings_surprises(symbol,limit), fetch_earnings_calendar(symbol), fetch_quote(symbol), fetch_history(symbol,days)
- `justhodl-portfolio-analytics` — 2 endpoints; helpers: fetch_returns(ticker,days), fetch_profile(ticker)
- `justhodl-post-earnings-mean-rev` — 3 endpoints; no tests dir; helpers: fmp_quote(symbol), fmp_history(symbol,days), fmp_earnings_recent(symbol,from_date)
- `justhodl-pump-mechanics` — 2 endpoints; helpers: fetch_profile(ticker), fetch_price_history(ticker,days)
- `justhodl-pump-positioning` — 3 endpoints; helpers: fetch_price_history(ticker,days), fetch_quote(ticker), fetch_profile(ticker), endpoint(ticker,kind,as_of)
- `justhodl-readthrough` — 4 endpoints; no tests dir; helpers: fmp_news(sym), fmp_profile(sym), fmp_revenue(sym), fmp_days_to_earnings(sym), consensus_snapshot(shortlist,meta), one(sym)
- `justhodl-revenue-acceleration` — 2 endpoints; helpers: fetch_quarterly_income(symbol,limit,max_retries), fetch_market_cap(symbol,stock_data), _revenue_fetch(ticker,endpoint), lambda_handler(event,context)
- `justhodl-russell-recon-frontrun` — 2 endpoints; no tests dir; helpers: _fmp_screener_single(exchange,limit_min_mcap,page,page_size), fmp_screener(limit_min_mcap,exchanges)
- `justhodl-sector-heatmap` — 1 endpoints; no tests dir; helpers: fmp_quote_batch(tickers,max_workers), _fetch_one(sym)
- `justhodl-skew-tail-hedging` — 2 endpoints; no tests dir; helpers: fmp_quote(symbol), fmp_history(symbol,days)
- `justhodl-squeeze-pretrigger` — 2 endpoints; helpers: fmp_profile(symbol), fmp_price_runup(symbol)
- `justhodl-stock-valuations` — 9 endpoints; no tests dir; helpers: fetch_ratios(t), fetch_keymetrics(t), fetch_growth(t), fetch_hist(t), fetch_hp_raw(t), hp_universe(), lambda_handler(event,context)
- `justhodl-theme-classifier` — 1 endpoints; helpers: fetch_profile(ticker), endpoint(ticker)
- `justhodl-theme-rotation-engine` — 3 endpoints; helpers: fetch_history(symbol,days), fetch_etf_holdings(ticker,fallback_top)
- `justhodl-trend-reversal` — 0 endpoints; no tests dir; helpers: closes(sym,days), build_universe()
- `justhodl-universe-builder` — 3 endpoints; no tests dir; helpers: _screener_call(low,high), fetch_quote_for_seed(symbol), fetch_profile_for_seed(symbol)
- `justhodl-us-cycle` — 1 endpoints; no tests dir; helpers: lambda_handler(event,context), fmp_eod(sym,days)
- `justhodl-velocity-acceleration` — 1 endpoints; helpers: fetch_price_rows(ticker), endpoint(ticker,as_of)
- `justhodl-vix9d-vix-inversion` — 2 endpoints; no tests dir; helpers: fmp_quote(symbol), fmp_history(symbol,days)
- `justhodl-vol-target-unwind` — 2 endpoints; no tests dir; helpers: fmp_history(symbol,years,retries), fmp_quote(symbol)
- `justhodl-volatility-squeeze-hunter` — 1 endpoints; helpers: fetch_history(symbol,days), lambda_handler(event,context), endpoint(ticker)
- `justhodl-vvix-vov-regime` — 2 endpoints; no tests dir; helpers: fmp_quote(symbol), fmp_history(symbol,days)

### inline-urls (52)

- `benzinga-news-agent` — 0 endpoints; no tests dir; helpers: -
- `fmp-fundamentals-agent` — 0 endpoints; helpers: -
- `justhodl-asymmetric-hunter` — 0 endpoints; no tests dir; helpers: -
- `justhodl-beneish` — 0 endpoints; no tests dir; helpers: -
- `justhodl-beta-laggard` — 0 endpoints; no tests dir; helpers: -
- `justhodl-bottleneck-research` — 0 endpoints; helpers: -
- `justhodl-brief-compiler` — 0 endpoints; helpers: -
- `justhodl-chart-patterns` — 0 endpoints; no tests dir; helpers: -
- `justhodl-convexity-scorer` — 0 endpoints; helpers: -
- `justhodl-cot-tracker` — 0 endpoints; no tests dir; helpers: -
- `justhodl-desk-returns` — 0 endpoints; no tests dir; helpers: -
- `justhodl-dislocation-ai` — 0 endpoints; no tests dir; helpers: -
- `justhodl-dislocation-detector` — 0 endpoints; no tests dir; helpers: -
- `justhodl-dividend-growth` — 0 endpoints; no tests dir; helpers: -
- `justhodl-earnings` — 0 endpoints; no tests dir; helpers: -
- `justhodl-earnings-sentiment` — 0 endpoints; no tests dir; helpers: -
- `justhodl-equity-research` — 0 endpoints; helpers: -
- `justhodl-eva-spread` — 0 endpoints; no tests dir; helpers: -
- `justhodl-factor-decomposition` — 0 endpoints; no tests dir; helpers: -
- `justhodl-failure-library` — 0 endpoints; helpers: -
- `justhodl-fmp-bank` — 0 endpoints; helpers: -
- `justhodl-fmp-walker` — 0 endpoints; helpers: -
- `justhodl-fundamental-graphs` — 0 endpoints; no tests dir; helpers: -
- `justhodl-index-recon` — 0 endpoints; helpers: -
- `justhodl-investor-agents` — 0 endpoints; no tests dir; helpers: -
- `justhodl-investor-lenses` — 0 endpoints; no tests dir; helpers: -
- `justhodl-liquidity-capacity` — 0 endpoints; helpers: -
- `justhodl-live-pulse` — 0 endpoints; no tests dir; helpers: -
- `justhodl-ma-target-predictor` — 0 endpoints; no tests dir; helpers: -
- `justhodl-ma-tracker` — 0 endpoints; no tests dir; helpers: -
- `justhodl-merger-arb` — 0 endpoints; no tests dir; helpers: -
- `justhodl-metals-miners` — 0 endpoints; no tests dir; helpers: -
- `justhodl-news-sentiment` — 0 endpoints; no tests dir; helpers: -
- `justhodl-pairs-arb` — 0 endpoints; no tests dir; helpers: -
- `justhodl-peer-comparison` — 0 endpoints; no tests dir; helpers: -
- `justhodl-political-trades` — 0 endpoints; no tests dir; helpers: -
- `justhodl-predictability` — 0 endpoints; no tests dir; helpers: -
- `justhodl-provider-catalog` — 0 endpoints; helpers: -
- `justhodl-quality-on-sale` — 0 endpoints; no tests dir; helpers: -
- `justhodl-research-backtest` — 0 endpoints; helpers: -
- `justhodl-smart-beta` — 0 endpoints; no tests dir; helpers: -
- `justhodl-smart-money-holdings` — 0 endpoints; no tests dir; helpers: -
- `justhodl-smart-money-tracker` — 0 endpoints; no tests dir; helpers: -
- `justhodl-spinoff-desk` — 0 endpoints; helpers: -
- `justhodl-spx-history` — 0 endpoints; no tests dir; helpers: -
- `justhodl-starmine` — 0 endpoints; no tests dir; helpers: -
- `justhodl-stock-ai-research` — 0 endpoints; no tests dir; helpers: -
- `justhodl-stock-analyzer` — 0 endpoints; no tests dir; helpers: -
- `justhodl-stock-screener` — 0 endpoints; no tests dir; helpers: -
- `justhodl-supply-chain-linkage` — 0 endpoints; no tests dir; helpers: -
- `justhodl-theme-tier-classifier` — 0 endpoints; no tests dir; helpers: -
- `justhodl-trend-engine` — 0 endpoints; no tests dir; helpers: -
