# ops 5217 -- orphan references vs S3 (read-only)

**Status:** success  
**Duration:** 18.1s  
**Finished:** 2026-09-07T19:09:42+00:00  

## Error

```
SystemExit: 0
```

## Data

| age_h | bytes | key | kind | status |
|---|---|---|---|---|
| 812.8 | 4250 | data/alpha-triage.json | page | PRESENT |
| 9.6 | 11153 | data/apac-flows.json | page | PRESENT |
| 8.9 | 3787 | data/apac-leadlag.json | page | PRESENT |
| 1970.0 | 113 | data/askdesk-config.json | page | PRESENT |
| 1.6 | 971 | data/bea-economic.json | page | PRESENT |
| 1.6 | 458149 | data/bls-employment.json | page | PRESENT |
| 1.6 | 3597 | data/bls-labor.json | page | PRESENT |
| 1.6 | 1699 | data/census-economic.json | page | PRESENT |
| 22.0 | 54498 | data/cryptoquant-onchain.json | page | PRESENT |
| 61.1 | 169661 | data/ecb-hist/eurusd.json | page | PRESENT |
| 61.1 | 37980 | data/ecb-hist/fx_claims_nonea.json | page | PRESENT |
| 61.1 | 4200 | data/ecb-hist/gdp_yoy.json | page | PRESENT |
| 61.1 | 36555 | data/ecb-hist/ilm_usd_claims.json | page | PRESENT |
| 61.1 | 7008 | data/ecb-hist/indprod_core.json | page | PRESENT |
| 61.1 | 11844 | data/ecb-hist/m1_growth.json | page | PRESENT |
| 61.1 | 6268 | data/ecb-hist/retail_turnover.json | page | PRESENT |
| 61.1 | 6191 | data/ecb-hist/unemployment_ea.json | page | PRESENT |
| 799.6 | 1329706 | data/history/ofr-stfm-charts.json | page | PRESENT |
| 1562.0 | 17920 | data/llm-cost-audit.json | page | PRESENT |
| 4.7 | 91520 | data/market-map.json | page | PRESENT |
| 799.6 | 30309 | data/ofr-stfm.json | page | PRESENT |
| 1538.3 | 127 | data/page-ai-live.json | page | PRESENT |
| 4.7 | 5389 | data/sector-groups.json | page | PRESENT |
| 1243.5 | 256 | data/signal-suppress.json | page | PRESENT |
| 5.4 | 31380 | data/term-premium.json | page | PRESENT |
| None | 0 | data/13f-aggregate.json | engine | MISSING |
| 773.8 | 1060 | data/_alerts/frontrun-sniffer-alert-state.json | engine | PRESENT |
| 785.8 | 328 | data/_alerts/macro-frontrun-sniffer-alert-state.json | engine | PRESENT |
| 5.8 | 677 | data/_alerts/targets-index.json | engine | PRESENT |
| 0.0 | 2114003 | data/_state/fred-queue.json.gz | engine | PRESENT |
| 185.7 | 133119 | data/_state/gdelt-missing-slots.json | engine | PRESENT |
| 0.1 | 4724 | data/_state/sdmx-walk-ecb.json | engine | PRESENT |
| 5.6 | 2951166 | data/_upside/state.json.gz | engine | PRESENT |
| 0.2 | 83 | data/a2a/inbox/claude-audit.json | engine | PRESENT |
| 0.2 | 77 | data/a2a/inbox/claude.json | engine | PRESENT |
| None | 0 | data/aaii.json | engine | MISSING |
| 812.8 | 4250 | data/alpha-triage.json | engine | PRESENT |
| None | 0 | data/analyst-consensus-history.json | engine | MISSING |
| 9.6 | 11153 | data/apac-flows.json | engine | PRESENT |
| 8.9 | 3787 | data/apac-leadlag.json | engine | PRESENT |
| None | 0 | data/apac.json | engine | MISSING |
| None | 0 | data/asymmetric-setups.json | engine | MISSING |
| 791.1 | 207 | data/audit/exemptions.json | engine | PRESENT |
| 788.8 | 78767 | data/audit/fabrication-sites.json | engine | PRESENT |
| 788.5 | 422455 | data/audit/lambda-graph.json | engine | PRESENT |
| None | 0 | data/backlog-miner.json | engine | MISSING |
| 2109.1 | 1237 | data/backtest-summary.json | engine | PRESENT |
| 1.9 | 407 | data/baltic-dry.json | engine | PRESENT |
| 1.6 | 971 | data/bea-economic.json | engine | PRESENT |
| None | 0 | data/beneish-m-score.json | engine | MISSING |
| 1.9 | 518 | data/bill-share.json | engine | PRESENT |
| None | 0 | data/bitcoin-rainbow.json | engine | MISSING |
| 1.6 | 458149 | data/bls-employment.json | engine | PRESENT |
| 1.6 | 3597 | data/bls-labor.json | engine | PRESENT |
| None | 0 | data/bond-regime.json | engine | MISSING |
| None | 0 | data/canaries.json | engine | MISSING |
| 1.6 | 1699 | data/census-economic.json | engine | PRESENT |
| None | 0 | data/commodity-curves-history.json | engine | MISSING |
| 22.0 | 21183 | data/config/cryptoquant-spec.json | engine | PRESENT |
| 1619.9 | 269 | data/config/finra-monthly-spec.json | engine | PRESENT |
| 1322.3 | 5071 | data/config/nyfed-pd-spec.json | engine | PRESENT |
| 1619.9 | 190 | data/config/quiver-offexchange.json | engine | PRESENT |
| None | 0 | data/correlations.json | engine | MISSING |
| 819.8 | 10009 | data/cq-catalog.json | engine | PRESENT |
| None | 0 | data/credit-spreads.json | engine | MISSING |
| None | 0 | data/crisis-brief.json | engine | MISSING |
| None | 0 | data/crypto-intel.json | engine | MISSING |
| 22.0 | 54498 | data/cryptoquant-onchain.json | engine | PRESENT |
| 8.6 | 451467 | data/cycle/features.json.gz | engine | PRESENT |
| None | 0 | data/divergence-current.json | engine | MISSING |
| None | 0 | data/divergence.json | engine | MISSING |
| None | 0 | data/divergence/current.json | engine | MISSING |
| None | 0 | data/dollar.json | engine | MISSING |
| None | 0 | data/earnings-calendar.json | engine | MISSING |
| None | 0 | data/ecb-cache.json | engine | MISSING |
| None | 0 | data/ecb-data.json | engine | MISSING |
| None | 0 | data/ecb-financial-stress.json | engine | MISSING |
| 61.1 | 281323 | data/ecb-hist/ciss_ea.json | engine | PRESENT |
| None | 0 | data/ecb-hist/excess-liquidity.json | engine | MISSING |
| 2155.1 | 15098 | data/ecb-hist/excess_liquidity.json | engine | PRESENT |
| None | 0 | data/edge-data.json | engine | MISSING |
| 1.9 | 534 | data/em-carry.json | engine | PRESENT |
| 1488.2 | 162164 | data/engine-registry.json | engine | PRESENT |
| None | 0 | data/etf-flows/daily.json | engine | MISSING |
| None | 0 | data/etf-flows/event-study.json | engine | MISSING |
| None | 0 | data/etf-fund-flows.json | engine | MISSING |
| None | 0 | data/factor-ranks.json | engine | MISSING |
| 910.0 | 321 | data/family-defs.json | engine | PRESENT |
| None | 0 | data/fed-liquidity.json | engine | MISSING |
| 907.5 | 319495 | data/fleet-inventory.json | engine | PRESENT |
| None | 0 | data/flow-data.json | engine | MISSING |
| None | 0 | data/foo.json | engine | MISSING |
| 5.8 | 566 | data/frontrun-sniffer.json | engine | PRESENT |
| None | 0 | data/fundamentals-engine.json | engine | MISSING |
| None | 0 | data/funding-canaries.json | engine | MISSING |
| None | 0 | data/gdelt-financial-sentiment.json | engine | MISSING |
| None | 0 | data/gdelt-sentiment.json | engine | MISSING |
| 1.9 | 1201 | data/global-m2.json | engine | PRESENT |
| 2977.2 | 130 | data/history-api-url.json | engine | PRESENT |
| None | 0 | data/history/causality-discoveries-history.json | engine | MISSING |
| None | 0 | data/history/convexity-scores-history.json | engine | MISSING |
| 22.1 | 119834 | data/history/eth-price-cm.json | engine | PRESENT |
| None | 0 | data/history/factor-ranks.json | engine | MISSING |
| 1619.9 | 2 | data/history/ici-flows.json | engine | PRESENT |
| 1619.9 | 2 | data/history/ici-mmf.json | engine | PRESENT |
| None | 0 | data/history/meta-improver-history.json | engine | MISSING |
| None | 0 | data/history/pre-disaster-history.json | engine | MISSING |
| 1.9 | 325 | data/implied-corr.json | engine | PRESENT |
| 209.6 | 3230 | data/index/ecb/flows.json.gz | engine | PRESENT |
| 209.6 | 106287 | data/index/eurostat/flows.json.gz | engine | PRESENT |
| None | 0 | data/insider-buys.json | engine | MISSING |
| None | 0 | data/insider-sell-clusters.json | engine | MISSING |
| 4.2 | 12483 | data/invest/leg-history.json | engine | PRESENT |
| None | 0 | data/khalid-index.json | engine | MISSING |
| 5.8 | 634 | data/macro-frontrun-sniffer.json | engine | PRESENT |
| None | 0 | data/macro-nowcast-v2.json | engine | MISSING |
| None | 0 | data/margin-debt.json | engine | MISSING |
| None | 0 | data/margin.json | engine | MISSING |
| 6.5 | 4428767 | data/market-internals-state.json.gz | engine | PRESENT |
| 4.7 | 91520 | data/market-map.json | engine | PRESENT |
| None | 0 | data/master-ranker-universe.json | engine | MISSING |
| 1.9 | 1098 | data/miner-margin.json | engine | PRESENT |
| None | 0 | data/morning-intel.json | engine | MISSING |
| 1.9 | 460 | data/muni-ratio.json | engine | PRESENT |
| None | 0 | data/news-sentiment.json | engine | MISSING |
| None | 0 | data/news-velocity-history.json | engine | MISSING |
| 1.9 | 6407 | data/ofr-fsi.json | engine | PRESENT |
| 799.6 | 30309 | data/ofr-stfm.json | engine | PRESENT |
| None | 0 | data/opex-gamma-pin.json | engine | MISSING |
| None | 0 | data/opportunity-engine.json | engine | MISSING |
| None | 0 | data/options-flow-scanner.json | engine | MISSING |
| 1538.3 | 65950 | data/page-ai-manifest.json | engine | PRESENT |
| None | 0 | data/portfolio-snapshot.json | engine | MISSING |
| None | 0 | data/portfolio.json | engine | MISSING |
| None | 0 | data/pre-disaster-library.json | engine | MISSING |
| 0.7 | 428 | data/providers/ecb/series-manifest.json | engine | PRESENT |
| 0.7 | 445 | data/providers/eurostat/series-manifest.json | engine | PRESENT |
| 501.6 | 10261 | data/providers/tic-cslt/FORLTTREASNET42609.json | engine | PRESENT |
| 862.1 | 11196 | data/quantum-desk-sources.json | engine | PRESENT |
| None | 0 | data/quote-snapshot.json | engine | MISSING |
| None | 0 | data/redflag-alerter.json | engine | MISSING |
| None | 0 | data/redflags.json | engine | MISSING |
| None | 0 | data/regime-read.json | engine | MISSING |
| 529.2 | 101907 | data/repo-master-inventory.json | engine | PRESENT |
| 24.0 | 311 | data/retail-divergence-track.json | engine | PRESENT |
| 24.0 | 312 | data/retail-momentum-track.json | engine | PRESENT |
| None | 0 | data/retail-sentiment-history.json | engine | MISSING |
| 1.9 | 798 | data/revision-breadth.json | engine | PRESENT |
| None | 0 | data/risk-composite.json | engine | MISSING |
| None | 0 | data/risk_gate.json | engine | MISSING |
| None | 0 | data/riskgate.json | engine | MISSING |
| None | 0 | data/rrp.json | engine | MISSING |
| None | 0 | data/screener-results.json | engine | MISSING |
| 4.7 | 5389 | data/sector-groups.json | engine | PRESENT |
| None | 0 | data/sentiment-extreme-composite.json | engine | MISSING |
| None | 0 | data/sentiment.json | engine | MISSING |
| 1.9 | 199 | data/sifma-issuance.json | engine | PRESENT |
| 1243.5 | 256 | data/signal-suppress.json | engine | PRESENT |
| 1.9 | 839 | data/sloos.json | engine | PRESENT |
| None | 0 | data/smart-money-cluster.json | engine | MISSING |
| None | 0 | data/sp500-screener.json | engine | MISSING |
| None | 0 | data/spx-breadth.json | engine | MISSING |
| None | 0 | data/spx-ma-command.json | engine | MISSING |
| 1.9 | 6457 | data/stock-bond-corr.json | engine | PRESENT |
| None | 0 | data/stress-index.json | engine | MISSING |
| None | 0 | data/structural-presignals.json | engine | MISSING |
| 1344.9 | 591449 | data/symbol-map.json | engine | PRESENT |
| 5.4 | 31380 | data/term-premium.json | engine | PRESENT |
| None | 0 | data/tga.json | engine | MISSING |
| 44.4 | 14004201 | data/thesis-state-v2.json.gz | engine | PRESENT |
| None | 0 | data/treasury-auction-crisis.json | engine | MISSING |
| None | 0 | data/user-alert-rules.json | engine | MISSING |
| None | 0 | data/vol-target.json | engine | MISSING |
| None | 0 | data/vol-unwind.json | engine | MISSING |
| 3.9 | 131426 | data/warm/banxico/core-series.json.gz | engine | PRESENT |
| 16.7 | 378 | data/warm/bls-full/manifest.json | engine | PRESENT |
| 16.7 | 384 | data/warm/boe-full/manifest.json | engine | PRESENT |
| 0.4 | 450 | data/warm/boj-full/manifest.json | engine | PRESENT |
| 1.6 | 538 | data/warm/bond-warroom/tv-bank.json.gz | engine | PRESENT |
| 327.2 | 1591 | data/warm/census-us/_state/grammar-overrides.json | engine | PRESENT |
| 0.1 | 36474 | data/warm/census-us/_state/state.json | engine | PRESENT |
| 136.6 | 2090 | data/warm/census-us/catalog.json.gz | engine | PRESENT |
| 16.7 | 306 | data/warm/dol-full/manifest.json | engine | PRESENT |
| 13.6 | 3057146 | data/warm/edgar-filings/2026/QTR3.json.gz | engine | PRESENT |
| 13.6 | 557 | data/warm/edgar-filings/latest-summary.json | engine | PRESENT |
| 5.5 | 178435 | data/warm/eurostat/catalog.json.gz | engine | PRESENT |
| 16.5 | 432 | data/warm/fiscaldata-full/manifest.json | engine | PRESENT |
| 0.1 | 486 | data/warm/gdelt-full/manifest.json | engine | PRESENT |
| None | 0 | data/warm/imf-full/catalog.json | engine | MISSING |
| None | 0 | data/warm/imf-full/catalog.json.gz | engine | MISSING |
| 136.4 | 459 | data/warm/imf-full/manifest.json | engine | PRESENT |
| None | 0 | data/warm/imf/catalog.json.gz | engine | MISSING |
| 3.9 | 9322 | data/warm/nasa-power/midwest-daily.json.gz | engine | PRESENT |
| 13.8 | 63126 | data/warm/nyfed-markets/ambs-history.json.gz | engine | PRESENT |
| 13.8 | 25509 | data/warm/nyfed-markets/fxs-history.json.gz | engine | PRESENT |
| 526.4 | 11426 | data/warm/nyfed-markets/pd-splice-map.json | engine | PRESENT |
| 12.9 | 42249 | data/warm/nyfed-markets/rp-repo-history.json.gz | engine | PRESENT |
| 13.8 | 189856 | data/warm/nyfed-markets/seclending-history.json.gz | engine | PRESENT |
| 13.8 | 41564 | data/warm/nyfed-markets/soma-summary-history.json.gz | engine | PRESENT |
| 0.9 | 41980 | data/warm/nyfed-markets/soma_summary.json.gz | engine | PRESENT |
| 13.8 | 43748 | data/warm/nyfed-markets/tsy-history.json.gz | engine | PRESENT |
| 16.6 | 321 | data/warm/nyfed-research/_last-check.json | engine | PRESENT |
| 5.5 | 29326 | data/warm/oecd/catalog.json.gz | engine | PRESENT |
| 1.6 | 3470 | data/warm/official-yields/_state.json | engine | PRESENT |
| 7.8 | 1396559 | data/warm/portwatch/history/daily-rows.json.gz | engine | PRESENT |
| 3.9 | 500186 | data/warm/statcan/cube-catalog.json.gz | engine | PRESENT |
| None | 0 | data/warm/statcan/cube-list.json.gz | engine | MISSING |
| 0.6 | 16442 | data/warm/te-mirror/_state.json | engine | PRESENT |
| 16.6 | 411 | data/warm/tic-full/manifest.json | engine | PRESENT |
| 1.5 | 446675 | data/warm/treasury-auctions/assets.json.gz | engine | PRESENT |
| 1.5 | 525719 | data/warm/treasury-auctions/history-full.json.gz | engine | PRESENT |
| 1.5 | 112151 | data/warm/treasury-auctions/history.json.gz | engine | PRESENT |
| None | 0 | data/warm/treasury-auctions/reactions.json | engine | MISSING |
| 69.6 | 179923 | data/warm/treasury-par/curve.json.gz | engine | PRESENT |
| 353.8 | 301485 | data/warm/worldbank-full/catalog.json.gz | engine | PRESENT |
| None | 0 | data/warm/worldbank-full/indicators.json.gz | engine | MISSING |
| 0.6 | 398 | data/warm/worldbank-full/manifest.json | engine | PRESENT |
| None | 0 | data/warm/worldbank/catalog.json.gz | engine | MISSING |

## Log
## page references (26)

- `19:09:26`    data/snapshots/data_${engine}-${date}.json: templated key, skipped
- `19:09:26`    {'PRESENT': 25, 'MISSING': 0, 'ERR': 0, 'PRESENT_STALE_7d': 7}
## engine references (219)

- `19:09:26`    data/_alerts/digest-{today_str}-close.json: templated key, skipped
- `19:09:27`    data/_state/sdmx-walk-{agency}.json: templated key, skipped
- `19:09:27`    data/_state/sdmx-walk-{slug}.json: templated key, skipped
- `19:09:27`    data/_state/series-extract-{provider}.json: templated key, skipped
- `19:09:30`    data/ecb-hist/{hid}.json: templated key, skipped
- `19:09:30`    data/ecb-hist/{hist_id}.json: templated key, skipped
- `19:09:31`    data/estimate-revisions/{today_iso}.json: templated key, skipped
- `19:09:31`    data/fundgraph/cache/{sym}_quarter_v21.json: templated key, skipped
- `19:09:33`    data/history/{base}-history.json: templated key, skipped
- `19:09:33`    data/history/{base}_history.json: templated key, skipped
- `19:09:33`    data/llm/self-critique/{yday}.json: templated key, skipped
- `19:09:34`    data/misses/{d}.json: templated key, skipped
- `19:09:35`    data/page-ai/{page}.json: templated key, skipped
- `19:09:35`    data/page-ai/{pg}.json: templated key, skipped
- `19:09:35`    data/predictions-snapshots/{yesterday}.json: templated key, skipped
- `19:09:36`    data/search/providers/{slug}.json.gz: templated key, skipped
- `19:09:37`    data/snapshots/{base}.json: templated key, skipped
- `19:09:39`    data/warm/fred-scoped/EU_Sovereign_Yields/{_fid}.json: templated key, skipped
- `19:09:39`    data/warm/fred-scoped/{cat}/{id}.json: templated key, skipped
- `19:09:41`    data/warm/ofr-hfm/series/{_fs}.json.gz: templated key, skipped
- `19:09:41`    data/warm/ofr/series/{_mm}.json.gz: templated key, skipped
- `19:09:41`    data/warm/ofr/series/{mn}.json.gz: templated key, skipped
- `19:09:42`    data/warm/treasury/{ds}.json.gz: templated key, skipped
- `19:09:42`    data/{guess}.json: templated key, skipped
- `19:09:42`    data/{key}.json: templated key, skipped
- `19:09:42`    data/{k}.json: templated key, skipped
- `19:09:42`    {'PRESENT': 106, 'MISSING': 87, 'ERR': 0, 'PRESENT_STALE_7d': 31}
- `19:09:42` ✅ done: {"page": {"PRESENT": 25, "MISSING": 0, "ERR": 0, "PRESENT_STALE_7d": 7}, "engine": {"PRESENT": 106, "MISSING": 87, "ERR": 0, "PRESENT_STALE_7d": 31}}
