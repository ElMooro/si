# ops 3808 — which institutional signals can actually join?

**Status:** success  
**Duration:** 1.6s  
**Finished:** 2026-07-24T17:19:12+00:00  

## Data

| capture_ledger | engine_version |
|---|---|
| 2393 | 4.4.1 |

## Log
## Candidate feeds — existence, freshness, schema, OVERLAP

- `17:19:10`   data/eps-revision-velocity.json                6h  syms=92     overlap=91      ['schema_version', 'method', 'generated_at', 'duration_s', 'stats', 'summary', 'all_qualifying']
- `17:19:10`   data/estimate-revisions.json                   3h  syms=393    overlap=236     ['engine', 'version', 'generated_at', 'status', 'thesis', 'horizon_days', 'n_tracked', 'direction_map', 'n_fmp_enriched']
- `17:19:11`   data/earnings-pead.json                        9h  syms=244    overlap=241     ['schema_version', 'method', 'generated_at', 'duration_s', 'stats', 'summary', 'all_qualifying']
- `17:19:11`   data/finra-short.json                         16h  syms=501    overlap=494     ['generated_at', 'generated_at_unix', 'version', 'data_date', 'elapsed_seconds', 'config', 'market_composite', 'squeeze_candidates', 'top_svr']
- `17:19:11`   data/13f-price-divergence.json                19h  syms=0      overlap=0       ['engine', 'version', 'as_of', 'state', 'signal_strength', 'n_divergences', 'n_bullish', 'n_bearish', 'n_high_conviction']
- `17:19:11`   data/insider-aggregate.json                   18h  syms=2      overlap=0       ['schema_version', 'method', 'generated_at', 'elapsed_s', 'n_transactions', 'data_coverage_days', 'windows', 'regime', 'regime_read']
- `17:19:11`   data/dark-pool.json                           19h  syms=939    overlap=912     ['engine', 'version', 'ok', 'generated_at', 'thesis', 'latest_week', 'weekly_source', 'n_scored', 'distribution']
- `17:19:11`   data/insider-buyback-confluence.json           3h  syms=0      overlap=0       ['engine', 'version', 'as_of', 'state', 'signal_strength', 'n_confluences', 'n_high_conviction', 'feeders', 'top_confluences']
- `17:19:11`   data/short-book.json                          19h  syms=5      overlap=5       ['ok', 'version', 'generated_at', 'elapsed_s', 'n_candidates', 'n_book', 'logged', 'squeeze_excluded', 'book']
- `17:19:11`   data/deal-scanner.json                        80h  syms=13     overlap=6       ['engine', 'version', 'generated_at', 'window', 'summary', 'deals', 'by_sector', 'by_cap', 'by_event']
- `17:19:11`   data/readthrough.json                          3h  syms=28     overlap=25      ['engine', 'version', 'ok', 'generated_at', 'status', 'degraded', 'thesis', 'params', 'n_events']
- `17:19:11`   data/industry-boom.json                        6h  syms=0      overlap=0       ['engine', 'version', 'generated_at', 'n_industries', 'n_universe', 'league', 'trouble', 'coverage', 'siblings']
## Ranked by join value (overlap with the capture ledger)

- `17:19:11`   data/dark-pool.json                        overlap 912    (38.1% of ledger)  POSITIONING — off-exchange accumulation
- `17:19:11`   data/finra-short.json                      overlap 494    (20.6% of ledger)  POSITIONING — short interest / crowded bear
- `17:19:11`   data/earnings-pead.json                    overlap 241    (10.1% of ledger)  CATALYST — post-earnings drift
- `17:19:11`   data/estimate-revisions.json               overlap 236    (9.9% of ledger)  WHY CHEAP — analyst revision breadth
- `17:19:11`   data/eps-revision-velocity.json            overlap 91     (3.8% of ledger)  WHY CHEAP — are estimates falling?
- `17:19:11`   data/readthrough.json                      overlap 25     (1.0% of ledger)  CATALYST — beneficiary propagation
- `17:19:11`   data/deal-scanner.json                     overlap 6      (0.3% of ledger)  CATALYST — event classes
- `17:19:11`   data/short-book.json                       overlap 5      (0.2% of ledger)  POSITIONING — short book
- `17:19:11` ✅ PROBE.usable :: 8 feeds join non-trivially
## Feeds too thin to inform a 2,393-name board (<10%)

- `17:19:11`   data/eps-revision-velocity.json            overlap 91 — informative for a subset only
- `17:19:11`   data/estimate-revisions.json               overlap 236 — informative for a subset only
- `17:19:11`   data/short-book.json                       overlap 5 — informative for a subset only
- `17:19:11`   data/deal-scanner.json                     overlap 6 — informative for a subset only
- `17:19:11`   data/readthrough.json                      overlap 25 — informative for a subset only
## Schema detail for the top joiners

- `17:19:11`   --- data/dark-pool.json ---
- `17:19:11`       engine                     str     18
- `17:19:11`       version                    str     5
- `17:19:11`       ok                         bool    True
- `17:19:11`       generated_at               str     32
- `17:19:11`       thesis                     str     209
- `17:19:11`       latest_week                str     10
- `17:19:11`       weekly_source              str     5
- `17:19:11`       n_scored                   int     939
- `17:19:11`       distribution               dict    2
- `17:19:11`       board                      list    60
- `17:19:11`       top_picks                  list    20
- `17:19:11`       top_accumulation           list    20
- `17:19:11`       board[0] keys: ['ats_shares_wk', 'conviction', 'daily_off_exch_vol', 'daily_short_pct', 'daily_short_z', 'dark_accel', 'dark_pool_pct', 'offex_pct', 'offex_shares_wk', 'score', 'state', 'ticker']
- `17:19:11`   --- data/finra-short.json ---
- `17:19:11`       generated_at               str     32
- `17:19:11`       generated_at_unix          int     1784854832
- `17:19:11`       version                    str     5
- `17:19:11`       data_date                  str     10
- `17:19:11`       elapsed_seconds            float   3.56
- `17:19:11`       config                     dict    3
- `17:19:11`       market_composite           dict    11
- `17:19:11`       squeeze_candidates         list    20
- `17:19:11`       top_svr                    list    30
- `17:19:11`       top_zscore                 list    30
- `17:19:11`       sectors                    dict    11
- `17:19:11`       tickers                    dict    501
- `17:19:11`       squeeze_candidates[0] keys: ['days_to_cover', 'momentum_pct', 'name', 'price_strength', 'sector', 'short_volume', 'squeeze_flags', 'squeeze_score', 'svr_pct', 'symbol', 'total_volume', 'z_score']
- `17:19:11`   --- data/earnings-pead.json ---
- `17:19:11`       schema_version             int     1
- `17:19:11`       method                     str     16
- `17:19:11`       generated_at               str     25
- `17:19:11`       duration_s                 float   11.3
- `17:19:11`       stats                      dict    7
- `17:19:11`       summary                    dict    3
- `17:19:11`       all_qualifying             list    244
- `17:19:11`       all_qualifying[0] keys: ['beat_streak', 'flags', 'metrics', 'score', 'surprise_history', 'symbol', 'tier']
- `17:19:11`   --- data/estimate-revisions.json ---
- `17:19:11`       engine                     str     27
- `17:19:11`       version                    str     5
- `17:19:11`       generated_at               str     32
- `17:19:11`       status                     str     4
- `17:19:11`       thesis                     str     265
- `17:19:11`       horizon_days               int     75
- `17:19:11`       n_tracked                  int     439
- `17:19:11`       direction_map              dict    393
- `17:19:11`       n_fmp_enriched             int     255
- `17:19:11`       n_with_history             int     436
- `17:19:11`       n_state_keys               int     460
- `17:19:11`       estimate_strength_leaders  list    40
- `17:19:11`       estimate_strength_leaders[0] keys: ['baseline_date', 'baseline_eps_est', 'company', 'current_eps_est', 'days_to_earnings', 'direction', 'dispersion_pct', 'earnings_date', 'eps_rev_pct', 'eps_rev_recent_pct', 'estimate_strength', 'fiscal_period']
- `17:19:12`   --- data/eps-revision-velocity.json ---
- `17:19:12`       schema_version             int     1
- `17:19:12`       method                     str     24
- `17:19:12`       generated_at               str     25
- `17:19:12`       duration_s                 float   5.5
- `17:19:12`       stats                      dict    5
- `17:19:12`       summary                    dict    3
- `17:19:12`       all_qualifying             list    92
- `17:19:12`       all_qualifying[0] keys: ['company', 'estimates', 'flag', 'fundamentals', 'ratings_breadth', 'rationale', 'score', 'status', 'symbol']
## VERDICT

- `17:19:12` Join priority is decided by OVERLAP, not by how good the idea sounds.
- `17:19:12` Next ops wires only the feeds above ~15%% ledger coverage, each as an
- `17:19:12` explicit leg with its own gate proving a non-zero join on live output.
- `17:19:12` ✅ PASS_ALL — probe complete
