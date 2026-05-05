
# 1) Fix smart-money schedule (currently 09:00 UTC, conflicts with deep-value)

- `21:54:38`     ✓ smart-money schedule updated to cron(0 16 * * ? *)

# 2) Force-deploy deep-value Lambda from current source

- `21:54:38`     source: 17925 chars
- `21:54:38`       ✓ MARGINAL flag: 'MARGINAL'
- `21:54:38`       ✓ company-name fin detection: 'fin_company_keywords'
- `21:54:38`       ✓ lowered threshold: 'net_cash_pct < 0.15'
- `21:54:47`     ✓ deployed at 2026-05-05T21:54:43.000+0000

# 3) Re-invoke deep-value

- `21:54:53`     status: 200, dur: 6.2s
- `21:54:53`     body: {"n_universe": 500, "n_qualifying": 39, "n_tier_a": 1, "duration_s": 5.4}
- `21:54:53`       [deep-value] seeded 503 from screener/data.json
- `21:54:53`       [deep-value] universe after asymmetric: 503
- `21:54:53`       [deep-value] universe after SP500 backup: 515
- `21:54:53`       [deep-value] universe size: 500
- `21:54:53`       [deep-value] evaluated 500, OK: 39, statuses: {'ok': 39, 'no_quote': 181, 'below_min_mcap': 0, 'no_balance': 3, 'below_min_net_cash': 276, 'no_income': 1, 'deadline_skip': 0}
- `21:54:53`       [deep-value] wrote 33739b to data/deep-value.json
- `21:54:53`       [deep-value] tier_a=1 tier_b=0 watch=4 contrarian=4
- `21:54:53`       [deep-value] TOP: [('CNC', 100, 'DEEP_VALUE_TIER_A'), ('HUM', 72.4, 'NET_CASH_WATCH'), ('EPAM', 68.5, 'MARGINAL'), ('ELV', 65.8, 'MARGINAL'), ('INCY', 37.1, 'NET_CASH_WATCH'), ('REGN', 36.4, 'NET_CASH_WATCH'), ('SWKS', 36.0, 'MARGINAL'), ('CPRT', 35.4, 'MARGINAL')]
- `21:54:53`       END RequestId: 1395412e-2288-4534-a986-6e4560844ffe
- `21:54:53`       REPORT RequestId: 1395412e-2288-4534-a986-6e4560844ffe	Duration: 5381.93 ms	Billed Duration: 5877 ms	Memory Size: 1024 MB	Max Memory Used: 109 MB	Init Duration: 494.32 ms

# 4) Inspect new top_25 (should NOT have BAC/WFC, should be larger)

- `21:54:53`     top_25_overall: 9
- `21:54:53`     top_25_excluded: 25
- `21:54:53`   
- `21:54:53`     ── new top_25 ──
- `21:54:53`       CNC     100.0  flag=DEEP_VALUE_TIER_A         sector=Healthcare
- `21:54:53`       HUM      72.4  flag=NET_CASH_WATCH            sector=Healthcare
- `21:54:53`       EPAM     68.5  flag=MARGINAL                  sector=Technology
- `21:54:53`       ELV      65.8  flag=MARGINAL                  sector=Healthcare
- `21:54:53`       INCY     37.1  flag=NET_CASH_WATCH            sector=Healthcare
- `21:54:53`       REGN     36.4  flag=NET_CASH_WATCH            sector=Healthcare
- `21:54:53`       SWKS     36.0  flag=MARGINAL                  sector=Technology
- `21:54:53`       CPRT     35.4  flag=MARGINAL                  sector=Industrials
- `21:54:53`       MRNA     32.0  flag=NET_CASH_WATCH            sector=Healthcare
- `21:54:53`   
- `21:54:53`     ── excluded leaders ──
- `21:54:53`       EG       20.0  flag=FINANCIAL_BOOK_EXCLUDED     sector=Financial Services
- `21:54:53`       AIZ      20.0  flag=FINANCIAL_BOOK_EXCLUDED     sector=Financial Services
- `21:54:53`       SYF      18.8  flag=FINANCIAL_BOOK_EXCLUDED     sector=Financial Services
- `21:54:53`       TRV      18.8  flag=FINANCIAL_BOOK_EXCLUDED     sector=Financial Services
- `21:54:53`       PFG      18.6  flag=FINANCIAL_BOOK_EXCLUDED     sector=Financial Services
- `21:54:53`       ACGL     18.0  flag=FINANCIAL_BOOK_EXCLUDED     sector=Financial Services
- `21:54:53`       WRB      17.8  flag=FINANCIAL_BOOK_EXCLUDED     sector=Financial Services
- `21:54:53`       CB       17.4  flag=FINANCIAL_BOOK_EXCLUDED     sector=Financial Services

# 5) Trigger compound-aggregator Lambda (will re-aggregate with new DV)

- `21:54:55`     status: 200, body: {"n_compound": 5, "n_3_plus": 0, "n_alerts": 1, "duration_s": 0.38}
- `21:54:55`       [compound] eps_velocity: 25 entries
- `21:54:55`       [compound] aggregated: 161 names, 5 multi-signal
- `21:54:55`       [compound] new alerts this run: 1
- `21:54:55`       [compound] wrote 2947b to data/compound-signals.json
- `21:54:55`       [compound] wrote state: 2 alerted_keys tracked
- `21:54:55`       [compound] alert send: ok=True info=684
- `21:54:55`       END RequestId: b8e26577-3009-4396-8239-efea1b7cc243
- `21:54:55`       REPORT RequestId: b8e26577-3009-4396-8239-efea1b7cc243	Duration: 948.53 ms	Billed Duration: 1487 ms	Memory Size: 512 MB	Max Memory Used: 101 MB	Init Duration: 538.21 ms

# 6) Verify new compound output

- `21:54:55`     feed_stats: {"nobrainers": 25, "insiders": 22, "smart_money": 85, "deep_value": 9, "eps_velocity": 25}
- `21:54:55`     stats: {"n_total_names": 161, "n_multi_signal": 5, "n_3_plus": 0, "n_compound_over_200": 2, "n_compound_over_300": 0}
- `21:54:55`   
- `21:54:55`     ── compound leaderboard (top 10) ──
- `21:54:55`       CSGP   #2  comp=  220.7  (eps_velocity,insiders)
- `21:54:55`       EPAM   #2  comp=  213.0  (deep_value,insiders)
- `21:54:55`       OXY    #2  comp=  178.4  (nobrainers,smart_money)
- `21:54:55`       HUM    #2  comp=  177.5  (deep_value,smart_money)
- `21:54:55`       FCX    #2  comp=  156.9  (nobrainers,smart_money)