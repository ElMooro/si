- `14:00:06`   source: 18374 chars

# 1) Wait + force-deploy compound v4

- `14:00:14`     ✓ deployed at 2026-05-06T14:00:09.000+0000

# 2) Force-invoke

- `14:00:17`     body: {"n_compound": 31, "n_3_plus": 6, "n_alerts": 6, "duration_s": 1.27}
- `14:00:17`       [compound] activist: 1 entries
- `14:00:17`       [compound] vol_squeeze: 25 entries
- `14:00:17`       [compound] rev_accel: 0 entries
- `14:00:17`       [compound] microcap_sq: 25 entries
- `14:00:17`       [compound] pead: 30 entries
- `14:00:17`       [compound] aggregated: 281 names, 31 multi-signal
- `14:00:17`       [compound] new alerts this run: 6
- `14:00:17`       [compound] wrote 20113b to data/compound-signals.json
- `14:00:17`       [compound] wrote state: 40 alerted_keys tracked
- `14:00:17`       [compound] alert send: ok=True info=729
- `14:00:17`       END RequestId: 2ec26f48-e1f4-49c2-8f28-d8033eeac763
- `14:00:17`       REPORT RequestId: 2ec26f48-e1f4-49c2-8f28-d8033eeac763	Duration: 1881.45 ms	Billed Duration: 2695 ms	Memory Size: 512 MB	Max Memory Used: 102 MB	Init Duration: 813.25 ms

# 3) Inspect compound v4 (13 feeds)

- `14:00:17`     feed_stats: {"nobrainers": 25, "insiders": 22, "smart_money": 85, "deep_value": 9, "eps_velocity": 25, "momentum": 25, "pre_pump": 25, "options_flow": 25, "activist": 1, "vol_squeeze": 25, "rev_accel": 0, "microcap_sq": 25, "pead": 30}
- `14:00:17`     stats:      {"n_total_names": 281, "n_multi_signal": 31, "n_3_plus": 6, "n_compound_over_200": 22, "n_compound_over_300": 6}
- `14:00:17`   
- `14:00:17`     ── TOP 20 COMPOUND (13-feed fusion) ──
- `14:00:17`       FCX    #5  comp= 1061  (eps_velocity,nobrainers,options_flow,pead,smart_money)
- `14:00:17`       FIX    #4  comp=  788  (eps_velocity,momentum,options_flow,pead)
- `14:00:17`       AVGO   #3  comp=  443  (eps_velocity,momentum,smart_money)
- `14:00:17`       EXAS   #3  comp=  403  (eps_velocity,pre_pump,vol_squeeze)
- `14:00:17`       AMZN   #3  comp=  397  (momentum,pre_pump,smart_money)
- `14:00:17`       HUM    #3  comp=  361  (deep_value,pre_pump,smart_money)
- `14:00:17`       CRDO   #2  comp=  267  (eps_velocity,options_flow)
- `14:00:17`       COHR   #2  comp=  248  (eps_velocity,options_flow)
- `14:00:17`       KLAC   #2  comp=  246  (eps_velocity,options_flow)
- `14:00:17`       HOOD   #2  comp=  244  (eps_velocity,options_flow)
- `14:00:17`       CRM    #2  comp=  243  (nobrainers,pead)
- `14:00:17`       GOOG   #2  comp=  233  (pead,smart_money)
- `14:00:17`       COIN   #2  comp=  231  (eps_velocity,options_flow)
- `14:00:17`       AMAT   #2  comp=  227  (eps_velocity,nobrainers)
- `14:00:17`       GOOGL  #2  comp=  223  (pead,smart_money)
- `14:00:17`       INTC   #2  comp=  220  (eps_velocity,nobrainers)
- `14:00:17`       FDX    #2  comp=  218  (pead,smart_money)
- `14:00:17`       EPAM   #2  comp=  213  (deep_value,insiders)
- `14:00:17`       CDNS   #2  comp=  212  (momentum,options_flow)
- `14:00:17`       GILD   #2  comp=  211  (options_flow,smart_money)

# 4) Build finale digest

- `14:00:18`     message: 1592 chars

# 5) Send finale digest

- `14:00:19`     ✅ delivered, message_id=730