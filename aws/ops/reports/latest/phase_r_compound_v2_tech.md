
# 1) Force-deploy compound aggregator with momentum + pre_pump feeds

- `00:21:14`     source: 14755 chars
- `00:21:14`       ✓ "momentum":       ("data/momentum-breakout.json"
- `00:21:14`       ✓ "pre_pump":       ("data/pre-pump-signals.json"
- `00:21:14`       ✓ elif name == "momentum":
- `00:21:14`       ✓ elif name == "pre_pump":
- `00:21:14`       ✓ "momentum": "🚀"
- `00:21:16`     ✓ deployed at 2026-05-06T00:21:15.000+0000

# 2) Force-invoke compound — should now see 7 feeds + new intersections

- `00:21:18`     status: 200
- `00:21:18`     body: {"n_compound": 15, "n_3_plus": 5, "n_alerts": 14, "duration_s": 0.68}
- `00:21:18`       [compound] smart_money: 85 entries
- `00:21:18`       [compound] deep_value: 22 entries
- `00:21:18`       [compound] eps_velocity: 25 entries
- `00:21:18`       [compound] momentum: 25 entries
- `00:21:18`       [compound] pre_pump: 25 entries
- `00:21:18`       [compound] aggregated: 209 names, 15 multi-signal
- `00:21:18`       [compound] new alerts this run: 14
- `00:21:18`       [compound] wrote 11750b to data/compound-signals.json
- `00:21:18`       [compound] wrote state: 21 alerted_keys tracked
- `00:21:18`       [compound] alert send: ok=True info=695
- `00:21:18`       END RequestId: 731fd7e9-7b80-4ce3-bef0-a1f198dacbce
- `00:21:18`       REPORT RequestId: 731fd7e9-7b80-4ce3-bef0-a1f198dacbce	Duration: 1314.56 ms	Billed Duration: 1870 ms	Memory Size: 512 MB	Max Memory Used: 101 MB	Init Duration: 554.45 ms

# 3) Updated compound state — full leaderboard

- `00:21:18`     generated_at: 2026-05-06T00:21:17+00:00
- `00:21:18`     feed_stats:   {"nobrainers": 25, "insiders": 22, "smart_money": 85, "deep_value": 22, "eps_velocity": 25, "momentum": 25, "pre_pump": 25}
- `00:21:18`     stats:        {"n_total_names": 209, "n_multi_signal": 15, "n_3_plus": 5, "n_compound_over_200": 11, "n_compound_over_300": 5}
- `00:21:18`   
- `00:21:18`     ── compound leaderboard (top 15) ──
- `00:21:18`       AVGO   #3  comp=  443.0  (eps_velocity,momentum,smart_money)
- `00:21:18`       AMZN   #3  comp=  396.8  (momentum,pre_pump,smart_money)
- `00:21:18`       FCX    #3  comp=  367.8  (eps_velocity,nobrainers,smart_money)
- `00:21:18`       OXY    #3  comp=  361.8  (nobrainers,pre_pump,smart_money)
- `00:21:18`       HUM    #3  comp=  360.6  (deep_value,pre_pump,smart_money)
- `00:21:18`       AMAT   #2  comp=  227.7  (eps_velocity,nobrainers)
- `00:21:18`       CSGP   #2  comp=  220.7  (eps_velocity,insiders)
- `00:21:18`       EPAM   #2  comp=  213.0  (deep_value,insiders)
- `00:21:18`       EXAS   #2  comp=  209.2  (eps_velocity,pre_pump)
- `00:21:18`       APA    #2  comp=  208.2  (nobrainers,pre_pump)
- `00:21:18`       FIX    #2  comp=  202.8  (eps_velocity,momentum)
- `00:21:18`       PLXS   #2  comp=  189.8  (momentum,pre_pump)
- `00:21:18`       GOOGL  #2  comp=  180.9  (momentum,smart_money)
- `00:21:18`       NUE    #2  comp=  178.2  (momentum,pre_pump)
- `00:21:18`       COP    #2  comp=  132.9  (pre_pump,smart_money)

# 4) NEW: Cross-system convergence between FUNDAMENTALS and TECHNICALS

- `00:21:18`     Names appearing on BOTH a fundamental hunter (NB/insider/SM/DV/EPS)
- `00:21:18`     AND a technical hunter (momentum or pre_pump) are the highest-conviction
- `00:21:18`     setups — confirmed by data + price action both.
- `00:21:18`   
- `00:21:18`     Found 9 cross-domain convergent setups:
- `00:21:18`       AVGO   #3  comp=  443.0  (eps_velocity,momentum,smart_money)
- `00:21:18`       AMZN   #3  comp=  396.8  (momentum,pre_pump,smart_money)
- `00:21:18`       OXY    #3  comp=  361.8  (nobrainers,pre_pump,smart_money)
- `00:21:18`       HUM    #3  comp=  360.6  (deep_value,pre_pump,smart_money)
- `00:21:18`       EXAS   #2  comp=  209.2  (eps_velocity,pre_pump)
- `00:21:18`       APA    #2  comp=  208.2  (nobrainers,pre_pump)
- `00:21:18`       FIX    #2  comp=  202.8  (eps_velocity,momentum)
- `00:21:18`       GOOGL  #2  comp=  180.9  (momentum,smart_money)
- `00:21:18`       COP    #2  comp=  132.9  (pre_pump,smart_money)

# 5) Pure technical setups (momentum + pre_pump only — early signals)

- `00:21:18`     2 names appear on BOTH momentum + pre_pump
- `00:21:18`       PLXS  comp=189.8
- `00:21:18`       NUE  comp=178.2