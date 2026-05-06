
# 1) Wait for any in-flight deploy on compound aggregator

- `10:53:38`     ✓ ready
- `10:53:38`     source: 15907 chars
- `10:53:38`       ✓ "options_flow":   ("data/options-flow.json"
- `10:53:38`       ✓ "activist":       ("data/activist-filings.json"
- `10:53:38`       ✓ elif name == "options_flow":
- `10:53:38`       ✓ elif name == "activist":

# 2) Force-deploy compound v3 (with retry)

- `10:53:38`     ✓ update accepted on attempt 1
- `10:53:41`     deployed at 2026-05-06T10:53:38.000+0000

# 3) Force-invoke

- `10:53:44`     body: {"n_compound": 27, "n_3_plus": 6, "n_alerts": 11, "duration_s": 0.85}
- `10:53:44`       [compound] eps_velocity: 25 entries
- `10:53:44`       [compound] momentum: 25 entries
- `10:53:44`       [compound] pre_pump: 25 entries
- `10:53:44`       [compound] options_flow: 25 entries
- `10:53:44`       [compound] activist: 1 entries
- `10:53:44`       [compound] aggregated: 208 names, 27 multi-signal
- `10:53:44`       [compound] new alerts this run: 11
- `10:53:44`       [compound] wrote 17918b to data/compound-signals.json
- `10:53:44`       [compound] wrote state: 33 alerted_keys tracked
- `10:53:44`       [compound] alert send: ok=True info=719
- `10:53:44`       END RequestId: 459a5265-150c-447d-a186-decab52d6a61
- `10:53:44`       REPORT RequestId: 459a5265-150c-447d-a186-decab52d6a61	Duration: 1488.13 ms	Billed Duration: 2032 ms	Memory Size: 512 MB	Max Memory Used: 101 MB	Init Duration: 543.40 ms

# 4) Inspect compound state with 9 feeds

- `10:53:44`     feed_stats: {"nobrainers": 25, "insiders": 22, "smart_money": 85, "deep_value": 9, "eps_velocity": 25, "momentum": 25, "pre_pump": 25, "options_flow": 25, "activist": 1}
- `10:53:44`     stats:      {"n_total_names": 208, "n_multi_signal": 27, "n_3_plus": 6, "n_compound_over_200": 20, "n_compound_over_300": 6}
- `10:53:44`   
- `10:53:44`     ── Top 15 compound (9-feed fusion) ──
- `10:53:44`       FCX    #4  comp=  660  (eps_velocity,nobrainers,options_flow,smart_money)
- `10:53:44`       AVGO   #3  comp=  443  (eps_velocity,momentum,smart_money)
- `10:53:44`       FIX    #3  comp=  440  (eps_velocity,momentum,options_flow)
- `10:53:44`       AMZN   #3  comp=  397  (momentum,pre_pump,smart_money)
- `10:53:44`       OXY    #3  comp=  362  (nobrainers,pre_pump,smart_money)
- `10:53:44`       HUM    #3  comp=  361  (deep_value,pre_pump,smart_money)
- `10:53:44`       CRDO   #2  comp=  267  (eps_velocity,options_flow)
- `10:53:44`       COHR   #2  comp=  248  (eps_velocity,options_flow)
- `10:53:44`       KLAC   #2  comp=  246  (eps_velocity,options_flow)
- `10:53:44`       HOOD   #2  comp=  244  (eps_velocity,options_flow)
- `10:53:44`       COIN   #2  comp=  231  (eps_velocity,options_flow)
- `10:53:44`       AMAT   #2  comp=  228  (eps_velocity,nobrainers)
- `10:53:44`       ADI    #2  comp=  220  (momentum,options_flow)
- `10:53:44`       EPAM   #2  comp=  213  (deep_value,insiders)
- `10:53:44`       CDNS   #2  comp=  212  (momentum,options_flow)

# 5) Build + send finale digest

- `10:53:44`     message: 1628 chars
- `10:53:45`     ✅ delivered, message_id=720