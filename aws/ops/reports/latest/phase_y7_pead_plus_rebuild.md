
# 1) Deploy earnings-pead (NEW)

- `13:47:33`     ✓ deployed + scheduled
- `13:47:33`     invoking PEAD...
- `13:47:50`     status: 200, dur: 16.5s
- `13:47:50`     body: {"n_evaluated": 0, "n_tier_s": 0, "n_tier_a": 0, "duration_s": 15.6}
- `13:47:50`       START RequestId: 7d587fdc-22e6-4466-b0d1-6bd2bda8761b Version: $LATEST
- `13:47:50`       [pead] starting v1.0
- `13:47:50`       [pead] universe: 1500 stocks
- `13:47:50`       [pead] OK: 0, no_data: 1500
- `13:47:50`       [pead] wrote 348b
- `13:47:50`       [pead] tier_s=0 tier_a=0
- `13:47:50`       END RequestId: 7d587fdc-22e6-4466-b0d1-6bd2bda8761b
- `13:47:50`       REPORT RequestId: 7d587fdc-22e6-4466-b0d1-6bd2bda8761b	Duration: 15686.00 ms	Billed Duration: 16237 ms	Memory Size: 1024 MB	Max Memory Used: 111 MB	Init Duration: 550.38 ms

# 2) Redeploy volatility-squeeze with multi-cap universe

- `13:48:02`     ✓ redeployed
- `13:48:02`     invoking squeeze...
- `13:48:26`     status: 200, dur: 24.5s
- `13:48:26`     body: {"n_evaluated": 715, "n_tier_s": 4, "n_tier_a": 17, "n_tier_b": 49, "duration_s": 23.5}

# 3) Redeploy revenue-acceleration with multi-cap universe

- `13:48:39`     ✓ redeployed
- `13:48:39`     invoking rev-accel...
- `13:48:47`     status: 200, dur: 8.4s
- `13:48:47`     body: {"n_evaluated": 0, "n_tier_s": 0, "n_tier_a": 0, "n_microcap_picks": 0, "duration_s": 7.6}

# 4) Inspect all 3 outputs

- `13:48:48`   
- `13:48:48`     ── data/earnings-pead.json ──
- `13:48:48`     stats: {"n_universe": 1500, "n_evaluated": 0, "n_no_data": 1500, "n_tier_s": 0, "n_tier_a": 0, "n_tier_b": 0, "top_100_by_cap_bucket": {}}
- `13:48:48`   
- `13:48:48`     ── data/volatility-squeeze.json ──
- `13:48:48`     stats: {"n_universe": 1500, "n_evaluated": 715, "n_no_data": 785, "n_tier_s": 4, "n_tier_a": 17, "n_tier_b": 49, "n_watch": 140}
- `13:48:48`       KVUE   score= 88.0  n_sig=5  base=127d  bb=0%
- `13:48:48`       STRD   score= 88.0  n_sig=5  base=198d  bb=4%
- `13:48:48`       GTLS   score= 80.0  n_sig=5  base=195d  bb=4%
- `13:48:48`       TXNM   score= 75.0  n_sig=5  base=199d  bb=2%
- `13:48:48`       COGT   score= 72.0  n_sig=4  base=122d  bb=0%
- `13:48:48`       WBD    score= 71.0  n_sig=4  base=110d  bb=9%
- `13:48:48`       CWAN   score= 71.0  n_sig=4  base=116d  bb=0%
- `13:48:48`       COLB   score= 67.0  n_sig=4  base=131d  bb=0%
- `13:48:48`   
- `13:48:48`     ── data/revenue-acceleration.json ──
- `13:48:48`     stats: {"n_universe": 1500, "n_evaluated": 0, "n_no_data": 1500, "n_tier_s": 0, "n_tier_a": 0, "n_tier_b": 0, "n_microcap_picks": 0}