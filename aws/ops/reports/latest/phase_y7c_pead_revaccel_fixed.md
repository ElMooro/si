
# 1) Redeploy + invoke PEAD

- `13:57:54`     ✓ deployed
- `13:57:54`     invoking PEAD...
- `13:58:15`     status: 200, dur: 20.7s
- `13:58:15`     body: {"n_evaluated": 603, "n_tier_s": 43, "n_tier_a": 72, "duration_s": 19.7}
- `13:58:15`       START RequestId: 0c1e5c53-f4df-4420-902c-8546626144a3 Version: $LATEST
- `13:58:15`       [pead] starting v1.0
- `13:58:15`       [pead] universe: 1500 stocks
- `13:58:15`       [pead] OK: 603, no_data: 897
- `13:58:15`       [pead] wrote 631419b
- `13:58:15`       [pead] tier_s=43 tier_a=72
- `13:58:15`       [pead] top: [('GOOGL', 90.0, 8), ('GOOG', 90.0, 8), ('SNDK', 90.0, 5), ('BE', 90.0, 6), ('FIX', 90.0, 8)]
- `13:58:15`       END RequestId: 0c1e5c53-f4df-4420-902c-8546626144a3
- `13:58:15`       REPORT RequestId: 0c1e5c53-f4df-4420-902c-8546626144a3	Duration: 19854.87 ms	Billed Duration: 20428 ms	Memory Size: 1024 MB	Max Memory Used: 122 MB	Init Duration: 572.32 ms

# 2) Redeploy + invoke rev-accel (workers=6)

- `13:58:20`     ✓ deployed with N_WORKERS=6
- `13:58:20`     invoking rev-accel...
- `13:58:29`     status: 200, dur: 9.1s
- `13:58:29`     body: {"n_evaluated": 0, "n_tier_s": 0, "n_tier_a": 0, "n_microcap_picks": 0, "duration_s": 8.2}
- `13:58:29`       START RequestId: 248b244a-27db-4896-8867-8b4ad55aeefe Version: $LATEST
- `13:58:29`       [rev-accel] starting v1.0
- `13:58:29`       [rev-accel] universe: 1500 stocks
- `13:58:29`       [rev-accel] OK: 0, no_data: 1500
- `13:58:29`       [rev-accel] wrote 351b
- `13:58:29`       [rev-accel] tier_s=0 tier_a=0
- `13:58:29`       END RequestId: 248b244a-27db-4896-8867-8b4ad55aeefe
- `13:58:29`       REPORT RequestId: 248b244a-27db-4896-8867-8b4ad55aeefe	Duration: 8280.90 ms	Billed Duration: 8880 ms	Memory Size: 1024 MB	Max Memory Used: 104 MB	Init Duration: 598.84 ms

# 3) Inspect outputs

- `13:58:30`   
- `13:58:30`     ── PEAD ──
- `13:58:30`     stats: {"n_universe": 1500, "n_evaluated": 603, "n_no_data": 897, "n_tier_s": 43, "n_tier_a": 72, "n_tier_b": 132, "top_100_by_cap_bucket": {"mega": 10, "large": 65, "mid": 25}}
- `13:58:30`       GOOGL  score= 90.0  streak=8Q  surprise=+93.6%  cap=mega    drift=✓
- `13:58:30`       GOOG   score= 90.0  streak=8Q  surprise=+90.7%  cap=mega    drift=✓
- `13:58:30`       SNDK   score= 90.0  streak=5Q  surprise=+60.1%  cap=mega    drift=✓
- `13:58:30`       BE     score= 90.0  streak=6Q  surprise=+255.1%  cap=large   drift=✓
- `13:58:30`       FIX    score= 90.0  streak=8Q  surprise=+54.3%  cap=large   drift=✓
- `13:58:30`       EL     score= 90.0  streak=8Q  surprise=+37.9%  cap=large   drift=✓
- `13:58:30`       MYRG   score= 90.0  streak=7Q  surprise=+43.1%  cap=mid     drift=✓
- `13:58:30`       MU     score= 85.0  streak=8Q  surprise=+32.8%  cap=mega    drift=✓
- `13:58:30`       PWR    score= 85.0  streak=8Q  surprise=+31.4%  cap=large   drift=✓
- `13:58:30`       MTZ    score= 85.0  streak=8Q  surprise=+40.5%  cap=large   drift=✓
- `13:58:30`       SNX    score= 85.0  streak=4Q  surprise=+43.8%  cap=large   drift=✓
- `13:58:30`       WAL    score= 85.0  streak=6Q  surprise=+50.0%  cap=mid     drift=✓
- `13:58:30`   
- `13:58:30`     ── Rev-Accel ──
- `13:58:30`     stats: {"n_universe": 1500, "n_evaluated": 0, "n_no_data": 1500, "n_tier_s": 0, "n_tier_a": 0, "n_tier_b": 0, "n_microcap_picks": 0}