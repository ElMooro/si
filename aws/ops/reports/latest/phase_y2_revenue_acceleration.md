- `11:18:46`     source: 16373 chars

# 1) Deploy

- `11:18:48`     ✓ deployed

# 2) Schedule daily 9:45 UTC

- `11:18:48`     ✓ permission added

# 3) Smoke invoke (heavy — fetches quarterly income for 600 stocks)

- `11:18:53`     status: 200, dur: 5.3s
- `11:18:53`     body: {"statusCode": 200, "body": "{\"n_evaluated\": 336, \"n_tier_s\": 0, \"n_tier_a\": 12, \"n_microcap_picks\": 0, \"duration_s\": 4.3}"}
- `11:18:53`       START RequestId: a9738230-f400-41b2-a09c-c1c4c0393ef1 Version: $LATEST
- `11:18:53`       [rev-accel] starting v1.0
- `11:18:53`       [rev-accel] universe: 338 stocks
- `11:18:53`       [rev-accel] OK: 336, no_data: 2
- `11:18:53`       [rev-accel] wrote 468458b
- `11:18:53`       [rev-accel] tier_s=0 tier_a=12
- `11:18:53`       [rev-accel] top: [('AGIO', 90.0, 2), ('FCEL', 85.0, 1), ('ET', 78.0, 3), ('AXTI', 76.0, 1), ('EXE', 75.0, 1)]
- `11:18:53`       END RequestId: a9738230-f400-41b2-a09c-c1c4c0393ef1
- `11:18:53`       REPORT RequestId: a9738230-f400-41b2-a09c-c1c4c0393ef1	Duration: 4421.46 ms	Billed Duration: 4955 ms	Memory Size: 1024 MB	Max Memory Used: 106 MB	Init Duration: 532.59 ms

# 4) Inspect output

- `11:18:54`     generated_at: 2026-05-06T11:18:53+00:00
- `11:18:54`     stats: {"n_universe": 338, "n_evaluated": 336, "n_no_data": 2, "n_tier_s": 0, "n_tier_a": 12, "n_tier_b": 32, "n_microcap_picks": 0}
- `11:18:54`   
- `11:18:54`     ── TIER_S INFLECTION (4Q+ accelerating, score >= 80) ──
- `11:18:54`       (none today — these are rare)
- `11:18:54`   
- `11:18:54`     ── TOP 15 OVERALL ──
- `11:18:54`       AGIO   score= 90.0  TIER_A_ACCELERATING       growth=+138%  Δ=+51.7pp  streak=2Q  GM_Δ=+10.8pp
- `11:18:54`         flags: ACCEL_30PP+,ACCEL_2Q_STREAK,GROWTH_100%+,GM_EXPAND_5PP+,OP_LEVERAGE_20PP+,EPS_ACCELERATING,SMALLCAP_GROWTH_INFLECTION
- `11:18:54`       FCEL   score= 85.0  TIER_B_BUILDING           growth=+61%  Δ=+49.2pp  streak=1Q  GM_Δ=+6.0pp
- `11:18:54`         flags: ACCEL_30PP+,GROWTH_50%+,GM_EXPAND_5PP+,OP_LEVERAGE_20PP+,EPS_ACCELERATING,SMALLCAP_GROWTH_INFLECTION,CROSSED_$100M_REVENUE
- `11:18:54`       ET     score= 78.0  TIER_A_ACCELERATING       growth=+32%  Δ=+17.4pp  streak=3Q  GM_Δ=+9.9pp
- `11:18:54`         flags: ACCEL_15PP+,ACCEL_3Q_STREAK,GROWTH_25%+,GM_EXPAND_5PP+,OP_LEVERAGE_20PP+,EPS_ACCELERATING
- `11:18:54`       AXTI   score= 76.0  TIER_B_BUILDING           growth=+39%  Δ=+47.3pp  streak=1Q  GM_Δ=+21.7pp
- `11:18:54`         flags: ACCEL_30PP+,GROWTH_25%+,GM_EXPAND_5PP+,OP_LEVERAGE_20PP+,EPS_ACCELERATING,CROSSED_$100M_REVENUE
- `11:18:54`       EXE    score= 75.0  TIER_B_BUILDING           growth=+100%  Δ=+47.8pp  streak=1Q  GM_Δ=+14.9pp
- `11:18:54`         flags: ACCEL_30PP+,GROWTH_100%+,GM_EXPAND_5PP+,OP_LEVERAGE_20PP+,EPS_ACCELERATING
- `11:18:54`       GOLD   score= 75.0  TIER_A_ACCELERATING       growth=+136%  Δ=+100.6pp  streak=2Q  GM_Δ=-3.9pp
- `11:18:54`         flags: ACCEL_30PP+,ACCEL_2Q_STREAK,GROWTH_100%+,OP_LEVERAGE_20PP+,EPS_ACCELERATING,SMALLCAP_GROWTH_INFLECTION
- `11:18:54`       KKR    score= 72.0  TIER_B_BUILDING           growth=+72%  Δ=+57.0pp  streak=1Q  GM_Δ=+38.3pp
- `11:18:54`         flags: ACCEL_30PP+,GROWTH_50%+,GM_EXPAND_5PP+,OP_LEVERAGE_20PP+,EPS_ACCELERATING
- `11:18:54`       FORM   score= 70.0  TIER_A_ACCELERATING       growth=+32%  Δ=+18.4pp  streak=2Q  GM_Δ=+10.7pp
- `11:18:54`         flags: ACCEL_15PP+,ACCEL_2Q_STREAK,GROWTH_25%+,GM_EXPAND_5PP+,OP_LEVERAGE_20PP+,EPS_ACCELERATING
- `11:18:54`       BCRX   score= 70.0  TIER_B_BUILDING           growth=+209%  Δ=+173.0pp  streak=1Q  GM_Δ=+0.7pp
- `11:18:54`         flags: ACCEL_30PP+,GROWTH_100%+,OP_LEVERAGE_20PP+,EPS_ACCELERATING,SMALLCAP_GROWTH_INFLECTION
- `11:18:54`       CDNA   score= 68.0  TIER_A_ACCELERATING       growth=+39%  Δ=+13.8pp  streak=3Q  GM_Δ=+4.5pp
- `11:18:54`         flags: ACCEL_5PP+,ACCEL_3Q_STREAK,GROWTH_25%+,GM_EXPAND_2PP+,OP_LEVERAGE_20PP+,EPS_ACCELERATING,SMALLCAP_GROWTH_INFLECTION
- `11:18:54`       AGEN   score= 68.0  TIER_A_ACCELERATING       growth=+28%  Δ=+7.1pp  streak=3Q  GM_Δ=+88.3pp
- `11:18:54`         flags: ACCEL_5PP+,ACCEL_3Q_STREAK,GROWTH_25%+,GM_EXPAND_5PP+,OP_LEVERAGE_20PP+,EPS_ACCELERATING
- `11:18:54`       APO    score= 67.0  TIER_A_ACCELERATING       growth=+54%  Δ=+27.2pp  streak=3Q  GM_Δ=-24.5pp
- `11:18:54`         flags: ACCEL_15PP+,ACCEL_3Q_STREAK,GROWTH_50%+,OP_LEVERAGE_20PP+,EPS_ACCELERATING
- `11:18:54`       EOG    score= 65.0  TIER_A_ACCELERATING       growth=+16%  Δ=+15.9pp  streak=3Q  GM_Δ=+14.9pp
- `11:18:54`         flags: ACCEL_15PP+,ACCEL_3Q_STREAK,GM_EXPAND_5PP+,EPS_ACCELERATING
- `11:18:54`       EBC    score= 65.0  TIER_A_ACCELERATING       growth=+1206%  Δ=+1182.2pp  streak=2Q  GM_Δ=+0.4pp
- `11:18:54`         flags: ACCEL_30PP+,ACCEL_2Q_STREAK,GROWTH_100%+,OP_LEVERAGE_20PP+
- `11:18:54`       FIX    score= 63.0  TIER_A_ACCELERATING       growth=+56%  Δ=+14.8pp  streak=3Q  GM_Δ=+2.9pp
- `11:18:54`         flags: ACCEL_5PP+,ACCEL_3Q_STREAK,GROWTH_50%+,GM_EXPAND_2PP+,OP_LEVERAGE_10PP+,EPS_ACCELERATING
- `11:18:54`   
- `11:18:54`     ── MICROCAP PICKS (mcap < $500M, growth > 30%, accelerating) ──