
# 1) Wait + force-deploy

- `14:08:39`     ✓ accepted (attempt 1)
- `14:08:46`     ✓ deployed at 2026-05-06T14:08:44.000+0000

# 2) Smoke invoke

- `14:09:05`     status: 200, dur: 18.7s
- `14:09:05`     body: {"statusCode": 200, "body": "{\"n_evaluated\": 1143, \"n_tier_s\": 0, \"n_tier_a\": 46, \"n_microcap_picks\": 0, \"duration_s\": 17.5}"}
- `14:09:05`       START RequestId: 645cc747-0a08-44d0-b767-bfaa95d4d118 Version: $LATEST
- `14:09:05`       [rev-accel] starting v1.0
- `14:09:05`       [rev-accel] universe: 1200 stocks
- `14:09:05`       [rev-accel] fetch err CORT: HTTP 429
- `14:09:05`       [rev-accel] fetch err LBRT: HTTP 429
- `14:09:05`       [rev-accel] fetch err CGON: HTTP 429
- `14:09:05`       [rev-accel] fetch err TMHC: HTTP 429
- `14:09:05`       [rev-accel] fetch err MTG: HTTP 429
- `14:09:05`       [rev-accel] OK: 1143, no_data: 57
- `14:09:05`       [rev-accel] wrote 1562105b
- `14:09:05`       [rev-accel] tier_s=0 tier_a=46
- `14:09:05`       [rev-accel] top: [('ECPG', 95.0, 3), ('MU', 93.0, 3), ('SNDK', 93.0, 3), ('AGIO', 90.0, 2), ('PGEN', 90.0, 2)]
- `14:09:05`       END RequestId: 645cc747-0a08-44d0-b767-bfaa95d4d118
- `14:09:05`       REPORT RequestId: 645cc747-0a08-44d0-b767-bfaa95d4d118	Duration: 17695.61 ms	Billed Duration: 18327 ms	Memory Size: 1024 MB	Max Memory Used: 114 MB	Init Duration: 631.33 ms

# 3) Inspect output

- `14:09:06`     stats: {"n_universe": 1200, "n_evaluated": 1143, "n_no_data": 57, "n_tier_s": 0, "n_tier_a": 46, "n_tier_b": 108, "n_microcap_picks": 0}
- `14:09:06`   
- `14:09:06`     ── TIER_S INFLECTION ──
- `14:09:06`   
- `14:09:06`     ── TOP 15 OVERALL ──
- `14:09:06`       ECPG   score= 95.0  TIER_A_ACCELERATING       growth=+78%  Δ=+53pp  streak=3Q  GM_Δ=+20.5pp
- `14:09:06`         flags: ACCEL_30PP+,ACCEL_3Q_STREAK,GROWTH_50%+,GM_EXPAND_5PP+,OP_LEVERAGE_20PP+,EPS_ACCELERATING,SMALLCAP_GROWTH_INFLECTION
- `14:09:06`       MU     score= 93.0  TIER_A_ACCELERATING       growth=+196%  Δ=+140pp  streak=3Q  GM_Δ=+36.7pp
- `14:09:06`         flags: ACCEL_30PP+,ACCEL_3Q_STREAK,GROWTH_100%+,GM_EXPAND_5PP+,OP_LEVERAGE_20PP+,EPS_ACCELERATING
- `14:09:06`       SNDK   score= 93.0  TIER_A_ACCELERATING       growth=+251%  Δ=+190pp  streak=3Q  GM_Δ=+52.2pp
- `14:09:06`         flags: ACCEL_30PP+,ACCEL_3Q_STREAK,GROWTH_100%+,GM_EXPAND_5PP+,OP_LEVERAGE_20PP+,EPS_ACCELERATING
- `14:09:06`       AGIO   score= 90.0  TIER_A_ACCELERATING       growth=+138%  Δ=+52pp  streak=2Q  GM_Δ=+10.8pp
- `14:09:06`         flags: ACCEL_30PP+,ACCEL_2Q_STREAK,GROWTH_100%+,GM_EXPAND_5PP+,OP_LEVERAGE_20PP+,EPS_ACCELERATING,SMALLCAP_GROWTH_INFLECTION
- `14:09:06`       PGEN   score= 90.0  TIER_A_ACCELERATING       growth=+284%  Δ=+77pp  streak=2Q  GM_Δ=+40.1pp
- `14:09:06`         flags: ACCEL_30PP+,ACCEL_2Q_STREAK,GROWTH_100%+,GM_EXPAND_5PP+,OP_LEVERAGE_20PP+,EPS_ACCELERATING,SMALLCAP_GROWTH_INFLECTION
- `14:09:06`       ASTS   score= 88.0  TIER_A_ACCELERATING       growth=+2731%  Δ=+1491pp  streak=2Q  GM_Δ=+3620.7pp
- `14:09:06`         flags: ACCEL_30PP+,ACCEL_2Q_STREAK,GROWTH_100%+,GM_EXPAND_5PP+,OP_LEVERAGE_20PP+,CROSSED_$100M_REVENUE
- `14:09:06`       ONDS   score= 88.0  TIER_A_ACCELERATING       growth=+629%  Δ=+47pp  streak=2Q  GM_Δ=+7.2pp
- `14:09:06`         flags: ACCEL_30PP+,ACCEL_2Q_STREAK,GROWTH_100%+,GM_EXPAND_5PP+,OP_LEVERAGE_20PP+,CROSSED_$100M_REVENUE
- `14:09:06`       NUVB   score= 88.0  TIER_B_BUILDING           growth=+2599%  Δ=+1966pp  streak=1Q  GM_Δ=+46.6pp
- `14:09:06`         flags: ACCEL_30PP+,GROWTH_100%+,GM_EXPAND_5PP+,OP_LEVERAGE_20PP+,EPS_ACCELERATING,SMALLCAP_GROWTH_INFLECTION,CROSSED_$100M_REVENUE
- `14:09:06`       TER    score= 85.0  TIER_A_ACCELERATING       growth=+87%  Δ=+43pp  streak=3Q  GM_Δ=+3.7pp
- `14:09:06`         flags: ACCEL_30PP+,ACCEL_3Q_STREAK,GROWTH_50%+,GM_EXPAND_2PP+,OP_LEVERAGE_20PP+,EPS_ACCELERATING
- `14:09:06`       INSM   score= 85.0  TIER_A_ACCELERATING       growth=+153%  Δ=+100pp  streak=2Q  GM_Δ=+5.4pp
- `14:09:06`         flags: ACCEL_30PP+,ACCEL_2Q_STREAK,GROWTH_100%+,GM_EXPAND_5PP+,OP_LEVERAGE_20PP+,EPS_ACCELERATING
- `14:09:06`       BW     score= 85.0  TIER_A_ACCELERATING       growth=+143%  Δ=+172pp  streak=2Q  GM_Δ=+3.5pp
- `14:09:06`         flags: ACCEL_30PP+,ACCEL_2Q_STREAK,GROWTH_100%+,GM_EXPAND_2PP+,OP_LEVERAGE_20PP+,EPS_ACCELERATING,SMALLCAP_GROWTH_INFLECTION
- `14:09:06`       LITE   score= 82.0  TIER_A_ACCELERATING       growth=+90%  Δ=+25pp  streak=3Q  GM_Δ=+10.9pp
- `14:09:06`         flags: ACCEL_15PP+,ACCEL_3Q_STREAK,GROWTH_50%+,GM_EXPAND_5PP+,OP_LEVERAGE_20PP+,EPS_ACCELERATING
- `14:09:06`       AUGO   score= 82.0  TIER_A_ACCELERATING       growth=+88%  Δ=+29pp  streak=3Q  GM_Δ=+13.8pp
- `14:09:06`         flags: ACCEL_15PP+,ACCEL_3Q_STREAK,GROWTH_50%+,GM_EXPAND_5PP+,OP_LEVERAGE_20PP+,EPS_ACCELERATING
- `14:09:06`       KYMR   score= 80.0  TIER_B_BUILDING           growth=+56%  Δ=+117pp  streak=1Q  GM_Δ=+18.2pp
- `14:09:06`         flags: ACCEL_30PP+,GROWTH_50%+,GM_EXPAND_5PP+,OP_LEVERAGE_20PP+,EPS_ACCELERATING,CROSSED_$100M_REVENUE
- `14:09:06`       RUN    score= 80.0  TIER_A_ACCELERATING       growth=+124%  Δ=+89pp  streak=2Q  GM_Δ=+15.7pp
- `14:09:06`         flags: ACCEL_30PP+,ACCEL_2Q_STREAK,GROWTH_100%+,GM_EXPAND_5PP+,OP_LEVERAGE_20PP+
- `14:09:06`   
- `14:09:06`     ── MICROCAP PICKS ──