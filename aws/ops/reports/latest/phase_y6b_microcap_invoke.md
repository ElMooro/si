
# 1) Wait for v2 deploy to complete

- `12:11:04`     ✓ ready, last modified 2026-05-06T12:03:56.000+0000

# 2) Smoke invoke

- `12:11:09`     status: 200, dur: 5.6s
- `12:11:09`     body: {"statusCode": 200, "body": "{\"n_evaluated\": 244, \"n_tier_s\": 2, \"n_tier_a\": 46, \"duration_s\": 4.7}"}
- `12:11:09`       START RequestId: 182a5232-bb93-49dc-9e8a-b8ae92a8325b Version: $LATEST
- `12:11:09`       [float-sq] starting v1.0
- `12:11:09`       [float-sq] universe: 600 stocks
- `12:11:09`       [float-sq] fetching FINRA short volume history...
- `12:11:09`       [float-sq] FINRA tickers: 12487
- `12:11:09`       [float-sq] OK: 244, filtered_out: 356
- `12:11:09`       [float-sq] wrote 144928b
- `12:11:09`       [float-sq] tier_s=2 tier_a=46
- `12:11:09`       END RequestId: 182a5232-bb93-49dc-9e8a-b8ae92a8325b
- `12:11:09`       REPORT RequestId: 182a5232-bb93-49dc-9e8a-b8ae92a8325b	Duration: 4810.42 ms	Billed Duration: 5362 ms	Memory Size: 2048 MB	Max Memory Used: 176 MB	Init Duration: 551.16 ms

# 3) Inspect output

- `12:11:09`     stats: {"n_universe": 600, "n_evaluated": 244, "n_filtered_out": 356, "n_tier_s": 2, "n_tier_a": 46, "n_tier_b": 91, "n_finra_tickers": 12487}
- `12:11:09`   
- `12:11:09`     ── TIER_S PARABOLIC ──
- `12:11:09`       JBLU
- `12:11:09`       DUOL
- `12:11:09`   
- `12:11:09`     ── TOP 20 OVERALL ──
- `12:11:09`       JBLU   score= 77.0  mcap=$1801M  float_turn=10.3%  short=57%  d2c=1.4d  Δshort=+6.2
- `12:11:09`         flags: FLOAT_HEAVY_TURNOVER,SHORT_PCT_50%+,SHORT_RISING_5PP+,BASE_FORMING_SETUP
- `12:11:09`       DUOL   score= 71.0  mcap=$4847M  float_turn=5.1%  short=64%  d2c=1.9d  Δshort=+3.6
- `12:11:09`         flags: FLOAT_HEAVY_TURNOVER,SHORT_PCT_50%+,SHORT_RISING,BASE_FORMING_SETUP
- `12:11:09`       MARA   score= 67.0  mcap=$4624M  float_turn=15.4%  short=57%  d2c=1.6d  Δshort=+5.5
- `12:11:09`         flags: FLOAT_HEAVY_TURNOVER,SHORT_PCT_50%+,SHORT_RISING_5PP+
- `12:11:09`       HCC    score= 67.0  mcap=$4572M  float_turn=2.1%  short=78%  d2c=2.3d  Δshort=+17.8
- `12:11:09`         flags: FLOAT_MOD_TURNOVER,SHORT_PCT_50%+,SHORT_RISING_5PP+,BASE_FORMING_SETUP
- `12:11:09`       CLSK   score= 67.0  mcap=$3430M  float_turn=9.7%  short=54%  d2c=1.2d  Δshort=+7.4
- `12:11:09`         flags: FLOAT_HEAVY_TURNOVER,SHORT_PCT_50%+,SHORT_RISING_5PP+
- `12:11:09`       BL     score= 67.0  mcap=$1926M  float_turn=2.2%  short=70%  d2c=1.3d  Δshort=+18.0
- `12:11:09`         flags: FLOAT_MOD_TURNOVER,SHORT_PCT_50%+,SHORT_RISING_5PP+,BASE_FORMING_SETUP
- `12:11:09`       OLLI   score= 67.0  mcap=$4985M  float_turn=2.9%  short=73%  d2c=1.4d  Δshort=+16.7
- `12:11:09`         flags: FLOAT_MOD_TURNOVER,SHORT_PCT_50%+,SHORT_RISING_5PP+,BASE_FORMING_SETUP
- `12:11:09`       EYE    score= 67.0  mcap=$1783M  float_turn=2.3%  short=64%  d2c=1.2d  Δshort=+11.8
- `12:11:09`         flags: FLOAT_MOD_TURNOVER,SHORT_PCT_50%+,SHORT_RISING_5PP+,BASE_FORMING_SETUP
- `12:11:09`       PRKS   score= 67.0  mcap=$1664M  float_turn=2.5%  short=79%  d2c=0.9d  Δshort=+11.5
- `12:11:09`         flags: FLOAT_MOD_TURNOVER,SHORT_PCT_50%+,SHORT_RISING_5PP+,BASE_FORMING_SETUP
- `12:11:09`       DNOW   score= 67.0  mcap=$1604M  float_turn=3.0%  short=62%  d2c=1.2d  Δshort=+10.7
- `12:11:09`         flags: FLOAT_MOD_TURNOVER,SHORT_PCT_50%+,SHORT_RISING_5PP+,BASE_FORMING_SETUP
- `12:11:09`       UAMY   score= 67.0  mcap=$1599M  float_turn=10.7%  short=51%  d2c=2.0d  Δshort=+9.6
- `12:11:09`         flags: FLOAT_HEAVY_TURNOVER,SHORT_PCT_50%+,SHORT_RISING_5PP+
- `12:11:09`       VRDN   score= 67.0  mcap=$1591M  float_turn=5.0%  short=50%  d2c=1.4d  Δshort=+12.6
- `12:11:09`         flags: FLOAT_HEAVY_TURNOVER,SHORT_PCT_50%+,SHORT_RISING_5PP+
- `12:11:09`       VOYG   score= 67.0  mcap=$1562M  float_turn=3.1%  short=63%  d2c=1.4d  Δshort=+7.5
- `12:11:09`         flags: FLOAT_MOD_TURNOVER,SHORT_PCT_50%+,SHORT_RISING_5PP+,BASE_FORMING_SETUP
- `12:11:09`       GBX    score= 67.0  mcap=$1552M  float_turn=2.1%  short=62%  d2c=0.8d  Δshort=+6.8
- `12:11:09`         flags: FLOAT_MOD_TURNOVER,SHORT_PCT_50%+,SHORT_RISING_5PP+,BASE_FORMING_SETUP
- `12:11:09`       HTZ    score= 64.0  mcap=$1902M  float_turn=5.8%  short=49%  d2c=1.2d  Δshort=+2.8
- `12:11:09`         flags: FLOAT_HEAVY_TURNOVER,SHORT_PCT_40%+,SHORT_RISING,BASE_FORMING_SETUP
- `12:11:09`       PLUG   score= 61.0  mcap=$3808M  float_turn=7.4%  short=51%  d2c=1.6d  Δshort=+3.1
- `12:11:09`         flags: FLOAT_HEAVY_TURNOVER,SHORT_PCT_50%+,SHORT_RISING
- `12:11:09`       FLO    score= 61.0  mcap=$1830M  float_turn=2.7%  short=63%  d2c=1.4d  Δshort=+3.0
- `12:11:09`         flags: FLOAT_MOD_TURNOVER,SHORT_PCT_50%+,SHORT_RISING,BASE_FORMING_SETUP
- `12:11:09`       UFPT   score= 61.0  mcap=$1639M  float_turn=3.4%  short=75%  d2c=2.1d  Δshort=+2.7
- `12:11:09`         flags: FLOAT_MOD_TURNOVER,SHORT_PCT_50%+,SHORT_RISING,BASE_FORMING_SETUP
- `12:11:09`       SON    score= 60.0  mcap=$4991M  float_turn=1.7%  short=79%  d2c=1.5d  Δshort=+6.4
- `12:11:09`         flags: SHORT_PCT_50%+,SHORT_RISING_5PP+,BASE_FORMING_SETUP
- `12:11:09`       MIR    score= 60.0  mcap=$4927M  float_turn=1.8%  short=66%  d2c=1.7d  Δshort=+20.9
- `12:11:09`         flags: SHORT_PCT_50%+,SHORT_RISING_5PP+,BASE_FORMING_SETUP