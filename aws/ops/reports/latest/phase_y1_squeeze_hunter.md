- `11:11:58`     source: 15770 chars

# 1) Build zip + create Lambda

- `11:11:58`     creating new
- `11:12:02`     ✓ deployed at 2026-05-06T11:11:58.935+0000

# 2) Schedule daily 11:30 UTC

- `11:12:03`     ✓ permission added

# 3) Smoke invoke (heavy — 600 stocks × 300 day history)

- `11:12:13`     status: 200, dur: 10.3s
- `11:12:13`     body: {"statusCode": 200, "body": "{\"n_evaluated\": 338, \"n_tier_s\": 2, \"n_tier_a\": 2, \"n_tier_b\": 16, \"duration_s\": 9.3}"}
- `11:12:13`       START RequestId: 47537d0e-5aa8-42ad-988b-df276ee4ff75 Version: $LATEST
- `11:12:13`       [squeeze] starting v1.0
- `11:12:13`       [squeeze] universe: 338 tickers
- `11:12:13`       [squeeze] OK: 338, no_data: 0
- `11:12:13`       [squeeze] wrote 176012b
- `11:12:13`       [squeeze] tier_s=2 tier_a=2 tier_b=16
- `11:12:13`       [squeeze] top: [('INFN', 100.0, 6), ('INFA', 83.0, 5), ('EXAS', 62.0, 4), ('EA', 60.0, 3), ('INCY', 60.0, 3)]
- `11:12:13`       END RequestId: 47537d0e-5aa8-42ad-988b-df276ee4ff75
- `11:12:13`       REPORT RequestId: 47537d0e-5aa8-42ad-988b-df276ee4ff75	Duration: 9417.01 ms	Billed Duration: 10025 ms	Memory Size: 1024 MB	Max Memory Used: 113 MB	Init Duration: 607.66 ms

# 4) Inspect output

- `11:12:13`     generated_at: 2026-05-06T11:12:13+00:00
- `11:12:13`     stats: {"n_universe": 338, "n_evaluated": 338, "n_no_data": 0, "n_tier_s": 2, "n_tier_a": 2, "n_tier_b": 16, "n_watch": 63}
- `11:12:13`   
- `11:12:13`     ── TIER_S (5+ signals firing — RARE) ──
- `11:12:13`       INFN
- `11:12:13`       INFA
- `11:12:13`   
- `11:12:13`     ── TIER_A (4 of 6 signals firing — strong setup) ──
- `11:12:13`     (top 12)
- `11:12:13`       INFN   score=100.0 n_sig=6  TIER_S_EXCEPTIONAL  base=137d  bb=5%  atr=7%  nr7=5  inside=40%
- `11:12:13`         flags: BB_SQUEEZE_TIGHT,TTM_SQUEEZE,NR7_CLUSTER,VCP_PATTERN,ATR_COMPRESSED,INSIDE_DAY_DENSE,LONG_BASE_90D,VOL_DRYING,MID_RANGE
- `11:12:13`       INFA   score= 83.0 n_sig=5  TIER_S_EXCEPTIONAL  base=125d  bb=9%  atr=4%  nr7=9  inside=53%
- `11:12:13`         flags: BB_SQUEEZE_TIGHT,NR7_CLUSTER,VCP_PATTERN,ATR_COMPRESSED,INSIDE_DAY_DENSE,LONG_BASE_90D,VOL_DRYING
- `11:12:13`       EXAS   score= 62.0 n_sig=4  TIER_A_STRONG_SQUE  base=83d  bb=20%  atr=8%  nr7=5  inside=30%
- `11:12:13`         flags: NR7_CLUSTER,VCP_PATTERN,ATR_COMPRESSED,INSIDE_DAY_DENSE,BASE_60D
- `11:12:13`       EA     score= 60.0 n_sig=3  TIER_B_BUILDING  base=152d  bb=18%  atr=28%  nr7=3  inside=10%
- `11:12:13`         flags: BB_NARROWING,TTM_SQUEEZE,NR7_CLUSTER,VCP_PATTERN,LONG_BASE_90D,MID_RANGE
- `11:12:13`       INCY   score= 60.0 n_sig=3  TIER_B_BUILDING  base=151d  bb=2%  atr=62%  nr7=4  inside=3%
- `11:12:13`         flags: BB_SQUEEZE_TIGHT,TTM_SQUEEZE,NR7_CLUSTER,LONG_BASE_90D,MID_RANGE
- `11:12:13`       JPM    score= 56.0 n_sig=3  TIER_B_BUILDING  base=199d  bb=8%  atr=44%  nr7=4  inside=13%
- `11:12:13`         flags: BB_SQUEEZE_TIGHT,TTM_SQUEEZE,NR7_CLUSTER,VCP_PARTIAL,LONG_BASE_90D,MID_RANGE
- `11:12:13`       FITB   score= 56.0 n_sig=3  TIER_B_BUILDING  base=110d  bb=10%  atr=62%  nr7=8  inside=7%
- `11:12:13`         flags: BB_SQUEEZE_TIGHT,TTM_SQUEEZE,NR7_CLUSTER,LONG_BASE_90D,MID_RANGE
- `11:12:13`       GBCI   score= 56.0 n_sig=3  TIER_B_BUILDING  base=112d  bb=9%  atr=28%  nr7=4  inside=23%
- `11:12:13`         flags: BB_SQUEEZE_TIGHT,TTM_SQUEEZE,NR7_CLUSTER,LONG_BASE_90D,MID_RANGE
- `11:12:13`       BMY    score= 55.0 n_sig=3  TIER_B_BUILDING  base=105d  bb=9%  atr=68%  nr7=4  inside=20%
- `11:12:13`         flags: BB_SQUEEZE_TIGHT,TTM_SQUEEZE,NR7_CLUSTER,VCP_PARTIAL,LONG_BASE_90D
- `11:12:13`       BPMC   score= 53.0 n_sig=4  TIER_A_STRONG_SQUE  base=33d  bb=5%  atr=0%  nr7=7  inside=30%
- `11:12:13`         flags: BB_SQUEEZE_TIGHT,NR7_CLUSTER,ATR_COMPRESSED,INSIDE_DAY_DENSE
- `11:12:13`       CCI    score= 51.0 n_sig=3  TIER_B_BUILDING  base=174d  bb=35%  atr=64%  nr7=3  inside=30%
- `11:12:13`         flags: TTM_SQUEEZE,NR7_CLUSTER,INSIDE_DAY_DENSE,LONG_BASE_90D
- `11:12:13`       CFG    score= 51.0 n_sig=3  TIER_B_BUILDING  base=105d  bb=2%  atr=54%  nr7=6  inside=10%
- `11:12:13`         flags: BB_SQUEEZE_TIGHT,TTM_SQUEEZE,NR7_CLUSTER,LONG_BASE_90D
- `11:12:13`       APD    score= 46.0 n_sig=3  TIER_B_BUILDING  base=66d  bb=8%  atr=43%  nr7=5  inside=13%
- `11:12:13`         flags: BB_SQUEEZE_TIGHT,TTM_SQUEEZE,NR7_CLUSTER,BASE_60D