- `08:50:16`     source: 16984 chars

# 1) Build zip + create/update Lambda

- `08:50:16`     zip: 17144b
- `08:50:16`     creating new
- `08:50:20`     ✓ deployed at 2026-05-06T08:50:16.966+0000, mem=2048MB to=600s

# 2) Schedule daily 21:30 UTC

- `08:50:21`     ✓ permission added

# 3) Smoke invoke (heavy — may take 3-8 minutes)

- `08:50:39`     status: 200, dur: 17.9s
- `08:50:39`     body: {"statusCode": 200, "body": "{\"n_evaluated\": 149, \"n_tier_a\": 39, \"n_tier_b\": 37, \"duration_s\": 16.7}"}
- `08:50:39`       START RequestId: f0d4d5f6-cef4-4a65-aff2-ab437958db7d Version: $LATEST
- `08:50:39`       [opt-flow] starting v1.0
- `08:50:39`       [opt-flow] universe: 150 tickers
- `08:50:39`       [opt-flow] fetching FINRA short volume history (20 days)...
- `08:50:39`       [opt-flow] FINRA: 12487 tickers, 1.9s
- `08:50:39`       [opt-flow] OK: 149, no_data: 1
- `08:50:39`       [opt-flow] wrote 80943b to data/options-flow.json
- `08:50:39`       [opt-flow] tier_a=39 tier_b=37
- `08:50:39`       [opt-flow] TOP: [('CBOE', 100.0, 'TIER_A_BULLISH_FLOW'), ('GILD', 93.0, 'TIER_A_BULLISH_FLOW'), ('HSY', 93.0, 'TIER_A_BULLISH_FLOW'), ('CRDO', 93.0, 'TIER_A_BULLISH_FLOW'), ('ECL', 88.0, 'TIER_A_BULLISH_FLOW'), ('HOOD', 86.0, 'TIER_A_BULLISH_FLOW'), ('KLAC', 85.0, 'TIER_A_BULLISH_FLOW'), ('AMGN', 85.0, 'TIER_A_BULLISH_FLOW')]
- `08:50:39`       END RequestId: f0d4d5f6-cef4-4a65-aff2-ab437958db7d
- `08:50:39`       REPORT RequestId: f0d4d5f6-cef4-4a65-aff2-ab437958db7d	Duration: 16864.56 ms	Billed Duration: 17380 ms	Memory Size: 2048 MB	Max Memory Used: 172 MB	Init Duration: 514.74 ms

# 4) Inspect output

- `08:50:40`     generated_at: 2026-05-06T08:50:39+00:00
- `08:50:40`     stats: {"n_universe": 150, "n_evaluated": 149, "n_no_data": 1, "n_tier_a": 39, "n_tier_b": 37, "n_finra_tickers": 12487}
- `08:50:40`   
- `08:50:40`     ── top 15 options-flow signals ──
- `08:50:40`       CBOE   100.0 TIER_A_BULLISH_FLOW     cpr= 5.6  cpr_chg=+460.3%  vol_surge=48.60x  short_chg=-10.3
- `08:50:40`       GILD    93.0 TIER_A_BULLISH_FLOW     cpr=141.8  cpr_chg=+4555.0%  vol_surge=13.48x  short_chg=-4.6
- `08:50:40`       HSY     93.0 TIER_A_BULLISH_FLOW     cpr= 4.0  cpr_chg=+691.3%  vol_surge=16.41x  short_chg=-1.9
- `08:50:40`       CRDO    93.0 TIER_A_BULLISH_FLOW     cpr=89.3  cpr_chg=+227.9%  vol_surge=3.53x  short_chg=-2.5
- `08:50:40`       ECL     88.0 TIER_A_BULLISH_FLOW     cpr= 2.2  cpr_chg=+581.0%  vol_surge=3.67x  short_chg=-15.7
- `08:50:40`       HOOD    86.0 TIER_A_BULLISH_FLOW     cpr= 7.7  cpr_chg=+142.3%  vol_surge=34.04x  short_chg=-2.9
- `08:50:40`       KLAC    85.0 TIER_A_BULLISH_FLOW     cpr= 3.5  cpr_chg=+164.6%  vol_surge=4.98x  short_chg=+1.5
- `08:50:40`       AMGN    85.0 TIER_A_BULLISH_FLOW     cpr= 3.9  cpr_chg=+5691.5%  vol_surge=180.24x  short_chg=+3.4
- `08:50:40`       ADI     85.0 TIER_A_BULLISH_FLOW     cpr=31.1  cpr_chg=+134.1%  vol_surge=7.74x  short_chg=-10.8
- `08:50:40`       IBKR    85.0 TIER_A_BULLISH_FLOW     cpr=294.8  cpr_chg=+769.3%  vol_surge=10.44x  short_chg=+7.8
- `08:50:40`       BKNG    85.0 TIER_A_BULLISH_FLOW     cpr= 6.0  cpr_chg=+35876.0%  vol_surge=274.00x  short_chg=-4.5
- `08:50:40`       KKR     85.0 TIER_A_BULLISH_FLOW     cpr=38.6  cpr_chg=+362.7%  vol_surge=7.14x  short_chg=-17.5
- `08:50:40`       FIX     85.0 TIER_A_BULLISH_FLOW     cpr=50.4  cpr_chg=+980.9%  vol_surge=14.10x  short_chg=+5.2
- `08:50:40`       COHR    85.0 TIER_A_BULLISH_FLOW     cpr=19.5  cpr_chg=+52.6%  vol_surge=8.18x  short_chg=-10.3
- `08:50:40`       CBRE    85.0 TIER_A_BULLISH_FLOW     cpr= 3.3  cpr_chg=+192.1%  vol_surge=7.67x  short_chg=-12.0