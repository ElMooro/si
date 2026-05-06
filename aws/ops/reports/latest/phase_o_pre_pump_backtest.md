- `00:03:03`     source: 14759 chars

# 1) Create + deploy pre-pump-detector Lambda

- `00:03:03`     zip: 15,971b
- `00:03:05`     ✓ deployed at 2026-05-06T00:03:03.915+0000

# 2) Schedule daily 13:15 UTC

- `00:03:05`     ✓ permission added

# 3) Smoke invoke

- `00:03:16`     status: 200, dur: 11.0s
- `00:03:16`     body: {"n_evaluated": 562, "n_tier_a": 0, "n_tier_b": 0, "duration_s": 10.1}
- `00:03:16`       START RequestId: 3d384c35-b711-45bc-95a6-180ff1b8afd7 Version: $LATEST
- `00:03:16`       [prepump] starting v1.0, max_tickers=600, min_dollar_vol=$1.0M
- `00:03:16`       [prepump] universe: 563 tickers
- `00:03:16`       [prepump] OK: 562, no_history: 0, no_signal: 1
- `00:03:16`       [prepump] wrote 285,578b
- `00:03:16`       [prepump] tier_a=0 tier_b=0
- `00:03:16`       [prepump] TOP: [('MMP', 55, 'WATCH'), ('EDR', 55, 'WATCH'), ('FISV', 52, 'WATCH'), ('LCID', 52, 'WATCH'), ('GDDY', 51, 'WATCH'), ('PDCO', 50, 'WATCH'), ('SCWX', 50, 'WATCH'), ('BILL', 49, 'WATCH')]
- `00:03:16`       END RequestId: 3d384c35-b711-45bc-95a6-180ff1b8afd7
- `00:03:16`       REPORT RequestId: 3d384c35-b711-45bc-95a6-180ff1b8afd7	Duration: 10259.81 ms	Billed Duration: 10776 ms	Memory Size: 1024 MB	Max Memory Used: 117 MB	Init Duration: 515.42 ms

# 4) Today's top 15 pre-pump signals

- `00:03:16`     generated_at: 2026-05-06T00:03:16+00:00
- `00:03:16`     stats: {"n_universe": 563, "n_evaluated": 562, "n_tier_a": 0, "n_tier_b": 0, "n_no_history": 0, "n_no_signal": 1}
- `00:03:16`   
- `00:03:16`     ── TIER_A_COILED candidates ──
- `00:03:16`       MMP     55.0 WATCH               range_pos=98.5%  vol_comp=1.6  obv=0.171  liq=2.02
- `00:03:16`       EDR     55.0 WATCH               range_pos=26.0%  vol_comp=0.59  obv=0.057  liq=1.51
- `00:03:16`       FISV    52.0 WATCH               range_pos=3.9%  vol_comp=1.54  obv=0.107  liq=0.7
- `00:03:16`       LCID    52.0 WATCH               range_pos=2.6%  vol_comp=1.01  obv=-0.342  liq=2.25
- `00:03:16`       GDDY    51.0 WATCH               range_pos=12.0%  vol_comp=0.7  obv=0.156  liq=1.13
- `00:03:16`       PDCO    50.0 WATCH               range_pos=99.4%  vol_comp=11.21  obv=-0.0  liq=1.63
- `00:03:16`       SCWX    50.0 WATCH               range_pos=89.0%  vol_comp=19.08  obv=0.209  liq=1.41
- `00:03:16`       BILL    49.0 WATCH               range_pos=16.7%  vol_comp=1.4  obv=-0.17  liq=0.66
- `00:03:16`       FLNC    48.0 WATCH               range_pos=23.3%  vol_comp=1.72  obv=-0.129  liq=0.86
- `00:03:16`       ABBV    47.0 WATCH               range_pos=18.6%  vol_comp=0.98  obv=-0.27  liq=1.0
- `00:03:16`       ROP     47.0 WATCH               range_pos=18.2%  vol_comp=1.03  obv=0.224  liq=0.85
- `00:03:16`       PAYX    47.0 WATCH               range_pos=13.2%  vol_comp=0.8  obv=0.104  liq=0.86
- `00:03:16`       FICO    47.0 WATCH               range_pos=15.0%  vol_comp=0.82  obv=-0.044  liq=1.43
- `00:03:16`       EXAS    47.0 WATCH               range_pos=100.0%  vol_comp=13.61  obv=0.253  liq=0.99
- `00:03:16`       PCTY    47.0 WATCH               range_pos=14.4%  vol_comp=0.79  obv=0.118  liq=1.04

# 5) HISTORICAL BACKTEST — would pre-pump have caught these EARLY?

- `00:03:16`     At various dates, simulate the pre-pump scoring using only data available
- `00:03:16`     AT THAT DATE. Track when each pump-list name would have been flagged.
- `00:03:16`   
- `00:03:16`     Walking history to find earliest pre-pump fire date for each name:
- `00:03:16`   
- `00:03:16`     AXTI (final +464%):
- `00:03:16`       PRE-PUMP never crossed 60 (silent name — needs different signal)
- `00:03:16`   
- `00:03:17`     LWLG (final +408%):
- `00:03:17`       PRE-PUMP never crossed 60 (silent name — needs different signal)
- `00:03:17`   
- `00:03:17`     AAOI (final +353%):
- `00:03:17`       PRE-PUMP never crossed 60 (silent name — needs different signal)
- `00:03:17`   
- `00:03:17`     AEHR (final +277%):
- `00:03:17`       PRE-PUMP never crossed 60 (silent name — needs different signal)
- `00:03:17`   
- `00:03:17`     ICHR (final +138%):
- `00:03:17`       PRE-PUMP never crossed 60 (silent name — needs different signal)
- `00:03:17`   
- `00:03:17`     MRVL (final +130%):
- `00:03:17`       PRE-PUMP never crossed 60 (silent name — needs different signal)
- `00:03:17`   
- `00:03:17`     INTC (final +122%):
- `00:03:17`       PRE-PUMP never crossed 60 (silent name — needs different signal)
- `00:03:17`   
- `00:03:18`     LITE (final +116%):
- `00:03:18`       PRE-PUMP never crossed 60 (silent name — needs different signal)
- `00:03:18`   
- `00:03:18`     CRDO (final +101%):
- `00:03:18`       PRE-PUMP never crossed 60 (silent name — needs different signal)
- `00:03:18`   