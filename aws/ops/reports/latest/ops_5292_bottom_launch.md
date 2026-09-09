# ops 5292 -- BOTTOM launch (Wyckoff bottom desk)

**Status:** success  
**Duration:** 975.2s  
**Finished:** 2026-09-09T16:34:52+00:00  

## Error

```
SystemExit: 0
```

## Log
- `16:18:37`   Lambda exists — updating
- `16:18:42` ✅   ✓ updated justhodl-bottom
- `16:18:48`    function state Active / Successful, 8192MB / 900s, env keys ['FMP_KEY', 'POLYGON_API_KEY']
- `16:18:48`    invoked async (prior generated_at=2026-09-09T15:57:57Z); polling data/bottom.json
- `16:21:50`    fresh artifact after 182s: v1.2.0 session 2026-09-08 elapsed 161.5s
## universe / states / desks

- `16:21:50`    universe {"instruments": 7666, "scored": 4899, "rows": 266, "sessions": 1260, "window": ["2021-08-31", "2026-09-08"], "crypto_symbols": 64}
- `16:21:50`    by_state {"TRIGGERED": 23, "ST_CONFIRMED": 13, "TESTING": 25, "CLIMAX": 22, "MARKUP": 71, "COMPLETED": 27, "STOPPED": 18, "FAILED": 4, "NO_TEST_BREAKOUT": 25, "EXPIRED": 1, "NO_RALLY": 37}
- `16:21:50`    by_desk  {"stocks": 253, "crypto": 5, "equity_etfs": 3, "bonds": 3, "countries": 2}
- `16:21:50`    breadth  {"in_bottom_process": 154, "share_of_universe_pct": 3.1, "climax_5d": 10, "testing": 25, "test_confirmed": 13, "triggered_5d": 26, "failed_10d": 14, "weekly_live": 81}
- `16:21:50`    read: selective bottoming processes
- `16:21:50`    benchmarks: {"SPY": {"last": 765.96, "daily": "NONE", "weekly": "NONE", "daily_sc_date": null, "n_sequences_5y": 1}, "QQQ": {"last": 718.36, "daily": "NONE", "weekly": "NONE", "daily_sc_date": null, "n_sequences_5y": 2}, "IWM": {"last": 294.67, "daily": "NONE", "weekly": "NONE", "daily_sc_date": null, "n_sequences_5y": 3}, "TLT": {"last": 82.2, "daily": "NONE", "weekly": "NONE", "daily_sc_date": null, "n_sequences_5y": 1}, "GLD": {"last": 399.72, "daily": "NONE", "weekly": "NONE", "daily_sc_date": null, "n_sequences_5y": 0}, "UUP": {"last": 27.99, "daily": "NONE", "weekly": "NONE", "daily_sc_date": null, 
- `16:21:50`    authority {"mode": "SELECTIVE", "allows_new_entries": true, "status": "DEGRADED", "cap_pct": 50.0, "policy_cap_pct": 50.0, "generated_at": "2026-09-09T15:20:50.173720+00:00"} | risk gate {"posture": "NEUTRAL", "sizing_multiplier": 0.75, "generated_at": "2026-09-09T15:40:55.323971+00:00"} | katlin posture SELECTIVE
## board / rows / contract

- `16:21:50`    board 266 rows (with charts), board_all 266 rows, weekly_live 81, top_picks 9
- `16:21:50`       ORN    stocks   TRIGGERED     D      89.9 A     sc 2026-07-29 8.04x  test 2026-08-21 11%  trig 2026-08-27  plan 9.33/8.8722/11.38  fleet 1
- `16:21:50`       WSO    stocks   TRIGGERED     D      88.3 A     sc 2026-07-29 5.5x  test 2026-08-25 11%  trig 2026-08-26  plan 313.445/296.4285/336.44  fleet 0
- `16:21:50`       PCT    stocks   TRIGGERED     W      83.4 A     sc 2026-02-27 2.25x  test 2026-07-31 47%  trig 2026-08-07  plan 6.55/5.5998/13.29  fleet 0
- `16:21:50`       STWD   stocks   TRIGGERED     D      79.5 B     sc 2026-08-06 3.06x  test 2026-09-01 47%  trig 2026-09-02  plan 15.7/15.4522/16.87  fleet 0
- `16:21:50`       CCB    stocks   TRIGGERED     W      78.8 A     sc 2026-07-31 5.3x  test 2026-08-28 15%  trig 2026-09-04  plan 48.7044/44.8192/53.02  fleet 0
- `16:21:50`       SITE   stocks   TRIGGERED     W      78.0 B     sc 2026-07-31 2.2x  test 2026-08-21 37%  trig 2026-09-04  plan 99.3059/92.1284/105.99  fleet 2
- `16:21:50`       HE     stocks   TRIGGERED     D      76.8 B     sc 2026-08-10 3.28x  test 2026-09-02 47%  trig 2026-09-03  plan 11.07/10.8234/12.1  fleet 1
- `16:21:50`       PFSI   stocks   TRIGGERED     D      75.7 A     sc 2026-07-30 4.57x  test 2026-09-01 25%  trig 2026-09-03  plan 72.865/70.3234/80.38  fleet 0
- `16:21:50`       HCKT   stocks   TRIGGERED     W      73.6 B     sc 2026-05-08 2.09x  test 2026-07-24 51%  trig 2026-07-31  plan 10.55/9.0127/12.2  fleet 2
- `16:21:50`       WGO    stocks   TRIGGERED     W      73.1 B     sc 2026-06-26 2.6x  test 2026-07-17 41%  trig 2026-08-07  plan 31.885/28.3983/32.045  fleet 0
- `16:21:50`       OLED   stocks   TRIGGERED     W      72.9 B     sc 2026-05-01 2.97x  test 2026-07-02 29%  trig 2026-08-07  plan 87.865/76.2559/101.55  fleet 0
- `16:21:50`       PSEC   stocks   TRIGGERED     W      63.9 B     sc 2026-05-15 1.89x  test 2026-07-24 60%  trig 2026-08-07  plan 2.26/2.0876/2.515  fleet 1
## base rates (graded from the tape)

- `16:21:50`    sequences 6595, with rally 4920, triggered 2048, outcome mix {"NO_TEST_BREAKOUT": 2187, "COMPLETED": 767, "NO_RALLY": 1714, "FAILED": 622, "STOPPED": 1281, "EXPIRED": 24}
- `16:21:50`    triggered all: {"n": 2048, "ret_21": {"n": 2026, "median": 0.66, "mean": 2.94, "hit": 52.0}, "ret_63": {"n": 1871, "median": 2.37, "mean": 11.37, "hit": 55.0}, "managed_21": {"n": 2048, "median": -3.87, "mean": 1.55, "hit": 41.0}, "managed_63": {"n": 2001, "median": -5.98, "mean": 6.4, "hit": 32.0}, "stop_hit_pct": 63.0, "target1_hit_pct": 77.0, "mae_median": -8.2, "mfe_median": 9.8, "target1_before_stop_pct": 80.0, "stops": {"rules": {"paper": {"n": 2048, "stop_hit_pct": 63.0, "managed_21": {"n": 2048, "media
- `16:21:50`    crowd vs pro: {"crowd_buys_the_bounce": {"n": 4877, "ret_21": {"n": 4877, "median": 2.43, "mean": 7.1, "hit": 57.0}, "ret_63": {"n": 4620, "median": 5.45, "mean": 15.94, "hit": 60.0}, "worst_drawdown_median_pct": -12.1, "later_undercut_climax_low_pct": 49.0, "sequence_failed_pct": 13.0}, "pro_waits_for_the_trigger": {"n": 2048, "ret_21": {"n": 2048, "median": -3.87, "mean": 1.55, "hit": 41.0}, "ret_63": {"n": 2001, "median": -5.98, "mean": 6.4, "hit": 32.0}, "worst_drawdown_median_pct": -8.2, "stopped_out_pct": 63.0, "share_of_sequences_that_ever_trigger_pct": 42.0}, "note": "measured from every selling-climax sequence in the 5-year window; crowd entry = close of the bar the automatic rally reached 2 ATR off the low; pro entry = close above the test candle's high; managed returns exit at the stop when it hits first"}
- `16:21:50`       bonds        n   56  +21 1.73 (hit 71.0%)  +63 -0.64 (hit 41.0%)  managed63 -1.77  stop 64.0%  t1 86.0%
- `16:21:50`       commodities  n    6  +21 1.71 (hit 50.0%)  +63 9.75 (hit 100.0%)  managed63 5.9  stop 17.0%  t1 100.0%
- `16:21:50`       countries    n   25  +21 4.0 (hit 64.0%)  +63 4.25 (hit 60.0%)  managed63 -2.79  stop 64.0%  t1 92.0%
- `16:21:50`       crypto       n   34  +21 4.11 (hit 65.0%)  +63 11.41 (hit 58.0%)  managed63 -6.3  stop 71.0%  t1 79.0%
- `16:21:50`       currencies   n    2  +21 2.54 (hit 100.0%)  +63 5.06 (hit 100.0%)  managed63 5.06  stop 0.0%  t1 100.0%
- `16:21:50`       equity_etfs  n   79  +21 1.82 (hit 57.0%)  +63 -4.11 (hit 45.0%)  managed63 -5.1  stop 66.0%  t1 84.0%
- `16:21:50`       gold_metals  n    7  +21 -0.17 (hit 43.0%)  +63 7.91 (hit 57.0%)  managed63 -4.23  stop 57.0%  t1 71.0%
- `16:21:50`       stocks       n 1839  +21 0.14 (hit 50.0%)  +63 2.64 (hit 55.0%)  managed63 -6.29  stop 62.0%  t1 76.0%
- `16:21:50`       test volume quiet (<=0.4x climax)  n 1299  +63 2.55 (hit 55.0%)  stop 65.0%
- `16:21:50`       test volume diminished (0.4-0.7x)  n  576  +63 1.89 (hit 53.0%)  stop 60.0%
- `16:21:50`       test volume loud (>0.7x)           n  173  +63 1.72 (hit 57.0%)  stop 55.0%
- `16:21:50`       stop rule paper   n  2048  hit 63.0%  bars-to-stop 10.0  managed +21 -3.87 (hit 41.0%)  +63 -5.98 (hit 32.0%)
- `16:21:50`       stop rule climax  n  2048  hit 50.0%  bars-to-stop 15.0  managed +21 -1.34 (hit 48.0%)  +63 -5.99 (hit 39.0%)
- `16:21:50`       stop rule wide    n  2048  hit 43.0%  bars-to-stop 18.0  managed +21 -0.09 (hit 50.0%)  +63 -5.63 (hit 43.0%)
- `16:21:50`    finding: 2048 sequences reached the trigger. Unmanaged, the trigger entry made a median +2.4% after 63 bars (hit 55%) and reached the rally high (target 1) in 77% of cases, 80% of them before the paper's stop was touched.
- `16:21:50`    finding: The paper's stop (0.1 ATR under the test low) was hit in 63% of triggered sequences (median 10 bars after entry); managed by that stop the 63-bar outcome is a median -6.0% (hit 32%). A stop that tight is whipsawed by the range itself: the tape revisits the tes
- `16:21:50`    finding: Alternatives measured on the same trades: a stop 0.25 ATR under the climax low is hit 50% of the time (managed 63-bar median -6.0%); a stop 1 ATR under the test low is hit 43% (median -5.6%). Best rule by median managed return: wide.
- `16:21:50`    finding: The crowd's bounce entry (n 4877) made a median +5.5% after 63 bars (hit 60%) but with a median worst drawdown of -12.1%; in 49% of sequences the tape later undercut the climax low and 13% failed outright. Waiting for the test forfeits the V-bottoms: 33% of al
- `16:21:50`    finding: Test volume: quiet tests (<=0.4x climax volume, n 1299) made a median +2.5% at 63 bars vs +1.7% for loud tests (n 173) -- the paper's variable carries an edge in this window.
- `16:21:50`    finding: Grade A sequences (n 474) made a median +19.6% at 63 bars vs +17.2% for grade C (n 58).
## v1.2.0 -- climax at the lows (vs the v1.1 artifact)

- `16:21:50`    sequences: v1.1.0 19756 -> v1.2.0 6595 | triggered 5969 -> 2048
- `16:21:50`    raw +63 median/hit: 2.65/55.0% -> 2.37/55.0% | paper-stop hit: 64.0% -> 63.0% | target1 hit: 78.0% -> 77.0%
- `16:21:50`    climax gates D: {"vol_x_ok": 344819, "rej_not_new_low": 88477, "rej_range": 242598, "climax": 4634, "rej_position": 742, "rej_shallow": 2020, "rej_not_dominant": 5920, "rej_volume_regime": 249, "rej_not_prolonged": 148, "rej_above_ma": 31}
- `16:21:50`    climax gates W: {"vol_x_ok": 78318, "rej_shallow": 3447, "rej_range": 45746, "rej_not_new_low": 25496, "climax": 2158, "rej_not_dominant": 1091, "rej_position": 221, "rej_volume_regime": 127, "rej_above_ma": 28, "rej_not_prolonged": 4}
- `16:21:50`    actionable rows: 0 of 266 (counts.actionable=26)
- `16:21:50`       ORN    stocks    TRIGGERED     D   act=True  pos52w 20.0%  vs52wH  -47.0%  climax 2026-07-29 (8.04x, 21 bars down, pos 24.0%)  test 2026-08-21 11%  trig 2026-08-27
- `16:21:50`       WSO    stocks    TRIGGERED     D   act=True  pos52w 11.0%  vs52wH  -31.2%  climax 2026-07-29 (5.5x, 63 bars down, pos -2.0%)  test 2026-08-25 11%  trig 2026-08-26
- `16:21:50`       PCT    stocks    TRIGGERED     W   act=True  pos52w 15.0%  vs52wH  -58.0%  climax 2026-02-27 (2.25x, 32 bars down, pos 14.0%)  test 2026-07-31 47%  trig 2026-08-07
- `16:21:50`       STWD   stocks    TRIGGERED     D   act=True  pos52w 7.0%  vs52wH  -23.8%  climax 2026-08-06 (3.06x, 63 bars down, pos 3.0%)  test 2026-09-01 47%  trig 2026-09-02
- `16:21:50`       CCB    stocks    TRIGGERED     W   act=True  pos52w 15.0%  vs52wH  -58.8%  climax 2026-07-31 (5.3x, 31 bars down, pos -7.0%)  test 2026-08-28 15%  trig 2026-09-04
- `16:21:50`       SITE   stocks    TRIGGERED     W   act=True  pos52w 12.0%  vs52wH  -41.0%  climax 2026-07-31 (2.2x, 24 bars down, pos -5.0%)  test 2026-08-21 37%  trig 2026-09-04
- `16:21:50`       HE     stocks    TRIGGERED     D   act=True  pos52w 2.0%  vs52wH  -37.3%  climax 2026-08-10 (3.28x, 120 bars down, pos 16.0%)  test 2026-09-02 47%  trig 2026-09-03
- `16:21:50`       PFSI   stocks    TRIGGERED     D   act=True  pos52w 2.0%  vs52wH  -55.1%  climax 2026-07-30 (4.57x, 119 bars down, pos -2.0%)  test 2026-09-01 25%  trig 2026-09-03
- `16:21:50`       HCKT   stocks    TRIGGERED     W   act=True  pos52w 17.0%  vs52wH  -47.1%  climax 2026-05-08 (2.09x, 52 bars down, pos -7.0%)  test 2026-07-24 51%  trig 2026-07-31
- `16:21:50`       WGO    stocks    TRIGGERED     W   act=True  pos52w 13.0%  vs52wH  -40.7%  climax 2026-06-26 (2.6x, 20 bars down, pos 11.0%)  test 2026-07-17 41%  trig 2026-08-07
- `16:21:50`       OLED   stocks    TRIGGERED     W   act=True  pos52w 8.0%  vs52wH  -46.3%  climax 2026-05-01 (2.97x, 46 bars down, pos 7.0%)  test 2026-07-02 29%  trig 2026-08-07
- `16:21:50`       PSEC   stocks    TRIGGERED     W   act=True  pos52w 8.0%  vs52wH  -29.9%  climax 2026-05-15 (1.89x, 52 bars down, pos -8.0%)  test 2026-07-24 60%  trig 2026-08-07
- `16:21:50`       BRSL   stocks    TRIGGERED     W   act=True  pos52w 14.0%  vs52wH  -40.2%  climax 2026-05-15 (1.94x, 32 bars down, pos None%)  test 2026-07-31 67%  trig 2026-08-07
- `16:21:50`       MFIC   stocks    ST_CONFIRMED  W   act=True  pos52w 1.0%  vs52wH  -29.5%  climax 2026-02-27 (3.19x, 52 bars down, pos -8.0%)  test 2026-07-24 22%  trig None
- `16:21:50`       VITL   stocks    ST_CONFIRMED  W   act=True  pos52w 5.0%  vs52wH  -80.6%  climax 2026-05-08 (3.62x, 36 bars down, pos -7.0%)  test 2026-09-04 15%  trig None
- `16:21:50`       ATEC   stocks    ST_CONFIRMED  W   act=True  pos52w 14.0%  vs52wH  -60.5%  climax 2026-05-08 (3.91x, 17 bars down, pos 14.0%)  test 2026-07-31 19%  trig None
- `16:21:50`    IVES on the board: no (no qualifying sequence)
- `16:21:50`    [bottom] feeds: finviz=11614 accum=17 phase=681 fortress=950 katlin=249 f13=7526 dark=941 insider=15 flows=300
## health

- `16:21:50`    degraded: []
- `16:21:50`    row_errors: {}
- `16:21:50`    feeds_asof: {"finviz": "2026-09-09T14:00:38.228355+00:00", "accumulation_radar": "2026-09-08T21:50:21.620419+00:00", "phase_detector": "2026-09-08T22:10:41.844108+00:00", "fortress": "2026-09-09T03:31:57+00:00", "katlin": "2026-09-09T16:06:06Z", "f13": "2026-09-09T15:07:02.645497+00:00", "dark_pool": "2026-09-09T14:01:20.450489+00:00", "insider": "2026-09-09T14:40:29.846595+00:00", "etf_flows": "2026-09-08T22:00:25.396312+00:00"}
- `16:21:50`       [bottom] feeds: finviz=11614 accum=17 phase=681 fortress=950 katlin=249 f13=7526 dark=941 insider=15 flows=300
- `16:21:50`       [bottom] universe: 7666 instruments (3838 stocks, 3828 etf wrappers)
- `16:21:50`       [bottom] bars 28/1260 sessions, 4599 tickers
- `16:21:50`       [bottom] bars 308/1260 sessions, 5056 tickers
- `16:21:50`       [bottom] bars 588/1260 sessions, 5510 tickers
- `16:21:50`       [bottom] bars 868/1260 sessions, 6180 tickers
- `16:21:50`       [bottom] bars 1148/1260 sessions, 7141 tickers
- `16:21:50`       [bottom] split repair: 4 names rebased (calendar banked 2026-09-08)
- `16:21:50`       [bottom] bars loaded: 1260 sessions 2021-08-31..2026-09-08, 7662 tickers in 80s
- `16:21:50`       [bottom] equities/wrappers: 4836 scored, 262 rows with a live or recent sequence, 6537 historical sequences in 159s
- `16:21:50`       [bottom] crypto lane: 64 symbols, 63 with >=200 days, 0 errors
- `16:21:50`    data/bottom.json = 1.88 MB
## schedule (EventBridge Scheduler, UTC)

- `16:21:50` ✅    justhodl-bottom-daily updated cron(45 3 ? * TUE-SAT *)
## page

- `16:21:51`    bottom.html carries marker BOTTOM_DESK_V1 at the edge: True
- `16:22:14`    1440px: {"headline": "selective bottoming processes", "funnel": 7, "board": 26, "helps": 25, "evid": 384, "svg": true, "defs": 15, "overflow": 0, "err": "", "rates_cards": 2, "rates_tables": 6} errors=[]
- `16:22:26`     390px: {"headline": "selective bottoming processes", "funnel": 7, "board": 26, "helps": 25, "evid": 389, "svg": true, "defs": 15, "overflow": 0, "err": "", "rates_cards": 2, "rates_tables": 6} errors=[]
## consumers

- `16:24:48`    katlin v2.5.1 fresh: 249 picks, 39 carry wyckoff_bottom ([('HCKT', 'TRIGGERED'), ('ADMA', 'ST_CONFIRMED'), ('PHR', 'MARKUP'), ('QDEL', 'MARKUP'), ('BUR', 'ST_CONFIRMED'), ('BRBR', 'FAILED')])
- `16:34:52`    alert-router invoked (check_bottom registered; Telegram delivery depends on the bot token Khalid must rotate)
- `16:34:52` ⚠    desk gold_metals has no live/recent sequence this session (possible, but check the classifier if it persists)
- `16:34:52` ⚠    desk commodities has no live/recent sequence this session (possible, but check the classifier if it persists)
- `16:34:52` ⚠    jhsignal-bridge redeploy not observed within 10 min -- fusion proof waits for the hourly bridge
- `16:34:52` ✅    GREEN: BOTTOM live -- engine, feed, schedule, page (with warnings)
