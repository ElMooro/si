# ops 6752 - capitulation desk baseline (read-only) 2026-11-05T14:45+00:00

**Status:** success  
**Duration:** 16.3s  
**Finished:** 2026-11-05T14:46:01+00:00  

## Data

| board | board_all | kept | served_rows | session |
|---|---|---|---|---|
| 320 | 366 | 60 | 61 | 2026-11-04 |

## Log
## A. data/bottom.json

- `14:45:45` written 2026-11-05T03:47:43+00:00 | generated 2026-11-05T03:47:42Z | session 2026-11-04 | version 1.2.0 | 2199455 bytes | elapsed 160.7s | universe {"instruments": 7728, "scored": 4957, "rows": 366, "sessions": 1260, "window": ["2021-10-28", "2026-11-04"], "crypto_symbols": 64} | counts {"by_state": {"TRIGGERED": 17, "ST_CONFIRMED": 3, "TESTING": 21, "CLIMAX": 112, "MARKUP": 42, "COMPLETED": 30, "STOPPED": 34, "FAILED": 17, "NO_TEST_BREAKOUT": 34, "EXPIRED": 1, "NO_RALLY": 55}, "by_desk": {"stocks": 326, "crypto": 2, "bonds": 31, "equity_etfs": 6, "countries": 1}, "actionable": 17} | degraded ['crypto lane: 59/64 Polygon errors']
- `14:45:45` board 320 rows by desk {"stocks": 286, "crypto": 1, "bonds": 28, "equity_etfs": 4, "countries": 1} | board_all 366 rows by desk {"stocks": 326, "crypto": 2, "bonds": 31, "equity_etfs": 6, "countries": 1}
- `14:45:45` board states {"TRIGGERED": 17, "ST_CONFIRMED": 3, "TESTING": 21, "CLIMAX": 112, "MARKUP": 42, "COMPLETED": 30, "STOPPED": 34, "FAILED": 17, "NO_TEST_BREAKOUT": 34, "EXPIRED": 1, "NO_RALLY": 9} | board_all states {"TRIGGERED": 17, "ST_CONFIRMED": 3, "TESTING": 21, "CLIMAX": 112, "MARKUP": 42, "COMPLETED": 30, "STOPPED": 34, "FAILED": 17, "NO_TEST_BREAKOUT": 34, "EXPIRED": 1, "NO_RALLY": 55}
- `14:45:45` the desk's Capitulation filter keeps 60 of the 320 board rows: by desk {"stocks": 60}
- `14:45:45`   AVTX   Avalo Therapeutics Inc         stocks       CLIMAX     sc 2026-10-22 vol 26.17x range 2.03x decline -69.3% off-low 8.3% last 7.07 state-bars 9
- `14:45:45`   BCAX   Bicara Therapeutics Inc        stocks       CLIMAX     sc 2026-10-28 vol 22.57x range 5.42x decline -68.5% off-low 13.0% last 8.89 state-bars 5
- `14:45:45`   STVN   Stevanato Group Spa            stocks       CLIMAX     sc 2026-11-03 vol 13.17x range 2.4x decline -39.4% off-low 11.5% last 13.44 state-bars 1
- `14:45:45`   CLW    Clearwater Paper Corp          stocks       CLIMAX     sc 2026-10-27 vol 11.25x range 3.47x decline -42.0% off-low 12.9% last 14.05 state-bars 6
- `14:45:45`   RYTM   Rhythm Pharmaceuticals Inc     stocks       CLIMAX     sc 2026-11-03 vol 10.83x range 2.0x decline -27.8% off-low 35.7% last 110.68 state-bars 1
- `14:45:45`   DOX    Amdocs Ltd                     stocks       CLIMAX     sc 2026-11-04 vol 9.5x range 1.69x decline -33.5% off-low 5.4% last 43.66 state-bars 0
- `14:45:45`   SFM    Sprouts Farmers Market Inc     stocks       TESTING    sc 2026-10-28 vol 9.43x range 2.11x decline -34.7% off-low 14.9% last 54.4 state-bars 0
- `14:45:45`   SGML   Sigma Lithium Corp             stocks       CLIMAX     sc 2026-11-02 vol 9.39x range 3.35x decline -75.3% off-low 12.6% last 5.62 state-bars 2
- `14:45:45`   BFAM   Bright Horizons Family Solutio stocks       CLIMAX     sc 2026-10-30 vol 8.93x range 2.97x decline -33.8% off-low 7.9% last 56.22 state-bars 3
- `14:45:45`   HDSN   Hudson Technologies Inc        stocks       CLIMAX     sc 2026-10-29 vol 8.08x range 2.17x decline -32.6% off-low 7.5% last 4.41 state-bars 4
- `14:45:45`   ALNY   Alnylam Pharmaceuticals Inc    stocks       CLIMAX     sc 2026-10-29 vol 8.06x range 2.44x decline -54.2% off-low 3.3% last 161.08 state-bars 4
- `14:45:45`   MDGL   Madrigal Pharmaceuticals Inc   stocks       CLIMAX     sc 2026-11-03 vol 7.86x range 4.41x decline -30.4% off-low 13.3% last 400.43 state-bars 1
- `14:45:45`   CRMD   CorMedix Inc                   stocks       CLIMAX     sc 2026-10-30 vol 7.34x range 4.55x decline -37.3% off-low 6.8% last 5.66 state-bars 3
- `14:45:45`   SNDX   Syndax Pharmaceuticals Inc     stocks       CLIMAX     sc 2026-11-04 vol 6.84x range 4.29x decline -58.6% off-low 4.6% last 10.45 state-bars 0
- `14:45:45`   CQP    Cheniere Energy Partners LP    stocks       CLIMAX     sc 2026-11-04 vol 6.82x range 3.09x decline -20.4% off-low 1.2% last 56.68 state-bars 0
- `14:45:45`   BA     Boeing Co                      stocks       CLIMAX     sc 2026-10-27 vol 6.79x range 3.36x decline -31.1% off-low 7.3% last 177.0 state-bars 6
- `14:45:45`   ECG    Everus Construction Group      stocks       CLIMAX     sc 2026-11-04 vol 6.76x range 2.69x decline -52.4% off-low 2.8% last 79.71 state-bars 0
- `14:45:45`   MOD    Modine Manufacturing Co        stocks       CLIMAX     sc 2026-11-04 vol 6.75x range 2.17x decline -69.5% off-low 7.8% last 98.56 state-bars 0
- `14:45:45`   XIFR   XPLR Infrastructure LP         stocks       CLIMAX     sc 2026-11-03 vol 6.72x range 4.07x decline -38.5% off-low 0.6% last 7.72 state-bars 1
- `14:45:45`   CMPS   Compass Pathways Plc ADR       stocks       CLIMAX     sc 2026-11-02 vol 6.65x range 4.65x decline -47.0% off-low 14.1% last 8.6 state-bars 2
- `14:45:45`   FIS    Fidelity National Information  stocks       CLIMAX     sc 2026-11-04 vol 6.06x range 1.69x decline -36.7% off-low 2.1% last 29.76 state-bars 0
- `14:45:45`   NSSC   NAPCO Security Technologies In stocks       CLIMAX     sc 2026-11-02 vol 6.0x range 2.19x decline -38.0% off-low 2.4% last 31.75 state-bars 2
- `14:45:45`   FOXF   Fox Factory Holding Corp       stocks       CLIMAX     sc 2026-10-16 vol 5.91x range 2.17x decline -27.4% off-low 19.9% last 17.37 state-bars 13
- `14:45:45`   UNM    Unum Group                     stocks       CLIMAX     sc 2026-11-04 vol 5.91x range 2.0x decline -22.7% off-low 2.3% last 74.79 state-bars 0
- `14:45:45`   NPKI   NPK International Inc          stocks       TESTING    sc 2026-10-30 vol 5.78x range 4.2x decline -42.2% off-low 9.7% last 9.04 state-bars 0
- `14:45:45`   DJT    Trump Media & Technology Group stocks       CLIMAX     sc 2026-11-04 vol 5.76x range 2.42x decline -57.4% off-low 0.7% last 4.49 state-bars 0
- `14:45:45`   TWI    Titan International Inc        stocks       CLIMAX     sc 2026-10-29 vol 5.62x range 3.93x decline -41.2% off-low 6.3% last 4.87 state-bars 4
- `14:45:45`   GBFH   GBank Financial Holdings Inc   stocks       CLIMAX     sc 2026-10-29 vol 5.33x range 3.44x decline -53.9% off-low 34.4% last 15.47 state-bars 4
- `14:45:45`   AFYA   Afya Ltd                       stocks       CLIMAX     sc 2026-10-26 vol 5.28x range 3.22x decline -31.6% off-low 8.7% last 10.5 state-bars 7
- `14:45:45`   KALU   Kaiser Aluminum Corp           stocks       TESTING    sc 2026-10-22 vol 5.18x range 3.08x decline -42.5% off-low 6.6% last 110.74 state-bars 2
- `14:45:45`   TIMB   TIM SA ADR                     stocks       TESTING    sc 2026-10-26 vol 4.99x range 3.08x decline -38.6% off-low 14.2% last 18.2 state-bars 2
- `14:45:45`   FLG    Flagstar Bank NA               stocks       CLIMAX     sc 2026-10-23 vol 4.91x range 2.15x decline -32.8% off-low 10.4% last 10.96 state-bars 8
- `14:45:45`   STRL   Sterling Infrastructure Inc    stocks       CLIMAX     sc 2026-11-04 vol 4.9x range 1.73x decline -71.8% off-low 1.4% last 283.66 state-bars 0
- `14:45:45`   PPTA   Perpetua Resources Corp        stocks       CLIMAX     sc 2026-11-04 vol 4.81x range 1.64x decline -48.8% off-low 3.6% last 16.35 state-bars 0
- `14:45:45`   COUR   Coursera Inc                   stocks       CLIMAX     sc 2026-10-30 vol 4.77x range 3.08x decline -47.7% off-low 13.0% last 3.9 state-bars 3
- `14:45:45`   FR     First Industrial Realty Trust  stocks       CLIMAX     sc 2026-10-22 vol 4.64x range 1.88x decline -20.9% off-low 2.5% last 55.51 state-bars 9
- `14:45:45`   XYL    Xylem Inc                      stocks       CLIMAX     sc 2026-10-27 vol 4.63x range 3.02x decline -27.0% off-low 15.6% last 101.05 state-bars 6
- `14:45:45`   KURA   Kura Oncology Inc              stocks       CLIMAX     sc 2026-11-04 vol 4.61x range 3.98x decline -44.6% off-low 1.9% last 7.61 state-bars 0
- `14:45:45`   SE     Sea Ltd ADR                    stocks       CLIMAX     sc 2026-10-20 vol 4.42x range 1.69x decline -39.0% off-low 12.1% last 80.19 state-bars 11
- `14:45:45`   FPI    Farmland Partners Inc          stocks       CLIMAX     sc 2026-10-29 vol 4.31x range 2.77x decline -24.7% off-low 8.3% last 8.96 state-bars 4
- `14:45:45` board_all: daily climaxes inside 21 days by desk {"stocks": 98, "crypto": 1, "bonds": 2}
- `14:45:45` board_all: daily climaxes inside 60 days by desk {"stocks": 139, "bonds": 4, "equity_etfs": 1, "crypto": 1}
- `14:45:45` board rows' sc_vol_x distribution: {">=5": 65, "2-3.5": 56, "3.5-5": 65}
- `14:45:45` market: {"benchmarks": {"SPY": {"last": 766.81, "daily": "NONE", "weekly": "NONE", "daily_sc_date": null, "n_sequences_5y": 1}, "QQQ": {"last": 691.95, "daily": "NONE", "weekly": "NONE", "daily_sc_date": null, "n_sequences_5y": 2}, "IWM": {"last": 284.02, "daily": "NONE", "weekly": "NONE", "daily_sc_date": null, "n_sequences_5y": 2}, "TLT": {"last": 81.76, "daily": "NONE", "weekly": "NONE", "daily_sc_date": null, "n_sequences_5y": 1}, "GLD": {"last": 372.49, "daily": "NONE", "weekly": "NONE", "daily_sc_date": null, "n_sequences_5y": 0}, "UUP": {"last": 28.9, "daily": "NONE", "weekly": "NONE", "daily_sc_date": null, "n_sequences_5y": 0}, "HYG": {"last": 77.52, "daily": "NONE", "weekly": "NONE", "daily_sc_date": null, "n_sequences_5y": 1}, "BTC": {"last": 80428.0, "daily": "NONE", "weekly": null, "daily_sc_date": null, "n_sequences_5y": 4}}, "breadth": {"in_bottom_process": 195, "share_of_universe_pct": 3.9, "climax_5d": 55, "testing": 21, "test_confirmed": 3, "triggered_5d": 12, "failed_10d": 34, "weekly_live": 36}, "read": "selective bottoming processes", "by_state": {"TRIGGERED": 17, "ST_CONFIRMED": 3, "TESTING": 21, "CLIMAX": 112, "MARKUP": 42, "COMPLETED": 30, "STOPPED": 34, "FAILED": 17, "NO_TEST_BREAKOUT": 34, "EXPIRED": 1, "NO_RALLY": 55}, "capital_authority": {"mode": "DATA_HOLD", "allows_new_entries": false, "status": "DATA_HOLD", "cap_pct": 0.0, "policy_cap_pct": 0.0, "generated_at": "2026-11-04T09:20:55.418838+00:00"}, "risk_gate": {"posture": "UNAVAILABLE", "sizing_multipl
- `14:45:45` climax_gates: {"D": {"vol_x_ok": 349066, "rej_range": 247956, "climax": 4629, "rej_not_new_low": 87125, "rej_position": 784, "rej_shallow": 2283, "rej_not_dominant": 5908, "rej_volume_regime": 211, "rej_not_prolonged": 138, "rej_above_ma": 32}, "W": {"vol_x_ok": 80009, "rej_shallow": 3748, "rej_range": 47353, "rej_not_new_low": 25348, "climax": 2107, "rej_not_dominant": 1068, "rej_position": 230, "rej_volume_regime": 121, "rej_above_ma": 29, "rej_not_prolonged": 5}, "note": "how many candidate bars (>= climax_vol_x volume) each gate rejected over the whole window: range, not a new low of the move, shallow decline, not prolonged, above the average, not in the bottom of the yearly range, not the heaviest volume of the decline, heavy only vs a rising volume regime"}
- `14:45:45` by_desk sizes: {"stocks": 80, "crypto": 2, "bonds": 31, "equity_etfs": 6, "countries": 1} | weekly_live 36 | top_picks 4 | changes {"new_triggered": ["RGNX", "CELH", "ARCO", "DBD", "TLK", "SFIX", "BAC"], "new_test_confirmed": [], "new_climax": ["CVS", "LMB", "RXST", "METC", "CMI", "KMT", "ANDE", "CQP", "LMUB", "DOX", "TBLA", "AAON", "ECG", "SNDX", "DJT", "KURA", "GSM", "CECO", "MOD", "NRG", "UNM", "STRL", "FIS", "PPTA", "PXED", "GOOD", "VTS", "EME", "KTOS", "ICL"], "new_failed": ["ASTS", "LBRT"], "prior_session": "2026-11-03"
- `14:45:45` feeds_asof: {"finviz": "2026-11-04T22:00:38.664398+00:00", "accumulation_radar": "2026-11-04T21:50:22.531629+00:00", "phase_detector": "2026-11-04T23:42:47.619976+00:00", "fortress": "2026-11-05T03:32:04+00:00", "katlin": "2026-11-04T23:51:12.896263+00:00", "f13": null, "dark_pool": null, "insider": "2026-11-04T14:40:38.850592+00:00", "etf_flows": null} | row_errors {} | log tail: [bottom] bars 868/1260 sessions, 6192 tickers || [bottom] bars 1148/1260 sessions, 7237 tickers || [bottom] split repair: 23 names rebased (calendar banked 2026-11-03) || [bottom] bars loaded: 1260 sessions 2021-10-28..2026-11-04, 7724 tickers in 78s || [bottom] equities/wrappers: 4894 scored, 365 rows with a live or recent sequence, 6429 historical sequences in 157s || [bottom] crypto lane: 64 symbols, 63 with >=200 days, 59 errors
- `14:45:46` history snapshots: 38 (data/bottom/history/2026-09-08.json.gz .. data/bottom/history/2026-11-04.json.gz)
## B. the engine

- `14:45:46` justhodl-bottom: 8192 MB / 900 s, modified 2026-10-26T21:35:19, state Active, env keys ['FMP_KEY', 'POLYGON_API_KEY']
- `14:45:46`    stream last event 2026-11-05T03:47+00:00
- `14:45:46`    stream last event 2026-11-04T03:47+00:00
- `14:45:46`    stream last event 2026-11-03T03:47+00:00
- `14:45:46`    stream last event 2026-10-31T03:47+00:00
- `14:45:46`    stream last event 2026-10-30T03:47+00:00
- `14:45:47`    2026-11-03T03:47+00:00 [bottom] wrote data/bottom.json (2170469 bytes) rows=335 triggered=51 test_confirmed=9 testing=23 climax=82 failed=15 elapsed=170s
## C. the served desk tab (headless Chrome)

- `14:45:47` installing playwright
- `14:46:01` served capit tab: 61 rows | heads ["TICKER", "NAME", "LAST", "READ", "VOLUME", "RANGE", "DECLINE", "AGE", "STILL AT LOW", "", "", "", "", "", "", "", ""] | kpis ["NAMES 60 TAB Capitulation VOL \u22653.5\u00d7 60"]
- `14:46:01`   ["AVTX", "Avalo Therapeutics Inc", "7.07", "CAPIT", "26.2× vol", "rng 2.0×", "-69.3%", "15d", "CLIMAX · 8.3% off low"]
- `14:46:01`   ["BCAX", "Bicara Therapeutics Inc", "8.89", "CAPIT", "22.6× vol", "rng 5.4×", "-68.5%", "9d", "CLIMAX · 13.0% off low"]
- `14:46:01`   ["STVN", "Stevanato Group Spa", "13.44", "CAPIT", "13.2× vol", "rng 2.4×", "-39.4%", "3d", "CLIMAX · 11.5% off low"]
- `14:46:01`   ["CLW", "Clearwater Paper Corp", "14.05", "CAPIT", "11.3× vol", "rng 3.5×", "-42.0%", "10d", "CLIMAX · 12.9% off low"]
- `14:46:01`   ["RYTM", "Rhythm Pharmaceuticals Inc", "110.68", "CAPIT", "10.8× vol", "rng 2.0×", "-27.8%", "3d", "CLIMAX · 35.7% off low"]
- `14:46:01`   ["DOX", "Amdocs Ltd", "43.66", "CAPIT", "9.5× vol", "rng 1.7×", "-33.5%", "2d", "CLIMAX · 5.4% off low"]
- `14:46:01`   ["SFM", "Sprouts Farmers Market Inc", "54.40", "CAPIT", "9.4× vol", "rng 2.1×", "-34.7%", "9d", "TESTING · 14.9% off low"]
- `14:46:01`   ["SGML", "Sigma Lithium Corp", "5.62", "CAPIT", "9.4× vol", "rng 3.4×", "-75.3%", "4d", "CLIMAX · 12.6% off low"]
- `14:46:01`   ["BFAM", "Bright Horizons Family Solutions Inc", "56.22", "CAPIT", "8.9× vol", "rng 3.0×", "-33.8%", "7d", "CLIMAX · 7.9% off low"]
- `14:46:01`   ["HDSN", "Hudson Technologies Inc", "4.41", "CAPIT", "8.1× vol", "rng 2.2×", "-32.6%", "8d", "CLIMAX · 7.5% off low"]
- `14:46:01`   ["ALNY", "Alnylam Pharmaceuticals Inc", "161.08", "CAPIT", "8.1× vol", "rng 2.4×", "-54.2%", "8d", "CLIMAX · 3.3% off low"]
- `14:46:01`   ["MDGL", "Madrigal Pharmaceuticals Inc", "400.43", "CAPIT", "7.9× vol", "rng 4.4×", "-30.4%", "3d", "CLIMAX · 13.3% off low"]
- `14:46:01` tabs: ["Selling climax", "Capitulation", "Buying climax", "Candle spread", "Spring", "Accumulation", "Bottom", "Pump", "Distribution", "Top", "i", "×", "GET THE MORNING BRIEF", "›"]
- `14:46:01` page text head: JUSTHODL·AI | Market observations unavailable or stale | 09:46:01 ET | COMMAND CENTER → | WYCKOFF DESK |  | Climax, capitulation, spring, accumulation, bottom, pump, distribution, and top. Each tab ends with the criteria that engine starts from. Capitulation is not a buy. A spring on a distribution range is a failed test. Click a ticker to open the chart. |  | Selling climax | Capitulation | Buying climax | Candle spread | Spring | Accumulation | Bottom | Pump | Distribution | Top | NAMES | 60 | TAB | Capitulation | VOL ≥3.5× | 60 | Panic print, not the whole SC sequence. Tape rule: wide down bar, close in the lower fifth, volume ≥1.75×. This board is stricter on the Bottom harvest: daily CLIMAX / fresh TESTING / NO_RALLY, vol ≥3.5× the 50d average, range ≥1.5× ATR, decline ≥15%, still within 15% of the climax low, SC bar ≤21 calendar days. Not a buy. Wait for the automatic rally and a light-volume test. | §3 | TICKER	NAME	LAST	READ	VOLUM
- `14:46:01` page errors: none
- `14:46:01` ✅ ops 6752 complete (read-only)
