# ops 6753 - capitulation desk first light (justhodl-bottom 1.3.0) 2026-11-05T15:55+00:00

**Status:** success  
**Duration:** 383.0s  
**Finished:** 2026-11-05T16:01:28+00:00  

## Data

| bytes | latest | prints_60 | regime | scored | session | sum_5 |
|---|---|---|---|---|---|---|
| 2214698 | 113 | 518 | HIGH | 4958 | 2026-11-04 | 113 |

## Log
## 1. the deployed function

- `15:55:06` justhodl-bottom: runtime python3.12, memory 8192, timeout 900, state Active, modified 2026-11-05T15:51:40, CodeSha256 68ICX9fZg5HwjbFdIAZMBrpdte1/KKPYMdR7UP9Go/M=
- `15:55:06` release receipt: commit 34ba2e8620d03605100c6c69a46f758dfd9ba365 deployed 2026-11-05T15:51:30Z code_sha256 68ICX9fZg5HwjbFdIAZMBrpdte1/KKPYMdR7UP9Go/M=; source files ['capitulation.py', 'lambda_function.py', 'wyckoff_proofs.py']
- `15:55:06` this checkout: 34ba2e8620d03605100c6c69a46f758dfd9ba365
- `15:55:06` schedule justhodl-bottom-daily: cron(45 3 ? * TUE-SAT *) ENABLED (config cron(45 3 ? * TUE-SAT *))
## 2. invoke once (RequestResponse)

- `16:01:27` invoke: status 200, function error None, 381s, payload {"ok": true, "session": "2026-11-04", "rows": 366, "bytes": 2214698, "elapsed_s": 379.7, "degraded": []}
## 3. data/capitulation-desk.json

- `16:01:27` written 2026-11-05T16:01:27+00:00 | 410015 bytes | contract capitulation.v1 | engine justhodl-bottom 1.3.0 | module 1.0.1 | session 2026-11-04 | window ['2021-10-28', '2026-11-04'] | generated 2026-11-05T16:01:26Z
- `16:01:27` READ: panic is broad: 113 prints in the last 5 sessions (96.7 percentile of the window, high) -- a market-wide capitulation cluster; last session 2026-11-04: 55 (stocks 46, equity_etfs 4, commodities 3)
- `16:01:27` breadth: scored 4958 by desk {"stocks": 3241, "gold_metals": 49, "countries": 336, "equity_etfs": 728, "bonds": 456, "crypto": 95, "commodities": 38, "currencies": 15} | last session {"date": "2026-11-04", "total": 55, "by_desk": {"stocks": 46, "commodities": 3, "equity_etfs": 4, "countries": 2}, "sum_5": 113, "pct_of_scored": 1.11} | sum_5 113 (2.28% of scored) percentile 96.7 regime HIGH | window 1260 sessions, median sum_5 31 p90 80 max 606 | clusters in window 1
- `16:01:27`   cluster 2025-04-04: 340 prints (6.86%) {"stocks": 99, "countries": 65, "equity_etfs": 145, "bonds": 22, "commodities": 4, "gold_metals": 5}
- `16:01:27`   desk stocks       {"scored": 3241, "prints_60": 454, "prints_5": 100, "prints_last_session": 46, "prints_5_pct_of_desk": 3.1}
- `16:01:27`   desk equity_etfs  {"scored": 728, "prints_60": 29, "prints_5": 7, "prints_last_session": 4, "prints_5_pct_of_desk": 1.0}
- `16:01:27`   desk countries    {"scored": 336, "prints_60": 5, "prints_5": 2, "prints_last_session": 2, "prints_5_pct_of_desk": 0.6}
- `16:01:27`   desk bonds        {"scored": 456, "prints_60": 17, "prints_5": 0, "prints_last_session": 0, "prints_5_pct_of_desk": 0.0}
- `16:01:27`   desk gold_metals  {"scored": 49, "prints_60": 1, "prints_5": 0, "prints_last_session": 0, "prints_5_pct_of_desk": 0.0}
- `16:01:27`   desk commodities  {"scored": 38, "prints_60": 4, "prints_5": 4, "prints_last_session": 3, "prints_5_pct_of_desk": 10.5}
- `16:01:27`   desk currencies   {"scored": 15, "prints_60": 0, "prints_5": 0, "prints_last_session": 0, "prints_5_pct_of_desk": 0.0}
- `16:01:27`   desk crypto       {"scored": 95, "prints_60": 8, "prints_5": 0, "prints_last_session": 0, "prints_5_pct_of_desk": 0.0}
- `16:01:27` prints: 60 sessions 518 | latest window (since 2026-10-29) 113 | follow-through {"n": 405, "min_sessions_since": 5, "held_pct": 8.0, "median_bounce_pct": 5.8, "median_undercut_pct": -10.3, "median_now_vs_print_pct": -1.3, "volume_dried_up_pct": 96.0, "by_context": {"DECLINE": {"n": 236, "held_pct": 7.0, "median_now_vs_print_pct": -2.0}, "SHOCK": {"n": 169, "held_pct": 8.0, "median_now_vs_print_pct": -0.6}}}
- `16:01:27` latest: contexts {"DECLINE": 65, "SHOCK": 48} | wyckoff states {"CLIMAX": 44, "none": 66, "STOPPED": 1, "COMPLETED": 1, "NO_TEST_BREAKOUT": 1} | desks {"stocks": 100, "equity_etfs": 7, "commodities": 4, "countries": 2}
- `16:01:27`   CRMD   CorMedix Inc                 stocks       2026-10-30 ret -25.5% vol 7.3x rng 6.25x loc 0.03 DECLINE score 97.6 dd20 -31.8% wy {"same_bar": true, "state": "CLIMAX", "grade": "D", "score": | since 3 bounce -0.6% under -5.9% held False volafter 0.33 now -0.6% | fleet 0 13f None
- `16:01:27`   SGML   Sigma Lithium Corp           stocks       2026-11-02 ret -25.6% vol 9.4x rng 4.09x loc 0.09 DECLINE score 95.5 dd20 -49.7% wy {"same_bar": true, "state": "CLIMAX", "grade": "D", "score": | since 2 bounce +7.9% under +3.2% held True volafter 0.28 now +7.9% | fleet 0 13f None
- `16:01:27`   ALNY   Alnylam Pharmaceuticals Inc  stocks       2026-10-29 ret -25.1% vol 8.1x rng 3.47x loc 0.06 SHOCK score 94.6 dd20 -30.9% wy {"same_bar": true, "state": "CLIMAX", "grade": "D", "score": | since 4 bounce -5.2% under -9.5% held False volafter 0.41 now -7.4% | fleet 0 13f None
- `16:01:27`   MDGL   Madrigal Pharmaceuticals Inc stocks       2026-11-03 ret -8.1% vol 7.7x rng 5.97x loc 0.09 DECLINE score 94.1 dd20 -21.4% wy {"same_bar": true, "state": "CLIMAX", "grade": "D", "score": | since 1 bounce +1.0% under -8.7% held False volafter 1.17 now +1.0% | fleet 1 13f None
- `16:01:27`   SNDX   Syndax Pharmaceuticals Inc   stocks       2026-11-04 ret -26.9% vol 6.8x rng 5.74x loc 0.1 DECLINE score 91.8 dd20 -40.9% wy {"same_bar": true, "state": "CLIMAX", "grade": "D", "score": | since 0 bounce - under - held None volafter None now - | fleet 0 13f None
- `16:01:27`   XIFR   XPLR Infrastructure LP       stocks       2026-11-03 ret -15.5% vol 6.7x rng 5.71x loc 0.09 DECLINE score 91.8 dd20 -22.9% wy {"same_bar": true, "state": "CLIMAX", "grade": "D", "score": | since 1 bounce -5.3% under -4.4% held False volafter 0.7 now -5.3% | fleet 0 13f None
- `16:01:27`   ECG    Everus Construction Group    stocks       2026-11-04 ret -29.3% vol 6.8x rng 3.55x loc 0.09 DECLINE score 89.4 dd20 -38.6% wy {"same_bar": true, "state": "CLIMAX", "grade": "D", "score": | since 0 bounce - under - held None volafter None now - | fleet 0 13f None
- `16:01:27`   SUPN   Supernus Inc                 stocks       2026-11-03 ret -32.9% vol 11.1x rng 1.88x loc 0.05 SHOCK score 87.4 dd20 -36.5% wy null | since 1 bounce +0.2% under -4.4% held False volafter 0.73 now +0.2% | fleet 0 13f None
- `16:01:27`   CMPS   Compass Pathways Plc ADR     stocks       2026-11-02 ret -27.3% vol 6.6x rng 6.46x loc 0.21 SHOCK score 85.4 dd20 -32.7% wy {"same_bar": true, "state": "CLIMAX", "grade": "D", "score": | since 2 bounce +2.3% under +6.8% held True volafter 0.42 now +2.3% | fleet 0 13f None
- `16:01:27`   COUR   Coursera Inc                 stocks       2026-10-30 ret -25.5% vol 4.8x rng 4.09x loc 0.02 DECLINE score 85.2 dd20 -34.8% wy {"same_bar": true, "state": "CLIMAX", "grade": "D", "score": | since 3 bounce +12.4% under +0.9% held True volafter 0.51 now +12.4% | fleet 0 13f None
- `16:01:27`   DJT    Trump Media & Technology Gro stocks       2026-11-04 ret -24.5% vol 5.8x rng 2.92x loc 0.03 DECLINE score 84.8 dd20 -47.5% wy {"same_bar": true, "state": "CLIMAX", "grade": "D", "score": | since 0 bounce - under - held None volafter None now - | fleet 0 13f None
- `16:01:27`   CQP    Cheniere Energy Partners LP  stocks       2026-11-04 ret -10.1% vol 6.8x rng 3.72x loc 0.1 SHOCK score 84.0 dd20 -16.3% wy {"same_bar": true, "state": "CLIMAX", "grade": "D", "score": | since 0 bounce - under - held None volafter None now - | fleet 1 13f None
- `16:01:27`   BFAM   Bright Horizons Family Solut stocks       2026-10-30 ret -17.9% vol 8.9x rng 3.98x loc 0.28 DECLINE score 84.0 dd20 -25.7% wy {"same_bar": true, "state": "CLIMAX", "grade": "D", "score": | since 3 bounce +2.3% under +2.2% held True volafter 0.24 now +2.3% | fleet 1 13f None
- `16:01:27`   KURA   Kura Oncology Inc            stocks       2026-11-04 ret -23.0% vol 4.6x rng 5.16x loc 0.05 DECLINE score 83.0 dd20 -30.8% wy {"same_bar": true, "state": "CLIMAX", "grade": "D", "score": | since 0 bounce - under - held None volafter None now - | fleet 0 13f None
- `16:01:27`   ATEC   Alphatec Holdings Inc        stocks       2026-10-30 ret -38.1% vol 11.8x rng 2.31x loc 0.19 SHOCK score 82.4 dd20 -42.3% wy {"same_bar": false, "state": "STOPPED", "sc_date": "2026-05- | since 3 bounce +17.6% under +2.8% held True volafter 0.34 now +17.6% | fleet 0 13f None
- `16:01:27`   TWI    Titan International Inc      stocks       2026-10-29 ret -25.8% vol 5.6x rng 5.9x loc 0.17 SHOCK score 82.2 dd20 -31.4% wy {"same_bar": true, "state": "CLIMAX", "grade": "D", "score": | since 4 bounce +1.6% under -1.9% held False volafter 0.39 now -0.8% | fleet 0 13f None
- `16:01:27`   TFII   TFI International Inc        stocks       2026-10-30 ret -17.1% vol 3.9x rng 4.66x loc 0.03 DECLINE score 80.6 dd20 -24.0% wy {"same_bar": true, "state": "CLIMAX", "grade": "D", "score": | since 3 bounce +7.4% under -1.5% held False volafter 0.55 now +7.4% | fleet 0 13f None
- `16:01:27`   NOW    ServiceNow Inc               stocks       2026-10-29 ret -26.9% vol 5.6x rng 2.13x loc 0.08 SHOCK score 77.8 dd20 -31.2% wy null | since 4 bounce -0.5% under -4.8% held False volafter 0.31 now -0.8% | fleet 0 13f None
- `16:01:27`   RDDT   Reddit Inc                   stocks       2026-10-30 ret -29.2% vol 7.2x rng 1.68x loc 0.19 DECLINE score 77.5 dd20 -44.4% wy {"same_bar": true, "state": "CLIMAX", "grade": "D", "score": | since 3 bounce +11.1% under +1.4% held True volafter 0.28 now +11.1% | fleet 0 13f None
- `16:01:27`   TRUT   VanEck Technology TruSector  equity_etfs  2026-11-02 ret -2.7% vol 10.1x rng 1.81x loc 0.01 DECLINE score 77.2 dd20 -12.6% wy null | since 2 bounce +1.9% under +0.3% held True volafter 0.08 now +1.9% | fleet 0 13f None
- `16:01:27`   LB     LandBridge Corp              stocks       2026-11-04 ret -12.3% vol 6.0x rng 3.16x loc 0.16 DECLINE score 76.9 dd20 -22.8% wy null | since 0 bounce - under - held None volafter None now - | fleet 0 13f None
- `16:01:27`   NUGO   Nuveen Growth Opportunities  equity_etfs  2026-10-30 ret -3.4% vol 33.7x rng 3.44x loc 0.14 DECLINE score 76.8 dd20 -9.5% wy null | since 3 bounce +0.5% under -0.5% held False volafter 0.06 now +0.3% | fleet 0 13f None
- `16:01:27`   GSM    Ferroglobe Plc               stocks       2026-11-04 ret -24.1% vol 3.7x rng 5.12x loc 0.08 DECLINE score 76.7 dd20 -32.5% wy {"same_bar": true, "state": "CLIMAX", "grade": "D", "score": | since 0 bounce - under - held None volafter None now - | fleet 0 13f None
- `16:01:27`   FIS    Fidelity National Informatio stocks       2026-11-04 ret -23.7% vol 6.1x rng 2.48x loc 0.19 SHOCK score 76.1 dd20 -27.0% wy {"same_bar": true, "state": "CLIMAX", "grade": "D", "score": | since 0 bounce - under - held None volafter None now - | fleet 0 13f None
- `16:01:27`   TH     Target Hospitality Corp      stocks       2026-11-04 ret -22.1% vol 3.2x rng 3.65x loc 0.02 DECLINE score 75.8 dd20 -40.5% wy null | since 0 bounce - under - held None volafter None now - | fleet 0 13f None
- `16:01:27`   MOD    Modine Manufacturing Co      stocks       2026-11-04 ret -21.4% vol 6.7x rng 2.55x loc 0.28 DECLINE score 75.0 dd20 -48.0% wy {"same_bar": true, "state": "CLIMAX", "grade": "D", "score": | since 0 bounce - under - held None volafter None now - | fleet 0 13f None
- `16:01:27`   CMPR   Cimpress plc                 stocks       2026-10-29 ret -19.4% vol 3.2x rng 4.63x loc 0.09 DECLINE score 74.2 dd20 -27.8% wy {"same_bar": true, "state": "CLIMAX", "grade": "D", "score": | since 4 bounce +5.0% under -4.4% held False volafter 0.68 now +5.0% | fleet 0 13f None
- `16:01:27`   STRL   Sterling Infrastructure Inc  stocks       2026-11-04 ret -21.5% vol 4.9x rng 1.93x loc 0.06 DECLINE score 73.8 dd20 -48.5% wy {"same_bar": true, "state": "CLIMAX", "grade": "D", "score": | since 0 bounce - under - held None volafter None now - | fleet 0 13f None
- `16:01:27`   ADTN   ADTRAN Holdings Inc          stocks       2026-11-03 ret -19.4% vol 2.8x rng 3.74x loc 0.03 DECLINE score 73.8 dd20 -36.0% wy {"same_bar": true, "state": "CLIMAX", "grade": "D", "score": | since 1 bounce -6.5% under -8.8% held False volafter 0.96 now -6.5% | fleet 0 13f None
- `16:01:27`   OILK   ProShares K-1 Free Crude Oil commodities  2026-11-04 ret -6.4% vol 11.1x rng 2.31x loc 0.21 DECLINE score 73.7 dd20 -18.4% wy null | since 0 bounce - under - held None volafter None now - | fleet 0 13f None
- `16:01:27`   board equity_etfs (29 rows, 60 sessions): XES 2026-11-04 -5.8% 3.6x s55.0; DRLL 2026-11-04 -5.3% 6.7x s53.0; RSPG 2026-11-04 -5.2% 4.9x s51.6; OIH 2026-11-04 -5.0% 2.5x s43.1; TRUT 2026-11-02 -2.7% 10.1x s77.2; NUGO 2026-10-30 -3.4% 33.7x s76.8; VOT 2026-10-30 -2.9% 2.8x s37.8; CRTC 2026-10-22 -2.2% 13.4x s59.4; IDRV 2026-10-16 -2.2% 26.5x s74.9; PSCT 2026-10-16 -3.5% 12.8x s73.8; AGIX 2026-10-16 -4.5% 8.3x s68.1; XNTK 2026-10-16 -3.6% 5.3x s55.2
- `16:01:27`   board countries (5 rows, 60 sessions): MLPA 2026-11-04 -3.1% 7.0x s69.3; IXC 2026-11-04 -4.1% 5.1x s50.3; EPU 2026-10-22 -4.4% 3.1x s38.8; AIQ 2026-10-16 -3.2% 3.3x s43.3; FLBR 2026-08-11 -3.5% 4.0x s58.2
- `16:01:27`   board bonds (17 rows, 60 sessions): CMDT 2026-10-27 -1.5% 9.9x s82.5; CLOZ 2026-10-22 -1.1% 2.6x s56.5; PFFD 2026-10-16 -1.3% 10.9x s71.9; PFF 2026-10-16 -1.1% 3.0x s47.5; VOX 2026-10-16 -2.2% 3.5x s38.8; BNDS 2026-10-14 -1.1% 3.0x s44.5; PFIX 2026-10-08 -4.4% 2.7x s37.4; HYMB 2026-10-02 -1.1% 5.4x s67.3; XMPT 2026-09-28 -2.3% 4.6x s60.4; CGHM 2026-09-28 -1.0% 2.6x s43.8; FMHI 2026-09-28 -1.1% 2.9x s41.8; GOVI 2026-09-23 -1.2% 5.5x s58.1
- `16:01:27`   board gold_metals (1 rows, 60 sessions): URNJ 2026-08-28 -7.6% 2.9x s45.6
- `16:01:27`   board commodities (4 rows, 60 sessions): OILK 2026-11-04 -6.4% 11.1x s73.7; CERY 2026-11-04 -2.5% 7.2x s62.7; COMT 2026-11-04 -4.3% 2.9x s42.4; PIT 2026-11-02 -3.8% 2.8x s45.5
- `16:01:27`   board crypto (8 rows, 60 sessions): STRK 2026-10-12 -30.9% 8.5x s84.4; HBAR 2026-09-29 -16.1% 5.0x s65.1; GRT 2026-09-28 -15.0% 6.5x s66.7; ONDO 2026-09-28 -11.7% 5.5x s52.9; JUP 2026-09-28 -11.9% 5.3x s49.9; LDO 2026-09-23 -11.1% 3.0x s45.4; XRP 2026-09-15 -9.9% 2.7x s56.6; ARB 2026-09-07 -10.5% 5.1x s46.1
- `16:01:27` benchmarks: SPY last 766.81 prints250 0; QQQ last 691.95 prints250 0; IWM last 284.02 prints250 0; TLT last 81.76 prints250 0; GLD last 372.49 prints250 0; UUP last 28.9 prints250 0; HYG last 77.52 prints250 0; BTC last 80428.31 prints250 0
- `16:01:27` scan errors 0 | degraded [] | feeds_asof {"finviz": "2026-11-04T22:00:38.664398+00:00", "accumulation_radar": "2026-11-04T21:50:22.531629+00:00", "phase_detector": "2026-11-04T23:42:47.619976+00:00", "fortress": "2026-11-05T03:32:04+00:00", "katlin": "2026-11-05T04:13:20Z", "f13": null, "dark_pool": null, "insider": "2026-11-05T14:40:37.089001+00:00", "etf_flows": null}
## 4. data/bottom.json (the pointer block, the consumers' contract)

- `16:01:27` bottom.json: version 1.3.0 session 2026-11-04 generated 2026-11-05T16:01:27Z 2214698 bytes elapsed 379.7s | board 320 board_all 366 | degraded []
- `16:01:27` pointer block: key data/capitulation-desk.json regime HIGH sum_5 113 pct 96.7 n_scored 4958 n_prints_60 518 latest 113 | read: panic is broad: 113 prints in the last 5 sessions (96.7 percentile of the window, high) -- a market-wide capitulation cluster; last session 2026-11-04: 55 (stocks 46, equity_etfs 4, commodities 3)
- `16:01:27` bottom_context.context_rows: 2 rows (the katlin / fortress / alert consumers' contract holds)
- `16:01:27`   engine log: [bottom] equities/wrappers: 4894 scored, 365 rows with a live or recent sequence, 6429 historical sequences, 510 capitulation prints kept in 191s
- `16:01:27`   engine log: [bottom] crypto lane: 64 symbols, 64 with >=200 days, 4 errors, sources {"err": 4, "ok:coinbase": 40, "ok:polygon": 20}
- `16:01:27`   engine log: [bottom] crypto lane errors: TRX err:polygon HTTP Error 429: Too Many Requests|coinbase HTTP Error 404:; RUNE err:polygon HTTP Error 429: Too Many Requests|coinbase HTTP Error 404:; KAS err:polygon HTTP Error 429: Too Many Requests|coinbase HTTP Error 404:; XMR err:polygon HTTP Error 429: Too Many R
- `16:01:27`   engine log: [bottom] capitulation: data/capitulation-desk.json (410015 bytes) scored=4958 prints_60=518 latest=113 regime=HIGH sum_5=113 pct=96.7 clusters=1 errors=0
- `16:01:27` history data/bottom/history/2026-11-04.json.gz: capitulation {"regime": "HIGH", "sum_5": 113, "sum_5_percentile": 96.7, "sum_5_pct_of_scored": 2.28, "n_scored": 4958, "last_session": {"date": "2026-11-04", "total": 55, "by_desk": {"stocks": 46, "commodities": 3, "equity_etfs": 4, "countries": 2}, "sum_5": 113, "pct_of_scored": 1.11}, "counts": {"stocks": {"sc
## 5. the crypto bank

- `16:01:27`   BTC   rows 2252 last 2026-11-04 banked 2026-11-05T03:47:39Z last_source None
- `16:01:28`   ETH   rows 2252 last 2026-11-04 banked 2026-11-05T03:47:39Z last_source None
- `16:01:28`   SOL   rows 2078 last 2026-11-04 banked 2026-11-05T04:13:13Z last_source None
- `16:01:28`   XRP   rows 2251 last 2026-11-04 banked 2026-11-05T03:47:39Z last_source None
- `16:01:28`   DOGE  rows 2252 last 2026-11-04 banked 2026-11-05T15:44:16Z last_source coinbase
- `16:01:28`   AVAX  rows 1821 last 2026-11-04 banked 2026-11-05T15:44:15Z last_source coinbase
- `16:01:28`   LINK  rows 2251 last 2026-11-04 banked 2026-11-05T15:44:16Z last_source coinbase
- `16:01:28`   PEPE  rows 1268 last 2026-11-04 banked 2026-11-05T15:44:17Z last_source coinbase
- `16:01:28`   HYPE  rows 416 last 2026-11-04 banked 2026-11-05T16:01:13Z last_source coinbase
- `16:01:28`   TAO   rows 861 last 2026-11-04 banked 2026-11-05T15:44:17Z last_source coinbase
- `16:01:28` sampled last_source: {"?": 4, "coinbase": 6}
- `16:01:28` ✅ ops 6753 complete
