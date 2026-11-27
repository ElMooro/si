# 6998 - the canaries' record, the war room 2.0 and the grid's history completion: dry run (read-only), 2026-11-27T16:57+00:00

**Status:** failure  
**Duration:** 114.6s  
**Finished:** 2026-11-27T16:59:15+00:00  

## Error

```
SystemExit: 1
```

## Data

| canaries_rebuilt | failed_checks | level_early_score | turn_early_score |
|---|---|---|---|
| 164 | 1 | 0.5081 | 0.6478 |

## Log
## 1. The record, built by the engine's own code from the live warehouse

- `16:57:51`     [record] warehouse did not answer for LRUNTTTTCHM156S: HTTPError
- `16:57:52`     [record] warehouse did not answer for LRHUTTTTCHM156S: HTTPError
- `16:58:57`     [record] {"ok": true, "tested": 164, "defined": 192, "verdicts": {"no edge shown": 129, "no record": 28, "wrong way": 7, "leads": 16, "short record": 5, "stuck": 7}, "months": 653, "missing": ["LRUNTTTTCHM156S", "LRHUTTTTCHM156S"], "duration_s": 96.9, "refused": null, "written": false}
- `16:58:57` built in 97 s; summary {"ok": true, "tested": 164, "defined": 192, "verdicts": {"no edge shown": 129, "no record": 28, "wrong way": 7, "leads": 16, "short record": 5, "stuck": 7}, "months": 653, "missing": ["LRUNTTTTCHM156S", "LRHUTTTTCHM156S"], "duration_s": 96.9, "refused": null, "written": false}
- `16:58:57` PASS  the dry run wrote nothing
- `16:58:57` PASS  164 of 192 canaries rebuilt (missing series: ['LRUNTTTTCHM156S', 'LRHUTTTTCHM156S'])
- `16:58:57` inputs: {"series_asked": 84, "series_kept": 82, "series_missing": ["LRUNTTTTCHM156S", "LRHUTTTTCHM156S"], "closes_raw": 25013, "closes_dropped": {"weekend": 51, "second_row": 112, "stray_quarter_row": 7}, "ciss_series": 88, "live_readings": 140, "ciss_document_at": "2026-11-27T07:12:24.706133+00:00"}
- `16:58:57` index: 24843 closes 19271230 to 20261125, last 7185.96, -8.1% from its one-year high; 646 month-ends counted, 138 followed by a 10% fall (21.4%); near a high 58 of 397 (14.6%)
- `16:58:57` PASS  the known falls are in the index and none is invented in 2021 (stray rows dropped: {"weekend": 51, "second_row": 112, "stray_quarter_row": 7})
- `16:58:57` falls since 2015: [[20150521, 20150825, -12.3], [20151103, 20160211, -13.3], [20180126, 20180208, -10.2], [20180920, 20181224, -19.8], [20200219, 20200323, -33.9], [20220103, 20220308, -13.1], [20220329, 20220616, -20.8], [20220816, 20221012, -16.9], [20230731, 20231027, -10.3], [20250219, 20250408, -18.9], [20261006, 20261120, -11.6]]
- `16:58:57` verdicts: {"no edge shown": 129, "no record": 28, "wrong way": 7, "leads": 16, "short record": 5, "stuck": 7}
- `16:58:57` barometer: 653 months from 19720731
- `16:58:57` LEVEL (each mechanism one vote): early score 0.5081 +- 0.0645 over 586 months; every month 0.5948
- `16:58:57`     CALM           0-40    months  326 fell  55 (0.1687) | near a high 268 fell 44 (0.1642)
- `16:58:57`     GUARDED       40-70    months  138 fell  29 (0.2101) | near a high  72 fell  7 (0.0972)
- `16:58:57`     ELEVATED      70-90    months   84 fell  19 (0.2262) | near a high  34 fell  2 (0.0588)
- `16:58:57`     HIGH RISK     90-97    months   24 fell   7 (0.2917) | near a high   2 fell  0 (0.0)
- `16:58:57`     CRISIS        97-100   months   14 fell   8 (0.5714) | near a high   0 fell  0 (None)
- `16:58:57` TURN (3 months, mechanisms weighted by their own record, walk-forward): early score 0.6478 +- 0.0633 over 508 months from 19840131; every month 0.669 +- 0.0582; weights now {"ciss": 0.0, "macro_grid": 0.1311, "plumbing": 0.0731}
- `16:58:57`     EASING          0-30    months  141 fell  15 (0.1064) deep 0.0638 | near a high 102 fell  6 (0.0588)
- `16:58:57`     STEADY         30-70    months  164 fell  31 (0.189) deep 0.0976 | near a high 127 fell 19 (0.1496)
- `16:58:57`     RISING         70-90    months   91 fell  33 (0.3626) deep 0.2308 | near a high  52 fell 13 (0.25)
- `16:58:57`     RISING FAST    90-100   months   52 fell  16 (0.3077) deep 0.25 | near a high  20 fell  4 (0.2)
- `16:58:57` every span tried: {"1": [0.5803, 0.0529, 0.5145], "3": [0.6478, 0.0633, 0.5487], "6": [0.5401, 0.0846, 0.5852], "12": [0.4537, 0.0939, 0.5225]}
- `16:58:57` by period: [[19840131, 19980331, 171, 0.5263, 0.1672], [19980430, 20120629, 171, 0.6908, 0.1326], [20120731, 20260430, 166, 0.7283, 0.0902]]
- `16:58:57`     level low    turn easing  months  78 fell   6 (0.0769) | near a high  68 fell  3 (0.0441)
- `16:58:57`     level low    turn steady  months 103 fell  21 (0.2039) | near a high  93 fell 18 (0.1935)
- `16:58:57`     level low    turn rising  months  67 fell  16 (0.2388) | near a high  57 fell 13 (0.2281)
- `16:58:57`     level middle turn easing  months  39 fell   5 (0.1282) | near a high  23 fell  2 (0.087)
- `16:58:57`     level middle turn steady  months  34 fell   5 (0.1471) | near a high  24 fell  1 (0.0417)
- `16:58:57`     level middle turn rising  months  34 fell  16 (0.4706) | near a high  12 fell  4 (0.3333)
- `16:58:57`     level high   turn easing  months  24 fell   4 (0.1667) | near a high  11 fell  1 (0.0909)
- `16:58:57`     level high   turn steady  months  27 fell   5 (0.1852) | near a high  10 fell  0 (0.0)
- `16:58:57`     level high   turn rising  months  42 fell  17 (0.4048) | near a high   3 fell  0 (0.0)
- `16:58:57`     mechanism ciss         88 canaries | level early 0.5036 +- 0.0697 | turn early 0.4918 +- 0.0469 | weight 0.0
- `16:58:57`     mechanism macro_grid   45 canaries | level early 0.5004 +- 0.0652 | turn early 0.6311 +- 0.0582 | weight 0.1311
- `16:58:57`     mechanism plumbing     31 canaries | level early 0.4575 +- 0.0766 | turn early 0.5731 +- 0.061 | weight 0.0731
- `16:58:57` PASS  the level does not warn: early score 0.5081 (near chance)
- `16:58:57` PASS  the turn's early score stands above chance beyond its error: 0.6478 - 1.64 x 0.0633
- `16:58:57` PASS  falls followed a rising turn more often than an easing one: [0.1064, 0.189, 0.3626, 0.3077]
- `16:58:57` PASS  the cross table holds the same months as the turn's bands
- `16:58:57` leads (16): em_hy_ytw 0.772; em_corp_oas 0.7222; em_hy_oas 0.6948; Netherlands, New Composite Indicator of Syst 0.6926; Euro area (changing composition), Contributi 0.6818; Euro area (changing composition), Contributi 0.6802; WTREGEN 0.6794; bbb_aaa 0.6662; hy_credit_oas 0.664; Austria, New Composite Indicator of Systemic 0.6516; Euro area (changing composition), Contributi 0.6516; Latvia, Country-Level Index of Financial Str 0.6511; Ireland, New Composite Indicator of Systemic 0.646; United Kingdom, Country-Level Index of Finan 0.6418; Euro area (changing composition), Stress sub 0.6257; United States, Systemic Stress Composite Ind 0.5998
- `16:58:57` wrong way (7): SWP1690 0.4004; ppi_pulp_paper 0.3941; Croatia, Country-Level Index of Financial St 0.375; Sweden, Country-Level Index of Financial Str 0.3681; swiss_unemp 0.335; swap_line_usage 0.3285; Euro area (changing composition), Stress sub 0.307
- `16:58:57` stuck (7): CASFRIW027SBOG 0.5424; WALCL 0.5336; RMFSL 0.5335; DPSFRIW027SBOG 0.518; SBCACBW027SBOG 0.5172; BOGZ1FL663067003Q 0.5045; USPBS 0.4719
- `16:58:57` short record (5): repo_sofr_iorb 0.6731; SOFR1 0.6615; Estonia, Country-Level Index of Financial St 0.5525; bill4w_floor 0.516; SOFR99 0.345
- `16:58:57` against the engines' own readings today: {"canaries": 114, "rebuilt": 46.7, "engines_own": 49.01, "gap": -2.31, "median_gap": 0.4, "flagged": ["ciss:Netherlands, Composite Indicator of Sovereign Stress, Index", "ciss:United States, New Composite Indicator of Systemic Stress (CISS), Index", "macro_grid:bbb_aaa", "macro_grid:em_corp_oas", "macro_grid:em_hy_oas", "macro_grid:em_hy_ytw", "macro_grid:euro_hy_oas", "macro_grid:hy_credit_oas", "macro_grid:repo_sofr_iorb", "macro_grid:semiconductor_ip", "macro_grid:us_hy_ytw"]}
- `16:58:57` PASS  the rebuild matches the engines: middle gap 0.4 points over 114 canaries
- `16:58:57`     flagged ciss:Netherlands, Composite Indicator of Sovereign Stress, Index: rebuilt 41.3 against the engine's own 56.8 today
- `16:58:57`     flagged ciss:United States, New Composite Indicator of Systemic Stress (CISS), Index: rebuilt 39.4 against the engine's own 17.9 today
- `16:58:57`     flagged macro_grid:bbb_aaa: rebuilt 34.0 against the engine's own 52.6 today
- `16:58:57`     flagged macro_grid:em_corp_oas: rebuilt 40.6 against the engine's own 67.8 today
- `16:58:57`     flagged macro_grid:em_hy_oas: rebuilt 42.1 against the engine's own 73.8 today
- `16:58:57`     flagged macro_grid:em_hy_ytw: rebuilt 54.9 against the engine's own 87.5 today
- `16:58:57`     flagged macro_grid:euro_hy_oas: rebuilt 44.9 against the engine's own 71.9 today
- `16:58:57`     flagged macro_grid:hy_credit_oas: rebuilt 52.2 against the engine's own 94.4 today
- `16:58:57`     flagged macro_grid:repo_sofr_iorb: rebuilt 63.2 against the engine's own 94.3 today
- `16:58:57`     flagged macro_grid:semiconductor_ip: the warehouse copy of its series does not give the engine's own reading today (34.0 against 23.3): its newest 18 months are left out
- `16:58:57`     flagged macro_grid:us_hy_ytw: rebuilt 71.1 against the engine's own 100.0 today
- `16:58:57` the document would be 423076 bytes
- `16:58:57` FAIL  nothing shaped like a security identifier is in the record
## 2. The macro grid: a short answer from FRED completed from the warehouse

- `16:58:59`     hy_credit_oas BAMLH0A0HYM2         asked  1300, FRED  793, after the warehouse  1300 (oldest 2021-12-16)
- `16:58:59` PASS  BAMLH0A0HYM2: FRED's own rows are kept and older ones added behind them
- `16:58:59`     hy_credit_oas the grid's own arithmetic on the completed history: 54.9 (z 0.22 over 1292 observations) | live today 94.4 | the record's rebuild 52.2
- `16:58:59` PASS  hy_credit_oas: the completed reading (54.9) agrees with the record's rebuild (52.2)
- `16:59:01`     euro_hy_oas  BAMLHE00EHYIOAS      asked  1300, FRED  793, after the warehouse  1300 (oldest 2016-10-20)
- `16:59:01` PASS  BAMLHE00EHYIOAS: FRED's own rows are kept and older ones added behind them
- `16:59:01`     euro_hy_oas  the grid's own arithmetic on the completed history: 45.0 (z -0.23 over 1292 observations) | live today 71.9 | the record's rebuild 44.9
- `16:59:01` PASS  euro_hy_oas: the completed reading (45.0) agrees with the record's rebuild (44.9)
- `16:59:02`     em_corp_oas  BAMLEMCBPIOAS        asked  1300, FRED  793, after the warehouse  1300 (oldest 2021-12-16)
- `16:59:02` PASS  BAMLEMCBPIOAS: FRED's own rows are kept and older ones added behind them
- `16:59:02`     em_corp_oas  the grid's own arithmetic on the completed history: 41.0 (z -0.41 over 1292 observations) | live today 67.8 | the record's rebuild 40.6
- `16:59:02` PASS  em_corp_oas: the completed reading (41.0) agrees with the record's rebuild (40.6)
- `16:59:03`     em_hy_oas    BAMLEMHBHYCRPIOAS    asked  1300, FRED  793, after the warehouse  1300 (oldest 2021-12-16)
- `16:59:03` PASS  BAMLEMHBHYCRPIOAS: FRED's own rows are kept and older ones added behind them
- `16:59:03`     em_hy_oas    the grid's own arithmetic on the completed history: 43.0 (z -0.32 over 1292 observations) | live today 73.8 | the record's rebuild 42.1
- `16:59:03` PASS  em_hy_oas: the completed reading (43.0) agrees with the record's rebuild (42.1)
- `16:59:04`     us_hy_ytw    BAMLH0A0HYM2SYTW     asked  1300, FRED  793, after the warehouse  1300 (oldest 2021-12-16)
- `16:59:04` PASS  BAMLH0A0HYM2SYTW: FRED's own rows are kept and older ones added behind them
- `16:59:05`     us_hy_ytw    the grid's own arithmetic on the completed history: 73.3 (z 1.06 over 1292 observations) | live today 100.0 | the record's rebuild 71.1
- `16:59:05` PASS  us_hy_ytw: the completed reading (73.3) agrees with the record's rebuild (71.1)
- `16:59:06`     em_hy_ytw    BAMLEMHBHYCRPISYTW   asked  1300, FRED  793, after the warehouse  1300 (oldest 2021-12-16)
- `16:59:06` PASS  BAMLEMHBHYCRPISYTW: FRED's own rows are kept and older ones added behind them
- `16:59:06`     em_hy_ytw    the grid's own arithmetic on the completed history: 54.8 (z 0.22 over 1292 observations) | live today 87.5 | the record's rebuild 54.9
- `16:59:06` PASS  em_hy_ytw: the completed reading (54.8) agrees with the record's rebuild (54.9)
- `16:59:07`     bbb_aaa      BAMLC0A4CBBB         asked  1300, FRED  793, after the warehouse  1300 (oldest 2021-12-16)
- `16:59:07` PASS  BAMLC0A4CBBB: FRED's own rows are kept and older ones added behind them
- `16:59:08`     bbb_aaa      BAMLC0A1CAAA         asked 10400, FRED  793, after the warehouse  7816 (oldest 1996-12-31)
- `16:59:08` PASS  BAMLC0A1CAAA: FRED's own rows are kept and older ones added behind them
- `16:59:08`     bbb_aaa      the grid's own arithmetic on the completed history: 36.9 (z -0.60 over 1292 observations) | live today 52.6 | the record's rebuild 34.0
- `16:59:08` PASS  bbb_aaa: the completed reading (36.9) agrees with the record's rebuild (34.0)
- `16:59:09` PASS  a series FRED answers in full is left to FRED alone (T10Y2Y: 2600 of 2600)
## 3. The war room 2.0 on today's live feeds (dry)

- `16:59:12` PASS  the war room's dry run wrote nothing
- `16:59:12` PASS  every key published today is still published (missing: []); added: ['calibrated', 'version']
- `16:59:12` PASS  barometer keeps every field it had (missing: [])
- `16:59:12` PASS  master keeps every field it had (missing: [])
- `16:59:12` headline: 127 of 263 canaries firing across 9 voting mechanisms (12 watched); early-warning 25.4 (WATCH). 1 cross-mechanism divergence.
- `16:59:12` existing views: per canary 44.8, per mechanism 46.4, earned 45.9, early-warning 25.4
- `16:59:12` PASS  the calibrated reading is there
- `16:59:12` today, feeds as they are: LEVEL 50.7 = 43.5 percentile, GUARDED (after it: 29 of 138 months fell, 0.2101) | TURN +9.1 against 20260831 (88 days, the record's own month-end rebuild) = 98.3 percentile, RISING FAST (after it: 16 of 52 fell, 0.3077; near a high 4 of 20, 0.2) | by mechanism {"ciss": 13.6, "macro_grid": 10.3, "plumbing": 7.1} | cell {"level": "middle", "turn": "rising", "months": 34, "fell": 16, "fall_share": 0.4706, "early_months": 12, "early_fell": 4, "early_fall_share": 0.3333}
- `16:59:12` PASS  110 canaries on the board carry a record; 153 do not, each with its reason
- `16:59:12` PASS  every canary without a record says why
- `16:59:15` after the grid's next run (credit canaries at their completed readings): LEVEL 49.2 = 38.0 percentile, CALM (after it: 55 of 326 months fell, 0.1687) | TURN +6.2 against 20260831 (88 days, the record's own month-end rebuild) = 93.2 percentile, RISING FAST (after it: 16 of 52 fell, 0.3077; near a high 4 of 20, 0.2) | by mechanism {"ciss": 13.6, "macro_grid": 5.6, "plumbing": 7.1} | cell {"level": "low", "turn": "rising", "months": 67, "fell": 16, "fall_share": 0.2388, "early_months": 57, "early_fell": 13, "early_fall_share": 0.2281}
- `16:59:15` existing views then: per canary 44.1, per mechanism 46.0, earned 45.6
- `16:59:15` ✗ 1 checks failed: nothing shaped like a security identifier is in the record
