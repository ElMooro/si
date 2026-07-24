# ops 3814 — can we detect industries TURNING, not just booming?

**Status:** success  
**Duration:** 0.5s  
**Finished:** 2026-07-24T18:26:05+00:00  

## Data

| booming | decaying | delta_max | delta_med | delta_min | delta_p25 | delta_p75 | fading_from_high | generated | n_industries | score_max | score_med | score_min | score_p25 | score_p75 | thresholds | turning |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  | 2026-07-24T10:50:04.095434+00:00 | 119 |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  | 83.1 | 62.4 | 42.1 | 57.2 | 69.7 |  |  |
|  |  | 9.9 | -0.5 | -12.8 | -1.7 | 0.7 |  |  |  |  |  |  |  |  |  |  |
| 14 | 24 |  |  |  |  |  | 16 |  |  |  |  |  |  |  | hi=69.7 lo=57.2 delta_p75=0.7 | 19 |

## Log
## 1. Fields available per industry

- `18:26:05`   keys: ['boom_score', 'comp', 'coverage_w', 'industry', 'mcap_b', 'n', 'n_component_families', 'score_delta_20d', 'sector', 'top_names']
- `18:26:05`   sample: {"industry": "Computer Hardware", "sector": "Technology", "n": 38, "mcap_b": 1373.7, "boom_score": 83.1, "n_component_families": 9, "coverage_w": 115.0, "comp": {"rev_mean": 79.10000000000001, "rev_breadth": 100.0, "deal_wins_30d": 2, "backlog_accel_share": 0.0, "inst_net_bps": 33.8, "insider_buys_3
## 2. Distribution of boom_score and its 20d delta

- `18:26:05` ✅ PROBE.boom_score :: 119 industries carry boom_score
- `18:26:05` ✅ PROBE.delta :: 116 carry score_delta_20d
## 3. Can we separate TURNING from already-BOOMING?

- `18:26:05` ✅ PROBE.turn_detectable :: 19 industries are rising fast from a non-top base
## TURNING — the interesting bucket (rising, not yet at the top)

- `18:26:05`   Steel                              score= 59.1  delta20d=  +9.9  n=22   mcap=248.3B
- `18:26:05`   Entertainment                      score= 69.3  delta20d=  +8.7  n=40   mcap=498.7B
- `18:26:05`   Railroads                          score= 68.2  delta20d=  +6.3  n=11   mcap=571.4B
- `18:26:05`   Solar                              score= 60.9  delta20d=  +2.9  n=22   mcap=53.2B
- `18:26:05`   Chemicals                          score= 56.6  delta20d=  +2.9  n=14   mcap=34.5B
- `18:26:05`   Packaged Foods                     score= 48.1  delta20d=  +2.8  n=52   mcap=194.0B
- `18:26:05`   Specialty Business Services        score= 64.6  delta20d=  +2.6  n=54   mcap=338.3B
- `18:26:05`   Restaurants                        score= 69.0  delta20d=  +2.4  n=52   mcap=523.8B
- `18:26:05`   REIT - Industrial                  score= 57.2  delta20d=  +2.3  n=25   mcap=374.5B
- `18:26:05`   Packaging & Containers             score= 59.7  delta20d=  +2.2  n=19   mcap=129.4B
- `18:26:05`   Oil & Gas Integrated               score= 50.0  delta20d=  +2.2  n=19   mcap=2275.9B
- `18:26:05`   Medical - Distribution             score= 61.2  delta20d=  +1.7  n=12   mcap=221.5B
## BOOMING — already visible, mostly priced

- `18:26:05`   Biotechnology                      score= 77.2  delta20d=  +2.2
- `18:26:05`   Engineering & Construction         score= 75.8  delta20d=  +1.7
- `18:26:05`   Medical - Devices                  score= 75.4  delta20d=  +0.8
- `18:26:05`   Industrial Materials               score= 75.0  delta20d=  +0.0
- `18:26:05`   Medical - Care Facilities          score= 74.2  delta20d=  +0.3
- `18:26:05`   Software - Services                score= 73.0  delta20d=  +1.7
- `18:26:05`   Apparel - Retail                   score= 71.5  delta20d=  +2.3
- `18:26:05`   Telecommunications Services        score= 71.4  delta20d=  +9.6
## FADING — high score but rolling over (the trap nobody sees)

- `18:26:05`   Gold                               score= 71.0  delta20d=  -4.5
- `18:26:05`   Electrical Equipment & Parts       score= 76.3  delta20d=  -3.9
- `18:26:05`   Aerospace & Defense                score= 72.4  delta20d=  -3.7
- `18:26:05`   Staffing & Employment Services     score= 74.2  delta20d=  -3.2
- `18:26:05`   Drug Manufacturers - General       score= 74.6  delta20d=  -3.0
- `18:26:05`   Agricultural - Machinery           score= 70.0  delta20d=  -2.8
- `18:26:05`   Software - Infrastructure          score= 70.5  delta20d=  -2.0
- `18:26:05`   Marine Shipping                    score= 73.8  delta20d=  -1.7
## 4. Ledger exposure to each bucket

- `18:26:05`   BOOMING    14 industries ->  769 scored names
- `18:26:05`   TURNING    19 industries ->  368 scored names
- `18:26:05`   DECAYING   24 industries ->  409 scored names
- `18:26:05`   FADING     16 industries ->  466 scored names
## VERDICT

- `18:26:05` A booming-industry book is only useful if it separates ALREADY PRICED
- `18:26:05` from NOT YET PRICED. The design that earns its place:
- `18:26:05`   TURNING  = boom score rising hard from a mid/low base  -> the alpha
- `18:26:05`   BOOMING  = high and still rising -> confirmation, mostly priced
- `18:26:05`   FADING   = high but rolling over -> a trap NO cheapness screen catches
- `18:26:05` ✅ PASS_ALL — probe complete
