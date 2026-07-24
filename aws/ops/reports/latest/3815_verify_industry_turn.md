# ops 3815 — industry TURN detection live

**Status:** success  
**Duration:** 19.2s  
**Finished:** 2026-07-24T18:35:07+00:00  

## Data

| invoke_seconds | invoke_status | names_confirmed_by_turn | names_disqualified_by_fade |
|---|---|---|---|
| 18.4 | 200 |  |  |
|  |  | 264 | 241 |

## Log
## Settle v5.1

- `18:34:48` ✅ v5.1 live (attempt 1)
- `18:34:48` ✅ DEPLOY.settled :: trend logic in deployed zip
## Invoke

- `18:35:07` ✅ LIVE.v51 :: version=5.1
## Trend distribution

- `18:35:07`   BOOMING    1107 names
- `18:35:07`   NEUTRAL    986 names
- `18:35:07`   DECAYING   556 names
- `18:35:07`   TURNING    264 names
- `18:35:07`   FADING     241 names
- `18:35:07` ✅ TREND.four_way :: 5 trend classes populated
- `18:35:07` ✅ TREND.turning_exists :: TURNING=264 names
## regime_tailwind_book — TURNING first

- `18:35:07` ✅ BOOK.populated :: 28 names
- `18:35:07` ✅ BOOK.turning_first :: book ordered TURNING before BOOMING
- `18:35:07`   HERE   Leisure                  TURNING  gap=+60.0 SI=44.1 UNPROVEN
- `18:35:07`   WDH    Insurance - Brokers      TURNING  gap=+22.9 SI=45.8 UNPROVEN
- `18:35:07`   PRCH   Insurance - Property & C TURNING  gap=+70.8 SI=44.9 UNPROVEN
- `18:35:07`   AFGB   Insurance - Property & C TURNING  gap=+24.8 SI=41.6 UNPROVEN
- `18:35:07`   NWSA   Entertainment            TURNING  gap=+21.4 SI=42.6 UNPROVEN
- `18:35:07`   FLYW   Specialty Business Servi TURNING  gap=+30.6 SI=41.4 UNPROVEN
- `18:35:07`   CGEN   Biotechnology            BOOMING  gap=+37.1 SI=45.0 UNPROVEN
- `18:35:07`   GOTU   Education & Training Ser BOOMING  gap=+43.2 SI=48.8 MISPRICED
- `18:35:07`   OBIO   Biotechnology            BOOMING  gap=+16.5 SI=47.4 NO_GAP
- `18:35:07`   CEVA   Semiconductors           BOOMING  gap=+59.2 SI=46.6 UNPROVEN
- `18:35:07`   INSP   Medical - Devices        BOOMING  gap=+19.6 SI=41.8 NO_GAP
- `18:35:07`   RR     Industrial - Machinery   BOOMING  gap=+66.9 SI=40.8 UNPROVEN
## industry_trends table — turning / booming / fading

- `18:35:07`   Steel                            TURNING  score= 59.1 delta= +9.9 n=15
- `18:35:07`   Entertainment                    TURNING  score= 69.3 delta= +8.7 n=28
- `18:35:07`   Railroads                        TURNING  score= 68.2 delta= +6.3 n=9
- `18:35:07`   Solar                            TURNING  score= 60.9 delta= +2.9 n=9
- `18:35:07`   Specialty Business Services      TURNING  score= 64.6 delta= +2.6 n=32
- `18:35:07`   Restaurants                      TURNING  score= 69.0 delta= +2.4 n=35
- `18:35:07`   Packaging & Containers           TURNING  score= 59.7 delta= +2.2 n=16
- `18:35:07`   Medical - Distribution           TURNING  score= 61.2 delta= +1.7 n=7
- `18:35:07`   Oil & Gas Refining & Marketing   TURNING  score= 60.2 delta= +1.4 n=19
- `18:35:07`   Insurance - Property & Casualty  TURNING  score= 69.3 delta= +1.1 n=47
- `18:35:07`   Leisure                          TURNING  score= 67.1 delta= +1.0 n=20
- `18:35:07`   Rental & Leasing Services        TURNING  score= 59.8 delta= +0.9 n=18
- `18:35:07`   Insurance - Brokers              TURNING  score= 61.0 delta= +0.7 n=9
- `18:35:07`   Telecommunications Services      BOOMING  score= 71.4 delta= +9.6 n=54
- `18:35:07` ✅ TRENDS.has_turning :: turning industries surfaced
- `18:35:07` ✅ TRENDS.has_fading :: fading industries surfaced (the hidden hazard)
## TURNING is now a confirmation leg in verdicts

## Served page v13

- `18:35:07` ✅ SERVED.stamp :: present
- `18:35:07` ✅ SERVED.tailwind_div :: present
- `18:35:07` ✅ SERVED.trend_chip :: present
- `18:35:07` ✅ SERVED.trend_col :: present
- `18:35:07` ✅ SERVED.regime_key :: present
- `18:35:07` ✅ SERVED.trends_key :: present
- `18:35:07` ✅ SERVED.gloss :: present
## Additive — v12 verdict surfaces intact

- `18:35:07` ✅ KEPT.Mispriced :: intact
- `18:35:07` ✅ KEPT.Value_Traps :: intact
- `18:35:07` ✅ KEPT.function_vdt( :: intact
- `18:35:07` ✅ KEPT.mispriced_book :: intact
- `18:35:07` ✅ KEPT.How_crucial :: intact
- `18:35:07` ✅ KEPT.Most_Undervalued :: intact
## VERDICT

- `18:35:07` ✅ PASS_ALL — the turn is detectable, and the mirror steers toward it
