# ops 3217 — non-wakers explained, mirror poison swept, ICE rates proxied

**Status:** success  
**Duration:** 96.5s  
**Finished:** 2026-07-13T05:45:05+00:00  

## Data

| active_before | active_now | coverage_now | ice_curated | mirror_candidates | n_fails | n_warns | newly_dormant | note | still_alive | verdict | woken | zero_point_swept |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  | 140 |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  | 0 |  |  | 140 |
|  |  |  | 1 |  |  |  |  |  |  |  |  |  |
|  |  | 74.4 |  |  |  |  |  | coverage drop = phantom entries leaving, honest |  |  |  |  |
| 117 | 117 |  |  |  |  |  | 0 |  |  |  | 0 |  |
|  |  |  |  |  | 0 | 0 |  |  |  | PASS |  |  |

## Log
## 1. Why the three still sleep (named reasons)

- `05:43:29`   Developed Markets                        state=DORMANT resolved=5 reason=needs >=6 members on a free source — map more of its indicators to act
- `05:43:29`   Europe Liquidity :BTPBUND  measure finan state=DORMANT resolved=2 reason=mapped members lack fetchable history (only 2 z-scorable of 6 mapped)
- `05:43:29`   Global Deposit Rates Which drains liquid state=DORMANT resolved=4 reason=mapped members lack fetchable history (only 4 z-scorable of 6 mapped)
## 2. FRED-OECD-mirror sweep (non-curated only)

- `05:43:32` ✅ 140 phantom mirrors out of the map — 'resolved' now means fetchable, fleet-wide
## 3. ICEEUR I/EON — candidate-laddered, probe-gated

- `05:43:32`   ✗ ICEEUR:EON2!: all candidates dry — stays open
- `05:43:34` ✅ ICEEUR:I2! → DBNOMICS~ECB/FM/M.U2.EUR.RT.MM.EURIBOR3MD_.HSTA~hundred_  (390 pts)
## 4. Fleet re-run

- `05:45:05`   DORMANT  49 × needs >=6 members on a free source — map more of its indicators 
- `05:45:05`   DORMANT  35 × mapped members lack fetchable history
- `05:45:05`   DORMANT   3 × only 0 weeks of joint activation history
- `05:45:05`   DORMANT   1 × only 20 weeks of joint activation history
- `05:45:05`   → Developed Markets                    now DORMANT (needs >=6 members on a free source — map more of i)
- `05:45:05`   → Europe Liquidity :BTPBUND  measure f now DORMANT (mapped members lack fetchable history (only 3 z-sc)
- `05:45:05`   → Global Deposit Rates Which drains li now DORMANT (mapped members lack fetchable history (only 4 z-sc)
