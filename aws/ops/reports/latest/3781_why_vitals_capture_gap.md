# ops 3781 — verify capture-gap tiles in why.html Vitals

**Status:** success  
**Duration:** 51.1s  
**Finished:** 2026-07-23T22:51:00+00:00  

## Log
## Feed precondition

- `22:50:10` ✅ FEED.rows :: capture ledger n=1771
- `22:50:10` ✅ FEED.note :: not-a-target note present in feed
## Sample tickers a user would actually open

- `22:50:10`   NVDA   gap=  -0.6pp global=  +0.0pp catchup=    -60% (EV/S+P/E) tier=WATCH
- `22:50:10`   TSM    gap= -10.6pp global=  -1.7pp catchup=    300% (EV/S+P/E) tier=WATCH
- `22:50:10`   ASML   gap=  +0.5pp global=  -0.1pp catchup=    -56% (EV/S+P/E) tier=WATCH
- `22:50:10`   AMD    gap= -30.4pp global= -14.3pp catchup=    -64% (EV/S+P/E) tier=WATCH
- `22:50:10`   GILD   gap= +18.8pp global=  +4.3pp catchup=    -43% (EV/S+P/E) tier=WATCH
- `22:50:10`   DBX    gap= +15.2pp global= +64.4pp catchup=    128% (EV/S+P/E) tier=WATCH
- `22:50:10`   MSFT   gap= -12.3pp global=  -2.1pp catchup=    -30% (EV/S+P/E) tier=WATCH
## Served page — markers unique to this change

- `22:50:10` attempt 1: HTTP 200 · 270963 bytes · 1/5 markers
- `22:50:35` attempt 2: HTTP 200 · 270963 bytes · 1/5 markers
- `22:51:00` attempt 3: HTTP 200 · 273411 bytes · 5/5 markers
- `22:51:00` ✅ SERVED.fetch_call :: present in served why.html
- `22:51:00` ✅ SERVED.tile_gap :: present in served why.html
- `22:51:00` ✅ SERVED.tile_catchup :: present in served why.html
- `22:51:00` ✅ SERVED.honesty :: present in served why.html
- `22:51:00` ✅ SERVED.fn :: present in served why.html
## Additive — existing Vitals tiles must survive

- `22:51:00` ✅ KEPT.P_E_TTM :: intact
- `22:51:00` ✅ KEPT.PEG :: intact
- `22:51:00` ✅ KEPT.P_S :: intact
- `22:51:00` ✅ KEPT.EV_EBITDA :: intact
- `22:51:00` ✅ KEPT.SHARES_OUT :: intact
- `22:51:00` ✅ KEPT.DILUTION :: intact
- `22:51:00` ✅ KEPT.renderJHVitals :: intact
- `22:51:00` ✅ KEPT.fillJHVitals :: intact
## VERDICT

- `22:51:00` ✅ PASS_ALL — capture gap now visible on the per-stock research page
- `22:51:00` Wiring set complete: engine -> page -> best-setups -> ranker -> why.html
