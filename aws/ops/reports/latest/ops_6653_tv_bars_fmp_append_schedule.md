# ops 6653 - tv-bars fmp-append: schedule, declaration, first run, bank verification

**Status:** success  
**Duration:** 4.1s  
**Finished:** 2026-10-25T22:26:17+00:00  

## Data

| appended_rows | appended_symbols | failed | fmp_bank_not_ready | step | through | unproven |
|---|---|---|---|---|---|---|
| 12 | 1 | 2 | 13 | fmp-append first run | 2026-10-23 | 0 |

## Log
## 1. the deployed function carries v1.3

- `22:26:13` justhodl-tv-bars modified 2026-10-25T21:58:44, timeout 600, memory 1024
- `22:26:14` ✅ deployed function answers mode=fmp-append (dry: {"ok": true, "mode": "fmp-append", "at": "2026-10-25T22:26:14+00:00", "through": "2026-10-23", "mapped": 16, "appended_symbols": 0, "appended_rows": 0, "failed": 0, "skipped": 0, "unproven": 0, "fmp_b)
## 2. schedule

- `22:26:15` ✅ created justhodl-tv-bars-fmp-append: cron(58 23 * * ? *) ENABLED target justhodl-tv-bars role justhodl-scheduler-role
- `22:26:15` ✅ manifest declared ['justhodl-polygon-eod-daily', 'justhodl-tv-bars-fmp-append'] (backup backups/schedule-manifest/20261025T222615Z/)
## 3. first append now (every verdict recorded)

- `22:26:17` append (2s): through 2026-10-23 mapped 16 appended_symbols 1 appended_rows 12 failed 2 skipped 0 unproven 0 fmp_bank_not_ready 13
- `22:26:17`   C:BRLUSD: error: Malformed predecessor bars; existing bank preserved
- `22:26:17`   COINBASE:BTCUSD: fmp_history_not_banked:BTCUSD
- `22:26:17`   COINBASE:ETHUSD: fmp_history_not_banked:ETHUSD
- `22:26:17`   FX:EURUSD: error: Malformed predecessor bars; existing bank preserved
- `22:26:17`   HKEX:700: fmp_history_not_banked:0700.HK
- `22:26:17`   I:NDX: fmp_history_not_banked:^NDX
- `22:26:17`   I:SPX: fmp_history_not_banked:^GSPC
- `22:26:17`   SSE:000001: appended 12 through 2026-10-23 from 000001.SS
- `22:26:17`   TVC:US10Y: fmp_history_not_banked:^TNX
- `22:26:17`   US:^IRX: fmp_history_not_banked:^IRX
- `22:26:17`   US:^MOVE: fmp_history_not_banked:^MOVE
- `22:26:17`   US:^TNX: fmp_history_not_banked:^TNX
- `22:26:17`   US:^TYX: fmp_history_not_banked:^TYX
- `22:26:17`   X:BTCUSD: fmp_history_not_banked:BTCUSD
- `22:26:17`   X:PEPEUSD: fmp_history_not_banked:PEPEUSD
- `22:26:17`   XETR:DAX: fmp_history_not_banked:^GDAXI
- `22:26:17` index fmp_append: at 2026-10-25T22:26:17+00:00 through 2026-10-23 appended_symbols 1
- `22:26:17` SSE:000001 bank after: n 7450 last 2026-10-23 source mixed-bank:unqualified; last 3 bars [('2026-10-21T01:30:00+00:00', 3713.03), ('2026-10-22T01:30:00+00:00', 3658.03), ('2026-10-23T01:30:00+00:00', 3690.48)] labels ['fmp-eod-full:bank', 'fmp-eod-full:bank', 'fmp-eod-full:bank']
- `22:26:17` ✅ ops 6653 complete
