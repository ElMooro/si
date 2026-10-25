# ops 6641 - provider fixes: sec-bulk weekly live, key-fix engines re-run, Polygon entitlement probe

**Status:** success  
**Duration:** 845.2s  
**Finished:** 2026-10-25T16:00:43+00:00  

## Data

| body | endpoint | http |
|---|---|---|
| {"status":"NOT_AUTHORIZED","request_id":"33916da129608a7a555b4aa5171caec2","message":"You are not entitled to this data. | last/trade | 403 |
| {"status":"NOT_AUTHORIZED","request_id":"7dc8c01bdc343b7c31d941de03698d5b","message":"You are not entitled to this data. | quotes | 403 |
| {"status":"GONE","error":"This endpoint has been deprecated and is undergoing a brownout. Please check the Sunset and Li | financials (must be refused) | 410 |
| {"status":"ERROR","request_id":"c21a28a7834640318ee6b2370b6c98c5","error":"You are not entitled to this data. Please upg | benzinga earnings | 403 |
| {"status":"OK","request_id":"134e69f86b5f4e3f9743894734ca39ee","results":[{"settlement_date":"2017-12-29","ticker":"AAPL | short-interest | 200 |

## Log
## 1. deployed schedule manifest := committed (one allowed state change)

- `15:46:38` S3 1027 entries, repo 1028; added [('scheduler', 'justhodl-fmp-bank-daily')] removed 0 changed [('scheduler', 'justhodl-sec-bulk-weekly')]
- `15:46:38` ✅ justhodl-sec-bulk-weekly: DISABLED -> ENABLED live (cron(0 9 ? * MON *))
- `15:46:38` ✅ manifest uploaded (added 1, changed 1), backup backups/schedule-manifest/20261025T154638Z/
## 2. the key-fix engines: redeployed? then one run each, read back the fixed leg

- `15:46:38` justhodl-freight-pulse modified 2026-10-25T15:43:27 (redeployed)
- `15:46:38` eia-energy-agent modified 2026-10-25T15:42:24 (redeployed)
- `15:46:39` justhodl-cryptoquant modified 2026-10-25T15:42:53 (redeployed)
- `15:46:39` justhodl-symbology-master modified 2026-10-25T15:44:29 (redeployed)
- `15:47:09` ✅ eia-energy-agent rewrote data/eia-energy.json at 15:46:51 - fixed leg PRESENT
- `15:47:40` ⚠ justhodl-symbology-master rewrote data/symbology/master.json at 15:47:11 - fixed leg not visible yet (read the file)
- `15:49:10` ✅ justhodl-cryptoquant rewrote data/cryptoquant-onchain.json at 15:49:06 - fixed leg PRESENT
- `16:00:42` ⚠ justhodl-freight-pulse: output not rewritten within 14 min
## 3. Polygon entitlement probe (read-only; key never printed)

- `16:00:43` ✅ ops 6641 complete
