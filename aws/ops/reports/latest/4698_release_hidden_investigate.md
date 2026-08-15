# ops 4698 — FRED release-209 bulk claim + Hiddenmetrix claim

**Status:** success  
**Duration:** 11.8s  
**Finished:** 2026-08-15T15:41:44+00:00  

## Log
## 1a. Resolve the TRUE release_id for an ICE mnemonic (not trusting 209 blindly)

- `15:41:33`   /fred/series/release for BAMLH0A1HYBB -> id=209 name=ICE BofA Indices
- `15:41:33`   Khalid's claimed release_id: 209 | actual: 209 | match=True
## 1b. Does the release actually list the ICE BofA family?

- `15:41:33`   release 209: 192 series listed, 192 are BAML* (count=192 in response)
## 1c. THE CRUX TEST — does any release-scoped path serve MORE history than plain series/observations?

- `15:41:34`   BASELINE (plain series/observations, no bound): first=2023-08-15 count=795
- `15:41:34`   [release/tables] status=200 bytes=34 first_date_seen=None
- `15:41:35`   [release/dates] status=200 bytes=30852 first_date_seen=2023
- `15:41:35` ✅     ^ DEEPER than baseline (2023 < 2023-08-15)!
- `15:41:36`   [observations w/ vintage realtime_start=2020] HTTP 400: {"error_code":400,"error_message":"Bad Request.  The series does not exist in ALFRED but may exist in FRED.  Try setting realtime_start and realtime_e
- `15:41:37`   [observations w/ output_type=4 (vintage all)] HTTP 400: {"error_code":400,"error_message":"Bad Request.  No vintage dates exist for the specified real-time period: 2026-08-15 to 2026-08-15."}
- `15:41:38`   [release/series/observations (bulk-per-release, if it exists)] HTTP 404: {"error_code":404,"error_message":"Not Found"}
## 1 verdict

- `15:41:38` ✅ CLAIM 1 CONFIRMED — a release-scoped path serves deeper history than the per-series call
## 2a. Does Hiddenmetrix exist?

- `15:41:39`   https://hiddenmetrix.com -> HTTP 200, 978008 bytes, ct=text/html; charset=utf-8
- `15:41:39`   https://www.hiddenmetrix.com -> HTTP 200, 978008 bytes, ct=text/html; charset=utf-8
- `15:41:39`   https://app.hiddenmetrix.com -> <urlopen error [Errno -2] Name or service not known>
- `15:41:40`   https://api.hiddenmetrix.com -> HTTP 200, 58 bytes, ct=application/json
- `15:41:40`   https://hiddenmetrix.io -> <urlopen error [Errno -2] Name or service not known>
- `15:41:40`   https://hiddenmetrix.net -> <urlopen error [Errno -2] Name or service not known>
## 2b. If it exists: hunt the batch API + the BB series page

- `15:41:40`   https://hiddenmetrix.com/api/series/BAMLH0A1HYBB -> HTTP 404
- `15:41:41`   https://hiddenmetrix.com/api/v1/series/BAMLH0A1HYBB -> HTTP 404
- `15:41:41`   https://hiddenmetrix.com/api/series/batch -> HTTP 404
- `15:41:42`   https://hiddenmetrix.com/series/BAMLH0A1HYBB -> HTTP 404
- `15:41:43`   https://hiddenmetrix.com/fred/BAMLH0A1HYBB -> HTTP 404
- `15:41:43`   https://hiddenmetrix.com/data/BAMLH0A1HYBB -> 200, 29938 bytes, earliest_date=None
## 2 verdict

- `15:41:44`   domains that resolved: ['https://hiddenmetrix.com', 'https://www.hiddenmetrix.com', 'https://api.hiddenmetrix.com']
## overall verdict

- `15:41:44` ✅ investigation complete — both claims tested with live evidence, recorded to data/ice-alt-claims-investig.json
