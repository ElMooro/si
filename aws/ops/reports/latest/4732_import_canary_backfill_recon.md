# ops 4732 -- import-canary backfill recon (read-only)

**Status:** success  
**Duration:** 5.7s  
**Finished:** 2026-08-16T04:16:04+00:00  

## Data

| check | value |
|---|---|
| hist_key_size_bytes | 1538 |
| n_lines_tracked | 33 |
| deepest_line_months | 2 |
| shallowest_line_months | 2 |
| census_range_query_works | True |
| census_data_exists_1995-01 | True |
| census_data_exists_2002-01 | True |
| census_data_exists_2008-01 | True |
| census_data_exists_2013-01 | True |

## Log
## 1. Real current depth of the self-building ledger (data/import-canary-history.json)

- `04:15:59` deepest line: HS6:854231 -- 2 months, 2026-05 -> 2026-06
- `04:15:59` shallowest line: N:3311 -- 2 months, 2026-05 -> 2026-06
- `04:15:59`   HS6:854231: 2 months banked, 2026-05 -> 2026-06
- `04:15:59`   HS6:854232: 2 months banked, 2026-05 -> 2026-06
- `04:15:59`   HS6:854233: 2 months banked, 2026-05 -> 2026-06
- `04:15:59`   HS4:8542: 2 months banked, 2026-05 -> 2026-06
- `04:15:59`   HS4:8486: 2 months banked, 2026-05 -> 2026-06
- `04:15:59`   HS6:848620: 2 months banked, 2026-05 -> 2026-06
- `04:15:59`   HS4:8471: 2 months banked, 2026-05 -> 2026-06
- `04:15:59`   HS4:8473: 2 months banked, 2026-05 -> 2026-06
## 2. Census API key -- is one configured, or running keyless?

- `04:15:59` ⚠ no /justhodl/census_api_key in SSM (ParameterNotFound) -- live lambda's CENSUS_API_KEY env default is empty, so it's running keyless right now (Census allows limited keyless access)
## 3. Does Census's timeseries API accept a real date RANGE (one call vs. hundreds)?

- `04:16:02` range probe (2013-01..2013-06, HS6 854231): {"ok": true, "status": 200, "elapsed_ms": 2777}
- `04:16:02`   body sample: <html style="font-size: 14px;">

<head>
    <title>Missing Key</title>
    <link rel="icon" type="image/x-icon" href="favicon.ico">
    <link rel="stylesheet" type="text/css" href="assets/styles.css">
    <script type="text/javascript" src="assets/jquery-1.4.4.min.js"></script>
    <script type="text/javascript">
        $(document).ready(function () {
            function getCookie(name) {
      
## 4. How far back does real (non-null) monthly data actually exist for this line?

- `04:16:03`   1995-01: {"ok": true, "status": 200, "elapsed_ms": 600} body: <html style="font-size: 14px;">

<head>
    <title>Missing Key</title>
    <link rel="icon" type="image/x-icon" href="favicon.ico">
    <link rel="stylesheet" type="text/css" href="assets/styles.css">
- `04:16:03`   2002-01: {"ok": true, "status": 200, "elapsed_ms": 573} body: <html style="font-size: 14px;">

<head>
    <title>Missing Key</title>
    <link rel="icon" type="image/x-icon" href="favicon.ico">
    <link rel="stylesheet" type="text/css" href="assets/styles.css">
- `04:16:04`   2008-01: {"ok": true, "status": 200, "elapsed_ms": 585} body: <html style="font-size: 14px;">

<head>
    <title>Missing Key</title>
    <link rel="icon" type="image/x-icon" href="favicon.ico">
    <link rel="stylesheet" type="text/css" href="assets/styles.css">
- `04:16:04`   2013-01: {"ok": true, "status": 200, "elapsed_ms": 601} body: <html style="font-size: 14px;">

<head>
    <title>Missing Key</title>
    <link rel="icon" type="image/x-icon" href="favicon.ico">
    <link rel="stylesheet" type="text/css" href="assets/styles.css">
## Summary

- `04:16:04` Read-only recon only. If the range query in section 3 works, the backfill script becomes one call per line for the whole available span instead of hundreds of serial monthly calls. Section 1's real depths replace the wrong 'zero history' conclusion from checking the wrong file in ops 4731.
