# ops 6900 - the dealer API's limit and DTCC's moved files (read-only), 2026-11-22T03:56+00:00

**Status:** success  
**Duration:** 326.0s  
**Finished:** 2026-11-22T04:02:04+00:00  

## Data

| dtcc_fails_last | dtcc_fails_rows | ids_alone_refused | ids_one_request_accepts | newest_week | split_size_that_works |
|---|---|---|---|---|---|
| 11/13/2026 | 250 | 0 | 35 | 2026-11-11 | 10 |

## Log
## 1. NY Fed primary dealer API

- `03:56:39` all 70 ids in one request (993 characters of URL): 422, 0 bytes | body:  | headers {'content-type': 'application/json;charset=utf-8'}
- `03:56:42`   first  1 ids, URL   56 characters: 200,   43240 bytes,    619 rows, 3.1 s
- `03:56:45`   first  2 ids, URL   69 characters: 200,   94890 bytes,   1330 rows, 3.1 s
- `03:56:49`   first  5 ids, URL  117 characters: 200,  240731 bytes,   3279 rows, 3.4 s
- `03:56:53`   first 10 ids, URL  202 characters: 200,  482271 bytes,   6466 rows, 3.7 s
- `03:56:58`   first 15 ids, URL  266 characters: 200,  740567 bytes,  10021 rows, 4.0 s
- `03:57:02`   first 20 ids, URL  319 characters: 200,  958426 bytes,  13119 rows, 4.2 s
- `03:57:07`   first 25 ids, URL  375 characters: 200, 1200728 bytes,  16582 rows, 4.7 s
- `03:57:13`   first 30 ids, URL  448 characters: 200, 1423878 bytes,  19680 rows, 4.9 s
- `03:57:18`   first 35 ids, URL  523 characters: 200, 1680557 bytes,  23143 rows, 5.3 s
- `03:57:19`   first 40 ids, URL  600 characters: 422,       0 bytes,      0 rows, 0.2 s | 
- `03:57:19`   first 50 ids, URL  742 characters: 422,       0 bytes,      0 rows, 0.1 s | 
- `03:57:20`   first 60 ids, URL  878 characters: 422,       0 bytes,      0 rows, 0.1 s | 
- `03:57:20`   first 69 ids, URL  980 characters: 422,       0 bytes,      0 rows, 0.1 s | 
- `03:57:21`   first 70 ids, URL  993 characters: 422,       0 bytes,      0 rows, 0.1 s | 
- `03:57:25`   last 10 ids: 200, 500974 bytes
- `03:57:29`   ids 31-40: 200, 491089 bytes
- `03:57:33`   ids 41-50: 200, 473747 bytes
- `03:57:37`   ids 51-60: 200, 482401 bytes
- `04:00:39`   each id alone: 70 of 70 answer 200 | not 200: []
- `04:01:08`   parts of 10 ids (7 requests): ok True | 70 series | 3373567 bytes | 29.1 s | newest weeks {'2026-11-11': 70} | ids not answered []
- `04:01:30`   parts of 14 ids (5 requests): ok True | 70 series | 3372089 bytes | 21.9 s | newest weeks {'2026-11-11': 70} | ids not answered []
- `04:01:45`   parts of 20 ids (4 requests): ok True | 70 series | 3371350 bytes | 15.1 s | newest weeks {'2026-11-11': 70} | ids not answered []
- `04:01:59`   parts of 24 ids (3 requests): ok True | 70 series | 3370611 bytes | 14.8 s | newest weeks {'2026-11-11': 70} | ids not answered []
- `04:02:01`   /api/pd/get/all/timeseries.csv: 200, 27172175 bytes, 1.4 s | "As Of Date","Time Series","Value (millions)" | "2026-11-11","PDSI5TSTS-T","9454" | "2026-11-11","PDSI5F-TR","417" | "2026-11-11","PDSI5F-TD","162" | "2026-11-11","PDSI7COT-WO","90827" | "2026-11-11","PDSI7NSP"
- `04:02:01`   /api/pd/latest/SBN2024.json: 200, 114798 bytes, 0.1 s | {"pd": { "timeseries": [ { "asofdate": "2026-11-11", "keyid": "PDWOTIPSC", "value": "-9341" }, { "asofdate": "2026-11-11", "keyid": "PDABTOTC", "value": "-565" 
- `04:02:02`   /api/pd/get/SBN2024/timeseries/PDFRN2NSP_PDPOSABS-TOT_PDPOSCS-TOT.json: 200, 26596 bytes, 1.0 s | {"pd": { "timeseries": [ { "asofdate": "2024-07-03", "keyid": "PDFRN2NSP", "value": "4310" }, { "asofdate": "2024-07-03", "keyid": "PDPOSABS-TOT", "value": "858
- `04:02:02`   /api/pd/get/SBN2024/timeseries/PDFRN2NSP_PDPOSABS-TOT_PDPOSCS-TOT_PDPOSCSBND-B: 422, 0 bytes, 0.2 s | 
## 2. DTCC's moved files, fetched without a browser

- `04:02:02` failsdata.csv, engine header: 200, 9946 bytes, type text/csv, last-modified Sat, 14 Nov 2026 02:51:57 GMT | 252 lines, 250 dated | head 'Date,Treasury Fails Amount,Agency Fails Amount' | first '11/13/2025,32399789493.28,87577085.4 ' | last '11/13/2026,22150765259.61,146276095.02 ' | tail 'TRAILER,250,,,,,'
- `04:02:03` failsdata.csv, plain urllib: 403, 17 bytes, type text/plain; charset=UTF-8, last-modified None | 1 lines, 0 dated | head 'error code: 1010' | first None | last None | tail 'error code: 1010'
- `04:02:03` failsdata.csv, browser header: 200, 9946 bytes, type text/csv, last-modified Sat, 14 Nov 2026 02:51:57 GMT | 252 lines, 250 dated | head 'Date,Treasury Fails Amount,Agency Fails Amount' | first '11/13/2025,32399789493.28,87577085.4 ' | last '11/13/2026,22150765259.61,146276095.02 ' | tail 'TRAILER,250,,,,,'
- `04:02:03` gcfindex.csv, engine header: 200, 13454 bytes, type text/csv, last-modified Fri, 20 Nov 2026 20:12:31 GMT | 252 lines, 250 dated | head 'Date,MBS Total PAR Value,MBS Weighted Average,Treasury Total PAR Value,Treasury Weighted Average' | first '11/20/2025,70824000000,  3.955,45918000000,  3.944 ' | last '11/20/2026,94972000000,  3.954,70800000000,  3.948 ' | tail 'TRAILER,250,,,,,'
- `04:02:04` gcfindex.csv, plain urllib: 403, 17 bytes, type text/plain; charset=UTF-8, last-modified None | 1 lines, 0 dated | head 'error code: 1010' | first None | last None | tail 'error code: 1010'
- `04:02:04` gcfindex.csv, browser header: 200, 13454 bytes, type text/csv, last-modified Fri, 20 Nov 2026 20:12:31 GMT | 252 lines, 250 dated | head 'Date,MBS Total PAR Value,MBS Weighted Average,Treasury Total PAR Value,Treasury Weighted Average' | first '11/20/2025,70824000000,  3.955,45918000000,  3.944 ' | last '11/20/2026,94972000000,  3.954,70800000000,  3.948 ' | tail 'TRAILER,250,,,,,'
- `04:02:04` ✅ read-only; nothing changed
