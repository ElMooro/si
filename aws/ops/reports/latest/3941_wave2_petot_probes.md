# ops 3941 — wave 2: PETOT wire + BOJ-API/MOF/BoE/SNB/IMF discovery

**Status:** success  
**Duration:** 507.2s  
**Finished:** 2026-07-27T00:33:04+00:00  

## Data

| coverage_pct | n_live | statuses |
|---|---|---|
| 80.6 | 452 | {'META': 1, 'LIVE': 452, 'DISCONTINUED': 2, 'NO_FREE_SOURCE': 106} |

## Log
## 1. BOJ new official API (notice 2026-02-18)

- `00:24:38`   notice: HTTP 200, 36624b
- `00:24:38`   api-ish link: https://fonts.googleapis.com
- `00:24:38`   api-ish link: https://fonts.googleapis.com/css2?family=Noto+Sans+JP:wght@100;300;400;500;700;900&display=swap
- `00:24:38`   api-ish link: https://fonts.googleapis.com/css2?family=Noto+Serif+JP:wght@600&display=swap
- `00:24:38`   api-ish link: https://www.stat-search.boj.or.jp/info/api_manual_en.pdf
- `00:24:38`   api-ish link: https://www.stat-search.boj.or.jp/info/api_notice_en.pdf
- `00:24:38`   probe https://fonts.googleapis.com -> HTTP Error 404: Not Found
- `00:24:38`   probe https://fonts.googleapis.com/css2?family=Noto+Sans+JP:wght@100;300;400;500;700;9 -> HTTP 200, 1398b, head b"@font-face {\n  font-family: 'Noto Sans JP';\n  font-style: normal;\n  font-weight: 100;\n  font-display"
- `00:24:38`   probe https://fonts.googleapis.com/css2?family=Noto+Serif+JP:wght@600&display=swap -> HTTP 200, 236b, head b"@font-face {\n  font-family: 'Noto Serif JP';\n  font-style: normal;\n  font-weight: 600;\n  font-displa"
## 2. MOF JGB — current CSV link from the index

- `00:24:39`   index: HTTP 200, 16323b
- `00:24:39`   csv link: /english/policy/jgbs/reference/interest_rate/jgbcme.csv
- `00:24:39`   csv link: historical/jgbcme_all.csv
- `00:24:39`   probe https://www.mof.go.jp/english/policy/jgbs/reference/interest_rate/jgbcme.csv -> HTTP 200, 1828b :: Interest Rate (July 2026),,,,,,,,,,,,,,,(Unit : %)
Date,1Y,2Y,3Y,4Y,5Y,6Y,7Y,8Y,9Y,10Y,15Y,20Y,25Y,30Y,40Y
2026/7/1,1.
## 3. BoE IADB param iteration (IUDSOIA)

- `00:24:40`   [TN+dates] HTTP 200, 41163b, csv=False :: <!DOCTYPE html>  <html lang="en" class="no-js">      <head>      <meta charset="utf-8">      <meta http-equiv=
- `00:24:40`   [TT+dates] HTTP 200, 41163b, csv=False :: <!DOCTYPE html>  <html lang="en" class="no-js">      <head>      <meta charset="utf-8">      <meta http-equiv=
- `00:24:41`   [iadb-fromshowcolumns] HTTP 200, 333b, csv=True :: DATE,IUDSOIA  01 Jul 2026,3.7309  02 Jul 2026,3.7305  03 Jul 2026,3.7302  06 Jul 2026,3.7297  07 Jul 2026,3.72
## 4. SNB cube list

- `00:24:42`   https://data.snb.ch/api/cube -> HTTP 200, 3587b, head b'<!doctype html>\n<html lang="de">\n<head>\n  <meta charset="utf-8">\n  <title>Datenportal der Schweizerischen Nationalbank</title>\n  <base href="/">\n  <me'
- `00:24:42`   https://data.snb.ch/en/api -> HTTP 200, 3467b, head b'<!doctype html>\n<html lang="en">\n<head>\n  <meta charset="utf-8">\n  <title>SNB data portal</title>\n  <base href="/">\n  <meta name="appVersion" content='
## 5. IMF new-API candidates

- `00:24:43`   https://api.imf.org/external/sdmx/2.1/dataflow -> HTTP 200, 444501b, head b'<?xml version=\'1.0\' encoding=\'UTF-8\'?>\n<mes:Structure xmlns:mes="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/messa'
- `00:24:43`   https://data.imf.org/api/dataflow -> HTTP Error 403: Forbidden
## deploy gate — v3.3 + PETOT

- `00:24:44` ✅   settled attempt 1 (alias in zip)
- `00:33:04` ✅   refreshed ~480s
- `00:33:04`   PETOT: LIVE value=182.731 src=bcrp-peru asof=bcrp:May.2026
- `00:33:04` ✅   v3.3 settled + PETOT alias in artifact
- `00:33:04` ✅   force run wrote
- `00:33:04` ✅   PETOT LIVE via bcrp-peru
- `00:33:04` ✅   n_live >= 452
- `00:33:04` ✅   zero bare UNRESOLVED
- `00:33:04` ✅ PASS_ALL — 452 LIVE (80.6%), PETOT 182.731 from BCRP (bcrp:May.2026)
