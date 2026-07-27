# ops 3950 — BOJ API verify + JPLG code hunt

**Status:** success  
**Duration:** 3.6s  
**Finished:** 2026-07-27T02:37:52+00:00  

## Log
## a. run the manual's exact example

- `02:37:49` ✅   HTTP 200, 600b
- `02:37:49`   SCHEMA HEAD: {
"STATUS":200,
"MESSAGEID":"M181000I",
"MESSAGE":"Successfully completed",
"DATE":"2026-07-27T11:37:49.520+09:00",
"PARAMETER":{
"FORMAT":"JSON",
"LANG":"EN",
"DB":"CO",
"STARTDATE":"202501",
"ENDDATE":"202504",
"STARTPOSITION":""
},
"NEXTPOSITION":null,
"RESULTSET":[
{
"SERIES_CODE":"TK99F1000601GCQ01000",
"NAME_OF_TIME_SERIES":"D.I./Business Conditions/Large Enterprises/Manufacturing/Actual result",
"UNIT":"% points",
"FREQUENCY":"QUARTERLY",
"CATEGORY":"TANKAN/Judgement Survey",
"LAST_UPDATE":20260702,
"VALUES":{
"SURVEY_DATES":[202501,202502,202503,202504],
"VALUES":[12,13,14,15]
}
}
]
}

## b. JP manual — endpoints, db table, loan mentions

- `02:37:50`   jp manual: 1035777b -> 6935 chars
- `02:37:50`   endpoints seen: ['getDataCode', 'getDataLayer']
- `02:37:50`   db codes seen: ['CO']
- `02:37:50`   fn ctx: …
API
 
API
API
API
 
https://www.stat
-
search.boj.or.jp/info/
api_notice.pdf
 
 
API
 
API
 
 
 
 
 
API
 
 
 
 
API
 
 
DB
 
API…
- `02:37:50`   fn ctx: …URL
JSON
CSV
 
URL
 
https://www.stat
-
search.boj.or.jp/api/v1/getDataCode?format=json&lang=jp&db=CO&start
Date=202401&endDate=20…
- `02:37:50`   fn ctx: …ps://www.stat
-
search.boj.or.jp/api/v1/getDataCode?format=json&lang=jp&db=CO&start
Date=202401&endDate=202504&code=TK99F1000601GC…
- `02:37:50`   fn ctx: … 
API
URL
URL
 
 
  
https://www.stat
-
search.boj.or.jp/info/api_tool.xlsx
 
 
 
4
 
 
 
 
URL
 
API
URL
API
URL
 
API
 
 
API
 
…
- `02:37:50`   fn ctx: … 
 
 
20
 
 
API
 
 
https://www.stat
-
search.boj.or.jp/api/v1/getDataLayer?format=csv&db=
 
 
 
 
 
CSV
 
 
 
 
 
 
 
 
 
 
 
 
…
- `02:37:50`   fn ctx: …ps://www.stat
-
search.boj.or.jp/api/v1/getDataLayer?format=csv&db=
 
 
 
 
 
CSV
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
API
 
 
 
…
## c. sibling endpoint probes

- `02:37:50`   getCode: HTTP Error 404: Not Found
- `02:37:50`   getSeries: HTTP Error 404: Not Found
- `02:37:50`   searchDataCode: HTTP Error 404: Not Found
- `02:37:51`   getDbList: HTTP Error 404: Not Found
- `02:37:51`   getDataList: HTTP Error 404: Not Found
- `02:37:51`   getMetaData: HTTP Error 404: Not Found
## d. loans code hunt — try MD/DL db guesses with the data endpoint

- `02:37:51`   db=MD: HTTP Error 400: Bad Request
- `02:37:51`   db=DL: HTTP Error 400: Bad Request
- `02:37:52`   db=LA: HTTP Error 400: Bad Request
- `02:37:52`   db=PF: HTTP Error 400: Bad Request
- `02:37:52`   db=MA: HTTP Error 400: Bad Request
- `02:37:52` ✅ VERIFY COMPLETE — wire JPLG next ops on confirmed code
