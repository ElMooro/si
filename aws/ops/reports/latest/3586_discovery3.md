# ops 3586 — discovery #3: PBoC AFRE recipe + TW retargets

**Status:** success  
**Duration:** 7.4s  
**Finished:** 2026-07-20T18:49:26+00:00  

## Error

```
SystemExit: 0
```

## Log
- `18:49:21` B1_stats_index: {"status": 200, "afre_links": [["http://www.pbc.gov.cn/en/3688247/3688978/3709140/index.html", "Aggregate Financing Reports"], ["http://www.pbc.gov.cn/en/3688247/3688978/3709140/index.html", "Aggregate Financing Reports"], ["http://www.pbc.gov.cn/en/3688247/3688978/3709140/index.html", "Aggregate Financing Reports"], ["http://www.pbc.gov.cn/en/3688247/3688978/3709140/5839519/index.html", "Report on Aggregate Financing to the Real Economy (Stock) (Aug..."],
- `18:49:22` B2_afre_listing: {"status": 200, "items": [["http://www.pbc.gov.cn/en/3688110/3688259/3689026/3706088/5624524/index.html", "2025"], ["http://www.pbc.gov.cn/en/3688110/3688259/3689026/3706088/5188168/index.html", "2024"], ["http://www.pbc.gov.cn/en/3688110/3688259/3689026/3706088/4756448/index.html", "2023"], ["http://www.pbc.gov.cn/en/3688110/3688259/3689026/3706088/4601725/index.html", "2022"], ["http://www.pbc.gov.cn/en/3688110/3688259/3689026/3706088/4437187/index.html"
- `18:49:24` B3_afre_item: {"status": 200, "url": "http://www.pbc.gov.cn/en/3688110/3688259/3689026/3706088/5624524/index.html", "attachments": [], "table_rows": []}
- `18:49:24` A1_dgtw_v2_singular: {"status": null, "json": false, "hits": [], "body_head": "HTTP Error 405: Method Not Allowed"}
- `18:49:24` A2_dgtw_front_list: {"status": null, "json": false, "hits": [], "body_head": "HTTP Error 405: Method Not Allowed"}
- `18:49:25` A3_dgtw_front_search: {"status": null, "json": false, "hits": [], "body_head": "HTTP Error 404: Not Found"}
- `18:49:26` A4_engstat_order_links: {"status": 200, "links": [["https://eng.stat.gov.tw/Point.aspx?sid=t.6&n=4205&sms=11713", "Value of Export Orders"], ["https://eng.stat.gov.tw/Point.aspx?sid=t.6&n=4205&sms=11713", "Value of Export Orders"], ["https://eng.stat.gov.tw/Point.aspx?sid=t.6&n=4205&sms=11713", "Value of Export Orders"]]}
- `18:49:26` VERDICT: PROBE_COMPLETE
