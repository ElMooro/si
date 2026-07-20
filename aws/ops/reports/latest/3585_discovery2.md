# ops 3585 — discovery #2: TW export orders + PBoC monthly TSF

**Status:** success  
**Duration:** 13.1s  
**Finished:** 2026-07-20T18:44:28+00:00  

## Error

```
SystemExit: 0
```

## Log
- `18:44:15` A1_datagovtw_search: {"err": "HTTP Error 404: Not Found", "top_keys": null, "hits": []}
- `18:44:15` A2_datagovtw_en: {"err": "HTTP Error 404: Not Found", "hits": []}
- `18:44:15` A3_moea_root: {"status": null, "len": null, "orders_str": false, "err": "HTTP Error 403: Forbidden"}
- `18:44:16` A4_moea_bulletin: {"status": null, "len": null, "orders_str": false, "err": "HTTP Error 403: Forbidden"}
- `18:44:17` A5_nstatdb: {"status": 200, "len": 21, "orders_str": false, "err": null}
- `18:44:18` A6_stat_eng: {"status": 200, "len": 112551, "orders_str": true, "err": null}
- `18:44:19` A7_dbn_taipei: {"err": null, "hits": []}
- `18:44:20` A8_dbn_tw_manuf: {"err": null, "hits": []}
- `18:44:24` B1_nbs_hgyd_root: {"err": "HTTP Error 403: Forbidden", "n": 0, "cats": []}
- `18:44:24` B2_nbs_financing_nodes: {"hits": []}
- `18:44:24` B3_nbs_sample_query: {"skipped": "no financing node found"}
- `18:44:26` B4_pboc_en_reach: {"status": 200, "afre_str": true, "err": null}
- `18:44:28` B5_dbn_cn_providers: {"err": null, "matches": ["NBS", "SAIS-CARI"]}
- `18:44:28` VERDICT: PROBE_COMPLETE
