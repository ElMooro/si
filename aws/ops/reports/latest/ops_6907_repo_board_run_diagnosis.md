# ops 6907 - the board's 06:10 UTC run, diagnosed (read-only), 2026-11-22T06:21+00:00

**Status:** success  
**Duration:** 0.7s  
**Finished:** 2026-11-22T06:21:11+00:00  

## Log
## 1. The board

- `06:21:10` engine 3.1 as of 2026-11-22T06:10:18+00:00 | counts {"series": 766, "skipped": 7, "history_files": 766, "history_written": 100, "history_unchanged": 666}
- `06:21:10` groups: DTCC daily settlement fails (FICC) 2, DVP bilateral cleared 36, Derived stress indicators 16, Dollar (FRED trade-weighted) 9, European sovereign collateral (ECB MMSR) 4, European sovereign yields & spreads 10, FICC sponsored repo 2, GCF 44, ICE BofA option-adjusted spreads (FRED) 192, Money market funds (OFR) 42, Primary dealer FAILS 18, Primary dealer financing 88, Primary dealer securities borrowed & lent 85, Rate percentiles (NY Fed) 12, Reference rates & volumes 31, SFTR EU/UK repo (ICMA weekly) 12, Standing Repo Facility 2, Tri-party (TRI legacy) 40, Tri-party (TRIV1, ex-Fed) 40, Tri-party haircuts (NY Fed monthly) 81
- `06:21:10` diag.bbk_de: "warm:7436"
- `06:21:10` diag.ecb_yc: "warm:5678"
- `06:21:10` diag.te_key_present: true
- `06:21:10` diag.te: {"germany": "HTTPError:HTTP Error 401: Unauthorized", "italy": "HTTPError:HTTP Error 401: Unauthorized", "france": "HTTPError:HTTP Error 401: Unauthorized", "spain": "HTTPError:HTTP Error 401: Unauthorized"}
- `06:21:10` diag.sftr: {"stage": "page", "links": 2, "files_new": 2}
- `06:21:10` diag.dtcc: {"url": "https://cms-prod.dtcc.com/data/failsdata.csv", "live_rows": 250, "treasury": 317, "agency": 317}
- `06:21:10` diag.own_sources: {"survey_rate": {"status": "written", "n": 4998, "first": "1998-02-20", "last": "2018-02-28"}, "haircuts": {"status": "written", "asked": true, "source_newest": "2026-10-09", "bank_newest_before": "2026-07-09", "written": 73, "unchanged": 0, "new_series": [], "observations_added": 219, "observations_revised": 0, "revised_sample": [], "dry_run": false, "old_workbook_read": false, "series": 81, "bank_newest_after": "2026-10-09"}}
## 2. The run's log

- `06:21:11` 9 log events in the last 75 minutes
- `06:21:11`   06:10:23 [repo] dollar refresh TWEXEMEGSMTH HTTPError
- `06:21:11`   06:10:49 [repo] frepo discovery failed: HTTPError
- `06:21:11`   06:10:57 [repo] dtcc: skipped
- `06:21:11`   06:11:06 [repo] own sources: {"survey_rate": {"status": "written", "n": 4998, "first": "1998-02-20", "last": "2018-02-28"}, "haircuts": {"status": "written", "asked": true, "source_newest": "2026-10-09", "bank_newest_before": "2026-07-09", "written": 73, "unchanged": 0, "new_series": [], "observations_added": 219, "observations_revised": 0, "revised_sample": [], "dry_run": false, "old_workbook_read": false, "series": 81, "ban
- `06:21:11`   06:12:22 [repo] {"ok": true, "v": "3.1", "ice": 192, "frepo": 0, "series": 766, "skipped": 7, "history_written": 100, "history_unchanged": 666, "barometer": 52.2, "label": "NORMAL", "secs": 125.0}
- `06:21:11`   06:12:23 REPORT RequestId: 3c444864-0ba5-4609-8514-a699253928fc	Duration: 125115.50 ms	Billed Duration: 125662 ms	Memory Size: 1536 MB	Max Memory Used: 467 MB	Init Duration: 546.37 ms	
XRAY TraceId: 1-6b028749-2b3fbf4d0856467832cad8c8	SegmentId: 8faa7b09f6f62d64	Sampled: true
- `06:21:11` ✅ read-only
