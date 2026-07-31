# ops 4155 — harvest pipeline forensics

**Status:** success  
**Duration:** 0.7s  
**Finished:** 2026-07-31T14:34:32+00:00  

## Data

| delta_vs_2019 | doc_generated_at | events_14h | first_event | last_event | sources_entries |
|---|---|---|---|---|---|
| 25 | 2026-07-31T14:27:16.757816+00:00 |  |  |  | 2044 |
|  |  | 250 |  |  |  |
|  |  |  | 2026-07-31 04:27 | 2026-07-31 04:28 |  |

## Log
## A. S3 truth — every data/tv* object, LastModified

- `14:34:32`   data/tv-watchlists.json              495,412B  age=   0.1h
- `14:34:32`   data/tv-sources.json                 287,274B  age=   0.1h
- `14:34:32`   data/tv-workbench.json             8,636,125B  age=   1.7h
- `14:34:32`   data/tv-watchlist-state.json         204,257B  age=   8.1h
- `14:34:32`   data/tv-watchlist-tracker.json         6,796B  age=   8.1h
- `14:34:32`   data/tv-crawler-status.json              336B  age=   8.6h
- `14:34:32`   data/tv-fleet-map.json                 3,539B  age= 113.2h
- `14:34:32`   data/tv-ingest-config.json               213B  age= 450.0h
- `14:34:32`   data/tv-bookmarklet.json               6,964B  age= 586.0h
- `14:34:32`   data/tv-pipeline-status.json             481B  age= 587.1h
## B. ingest lambda — overnight log truth

- `14:34:32`   message kinds: {"START": 63, "END": 63, "REPORT": 62, "{\"ok\":": 57, "[ingest]": 3, "INIT_START": 2}
- `14:34:32`   [04:28] REPORT RequestId: 99dc5f4f-73f8-4c93-b8eb-f07f45b1c861	Duration: 578.19 ms	Billed Duration: 579 ms	Memory Size: 1024 MB	Max Memory
- `14:34:32`   [04:28] START RequestId: 485127e4-f35f-4c6c-9113-9031a71e1d63 Version: $LATEST
- `14:34:32`   [04:28] START RequestId: c296457e-42e9-49a8-a969-01ab523a0a61 Version: $LATEST
- `14:34:32`   [04:28] {"ok": true, "brain_error_sample": null, "received": 40, "normalized": 40, "rejected": 0, "brain_upserted": 40, "brain_failed": 0,
- `14:34:32`   [04:28] END RequestId: 485127e4-f35f-4c6c-9113-9031a71e1d63
- `14:34:32`   [04:28] REPORT RequestId: 485127e4-f35f-4c6c-9113-9031a71e1d63	Duration: 441.58 ms	Billed Duration: 442 ms	Memory Size: 1024 MB	Max Memory
- `14:34:32`   [04:28] {"ok": true, "brain_error_sample": null, "received": 40, "normalized": 40, "rejected": 0, "brain_upserted": 40, "brain_failed": 0,
- `14:34:32`   [04:28] END RequestId: c296457e-42e9-49a8-a969-01ab523a0a61
- `14:34:32` ✅ FORENSICS — sources 2044, see stage ages above
