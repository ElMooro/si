# ops 6617 - futures-curves request records since 16:30 UTC, paginated (read-only)

**Status:** success  
**Duration:** 17.2s  
**Finished:** 2026-10-23T00:25:55+00:00  

## Data

| head_generated | head_quality | published | records | reserved_concurrency |
|---|---|---|---|---|
| 2026-10-22T16:48:39 | {"complete_datasets": 25, "products": 7, "selected_contracts": 15, "status": "descriptive"} | 2 | 4 | None |

## Log
- `00:25:39` objects under requests/: 2494; top-level records modified since 16:30 UTC: 4
- `00:25:41` {"written": "16:31:12", "request_id": "b381600e-bd3e-4fe0-a234-521163", "status": "complete", "phase": "complete", "published": false, "error": null, "failure_class": null, "started": "16:30:30", "completed": "16:31:11", "requests": 4, "quality": {"complete_datasets": 4, "products": 7, "selected_contracts": 3, "status": "descriptive"}, "generated_at": "16:31:09", "datasets": 24, "stops": {"time_budget": 20, "complete_returned_pagination": 4}}
- `00:25:46` {"written": "16:45:03", "request_id": "21545d3f-9142-4e76-8527-07cd0b", "status": "complete", "phase": "complete", "published": true, "error": null, "failure_class": null, "started": "16:35:30", "completed": "16:45:02", "requests": 46, "quality": {"complete_datasets": 42, "products": 7, "selected_contracts": 21, "status": "descriptive"}, "generated_at": "16:44:53", "datasets": 42, "stops": {"complete_returned_pagination": 42}}
- `00:25:50` {"written": "16:48:46", "request_id": "d49cfef3-da96-4609-bc7c-973743", "status": "complete", "phase": "complete", "published": true, "error": null, "failure_class": null, "started": "16:40:30", "completed": "16:48:45", "requests": 40, "quality": {"complete_datasets": 25, "products": 7, "selected_contracts": 15, "status": "descriptive"}, "generated_at": "16:48:39", "datasets": 36, "stops": {"provider_rate_limited": 11, "complete_returned_pagination": 25}}
- `00:25:54` {"written": "16:49:14", "request_id": "ops6614-20261022T164200Z", "status": "complete", "phase": "complete", "published": false, "error": null, "failure_class": null, "started": "16:42:01", "completed": "16:49:13", "requests": 35, "quality": {"complete_datasets": 18, "products": 7, "selected_contracts": 12, "status": "descriptive"}, "generated_at": "16:49:08", "datasets": 33, "stops": {"complete_returned_pagination": 18, "provider_rate_limited": 15}}
- `00:25:54` public head now: generated_at 2026-10-22T16:48:39 quality {"complete_datasets": 25, "products": 7, "selected_contracts": 15, "status": "descriptive"}
- `00:25:55` reserved concurrency None; since 16:30 per 10 min: {"Invocations": [["16:30", 2], ["16:40", 2]], "Errors": [], "Throttles": []}
- `00:25:55` ✅ read-only; nothing changed
