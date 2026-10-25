# ops 6649 - bank engines: CloudWatch REPORT/ERROR lines + S3 state (read-only)

**Status:** success  
**Duration:** 3.2s  
**Finished:** 2026-10-25T16:56:55+00:00  

## Log
## justhodl-fmp-walker

- `16:56:52` config: memory 1536 timeout 900 ephemeral 512 modified 2026-10-25T16:16:29 state Active
- `16:56:52` reserved concurrency: None
- `16:56:53` events in last 3 h: 7, REPORT lines: 2, error-ish lines: 0
- `16:56:53` 16:34:38 REPORT RequestId: 50192199-3fed-4244-98ea-4c4f6234712c	Duration: 462472.59 ms	Billed Duration: 462894 ms	Memory Size: 1536 MB	Max Memory Used: 1536 MB	Init Duration: 421.24 ms	Status: error	Error Type: Runtime.OutOfMemory
XRAY TraceId: 1-6ade2dce-34addb8a2c0445d7289ac008	SegmentId: a6dabb2caaf202be	
- `16:56:53` 16:35:37 REPORT RequestId: 50192199-3fed-4244-98ea-4c4f6234712c	Duration: 436.95 ms	Billed Duration: 437 ms	Memory Size: 1536 MB	Max Memory Used: 92 MB	
XRAY TraceId: 1-6ade2dce-34addb8a2c0445d7289ac008	SegmentId: a429e8cad740bb82	Sampled: true
- `16:56:53` state: runs 1 phase BACKFILL key_status unknown lease_until 1792946515.6810875 updated_at None
- `16:56:53` last_run: calls None retries None puts None unchanged None seconds None http null steps null
- `16:56:53` manifest: objects None MB 0.0 datasets None/None phase None refused null
## justhodl-fmp-bank

- `16:56:53` config: memory 2048 timeout 900 ephemeral 4096 modified 2026-10-25T16:36:41 state Active
- `16:56:53` reserved concurrency: None
- `16:56:54` events in last 3 h: 7, REPORT lines: 2, error-ish lines: 0
- `16:56:54` 16:13:37 REPORT RequestId: b494573d-23ab-48da-af93-785148bad6a0	Duration: 740988.93 ms	Billed Duration: 741496 ms	Memory Size: 2048 MB	Max Memory Used: 540 MB	Init Duration: 506.69 ms	
XRAY TraceId: 1-6ade27cc-749f3b10033d2651192b725b	SegmentId: 75f77eb72c9afa60	Sampled: true
- `16:56:54` 16:26:03 REPORT RequestId: 848f290a-03b6-4d36-bf1d-08aefc12d54b	Duration: 735208.91 ms	Billed Duration: 735209 ms	Memory Size: 2048 MB	Max Memory Used: 540 MB	
XRAY TraceId: 1-6ade2abc-0d3410f87ba45f107e932e53	SegmentId: 9f29e65cd17945b9	Sampled: true
- `16:56:54` state: runs 3 phase BACKFILL key_status ok lease_until 0 updated_at 2026-10-25T16:26:03.232646+00:00
- `16:56:54` last_run: calls 190 retries 113 puts None unchanged None seconds 735.1 http null steps [{"step": "snapshot", "complete": true, "seconds": 57.5}, {"step": "dated", "complete": false, "seconds": 677.4}]
- `16:56:54` manifest: objects 145 MB 1700.8 datasets 116/None phase BACKFILL refused ["etf-holder-bulk?part=3", "financial-growth-bulk", "profile-bulk?part=4"]
## justhodl-polygon-bank

- `16:56:54` config: memory 1536 timeout 900 ephemeral 512 modified 2026-10-25T16:31:31 state Active
- `16:56:54` reserved concurrency: None
- `16:56:54` events in last 3 h: 4, REPORT lines: 1, error-ish lines: 0
- `16:56:54` 16:56:14 REPORT RequestId: b97d4a85-5a77-43d5-bf88-ee4233de2ca9	Duration: 760976.64 ms	Billed Duration: 761482 ms	Memory Size: 1536 MB	Max Memory Used: 555 MB	Init Duration: 504.52 ms	
XRAY TraceId: 1-6ade31b4-67b1b920012e038227e814b7	SegmentId: b44613a83c5cdc75	Sampled: true
- `16:56:54` state: runs 1 phase BACKFILL key_status ok lease_until 0 updated_at 2026-10-25T16:56:14.437204+00:00
- `16:56:54` last_run: calls 3032 retries 1898 puts 51 unchanged 0 seconds 760.8 http {"ok": 383, "err:AttributeError": 1, "http403": 66, "http429": 624, "empty": 60} steps [{"step": "lists", "seconds": 31.8, "calls": 83}, {"step": "dated+window", "seconds": 688.2, "calls": 2734}, {"step": "per_entity", "seconds": 40.3, "calls": 3032}]
- `16:56:54` manifest: objects 51 MB 26.6 datasets 12/31 phase BACKFILL refused [["grouped-crypto", "http429"], ["grouped-fx", "http429"], ["market-holidays", "err:AttributeError"], ["snapshot-indices", "http403"]]
- `16:56:54` universes: {"us_tickers": 13262, "us_etfs": 5796, "chart": 330, "asset_classes": 5, "snapshot_indices": 14}
## justhodl-provider-bank

- `16:56:55` config: memory 1024 timeout 900 ephemeral 512 modified 2026-10-25T16:50:35 state Active
- `16:56:55` reserved concurrency: None
- `16:56:55` ⚠ logs /aws/lambda/justhodl-provider-bank: ResourceNotFoundException An error occurred (ResourceNotFoundException) when calling the FilterLogEvents operation: The specified log group does not exist.
- `16:56:55` events in last 3 h: 0, REPORT lines: 0, error-ish lines: 0
- `16:56:55` state: runs None phase None key_status None lease_until None updated_at None
- `16:56:55` last_run: calls None retries None puts None unchanged None seconds None http null steps null
- `16:56:55` manifest: objects None MB 0.0 datasets None/None phase None refused null
- `16:56:55` ✅ ops 6649 complete (read-only)
