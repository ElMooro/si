# ops 6665 - census follow-up: last-run logs after the fleet fix (read-only)

**Status:** success  
**Duration:** 26.8s  
**Finished:** 2026-10-26T06:51:13+00:00  

## Log
- `06:50:47` equity-research deployed: modified 2026-10-26T06:45:28, 1024 MB, 300 s
- `06:50:48` equity-research AAPL invoke: status 200, 156 bytes, FunctionError None, head b'{"statusCode": 404, "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}, "body": "{\\"error\\": \\"No data for ticker AAPL\\"}"}'
## justhodl-equity-research

- `06:51:08` REPORT: 40 events
- `06:51:08`    03:56:27 | REPORT RequestId: e6f0dac4-7d9f-4d96-8ddd-f5ea14ed1226	Duration: 730.61 ms	Billed Duration: 731 ms	Memory Size: 1024 MB	Max Memory Used: 1023 MB	 ⏎ XRAY TraceId: 1-6adecf6a-79a7be8a639e642c7596c432	SegmentId: aa379deedb6116cd	Sampled: true
- `06:51:08`    03:57:02 | REPORT RequestId: ecb265ec-75f2-470e-91ab-587e6b183695	Duration: 235.13 ms	Billed Duration: 236 ms	Memory Size: 1024 MB	Max Memory Used: 1023 MB	 ⏎ XRAY TraceId: 1-6adecf8e-066428af246cc1611c71bfe6	SegmentId: b85d686f39930722	Sampled: true
- `06:51:08`    03:57:02 | REPORT RequestId: 66973cd6-b973-4b6e-9b24-a5ac80c1cedb	Duration: 262.77 ms	Billed Duration: 263 ms	Memory Size: 1024 MB	Max Memory Used: 1023 MB	 ⏎ XRAY TraceId: 1-6adecf8e-2611bec350ba5de67e091f73	SegmentId: c341f7b19fba53db	Sampled: true
- `06:51:08`    03:57:28 | REPORT RequestId: 8ae8597f-4f33-4e23-937b-b5a4e504f8a0	Duration: 213.84 ms	Billed Duration: 214 ms	Memory Size: 1024 MB	Max Memory Used: 1023 MB	 ⏎ XRAY TraceId: 1-6adecfa8-0d0a6b23231c1e25025e0219	SegmentId: 52fe8ffbca9142a6	Sampled: true
- `06:51:08`    03:57:28 | REPORT RequestId: 26cedbb1-2273-4e7c-a806-3f52d8c7839a	Duration: 190.18 ms	Billed Duration: 191 ms	Memory Size: 1024 MB	Max Memory Used: 1023 MB	 ⏎ XRAY TraceId: 1-6adecfa8-0e792e2219e45cba5c15c8a6	SegmentId: b3cb52df1c7b29b7	Sampled: true
- `06:51:08`    03:57:31 | REPORT RequestId: 5d6b0b59-9da6-4cd6-aaf9-50d29098ea8c	Duration: 2672.79 ms	Billed Duration: 2673 ms	Memory Size: 1024 MB	Max Memory Used: 1023 MB	Status: error	Error Type: Runtime.OutOfMemory ⏎ XRAY TraceId: 1-6adecfa8-6b367d3e0ec0b82f550f9ad2	SegmentId: 905d4a295e62ad5b	Sampled: true
- `06:51:08`    03:57:45 | REPORT RequestId: adc80629-c768-4724-ab97-1b7172c53829	Duration: 224.54 ms	Billed Duration: 225 ms	Memory Size: 1024 MB	Max Memory Used: 1023 MB	 ⏎ XRAY TraceId: 1-6adecfb9-185357b1050d634e3845eb1a	SegmentId: 5fea0d0a148c45d3	Sampled: true
- `06:51:08`    03:57:46 | REPORT RequestId: 80b8ceb9-f441-44d6-9c8f-ef1f25df762e	Duration: 790.06 ms	Billed Duration: 791 ms	Memory Size: 1024 MB	Max Memory Used: 1023 MB	 ⏎ XRAY TraceId: 1-6adecfb9-0e41d7c47536064226db5bd9	SegmentId: 3005b02ab892b0c7	Sampled: true
- `06:51:08`    03:58:04 | REPORT RequestId: 08590da5-cbdd-4f4a-8b4d-4a7e3bb27ca9	Duration: 517.06 ms	Billed Duration: 518 ms	Memory Size: 1024 MB	Max Memory Used: 1023 MB	 ⏎ XRAY TraceId: 1-6adecfcb-1fdbe49e41058daf64936d73	SegmentId: df6521bc7bed8950	Sampled: true
- `06:51:08`    03:58:05 | REPORT RequestId: 68f92a3a-cb1a-442e-b42e-3220acbf062b	Duration: 206.92 ms	Billed Duration: 207 ms	Memory Size: 1024 MB	Max Memory Used: 1023 MB	 ⏎ XRAY TraceId: 1-6adecfcd-73e599124555e6073ceb00a2	SegmentId: 2246944833674298	Sampled: true
- `06:51:08`    03:58:06 | REPORT RequestId: fc7908e1-eb43-464d-b086-2d7628ef4464	Duration: 219.61 ms	Billed Duration: 220 ms	Memory Size: 1024 MB	Max Memory Used: 1023 MB	 ⏎ XRAY TraceId: 1-6adecfce-08a126b73b6d37725ce1497a	SegmentId: b5bd2fa6cb0720f0	Sampled: true
- `06:51:08`    03:58:06 | REPORT RequestId: 4d8d5150-00ad-4c79-8748-20fc93831e24	Duration: 217.85 ms	Billed Duration: 218 ms	Memory Size: 1024 MB	Max Memory Used: 1023 MB	 ⏎ XRAY TraceId: 1-6adecfce-5be98a610cb042e46aa0fc6c	SegmentId: dccb2e0b4763838a	Sampled: true
- `06:51:11` errors: 11 events
- `06:51:11`    03:57:31 | REPORT RequestId: 5d6b0b59-9da6-4cd6-aaf9-50d29098ea8c	Duration: 2672.79 ms	Billed Duration: 2673 ms	Memory Size: 1024 MB	Max Memory Used: 1023 MB	Status: error	Error Type: Runtime.OutOfMemory ⏎ XRAY TraceId: 1-6adecfa8-6b367d3e0ec0b82f550f9ad2	SegmentId: 905d4a295e62ad5b	Sampled: true
- `06:51:11`    04:01:51 | REPORT RequestId: 3b6e9d74-2eed-4910-90ef-f5d1f49c6211	Duration: 2640.29 ms	Billed Duration: 2641 ms	Memory Size: 1024 MB	Max Memory Used: 1023 MB	Status: error	Error Type: Runtime.OutOfMemory ⏎ XRAY TraceId: 1-6aded0ac-7913281c1ea53ae75d326b12	SegmentId: 65a1637e94935459	Sampled: true
- `06:51:11`    04:13:45 | REPORT RequestId: 38298107-14bc-447d-98b8-9c0b5e30f983	Duration: 3046.68 ms	Billed Duration: 3047 ms	Memory Size: 1024 MB	Max Memory Used: 1023 MB	Status: error	Error Type: Runtime.OutOfMemory ⏎ XRAY TraceId: 1-6aded376-6af384e33acd4e6e260c0ffd	SegmentId: 30478a7533b4de89	Sampled: true
- `06:51:11`    04:24:24 | REPORT RequestId: 8829a9dd-f294-4eab-b0e6-af7ceb6ac0b2	Duration: 3340.52 ms	Billed Duration: 3341 ms	Memory Size: 1024 MB	Max Memory Used: 1023 MB	Status: error	Error Type: Runtime.OutOfMemory ⏎ XRAY TraceId: 1-6aded5f5-0bede0141c86e66a0d63d834	SegmentId: fea233243c3dcbaa	Sampled: true
- `06:51:11`    04:28:52 | REPORT RequestId: d4d52c7c-6813-458e-bfd9-4d2376de8e02	Duration: 1792.14 ms	Billed Duration: 1793 ms	Memory Size: 1024 MB	Max Memory Used: 1023 MB	Status: error	Error Type: Runtime.OutOfMemory ⏎ XRAY TraceId: 1-6aded702-5730f5bb3c025fe00dbc6cdf	SegmentId: 99edb77f39464040	Sampled: true
- `06:51:11`    04:41:19 | REPORT RequestId: 29984910-67ec-4594-a80b-777bf4ac4eff	Duration: 1700.90 ms	Billed Duration: 1701 ms	Memory Size: 1024 MB	Max Memory Used: 1024 MB	Status: error	Error Type: Runtime.OutOfMemory ⏎ XRAY TraceId: 1-6aded9ee-40525c172158594a4aed4d65	SegmentId: 28654ba1f58c65a8	Sampled: true
- `06:51:11`    04:57:05 | REPORT RequestId: 10b413e1-d922-4e85-b106-ac360729b1d0	Duration: 1853.04 ms	Billed Duration: 1854 ms	Memory Size: 1024 MB	Max Memory Used: 1024 MB	Status: error	Error Type: Runtime.OutOfMemory ⏎ XRAY TraceId: 1-6adedd9f-4c4bc8f70be39519377e2928	SegmentId: d3d9d669ac3e000b	Sampled: true
- `06:51:11`    05:09:55 | REPORT RequestId: 828dc7ba-bc2c-4b52-8290-468e7ff8cd74	Duration: 28001.50 ms	Billed Duration: 28002 ms	Memory Size: 1024 MB	Max Memory Used: 1023 MB	Status: error	Error Type: Runtime.OutOfMemory ⏎ XRAY TraceId: 1-6adee087-2677c4ba2b7c908440eeb6cf	SegmentId: 40033caddb19689d	Sampled: true
- `06:51:11`    05:17:44 | REPORT RequestId: 8fce9cb0-0c5c-4c14-b475-010fc2685c54	Duration: 1952.40 ms	Billed Duration: 1953 ms	Memory Size: 1024 MB	Max Memory Used: 1024 MB	Status: error	Error Type: Runtime.OutOfMemory ⏎ XRAY TraceId: 1-6adee276-48a736067b6fab1b56eb8ff9	SegmentId: 1777d2b116462f8e	Sampled: true
- `06:51:11`    05:34:38 | REPORT RequestId: d18461ce-5c9a-4709-84f4-fdc675eb681d	Duration: 32796.93 ms	Billed Duration: 32797 ms	Memory Size: 1024 MB	Max Memory Used: 1024 MB	Status: error	Error Type: Runtime.OutOfMemory ⏎ XRAY TraceId: 1-6adee64d-24e4d4984548744077baf9fd	SegmentId: 458f3ad49de90c97	Sampled: true
- `06:51:11`    05:42:21 | REPORT RequestId: cee91319-1ef7-47cd-8de1-f5cb41451ec5	Duration: 3781.30 ms	Billed Duration: 3782 ms	Memory Size: 1024 MB	Max Memory Used: 1024 MB	Status: error	Error Type: Runtime.OutOfMemory ⏎ XRAY TraceId: 1-6adee839-286503a20f6842791f2d6f4c	SegmentId: a181139c2f4060dc	Sampled: true
- `06:51:12` fusion lines: 7 events
- `06:51:12`    03:57:30 | [fmp-bankfirst] bank stepped aside for the rest of this invocation: table cache thrashing (working set exceeds this container's text budget) (attempts=24 bank_time=3.1s)
- `06:51:12`    04:01:50 | [fmp-bankfirst] bank stepped aside for the rest of this invocation: table cache thrashing (working set exceeds this container's text budget) (attempts=22 bank_time=2.5s)
- `06:51:12`    04:13:44 | [fmp-bankfirst] bank stepped aside for the rest of this invocation: table cache thrashing (working set exceeds this container's text budget) (attempts=22 bank_time=2.6s)
- `06:51:12`    04:24:24 | [fmp-bankfirst] bank stepped aside for the rest of this invocation: table cache thrashing (working set exceeds this container's text budget) (attempts=30 bank_time=3.7s)
- `06:51:12`    05:17:43 | [fmp-bankfirst] bank stepped aside for the rest of this invocation: table cache thrashing (working set exceeds this container's text budget) (attempts=25 bank_time=2.2s)
- `06:51:12`    05:42:19 | [fmp-bankfirst] bank stepped aside for the rest of this invocation: table cache thrashing (working set exceeds this container's text budget) (attempts=25 bank_time=2.4s)
- `06:51:12`    05:42:20 | [fmp-bankfirst] first hit: hits=4 stale=0 misses=46 unshaped=16 passthrough=59 bypassed=2563 errors=0 bank_time=4.4s off=table cache thrashing (working set exceeds this container's text budget)
## justhodl-gap-metrics

- `06:51:12` REPORT: 0 events
- `06:51:12` errors: 0 events
- `06:51:12` fusion lines: 0 events
## justhodl-event-flow-monitor

- `06:51:12` REPORT: 3 events
- `06:51:12`    05:02:41 | REPORT RequestId: b4a4508b-229b-4e91-a86b-8814760beba2	Duration: 120000.00 ms	Billed Duration: 120435 ms	Memory Size: 512 MB	Max Memory Used: 512 MB	Init Duration: 434.99 ms	Status: timeout ⏎ XRAY TraceId: 1-6adede79-423beaf20b3bb96015f5125f	SegmentId: 124c93553771125c	Sampled: true
- `06:51:12`    05:05:47 | REPORT RequestId: b4a4508b-229b-4e91-a86b-8814760beba2	Duration: 120000.00 ms	Billed Duration: 120000 ms	Memory Size: 512 MB	Max Memory Used: 511 MB	Status: timeout ⏎ XRAY TraceId: 1-6adede79-423beaf20b3bb96015f5125f	SegmentId: 77c7c92821d1de7e	Sampled: true
- `06:51:12`    05:09:51 | REPORT RequestId: b4a4508b-229b-4e91-a86b-8814760beba2	Duration: 120000.00 ms	Billed Duration: 120000 ms	Memory Size: 512 MB	Max Memory Used: 511 MB	Status: timeout ⏎ XRAY TraceId: 1-6adede79-423beaf20b3bb96015f5125f	SegmentId: d29585e57de01f02	Sampled: true
- `06:51:13` errors: 0 events
- `06:51:13` fusion lines: 0 events
## justhodl-eva-spread

- `06:51:13` REPORT: 1 events
- `06:51:13`    06:36:12 | REPORT RequestId: 83c68f0d-9b08-4f0b-9311-7780013e4986	Duration: 11823.18 ms	Billed Duration: 12350 ms	Memory Size: 1024 MB	Max Memory Used: 444 MB	Init Duration: 526.53 ms	 ⏎ XRAY TraceId: 1-6adef4cf-28a14d544c58221a660f5d07	SegmentId: 13d5ac7e7746f3d6	Sampled: true
- `06:51:13` errors: 0 events
- `06:51:13` fusion lines: 0 events
- `06:51:13` ✅ ops 6665 complete (read-only)
