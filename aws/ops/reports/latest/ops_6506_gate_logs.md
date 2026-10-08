# ops 6506 - contract-gate logs (read-only)

- config: {"Timeout": 600, "MemorySize": 1024, "LastModified": "2026-10-08T17:14:57.000+0000", "CodeSha256": "W9Zpbpub7PIS9cjys2Eqd9tOW4dFptFlPRmtuv/DDac="}
- events: 138

```
16:36:11 INIT_START Runtime Version: python:3.12.mainlinev2.v43	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:36aeb19f3326e129d9f096dea0a3c6a9ca8dce2610e62d2b2060747602d863ae
16:36:11 START RequestId: 8d5958b5-559f-43f3-b278-b71dd7dd0e01 Version: $LATEST
16:36:11 REPORT RequestId: 8d5958b5-559f-43f3-b278-b71dd7dd0e01	Duration: 2.46 ms	Billed Duration: 417 ms	Memory Size: 1024 MB	Max Memory Used: 95 MB	Init Duration: 413.61 ms	
XRAY TraceId: 1-6ac7c67a-1c0aa43118a5d54708c0da61	SegmentId: 2d54219895e821dd	Sampled: true
16:36:12 START RequestId: 6556e9bb-dd89-49e6-9dc4-d8344fdc37cb Version: $LATEST
16:36:12 [contracts] cadence map: 1826 artifacts resolvable
16:36:17 [contracts] rowcount history: 69 days, 720 artifacts; previous contracts: 872; producers: 1998
16:37:44 [contracts] learned 1130 contracts (786 cadence-bounded, 250 suspects, 94 regressed, 41 orphaned)
16:37:44 [contracts] SUSPECT data/13f-clone-alpha.json age=80.1h bound=78.0h
16:37:44 [contracts] SUSPECT data/13f-cusip-map.json age=449.5h bound=12.0h
16:37:44 [contracts] SUSPECT data/13f-price-anchors.json age=449.5h bound=12.0h
16:37:44 [contracts] SUSPECT data/_freshness-manifest.json age=2891.9h bound=12.0h
16:37:44 [contracts] SUSPECT data/aaii-research-verification.json age=434.2h bound=72.0h
16:37:44 [contracts] SUSPECT data/activist-13d.json age=1410.6h bound=72.0h
16:37:44 [contracts] SUSPECT data/activist-filings-state.json age=269.6h bound=72.0h
16:37:44 [contracts] SUSPECT data/activity-research-verification.json age=422.3h bound=72.0h
16:37:44 [contracts] SUSPECT data/ai-council.json age=1554.7h bound=72.0h
16:37:44 [contracts] SUSPECT data/alert-history.json age=696.0h bound=72.0h
16:37:44 [contracts] SUSPECT data/alfred-vintages.json age=619.0h bound=72.0h
16:37:44 [contracts] SUSPECT data/alpha-compass-history.json age=465.8h bound=12.0h
16:37:44 [contracts] SUSPECT data/alpha-research-verification.json age=462.8h bound=72.0h
16:37:44 [contracts] SUSPECT data/apac-leadlag-series.json age=54.2h bound=54.0h
16:37:44 [contracts] SUSPECT data/apac-leadlag.json age=54.2h bound=54.0h
16:37:44 REPORT RequestId: 6556e9bb-dd89-49e6-9dc4-d8344fdc37cb	Duration: 92394.25 ms	Billed Duration: 92395 ms	Memory Size: 1024 MB	Max Memory Used: 539 MB	
XRAY TraceId: 1-6ac7c67c-4fb697db28b006524e491c37	SegmentId: 4a861782660d0257	Sampled: true
16:37:44 START RequestId: 316dc9b3-9913-4d30-8bde-a272131a7a63 Version: $LATEST
16:39:01 [contracts] 1111 contracts, 242 violations (sev1=5) {'FIELD_VALUE': 5, 'STALE': 237}
16:39:01 [contracts] S1 FIELD_VALUE    data/ka-analysis.json — source contract identity/status mismatch: llm_status
16:39:01 [contracts] S1 FIELD_VALUE    data/khalid-analysis.json — source contract identity/status mismatch: llm_status
16:39:01 [contracts] S1 FIELD_VALUE    data/momentum-themes.json — source contract identity/status mismatch: schema_version
16:39:01 [contracts] S1 FIELD_VALUE    data/options-flow-scanner.json — source contract identity/status mismatch: schema_version
16:39:01 [contracts] S1 FIELD_VALUE    data/options-flow-scanner.json — source contract identity/status mismatch: method
16:39:01 [contracts] S2 STALE          calibration/latest.json — 617h old, bound is 54h
16:39:01 [contracts] S2 STALE          data/13f-clone-alpha.json — 80h old, bound is 78h
16:39:01 [contracts] S2 STALE          data/13f-cusip-map.json — 450h old, bound is 78h
16:39:01 [contracts] S2 STALE          data/13f-price-anchors.json — 449h old, bound is 54h
16:39:01 [contracts] S2 STALE          data/_freshness-manifest.json — 2892h old, bound is 12h
16:39:01 [contracts] S2 STALE          data/aaii-research-verification.json — 434h old, bound is 72h
16:39:01 [contracts] S2 STALE          data/activist-13d.json — 1411h old, bound is 36h
16:39:01 [contracts] S2 STALE          data/activist-filings-state.json — 270h old, bound is 36h
16:39:01 [contracts] S2 STALE          data/activity-research-verification.json — 422h old, bound is 72h
16:39:01 [contracts] S2 STALE          data/ai-council.json — 1555h old, bound is 72h
16:39:01 [contracts] S2 STALE          data/alert-history.json — 696h old, bound is 12h
16:39:01 [contracts] S2 STALE          data/alfred-vintages.json — 619h old, bound is 72h
16:39:01 [contracts] S2 STALE          data/alpha-compass-history.json — 466h old, bound is 12h
16:39:01 [contracts] S2 STALE          data/alpha-research-verification.json — 463h old, bound is 72h
16:39:01 [contracts] S2 STALE          data/apac-leadlag-series.json — 54h old, bound is 54h
16:39:01 REPORT RequestId: 316dc9b3-9913-4d30-8bde-a272131a7a63	Duration: 77089.90 ms	Billed Duration: 77090 ms	Memory Size: 1024 MB	Max Memory Used: 548 MB	
XRAY TraceId: 1-6ac7c6d8-665669a90cb8c9756627b32a	SegmentId: 9d5cf5648a07452f	Sampled: true
17:39:33 INIT_START Runtime Version: python:3.12.mainlinev2.v43	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:36aeb19f3326e129d9f096dea0a3c6a9ca8dce2610e62d2b2060747602d863ae
17:39:33 START RequestId: 5009fcc8-5032-41fd-9a62-fc133fe04d67 Version: $LATEST
17:39:33 REPORT RequestId: 5009fcc8-5032-41fd-9a62-fc133fe04d67	Duration: 2.37 ms	Billed Duration: 427 ms	Memory Size: 1024 MB	Max Memory Used: 95 MB	Init Duration: 423.98 ms	
XRAY TraceId: 1-6ac7d554-62c6be2f6c1bd8032dac9acf	SegmentId: 854892f93105b5ae	Sampled: true
17:39:34 START RequestId: 276ff3b9-1609-4ee7-a288-db3752630767 Version: $LATEST
17:39:45 REPORT RequestId: 276ff3b9-1609-4ee7-a288-db3752630767	Duration: 10935.70 ms	Billed Duration: 10936 ms	Memory Size: 1024 MB	Max Memory Used: 1024 MB	Status: error	Error Type: Runtime.OutOfMemory
XRAY TraceId: 1-6ac7d556-032906e375c910cf42cb849e	SegmentId: 90cbc97f4ee64a7f	Sampled: true
17:39:45 START RequestId: f1daf858-4179-4879-a109-372348fc91a2 Version: $LATEST
17:41:14 [contracts] 1111 contracts, 240 violations (sev1=3) {'FIELD_VALUE': 2, 'MISSING_KEYS': 1, 'STALE': 237}
17:41:14 [contracts] S1 FIELD_VALUE    data/ka-analysis.json — source contract identity/status mismatch: llm_status
17:41:14 [contracts] S1 FIELD_VALUE    data/khalid-analysis.json — source contract identity/status mismatch: llm_status
17:41:14 [contracts] S1 MISSING_KEYS   data/bea-economic.json — absent top-level keys: corporate_profits, gdp_contributions_pp, gdp_gdi
17:41:14 [contracts] S2 STALE          calibration/latest.json — 618h old, bound is 54h
17:41:14 [contracts] S2 STALE          data/13f-clone-alpha.json — 81h old, bound is 78h
17:41:14 [contracts] S2 STALE          data/13f-cusip-map.json — 451h old, bound is 78h
17:41:14 [contracts] S2 STALE          data/13f-price-anchors.json — 451h old, bound is 54h
17:41:14 [contracts] S2 STALE          data/_freshness-manifest.json — 2893h old, bound is 12h
17:41:14 [contracts] S2 STALE          data/aaii-research-verification.json — 435h old, bound is 72h
17:41:14 [contracts] S2 STALE          data/activist-13d.json — 1412h old, bound is 36h
17:41:14 [contracts] S2 STALE          data/activist-filings-state.json — 271h old, bound is 36h
17:41:14 [contracts] S2 STALE          data/activity-research-verification.json — 423h old, bound is 72h
17:41:14 [contracts] S2 STALE          data/ai-council.json — 1556h old, bound is 72h
17:41:14 [contracts] S2 STALE          data/alert-history.json — 697h old, bound is 12h
17:41:14 [contracts] S2 STALE          data/alfred-vintages.json — 620h old, bound is 72h
17:41:14 [contracts] S2 STALE          data/alpha-compass-history.json — 467h old, bound is 12h
17:41:14 [contracts] S2 STALE          data/alpha-research-verification.json — 464h old, bound is 72h
17:41:14 [contracts] S2 STALE          data/apac-leadlag-series.json — 55h old, bound is 54h
17:41:14 [contracts] S2 STALE          data/apac-leadlag.json — 55h old, bound is 54h
17:41:14 [contracts] S2 STALE          data/asymmetric-scorer.json — 1556h old, bound is 54h
17:41:14 REPORT RequestId: f1daf858-4179-4879-a109-372348fc91a2	Duration: 88944.96 ms	Billed Duration: 88945 ms	Memory Size: 1024 MB	Max Memory Used: 532 MB	
XRAY TraceId: 1-6ac7d561-0edfbbd4045df83612f1477d	SegmentId: 038754922e919fee	Sampled: true
17:43:26 START RequestId: 4591e223-9f68-457f-b518-f6a03fab0046 Version: $LATEST
17:43:26 REPORT RequestId: 4591e223-9f68-457f-b518-f6a03fab0046	Duration: 3.86 ms	Billed Duration: 4 ms	Memory Size: 1024 MB	Max Memory Used: 532 MB	
XRAY TraceId: 1-6ac7d63e-397ed61b1542f1a3653bcd46	SegmentId: b7d5257b6249ab8a	Sampled: true
17:43:27 START RequestId: 054d8776-b852-42f6-a1a6-752552daea41 Version: $LATEST
17:43:27 [contracts] cadence map: 1826 artifacts resolvable
17:43:31 [contracts] rowcount history: 69 days, 943 artifacts; previous contracts: 1132; producers: 1998
17:47:55 INIT_START Runtime Version: python:3.12.mainlinev2.v43	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:36aeb19f3326e129d9f096dea0a3c6a9ca8dce2610e62d2b2060747602d863ae
17:47:55 START RequestId: c4c2a78d-5976-4177-91ae-64e7418b9769 Version: $LATEST
17:47:55 [contracts] cadence map: 1826 artifacts resolvable
17:48:00 [contracts] rowcount history: 69 days, 943 artifacts; previous contracts: 1132; producers: 1998
17:52:22 INIT_START Runtime Version: python:3.12.mainlinev2.v43	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:36aeb19f3326e129d9f096dea0a3c6a9ca8dce2610e62d2b2060747602d863ae
17:52:22 START RequestId: 3b9e07c5-5fa2-47ad-af06-e44d2b64ec8f Version: $LATEST
17:52:23 [contracts] cadence map: 1826 artifacts resolvable
17:52:28 [contracts] rowcount history: 69 days, 943 artifacts; previous contracts: 1132; producers: 1998
17:53:27 REPORT RequestId: 054d8776-b852-42f6-a1a6-752552daea41	Duration: 600000.00 ms	Billed Duration: 600000 ms	Memory Size: 1024 MB	Max Memory Used: 1024 MB	Status: timeout
XRAY TraceId: 1-6ac7d63f-0104e99861c65c8039f183da	SegmentId: 7c4eb6a6d56f2a27	Sampled: true
17:56:54 START RequestId: c14902aa-5807-41ca-a1b7-40cc90a7a544 Version: $LATEST
17:56:54 [contracts] cadence map: 1826 artifacts resolvable
17:56:58 [contracts] rowcount history: 69 days, 943 artifacts; previous contracts: 1132; producers: 1998
17:57:55 REPORT RequestId: c4c2a78d-5976-4177-91ae-64e7418b9769	Duration: 600000.00 ms	Billed Duration: 600517 ms	Memory Size: 1024 MB	Max Memory Used: 1024 MB	Init Duration: 516.31 ms	Status: timeout
XRAY TraceId: 1-6ac7d74a-019546e11f8bb3cd382c293c	SegmentId: f8f17b1699891cfe	Sampled: true
17:58:07 REPORT RequestId: 3b9e07c5-5fa2-47ad-af06-e44d2b64ec8f	Duration: 344356.22 ms	Billed Duration: 344756 ms	Memory Size: 1024 MB	Max Memory Used: 1024 MB	Init Duration: 399.33 ms	Status: error	Error Type: Runtime.OutOfMemory
XRAY TraceId: 1-6ac7d856-23c5b0d2790c4b5677597fc1	SegmentId: 382f97d370ef6f64	
```

VERDICT: PASS
