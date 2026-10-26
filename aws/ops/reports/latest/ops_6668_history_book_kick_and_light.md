# ops 6668 - history book: one kick + first light + equity-research on it

**Status:** success  
**Duration:** 827.1s  
**Finished:** 2026-10-26T08:05:22+00:00  

## Log
- `07:51:35` kick -> 202 (previous state updated_at None)
- `08:04:42` state written at 2026-10-26T08:04:16.156245+00:00 after 786 s
## 0. history book

- `08:04:42` manifest: phase BACKFILL updated 2026-10-26T07:51:36.623932+00:00 as_of 2026-10-26T02:55:44.008955+00:00 universe factor-book/27215 pending ['key-metrics-bulk/annual', 'key-metrics-bulk/quarter', 'income-statement-growth-bulk/annual', 'income-statement-growth-bulk/quarter', 'balance-sheet-statement-growth-bulk/annual', 'balance-sheet-statement-growth-bulk/quarter', 'cash-flow-statement-growth-bulk/annual', 'cash-flow-statement-growth-bulk/quarter'] stale []
- `08:04:42`    balance-sheet-statement-bulk/annual: files 5 years [2022, 2026] rows 59456 symbols 15434 written 15434 unchanged 0 failed 0 75s refreshed 2026-10-26T07:51:36
- `08:04:42`    balance-sheet-statement-bulk/quarter: files 20 years [2022, 2026] rows 231738 symbols 15559 written 15559 unchanged 0 failed 0 112s refreshed 2026-10-26T07:51:36
- `08:04:42`    cash-flow-statement-bulk/annual: files 5 years [2022, 2026] rows 59017 symbols 15387 written 15387 unchanged 0 failed 0 70s refreshed 2026-10-26T07:51:36
- `08:04:42`    cash-flow-statement-bulk/quarter: files 20 years [2022, 2026] rows 227688 symbols 15420 written 15420 unchanged 0 failed 0 102s refreshed 2026-10-26T07:51:36
- `08:04:42`    financial-growth-bulk/annual: files 0 years None rows None symbols None written None unchanged None failed None 0s refreshed 
- `08:04:42`    financial-growth-bulk/quarter: files 0 years None rows None symbols None written None unchanged None failed None 0s refreshed 
- `08:04:42`    income-statement-bulk/annual: files 5 years [2022, 2026] rows 59840 symbols 15493 written 15493 unchanged 0 failed 0 69s refreshed 2026-10-26T07:51:36
- `08:04:42`    income-statement-bulk/quarter: files 20 years [2022, 2026] rows 233452 symbols 15546 written 15546 unchanged 0 failed 0 96s refreshed 2026-10-26T07:51:36
- `08:04:42`    ratios-bulk/annual: files 4 years [2023, 2026] rows 45157 symbols 15091 written 15091 unchanged 0 failed 0 84s refreshed 2026-10-26T07:51:36
- `08:04:42`    ratios-bulk/quarter: files 16 years [2023, 2026] rows 183316 symbols 15391 written 15391 unchanged 0 failed 0 151s refreshed 2026-10-26T07:51:36
- `08:04:42` last_run: {"started": "2026-10-26T07:51:36.623932+00:00", "seconds": 759.5, "blocks": ["income-statement-bulk/annual", "income-statement-bulk/quarter", "balance-sheet-statement-bulk/annual", "balance-sheet-statement-bulk/quarter", "cash-flow-statement-bulk/annual", "cash-flow-statement-bulk/quarter", "ratios-bulk/annual", "ratios-bulk/quarter"], "notes": ["budget: key-metrics-bulk/annual not reached", "budget: key-metrics-bulk/quarter not reached", "budget: income-statement-growth-bulk/annual not reached", "budget: income-statement-growth-bulk/quarter not reached", "budget: balance-sheet-statement-growt
- `08:04:42` income-statement-bulk/annual/AAPL.json.gz: n 4 years 2022..2025 first row date 2025-09-27 period FY keys ['acceptedDate', 'bottomLineNetIncome', 'cik', 'costAndExpenses', 'costOfRevenue', 'date', 'depreciationAndAmortization', 'ebit', 'ebitda', 'eps', 'epsDiluted', 'filingDate']
- `08:04:42` income-statement-bulk/quarter/AAPL.json.gz: n 19 years 2022..2026 first row date 2026-06-27 period Q3 keys ['acceptedDate', 'bottomLineNetIncome', 'cik', 'costAndExpenses', 'costOfRevenue', 'date', 'depreciationAndAmortization', 'ebit', 'ebitda', 'eps', 'epsDiluted', 'filingDate']
- `08:04:42` ratios-bulk/annual/MSFT.json.gz: n 4 years 2023..2026 first row date 2026-06-30 period FY keys ['assetTurnover', 'bookValuePerShare', 'bottomLineProfitMargin', 'capexPerShare', 'capitalExpenditureCoverageRatio', 'cashPerShare', 'cashRatio', 'continuousOperationsProfitMargin', 'currentRatio', 'date', 'debtServiceCoverageRatio', 'debtToAssetsRatio']
- `08:04:43` balance-sheet-statement-bulk/annual/GME.json.gz: n 4 years 2022..2025 first row date 2026-01-31 period FY keys ['acceptedDate', 'accountPayables', 'accountsReceivables', 'accruedExpenses', 'accumulatedOtherComprehensiveIncomeLoss', 'additionalPaidInCapital', 'capitalLeaseObligations', 'capitalLeaseObligationsCurrent', 'capitalLeaseObligationsNonCurrent', 'cashAndCashEquivalents', 'cashAndShortTermInvestments', 'cik']
- `08:04:43` equity-research deployed: modified 2026-10-26T07:24:34, 1024 MB, 300 s
- `08:04:57` equity-research AAPL invoke: status 200, 84589 bytes, FunctionError None, head b'{"statusCode": 200, "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*", "Cache-Control": "public, max-age=3600"}, "body": "{\\"ca'
## justhodl-equity-research

- `08:05:19` REPORT: 40 events
- `08:05:19`    08:00:38 | REPORT RequestId: 9ed77e81-b60a-41da-bb8d-5b926533c943	Duration: 979.93 ms	Billed Duration: 980 ms	Memory Size: 1024 MB	Max Memory Used: 1023 MB	 ⏎ XRAY TraceId: 1-6adf08a5-7bd64a7c2016988f4acec473	SegmentId: aa2c8ec4e96fa85a	Sampled: true
- `08:05:19`    08:00:38 | REPORT RequestId: 0a881f82-ed12-49cc-82c3-543ed1dc8592	Duration: 207.89 ms	Billed Duration: 208 ms	Memory Size: 1024 MB	Max Memory Used: 1023 MB	 ⏎ XRAY TraceId: 1-6adf08a6-66cad4b071a29bbf3e0037c0	SegmentId: d224ad44d3a1e009	Sampled: true
- `08:05:19`    08:00:39 | REPORT RequestId: a1b9ede9-40f0-4b3a-869c-04c248e843b1	Duration: 568.32 ms	Billed Duration: 569 ms	Memory Size: 1024 MB	Max Memory Used: 1023 MB	 ⏎ XRAY TraceId: 1-6adf08a7-26b7280458e7272104bc3bae	SegmentId: 3156639ec9bd3719	Sampled: true
- `08:05:19`    08:00:41 | REPORT RequestId: f26c6814-a9c4-4acc-922d-da8f60d6e9b6	Duration: 535.00 ms	Billed Duration: 535 ms	Memory Size: 1024 MB	Max Memory Used: 1023 MB	 ⏎ XRAY TraceId: 1-6adf08a8-13236e04450a6fc25c4553c3	SegmentId: 935a698348e6da07	Sampled: true
- `08:05:19`    08:00:42 | REPORT RequestId: 98818811-29e9-4470-9dbc-426e2cfa6ee7	Duration: 753.84 ms	Billed Duration: 754 ms	Memory Size: 1024 MB	Max Memory Used: 1023 MB	 ⏎ XRAY TraceId: 1-6adf08a9-4854596c373b93877d4f8cd1	SegmentId: ef87d2875faf1511	Sampled: true
- `08:05:19`    08:00:43 | REPORT RequestId: 73403b48-90f5-4644-a9eb-a5de43b46bd5	Duration: 241.51 ms	Billed Duration: 242 ms	Memory Size: 1024 MB	Max Memory Used: 1023 MB	 ⏎ XRAY TraceId: 1-6adf08ab-3f6cedfa4a9ee36d44966cad	SegmentId: 06e323d3d88a9310	Sampled: true
- `08:05:19`    08:00:45 | REPORT RequestId: 9081d87a-f774-43cb-affa-dd47aa371d46	Duration: 563.38 ms	Billed Duration: 564 ms	Memory Size: 1024 MB	Max Memory Used: 1023 MB
- `08:05:19`    08:00:46 | REPORT RequestId: c9070c31-4e1a-4b49-baef-fb87c7469ba1	Duration: 832.40 ms	Billed Duration: 833 ms	Memory Size: 1024 MB	Max Memory Used: 1023 MB	 ⏎ XRAY TraceId: 1-6adf08ad-425d61d92c3eb5855db59bfd	SegmentId: ec810e8d06836fbd	Sampled: true
- `08:05:19`    08:00:46 | REPORT RequestId: b4e0e3bb-3c5c-4af8-b504-057ebe5b00b8	Duration: 490.98 ms	Billed Duration: 491 ms	Memory Size: 1024 MB	Max Memory Used: 1023 MB	 ⏎ XRAY TraceId: 1-6adf08ae-2282e01227be742e6734daf4	SegmentId: cf978c31bd785aa1	Sampled: true
- `08:05:19`    08:00:47 | REPORT RequestId: c909821e-3116-469a-8376-a75ebdea57d6	Duration: 202.61 ms	Billed Duration: 203 ms	Memory Size: 1024 MB	Max Memory Used: 1023 MB	 ⏎ XRAY TraceId: 1-6adf08ae-2ba94bf515dede2f58e2ea33	SegmentId: c92336d2e52389fd	Sampled: true
- `08:05:19`    08:00:48 | REPORT RequestId: f2eb1547-80f9-4618-9e15-ea897819472e	Duration: 250.24 ms	Billed Duration: 251 ms	Memory Size: 1024 MB	Max Memory Used: 1023 MB	 ⏎ XRAY TraceId: 1-6adf08b0-28a36190272840cf43016ae7	SegmentId: 3a19eb10ff0ce9c8	Sampled: true
- `08:05:19`    08:00:48 | REPORT RequestId: 4f6a53ca-d41f-4088-b925-e71581587f55	Duration: 253.08 ms	Billed Duration: 254 ms	Memory Size: 1024 MB	Max Memory Used: 1023 MB	 ⏎ XRAY TraceId: 1-6adf08b0-121f98ad78bfb4cb2ac1e147	SegmentId: 87e4787006478bcd	Sampled: true
- `08:05:19` errors: 1 events
- `08:05:19`    07:16:21 | REPORT RequestId: 4b010705-dbe0-4014-a3a8-eaaa89767607	Duration: 960.69 ms	Billed Duration: 961 ms	Memory Size: 1024 MB	Max Memory Used: 1024 MB	Status: error	Error Type: Runtime.OutOfMemory ⏎ XRAY TraceId: 1-6adefe44-27513f0827cdf0ec3f1adf9e	SegmentId: 555cfbf30a02e31b	Sampled: true
- `08:05:20` fusion lines: 0 events
## justhodl-bank-histories

- `08:05:20` REPORT: 1 events
- `08:05:20`    08:04:16 | REPORT RequestId: 75542654-f5c6-49fa-8b8b-7df218360d46	Duration: 759570.21 ms	Billed Duration: 760092 ms	Memory Size: 3008 MB	Max Memory Used: 897 MB	Init Duration: 521.64 ms	 ⏎ XRAY TraceId: 1-6adf0687-3a8960df0ccfd35e413c1854	SegmentId: b41eeb5f182e978c	Sampled: true
- `08:05:22` errors: 0 events
- `08:05:22` fusion lines: 0 events
- `08:05:22` ✅ ops 6668 complete
