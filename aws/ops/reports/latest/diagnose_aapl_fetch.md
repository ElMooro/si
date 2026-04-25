# Why did stock-ai-research return 404 for AAPL?

**Status:** success  
**Duration:** 0.5s  
**Finished:** 2026-04-25T23:29:18+00:00  

## Data

| had_anthropic | had_fmp_err | had_start | n_events |
|---|---|---|---|
| False | False | True | 5 |

## Log
## A. Recent log streams

- `23:29:18`   2026/04/25/[$LATEST]b0d60394d71f46ea8c6d93fe9bb0400e         last=2026-04-25 23:24:14
- `23:29:18` 
  Using: 2026/04/25/[$LATEST]b0d60394d71f46ea8c6d93fe9bb0400e
## B. All log events from most recent stream

- `23:29:18`   Pulled 5 events
- `23:29:18` 
- `23:29:18`   All non-empty messages:
- `23:29:18`     INIT_START Runtime Version: python:3.11.mainlinev2.v7	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:a0dd170e909c9230a7e18f978320f0053271b75d0b703c836ab4f98f2e3f6a5a
- `23:29:18`     START RequestId: ccbe2796-4347-4ec9-9f13-47aed788efb2 Version: $LATEST
- `23:29:18`     === AI RESEARCH: AAPL ===
- `23:29:18`     END RequestId: ccbe2796-4347-4ec9-9f13-47aed788efb2
- `23:29:18`     REPORT RequestId: ccbe2796-4347-4ec9-9f13-47aed788efb2	Duration: 49374.15 ms	Billed Duration: 49983 ms	Memory Size: 512 MB	Max Memory Used: 95 MB	Init Duration: 608.76 ms
## C. Key markers

- `23:29:18`   Got '=== AI RESEARCH ===' marker: True
- `23:29:18`   FMP error logs:                 False
- `23:29:18`   Anthropic call logged:          False
- `23:29:18`   Lambda init/timeout error:      False
- `23:29:18` ⚠ 
  → Lambda started, FMP didn't log errors, Anthropic wasn't called
- `23:29:18` ⚠   → gather_facts() ran successfully, but profile.name was None
- `23:29:18` ⚠   → Most likely: FMP returned a dict (not list), my parser dropped it
- `23:29:18` ⚠   → Fix: use safe() helper from investor-agents to handle both shapes
- `23:29:18` Done
