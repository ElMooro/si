# ops 3888 — redeploy with the CORRECTED inherit_env source (equity-research, verified real)

**Status:** failure  
**Duration:** 97.1s  
**Finished:** 2026-07-25T22:12:36+00:00  

## Error

```
SystemExit: 1
```

## Data

| news_sentiment_bearish | news_sentiment_bullish | news_sentiment_neutral |
|---|---|---|
| 0 | 0 | 503 |

## Log
## 1. news-wire — env-key check against the corrected source

- `22:10:59`   justhodl-news-wire: attempt 1, ANTHROPIC_API_KEY=still placeholder/short
- `22:11:14`   justhodl-news-wire: attempt 2, ANTHROPIC_API_KEY=still placeholder/short
- `22:11:30` ✅   justhodl-news-wire: ANTHROPIC_API_KEY is live and non-placeholder (len=108) on attempt 3
- `22:11:35`   recent log tail: INIT_START Runtime Version: python:3.12.mainlinev2.v18	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:9819e0b13863c84a43e11f5c724871d909046d3cfb807eeb19460c63f974f26f

START RequestId: 6432db43-e135-495e-9393-d5698e9a63c4 Version: $LATEST

[news-wire] 20 new headlines

[llm_router] mode=on_demand: background call gated -> empty; engine uses deterministic fallback

[anthropic batch] HTTP Error 400: Bad Request

[news-wire] done · new=20 · top24h=0 · high-impact=0 · elapsed=1.1s

END RequestId: 6432db43-e135-495e-9393-d5698e9a63c4

REPORT RequestId: 6432db43-e135-495e-9393-d5698e9a63c4	Duration: 1174.14 ms	Billed Duration: 1666 ms	Memory Size: 512 MB	Max Memory Used: 111 MB	Init Duration: 491.36 ms	
XRAY TraceId: 1-6a653492-52f15b514e5465b138a098df	SegmentId: 66a71ad90018828b	Sampled: true	

## 2. news-sentiment — zip-settle + env-key + invoke + gate on real scoring

- `22:11:35` ✅   justhodl-news-sentiment: new artifact live on attempt 1
- `22:11:35` ✅   justhodl-news-sentiment: ANTHROPIC_API_KEY is live and non-placeholder (len=108) on attempt 1
- `22:12:36` ✅   sentiment/data.json rewritten on attempt 4
- `22:12:36`   recent log tail: emand: background call gated -> empty; engine uses deterministic fallback

claude err: HTTP Error 400: Bad Request

[llm_router] mode=on_demand: background call gated -> empty; engine uses deterministic fallback

claude err: HTTP Error 400: Bad Request

[llm_router] mode=on_demand: background call gated -> empty; engine uses deterministic fallback

claude err: HTTP Error 400: Bad Request

[llm_router] mode=on_demand: background call gated -> empty; engine uses deterministic fallback

claude err: HTTP Error 400: Bad Request

[llm_router] mode=on_demand: background call gated -> empty; engine uses deterministic fallback

claude err: HTTP Error 400: Bad Request

[llm_router] mode=on_demand: background call gated -> empty; engine uses deterministic fallback

claude err: HTTP Error 400: Bad Request

scored 0 in 8.3s

=== DONE · B:0 Bear:0 N:503 · 47.0s ===

wrote 436.6 KB

END RequestId: b825580a-e94e-4418-a70d-66692c8fd237

REPORT RequestId: b825580a-e94e-4418-a70d-66692c8fd237	Duration: 47105.38 ms	Billed Duration: 47610 ms	Memory Size: 512 MB	Max Memory Used: 155 MB	Init Duration: 504.04 ms	
XRAY TraceId: 1-6a653497-0a2a416d246afffe6cac660a	SegmentId: 2bf6cf6268cb1b8c	Sampled: true	

## 3. THE HARD GATE

- `22:12:36` ✅   news-wire: no 401/Unauthorized after the corrected fix
- `22:12:36` ✗   news-sentiment: no 400 in log tail
- `22:12:36` ✗   news-sentiment: at least some non-neutral scores (was 0/0/503)
- `22:12:36` ✗ FAILED 2: ['news-sentiment: no 400 in log tail', 'news-sentiment: at least some non-neutral scores (was 0/0/503)']
