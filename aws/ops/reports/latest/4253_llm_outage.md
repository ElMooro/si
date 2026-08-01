# ops 4253 — the LLM-layer outage

**Status:** failure  
**Duration:** 62.7s  
**Finished:** 2026-08-01T20:09:30+00:00  

## Error

```
SystemExit: FAILS: selftest failing: []
```

## Data

| detail | duration_s | engine | floor_s | http | line | name | recovered | section | value |
|---|---|---|---|---|---|---|---|---|---|
|  |  | justhodl-ai-brief |  |  | INIT_START Runtime Version: python:3.12.mainlinev2.v14	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:40182b778d40c8bdb13a6ef86990df74f5066cdb7d40aac1845f6f3fa5a1 |  |  | log_evidence |  |
|  |  | justhodl-ai-brief |  |  | [llm_router] mode=on_demand: background call gated -> empty; engine uses deterministic fallback |  |  | log_evidence |  |
|  |  | justhodl-ai-brief |  |  | [ai-brief] telegram digest sent: ok=False chars=469 info=HTTP Error 401: Unauthorized / fallback: HTTP Error 401: Unauthorized |  |  | log_evidence |  |
|  |  | justhodl-divergence-interpreter |  |  | INIT_START Runtime Version: python:3.12.mainlinev2.v14	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:40182b778d40c8bdb13a6ef86990df74f5066cdb7d40aac1845f6f3fa5a1 |  |  | log_evidence |  |
|  |  | justhodl-divergence-interpreter |  |  | [llm_router] mode=on_demand: background call gated -> empty; engine uses deterministic fallback |  |  | log_evidence |  |
|  |  | justhodl-debate-engine |  |  | INIT_START Runtime Version: python:3.12.mainlinev2.v14	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:40182b778d40c8bdb13a6ef86990df74f5066cdb7d40aac1845f6f3fa5a1 |  |  | log_evidence |  |
|  |  | justhodl-debate-engine |  |  | [llm_router] mode=on_demand: background call gated -> empty; engine uses deterministic fallback |  |  | log_evidence |  |
|  |  |  |  |  |  | /justhodl/anthropic/api-key |  | knob | <secret len=356> |
|  |  |  |  |  |  | /justhodl/anthropic/api_key |  | knob | <secret len=356> |
|  |  |  |  |  |  | /justhodl/llm/daily-budget-usd |  | knob | 8 |
|  |  |  |  |  |  | /justhodl/llm/engine-daily-cap |  | knob | {"justhodl-page-ai": 40, "justhodl-ticket-ai-rationale": 10, "justhodl-signal-board": 8, "justhodl-u |
|  |  |  |  |  |  | /justhodl/llm/mode |  | knob | on_demand |
| {"type":"error","error":{"type":"invalid_request_error","message":"Your credit balance is too low to access the Anthropic API. Please go to Plans & Bi |  |  |  | 400 |  |  |  | credential |  |
|  | 1.4 | justhodl-divergence-interpreter | 6.0 |  |  |  | False | recovery |  |
|  | 4.9 | justhodl-ai-brief | 12.0 |  |  |  | False | recovery |  |

## Log
## 1. The failure line, from the engines' own logs

- `20:08:28` ✗   [brief] INIT_START Runtime Version: python:3.12.mainlinev2.v14	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:40182b778d40c8bdb13a6ef86990df74f5066cdb7d40aac1845f6f3fa5a1b20f
- `20:08:28` ✗   [brief] [llm_router] mode=on_demand: background call gated -> empty; engine uses deterministic fallback
- `20:08:28` ✗   [brief] [ai-brief] telegram digest sent: ok=False chars=469 info=HTTP Error 401: Unauthorized / fallback: HTTP Error 401: Unauthorized
- `20:08:29` ✗   [interpreter] INIT_START Runtime Version: python:3.12.mainlinev2.v14	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:40182b778d40c8bdb13a6ef86990df74f5066cdb7d40aac1845f6f3fa5a1b20f
- `20:08:29` ✗   [interpreter] [llm_router] mode=on_demand: background call gated -> empty; engine uses deterministic fallback
- `20:08:29` ✗   [engine] INIT_START Runtime Version: python:3.12.mainlinev2.v14	Runtime Version ARN: arn:aws:lambda:us-east-1::runtime:40182b778d40c8bdb13a6ef86990df74f5066cdb7d40aac1845f6f3fa5a1b20f
- `20:08:29` ✗   [engine] [llm_router] mode=on_demand: background call gated -> empty; engine uses deterministic fallback
## 2. The LLM control surface

- `20:08:31`   /justhodl/anthropic/api-key                          = <secret len=356>
- `20:08:31`   /justhodl/anthropic/api_key                          = <secret len=356>
- `20:08:31`   /justhodl/llm/daily-budget-usd                       = 8
- `20:08:31`   /justhodl/llm/engine-daily-cap                       = {"justhodl-page-ai": 40, "justhodl-ticket-ai-rationale": 10, "justhodl-signal-board": 8, "justhodl-upside-thesis": 4, "j
- `20:08:31`   /justhodl/llm/mode                                   = on_demand
- `20:08:31`   data/llm-health.json (0h old): {"engine": "llm-health", "version": "1.0", "generated_at": "2026-08-01T19:38:55.269255+00:00", "status": "CRITICAL", "headline": "LLM providers 0/2 up [CRITICAL] \u2014 anthropic=DOWN, zai_glm=DOWN | BILLING action needed: anthropic, zai_glm | 2 AI output(s) r
- `20:08:31`   data/llm-cost-audit.json (675h old): {"generated_at": "2026-07-04T17:10:01.721294+00:00", "engines": [{"engine": "justhodl-news-wire", "inv_24h": 144, "inv_7d": 1005, "err_7d": 0, "callsites": 1, "max_tokens": 2500, "model": "haiku", "loop": false, "on_demand": false, "runs_per_day_sched": 1.0, "
## 3. Test the credential itself — one token, live

- `20:08:32` ✗   Anthropic key FAILS live: HTTP 400 {"type":"error","error":{"type":"invalid_request_error","message":"Your credit balance is too low to access the Anthropic API. Please go to Plans & Billing to upgrade or purchase credits."},"request_id":"req_011CdcdhSMrg
## 4. Fix what the evidence names

- `20:08:32` ✗ ROOT CAUSE: dead credential. Only Khalid can mint a new Anthropic key. Store it with:
- `20:08:32` ✗ aws ssm put-parameter --name /justhodl/anthropic/api-key --type SecureString --overwrite --value 'sk-ant-...'
## 5. Verify recovery — invoke two collapsed engines

- `20:08:34` ✗   justhodl-divergence-interpreter      duration=1.4s (collapse band was 1-4s; former band 6s+) err=None
- `20:08:34`      tail: 4feda01-4e66-40ff-a833-2c11f3c8c290 | REPORT RequestId: b4feda01-4e66-40ff-a833-2c11f3c8c290	Duration: 1448.90 ms	Billed Duration: 1984 ms	Memory Size: 256 MB	Max Memory Used: 109 MB	Init Duration: 534.39 ms	 | XRAY TraceId: 1-6a6e5240-03e5d6057c15c21e759bbce9	SegmentId: 2f63ed07ce77e1fb	Sampled: true	 | 
- `20:08:40` ✗   justhodl-ai-brief                    duration=4.9s (collapse band was 1-4s; former band 12s+) err=None
- `20:08:40`      tail: 3e6859e-008c-4768-a456-813ab1af7e39 | REPORT RequestId: 63e6859e-008c-4768-a456-813ab1af7e39	Duration: 4916.44 ms	Billed Duration: 5460 ms	Memory Size: 512 MB	Max Memory Used: 113 MB	Init Duration: 543.55 ms	 | XRAY TraceId: 1-6a6e5243-08a278675699e46a568affb2	SegmentId: 08b69950ef08e6df	Sampled: true	 | 
- `20:08:40` ⚠ recovery blocked on the credential — expected until the new key is stored
## 6. Alarm the layer — never 2 silent days again

- `20:08:41` ✅ alarm justhodl-llm-layer-down armed on ai-brief p90 duration < 6s over 12h -> jh-ops-alerts (a healthy run is 20-30s; only a dead LLM layer produces sustained 2s completions)
## 7. contract-gate self-test regression (4252 E-gate)

## RESULT

- `20:09:30` ✗   selftest failing: []
