## P0 function config

**Status:** failure  
**Duration:** 19.4s  
**Finished:** 2026-08-25T18:46:23+00:00  

## Error

```
SystemExit: 1
```

## Log
- `18:46:03`   timeout=780 mem=1024 state=Active lastmod=2026-08-25T18:45:56.000+0000
- `18:46:03`   reserved_concurrency=None
- `18:46:04`   event_invoke_config: none (defaults)
- `18:46:04`   event_invoke_config reset to sane defaults
## P0b mint attempts (Khalid: try this one)

- `18:46:04`   no-secret -> HTTP 400 {"error_message":"Invalid credentials format","error":"invalid_client"}
- `18:46:04`   id-as-secret -> HTTP 400 {"error_message":"Invalid Credentials","error":"invalid_client"}
## P1 sync rediscover

- `18:46:05`   FunctionError=None payload={"ok": true, "phase": "DRAIN", "banked": 0, "rows": 0, "queue_left": 0, "chained": false, "elapsed_s": 0.4}
- `18:46:05`   universe=0 invalid=19 phase=DRAIN
## P2 sync drain x5

- `18:46:06`   link1 err=None banked=0 rows=0 q=0
- `18:46:06`   link2 err=None banked=0 rows=0 q=0
- `18:46:06`   link3 err=None banked=0 rows=0 q=0
- `18:46:07`   link4 err=None banked=0 rows=0 q=0
- `18:46:07`   link5 err=None banked=0 rows=0 q=0
- `18:46:07`     fail _meta_otcMarket: "metadata refused (HTTP Error 400: Bad Request) -> seed fallback"
- `18:46:07`     fail _meta_fixedIncomeMarket: "metadata refused (HTTP Error 400: Bad Request) -> seed fallback"
## P3 async verdict

- `18:46:23`   async events: ALIVE (15s)
- `18:46:23` ops 4981 RED: P1; P2
