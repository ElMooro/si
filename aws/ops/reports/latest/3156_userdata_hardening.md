# ops 3156 — /userdata hardening E2E

**Status:** success  
**Duration:** 21.4s  
**Finished:** 2026-07-12T18:14:19+00:00  

## Error

```
SystemExit: 0
```

## Data

| anon_get | anon_put | checkout_status | has_stripe_url | n_fails | n_warns | verdict |
|---|---|---|---|---|---|---|
| 200 | 200 |  |  |  |  |  |
|  |  | 200 | True |  |  |  |
|  |  |  |  | 0 | 0 | PASS |

## Log
## 0. Wait for worker deploy

- `18:14:18` ✅ hardened worker live after ~20s
## 1. Anonymous roundtrip + namespacing

- `18:14:18` ✅ anonymous device roundtrip intact
- `18:14:18` ✅ uid isolation holds
## 2. Auth wall

- `18:14:19` ✅ invalid Bearer → 401
- `18:14:19` ✅ /billing-portal unauth → 401
## 3. Billing plumbing

- `18:14:19` ✅ checkout session live (plan metadata attached)
- `18:14:19` ✅ webhook rejects unsigned payloads
