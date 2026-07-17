# ops 3367 — /userdata/self alias security re-gate

**Status:** success  
**Duration:** 20.8s  
**Finished:** 2026-07-17T02:35:00+00:00  

## Error

```
SystemExit: 0
```

## Log
- `02:35:00` PASS  G1_tokenless_self_401 — http 401
- `02:35:00` PASS  G2_garbage_bearer_self_401 — http 401
- `02:35:00` PASS  G3_garbage_bearer_devuid_401 — http 401
- `02:35:00` PASS  G4_anon_roundtrip — put=200 get=200 echo=True
- `02:35:00` PASS  G5_short_uid_400 — http 400
- `02:35:00` VERDICT: PASS_ALL
