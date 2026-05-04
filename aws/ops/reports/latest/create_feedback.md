# Create justhodl-feedback + table + URL

**Status:** success  
**Duration:** 40.5s  
**Finished:** 2026-05-04T12:47:36+00:00  

## Log
- `12:47:16` ✅   ✓ table justhodl-feedback created
- `12:47:16` ✅   ✓ generated auth-token in SSM
- `12:47:16`   zip size: 2,879b
- `12:47:17` ✅   ✓ created
## Function URL

- `12:47:17` ✗   ✗ URL setup: An error occurred (ValidationException) when calling the CreateFunctionUrlConfig operation: 1 validation error detected: Value '[GET, POST, OPTIONS]' at 'cors.allowMethods' failed to satisfy constraint: Member must satisfy constraint: [Member must have length less than or equal to 6, Member must have length greater than or equal to 0, Member must satisfy regular expression pattern: .*, Member must not be null]
## Smoke test — GET /signals

- `12:47:35`   status: 200 duration: 13.3s
- `12:47:35`   signals returned: 5
- `12:47:35`     d0881bb8-1e8e-4af0-9b28-9f6a0bd6affa     type=corr_break_top_pair       val=vixcls_dgs10
- `12:47:35`     6a947252-c543-4a16-a38d-e3bc89070a66     type=corr_break_composite_vs_vxx val=NORMAL
- `12:47:35`     463f4578-c03a-456f-a8b3-f82062eb8ebb     type=corr_break_composite_vs_spy val=NORMAL
## Smoke test — POST /label

- `12:47:36`   status: 200
- `12:47:36`   body: {"ok": true, "item": {"signal_id": "test_smoke_1777898855", "label": "GOOD_CALL", "note": "smoke test from deployment", "asset": "", "user": "khalid", "updated_at": "2026-05-04T12:47:35.889315+00:00", "created_at": "2026-05-04T12:47:35.889315+00:00"}}
## Smoke test — GET /list

- `12:47:36`   feedback count: 1
- `12:47:36`     test_smoke_1777898855                    label=GOOD_CALL    at=2026-05-04T12:47:35.889315+00:00
