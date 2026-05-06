# Public API Auth Tiers — Phase 1 Complete

End-to-end auth + rate-limiting infrastructure for JustHodl.AI's public
Lambda APIs. Phase 1 ships the foundation; Phase 2+ rolls it out across
the ~40 existing public-URL Lambdas.

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│  PUBLIC USER                                                         │
│   curl <api-url> -H "Authorization: Bearer jhd_..."                  │
└─────────────────────────┬────────────────────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────────────────────┐
│  ANY PUBLIC LAMBDA  (e.g. justhodl-fred-proxy, stock-screener, …)    │
│                                                                       │
│  from api_auth import authorize                                       │
│  key_meta, err = authorize(event)        ◄─ ONE LINE auth gate        │
│  if err: return err                                                   │
│                                                                       │
│  # business logic uses key_meta["tier"] for tier-specific behavior   │
└─────────────────────────┬────────────────────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────────────────────┐
│  aws/shared/api_auth.py                                              │
│   - SHA-256 hash of key                                              │
│   - DDB GetItem on justhodl-api-keys → tier resolution              │
│   - 3 atomic UpdateItems on justhodl-api-rate (sec/hour/day)        │
│   - Returns Lambda response dict on auth/rate failures               │
│   - Fail-open on transient DDB errors (over-serve > break-all)       │
└──────────────────────────────────────────────────────────────────────┘
```

## Tier limits

| Tier        | per second | per hour    | per day    |
|-------------|-----------:|------------:|-----------:|
| FREE        | 5          | 100         | 500        |
| PRO         | 20         | 5,000       | 100,000    |
| ENTERPRISE  | unlimited  | unlimited   | unlimited  |

Edit `TIERS` in `aws/shared/api_auth.py` to adjust. Changes take effect
on next Lambda cold start (or immediately if you also redeploy users).

## Live infrastructure

```
DynamoDB tables (us-east-1):
  justhodl-api-keys    PK=key_hash  PITR enabled
  justhodl-api-rate    PK=pk        TTL on 'ttl' attribute

Admin Lambda:
  Name: justhodl-api-keys-admin
  URL:  https://drmkbf2q3bvlb6mqy7vvjbomf40dnlod.lambda-url.us-east-1.on.aws/
  Auth: Bearer token from SSM /justhodl/api-admin/token
  Reserved concurrency: 2

Demo / reference Lambda:
  Name: justhodl-public-api-demo
  URL:  https://odoy2bydzufzjbp765n3ix6w5u0rvqmj.lambda-url.us-east-1.on.aws/
  Auth: Bearer jhd_<key>
  Reserved concurrency: 5

IAM policy on lambda-execution-role:
  api-keys-admin-permissions
    DDB read/write on justhodl-api-keys + justhodl-api-rate
    SSM read/write on /justhodl/api-admin/*
```

## Issuing keys (admin operations)

The admin Function URL is unauthenticated at the URL level, but every
request is checked against the SSM-stored admin token. Without the
token, every request gets 401/403.

```bash
ADMIN_URL="https://drmkbf2q3bvlb6mqy7vvjbomf40dnlod.lambda-url.us-east-1.on.aws/"
ADMIN_TOKEN=$(aws ssm get-parameter --name /justhodl/api-admin/token \
  --with-decryption --query Parameter.Value --output text)

# Create a FREE-tier key
curl -X POST "$ADMIN_URL" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"action":"create","tier":"FREE","owner_email":"alice@example.com","label":"Alice prod"}'
# → {"key":"jhd_...","key_hash":"...","tier":"FREE","warning":"Save this key..."}

# List all keys
curl -X POST "$ADMIN_URL" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"action":"list"}'

# List only PRO-tier keys
curl -X POST "$ADMIN_URL" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -d '{"action":"list","tier":"PRO"}'

# Revoke a key (revokes by hash, not plain)
curl -X POST "$ADMIN_URL" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -d '{"action":"revoke","key_hash":"abc123..."}'

# Rotate (creates new + revokes old in one call, preserves owner+tier)
curl -X POST "$ADMIN_URL" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -d '{"action":"rotate","key_hash":"abc123..."}'
```

## Integrating auth into an existing Lambda

```python
# In aws/lambdas/<your-lambda>/source/lambda_function.py

import os
import sys

# Bundle api_auth.py alongside lambda_function.py — see step below
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from api_auth import authorize, TIERS


def lambda_handler(event, context):
    # First-line auth gate
    key_meta, err = authorize(event)
    if err:
        return err   # already a 401/403/429 with proper headers

    # ... your business logic ...

    # Optional: tier-aware behavior
    if key_meta["tier"] == "FREE":
        # Maybe limit response size, or omit premium fields
        ...

    return {"statusCode": 200, "body": "..."}
```

**Bundling step** (one-time per Lambda):

```bash
cp aws/shared/api_auth.py aws/lambdas/<your-lambda>/source/api_auth.py
```

The deploy-lambdas.yml workflow zips everything in `source/` together,
so `api_auth.py` ships in the package and Python finds it via `sys.path`.

> **Future improvement (Phase 3+):** publish `api_auth` as a Lambda
> Layer so updates propagate to all consumers without per-Lambda copies.

## Customizing per-Lambda

```python
import os
os.environ.setdefault("JUSTHODL_API_KEYS_TABLE", "justhodl-api-keys")
os.environ.setdefault("JUSTHODL_API_RATE_TABLE", "justhodl-api-rate")
# Override these env vars to point at staging tables, or to fork the
# tier system per-product.
```

## Phase 2 — Rollout plan

These ~40 Lambdas have public Function URLs. Phase 2 rollout completed
across 5 phases (Phase 1 + 2A + 2B + 2C + 2D):

**Tier A (read-heavy data passthroughs)**
- ✅ justhodl-fred-proxy        (Phase 2A, Origin-bypass)
- ✅ justhodl-ecb-proxy         (Phase 2A, strict mode)
- ✅ justhodl-treasury-proxy    (Phase 2B, Origin-bypass)
- ✅ nasdaq-datalink-agent      (Phase 2B, Origin-bypass)
- ✅ bea-economic-agent         (Phase 2D, Origin-bypass via CF Worker)

**Tier B (analysis endpoints)**
- 🚫 justhodl-stock-screener  (PROTECTED — never modify without explicit approval)
- ✅ justhodl-stock-analyzer   (Phase 2C, Origin-bypass)
- ✅ justhodl-options-flow     (Phase 2C, Origin-bypass)
- ✅ justhodl-charts-agent     (Phase 2C, strict mode — auth verified;
                                 Lambda has pre-existing business logic
                                 issues w/ deprecated orchestrator dependency,
                                 unrelated to auth)
- ✅ justhodl-edge-engine      (Phase 2C, Origin-bypass)
- ✅ justhodl-investor-agents  (Phase 2D, Origin-bypass via CF Worker)

**Tier C (special handling — keep as-is for now)**
- justhodl-ai-chat       (already SSM-token auth; coexist or migrate later)
- justhodl-telegram-bot  (Telegram webhook signature, different auth model)

## Cloudflare Worker chain pattern (Phase 2D learning)

Lambdas called via the `justhodl-ai-proxy` Cloudflare Worker work
seamlessly with Origin-bypass mode because the Worker source at
`cloudflare/workers/justhodl-ai-proxy/src/index.js` already injects
`'Origin': 'https://justhodl.ai'` on every upstream fetch (lines 95,
124, 161, 201). No special internal-key handling needed — just use
the standard `allowed_origins` list.

## Session results — 56 assertions across 6 verification rounds

  Phase 1 (step 289):   5/5 pass
  Phase 2A (step 291): 10/10 pass
  Phase 2B (step 292): 12/12 pass
  Phase 2C (step 293): 20/21 pass (charts-agent biz logic, not auth)
  Phase 2D (step 297):  8/8 pass
                       ────────
                       55/56 — auth system fully verified

## Phase 2A migration recipe (verified working)

For each Lambda being migrated, three steps:

**1. Add the import + auth gate to lambda_function.py:**

```python
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from api_auth import authorize

# At the top of lambda_handler, AFTER the OPTIONS preflight check:
def lambda_handler(event, context):
    if event.get("requestContext",{}).get("http",{}).get("method") == "OPTIONS":
        return {"statusCode": 200, "headers": cors_headers, "body": ""}

    # ─── auth gate ───
    # Strict mode (no frontend callers):
    key_meta, err = authorize(event)
    # OR Origin-bypass mode (justhodl.ai pages call this Lambda):
    key_meta, err = authorize(event, allowed_origins=[
        "https://justhodl.ai",
        "https://www.justhodl.ai",
    ])
    if err:
        return err

    # ... existing business logic, with key_meta["tier"] available
```

**2. Bundle the shared module into the Lambda's source/:**

```bash
cp aws/shared/api_auth.py aws/lambdas/<your-lambda>/source/api_auth.py
```

**3. Add `Authorization, x-api-key` to the Lambda's CORS `AllowHeaders`:**

```python
headers = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, OPTIONS",  # or POST
    "Access-Control-Allow-Headers": "Content-Type, Authorization, x-api-key",
}
```

That's it. Push + the deploy-lambdas.yml workflow will redeploy. IAM
permissions are already on lambda-execution-role (granted in step 287),
so any Lambda using that role gets DDB access automatically.

## Determining strict vs Origin-bypass mode

**Audit which frontend pages call the Lambda** before deciding:

```bash
# Get the Lambda's URL prefix (8-char subdomain id)
URL_PREFIX=$(python3 -c "import json; c=json.load(open('aws/lambdas/<NAME>/config.json')); fu=c.get('function_url',{}); print((fu.get('url') if isinstance(fu,dict) else fu).split('//')[1].split('.')[0])")

# Find HTML files referencing it
grep -rln --include="*.html" "$URL_PREFIX" . | grep -v "/historical/\|/ran/\|/reports/"
```

- **No matches** → safe for strict mode. External callers must use a key.
- **Matches found** → use Origin-bypass mode. Frontend pages keep working;
  external callers still need keys.


## Phase 3 — Self-serve + billing

- API docs page on `justhodl.ai/api` (OpenAPI spec)
- Self-serve key issuance flow (form → email magic-link → key delivery)
- Stripe integration for paid tier upgrades
- Per-endpoint pricing differentiation
- Usage analytics dashboard

## Operational notes

- **Key storage:** only SHA-256 hashes are persisted; plain keys are
  shown to users exactly once. Rotate via the admin Lambda's `rotate` action.
- **Rate-limit behavior on DDB outage:** the auth module fails OPEN —
  it logs the error but allows the request. Better to over-serve briefly
  than to break the whole API on a transient AWS hiccup.
- **DDB cost:** PAY_PER_REQUEST on both tables. Rate table TTL auto-expires
  rows hourly/daily — storage stays bounded regardless of traffic.
- **Reserved concurrency:** admin Lambda capped at 2 (DDoS protection on
  the unauthenticated entry point). Per-Lambda demo at 5.
- **Admin token rotation:** simply `aws ssm put-parameter --overwrite
  --name /justhodl/api-admin/token --value <new-token> --type SecureString`.
