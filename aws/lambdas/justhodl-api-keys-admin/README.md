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

These ~40 Lambdas have public Function URLs. Phase 2 picks the order:

**Tier A (read-heavy data passthroughs — quick wins)**
- justhodl-fred-proxy
- justhodl-treasury-proxy
- justhodl-ecb-proxy
- bea-economic-agent
- nasdaq-datalink-agent

**Tier B (analysis endpoints — main value)**
- justhodl-stock-screener  ⚠️ PROTECTED (do not modify without explicit approval)
- justhodl-stock-analyzer
- justhodl-investor-agents
- justhodl-options-flow
- justhodl-charts-agent
- justhodl-edge-engine

**Tier C (special handling)**
- justhodl-ai-chat (already has its own SSM-token auth — coexist or migrate)
- justhodl-telegram-bot (webhook from Telegram, different auth model)

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
