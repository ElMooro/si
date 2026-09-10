# justhodl-ai-proxy

Cloudflare Worker that sits between `justhodl.ai` and AWS AI services. It
injects Lambda service credentials server-side and protects the `/ai` control
plane with verified bearer identity, owner/admin authorization, quotas, replay
checks, and bounded JSON bodies.

## Architecture

```
Browser (justhodl.ai)
     │ POST /chat
     ▼
justhodl-ai-proxy (Cloudflare Worker)
     │ - CORS policy (only justhodl.ai + www can read in a browser)
     │ - Adds x-justhodl-token header from encrypted Secret
     ▼
justhodl-ai-chat (AWS Lambda)
     │ - Validates x-justhodl-token
     │ - Validates Origin
     │ - Runs Claude Haiku 4.5
     ▼
Response streamed back through Worker to browser
```

`/chat`, `/research`, `/investor`, `/portfolio-admin`, `/agent/*`, and the
existing data-proxy bridge routes retain their prior behavior. The controls
below apply specifically to `/ai` and `/ai/*`.

## AI control security

### Authentication and authorization

Every non-preflight `/ai` request requires
`Authorization: Bearer <access-token>`. The Worker verifies an asymmetric OIDC
JWT against `AUTH_JWKS_URL`, including signature, issuer, audience, expiry,
not-before time, issued-at time, and subject. Supabase's authenticated
`/auth/v1/user` endpoint is the fail-closed verifier for legacy symmetric
tokens. The publishable key used for that call is not a service credential.

The verified subject is authorized only when:

- its email is in `OWNER_EMAILS`, or its subject is in optional
  `OWNER_SUBJECTS`; or
- a signed `app_metadata.role`/`app_metadata.roles` value is in `ADMIN_ROLES`.

`user_metadata` is never an authorization source. `Origin` is never an
identity or authorization source: an allowed Origin without a bearer token is
still rejected, while non-browser clients may omit Origin.

### Mutation and replay contract

Every `POST /ai/*` request must include:

```
Content-Type: application/json
X-Request-Timestamp: <current Unix epoch milliseconds>
X-Request-Nonce: <16-128 URL-safe characters, freshly generated per request>
```

The timestamp must be within `AI_REPLAY_WINDOW_SECONDS` (300 seconds by
default). The guard atomically records a hash of the verified subject and
nonce, making each nonce single-use even if a replay changes the route,
timestamp, or body. Reuse returns `409`; missing/stale replay headers return
`400`. Every accepted nonce remains stored for the full replay window;
expiration cleanup is time-ordered and limited to a fixed batch per guard
transaction, never a fixed count of live nonces. POST bodies default to a
65,536-byte maximum, enforced against both `Content-Length` and bytes actually
read.

### Quotas and failure behavior

`AI_REQUEST_GUARD` is a Durable Object binding keyed by a SHA-256 hash of the
verified subject. It atomically maintains independent fixed-window counters
for each method and exact route. Defaults are defined beside the route
allowlist in `src/index.js`; optional `AI_ROUTE_QUOTAS` JSON can override only
the numeric limit/window, for example:

```
{"POST /infer":{"limit":10,"window_seconds":60}}
```

Quota responses use `429` and include `RateLimit-Limit`,
`RateLimit-Remaining`, and `Retry-After` when available. All paid or mutating
routes fail closed with `503` when the guard is missing or unavailable.
Read-only routes may continue in explicitly marked degraded mode so a guard
outage does not hide status needed for recovery.

Authentication, guard, and upstream exceptions return stable JSON errors with
a request ID; exception strings and upstream error bodies are not exposed.
Successful AI responses are accepted only as JSON and are always
`private, no-store`.

### Governance route behavior

`POST /ai/governance/signals/ingest` is dry-run by default. A live request must
set `"dry_run": false`; it is staged in the Lambda's private S3 outbox before
archive and EventBridge delivery. Repeating the same envelope safely redrives a
pending/expired attempt and becomes a no-op after `PUBLISHED`. Event delivery
is at-least-once and consumers deduplicate on the returned fingerprint.

`POST /ai/governance/features/assemble` reads the configured SageMaker Feature
Store offline table when `observations` is omitted. The Lambda applies exact
entity/feature scoping, both event and availability cutoffs, taint exclusion,
and input/result limits. Supplying `observations` selects the no-AWS
`SUPPLIED_OBSERVATIONS_DRY_RUN` path for fixture review; it never calls
SageMaker or Athena. `allow_tainted` is confined to that diagnostic path.

## Deployment

Push any change to `cloudflare/workers/justhodl-ai-proxy/**` on `main`.
GitHub Actions runs `.github/workflows/deploy-workers.yml` which:

1. Reads `AI_CHAT_TOKEN` from AWS SSM (`/justhodl/ai-chat/auth-token`)
2. Runs `wrangler deploy`
3. Sets the fetched token as the Worker's `AI_CHAT_TOKEN` secret

No manual wrangler calls required.

The `AiRequestGuard` Durable Object binding and its first SQLite migration are
declared in `wrangler.toml`. A deployment must retain that binding. OIDC,
owner/admin, body, and replay values are non-secret Worker vars; Lambda
credentials remain encrypted Worker Secrets.

## Rotation

Rotating the Lambda auth token is a single command (run locally):

```
aws ssm put-parameter \
  --name /justhodl/ai-chat/auth-token \
  --value "$(python -c 'import secrets; print(secrets.token_urlsafe(32))')" \
  --type SecureString --overwrite --region us-east-1
```

Then push any trivial change to the Worker dir (or trigger the workflow
manually) and the Worker picks up the new value on next deploy.

## Local testing

```
cd cloudflare/workers/justhodl-ai-proxy
node --test tests/index.test.mjs
```

For an interactive local Worker, install Wrangler and run
`wrangler dev --local`.

## Monitoring

Cloudflare dashboard → Workers & Pages → `justhodl-ai-proxy` → Logs tab.
With `observability` enabled in wrangler.toml, every request logs the
status code, CPU time, and any console output.

## Deployment log

- 2026-04-22 — initial deploy
