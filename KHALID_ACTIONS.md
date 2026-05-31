# KHALID — Manual Action Queue

This file tracks items that require **your direct action** (registration, credentials, one-click workflows). Claude maintains and clears items as you complete them.

---

## 🔴 Time-sensitive

### 1. PAT Rotation — ⚠️ 6 DAYS OVERDUE (was due 2026-05-25)

The fine-grained PAT on `justhodl-dex-scanner` Lambda was set with 30-day expiration on 2026-04-25 and is now past expiration. Dex scanner may be failing silently.

**Steps (3 min):**
1. Generate fresh fine-grained PAT at https://github.com/settings/personal-access-tokens/new
   - Name: `justhodl-dex-scanner-202605`
   - Expiration: 30 days (or your preference)
   - Repository access: **Only select ElMooro/si**
   - Permissions: **Contents → Read and write** (only)
2. Visit https://github.com/ElMooro/si/actions/workflows/rotate-dex-scanner-pat.yml
3. Click **Run workflow**, paste the new PAT, click **Run**

Workflow auto-validates, swaps, smoke-tests, and rolls back on failure. Old PAT can then be revoked at https://github.com/settings/tokens.

---

## 🟡 Unblocks autonomous work

### 2. FINRA Gateway Registration — unblocks the short-interest data-source fix

The Polygon `/stocks/v1/short-interest` endpoint is dead post-2018 (confirmed via ops 1014-1020). The replacement scaffold is already built at `aws/shared/finra_si.py` (376L, OAuth2 + token caching + 4 public functions). It needs credentials in SSM to activate.

**Steps (~5 min real-time + 2-3 business day FINRA approval):**

1. Register at https://gateway.finra.org/
   - Sign Up → **Developer / Data Consumer** account type
   - Free
   - Wait for approval email (typically 2-3 business days)

2. Once approved, log in at https://gateway.finra.org/developer
   - Click **Create Application**
   - Name: `JustHodl-SI`
   - Scope: `data.public.read` (covers `equityShortInterestStandardized` dataset)
   - Copy the assigned `clientId` and `clientSecret` (**shown only once**)

3. Add the credentials to AWS SSM Parameter Store:

   ```bash
   aws ssm put-parameter \
     --name /justhodl/finra/client_id \
     --value YOUR_CLIENT_ID \
     --type SecureString --region us-east-1

   aws ssm put-parameter \
     --name /justhodl/finra/client_secret \
     --value YOUR_CLIENT_SECRET \
     --type SecureString --region us-east-1
   ```

**After step 3:** ping Claude with "FINRA creds in SSM" and the next session will patch `justhodl-short-interest` to use the new module, verify end-to-end via `health_check()`, and the entire squeeze-pretrigger ecosystem starts producing real 2026 setups (instead of running on schema-fixed but data-stale state).

---

## 🟢 Optional — provider alternative if FINRA approval delays

If FINRA approval takes too long, the alternative paid SI providers (also pre-scoped):

| Provider | Cost | Pros | Cons |
|---|---|---|---|
| Ortex | ~$25-50/mo | Real-time SI + utilization | Recurring cost |
| S3 Partners | ~$30+/mo | Best institutional data | More expensive |
| IBKR Short Sale Locator API | Free if IBKR account | Real-time | Different schema |

If you want to go this route, ping Claude with the provider name and credentials, and the FINRA scaffold can be adapted (the public API surface in `finra_si.py` stays the same).

---

## 🟢 Low priority — long-standing TODOs

### Canary Grid Phase 3 — Korea/China exports + Swiss unemployment

The `justhodl-canary-grid` Lambda (leading ex-US early-warning engine, 9 FRED signals, 4 sub-grids → 0-100 early_warning_level) has had 3 signals dark since launch because DBnomics series codes need to be found manually:
- Korea exports
- China exports
- Swiss unemployment (the original FRED `LRHUTTTTCHQ156S` series was dead; freshness guard caught it and auto-excluded)

The engine has a freshness guard that excludes >95d stale signals, so this isn't urgent. But filling in those 3 signals would bring the early-warning grid from 9 → 12 signals.

To find replacement DBnomics codes: visit https://db.nomics.world/ and search by country + indicator name. The DBnomics search API times out from our Lambda runner, so this needs manual lookup. Once found, add the codes to the Canary Grid Lambda's `SIGNALS` dict.

---

## Completed (for audit trail)

_(none yet — file initialized 2026-05-21)_
