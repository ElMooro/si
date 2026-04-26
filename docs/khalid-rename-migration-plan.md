# Khalid → [NEW_NAME] Full Migration Plan

**Status:** Phases 1+2+3 SHIPPED 2026-04-26. Phases 4-8 pending.
**Decided:** new name = 'KA' (initialism).
**Trigger:** user request 2026-04-26 to fully purge personal name from product brand.

## Why this is a multi-step migration, not a sed

Renaming "Khalid Index" sounds like a search-and-replace, but the brand permeates:
- **8 production Lambdas** read or write fields named `khalid_*`
- **3 S3 keys** contain `khalid` in their path
- **15 HTML pages** display "Khalid Index" / read `khalid_*` fields
- **1 Lambda function name** (`justhodl-khalid-metrics`) — renaming this changes its ARN and breaks every EventBridge rule + Function URL pointing at it
- **1 frontend route** (`/khalid/`)
- **DynamoDB items** keyed by `signal_type='khalid_index'` — renaming the partition key in-place is impossible
- **Readers depend on field names** — flipping `khalid_score` → `plumbing_score` mid-flight breaks every consumer until they're all updated

If we change pieces in the wrong order, the system is in a broken state for the duration. The plan below uses **dual-writes + grace period + cutover** to avoid that.

## Naming convention

For this doc, replace `KA` and `ka` with the chosen name.
Examples below assume `KA` = `Plumbing` and `ka` = `plumbing` for illustration.

## Phase 1 — Frontend cosmetic rename ✅ SHIPPED b750453 (2026-04-26)

User-facing strings only. Backend untouched. Page reads still use `khalid_*` field names.

**Files (15):**
- `index.html` — strip-cell label, meta description
- `intelligence.html` — score card label, regime consensus block
- `desk.html` — "Khalid strategy" → "KA strategy"
- `desk-v2.html` — strip-cell label
- `investor.html` — `khalid_score` display label
- `reports.html` — chart label "Khalid Index" → "KA Index", page subtitle
- `khalid/index.html` — page title, logo, all body copy (page route stays `/khalid/` until phase 4)
- `euro/index.html` — logo, localStorage key (rename localStorage key with migration shim)
- `bot/index.html` — `/khalid` slash command stays for now (the Telegram bot also accepts `/ka` as alias in phase 2), command description text changes
- `_partials/sidebar.html` — link label
- `archive/pro.html` — leave (it's archived, label change not worth touching)
- `index-old.html` — leave (archived)
- `downloads.html` — feature description
- `system.html`, `volatility.html`, `dxy.html`, `bonds.html`, `macro-data.html`, `sentiment.html`, `repo.html` — none of these mention Khalid yet
- `flow.html` — none

**Strategy:** display label is decoupled from data field name. Pages can show "KA Index" while still reading `obj.khalid_index` from JSON. This is the cheapest part.

**Rollback:** revert single commit.

## Phase 2 — Add aliases in data layer (dual-write) ✅ SHIPPED 56acd0f (2026-04-26)

Producers (Lambdas) write BOTH old and new keys. Consumers (pages) read either, preferring new with old fallback.

**Lambdas to update (8):**

| Lambda | What changes |
|---|---|
| `justhodl-khalid-metrics` | Output JSON gets `ka_index`, `ka_score`, etc. AS NEW KEYS, alongside existing `khalid_index`, `khalid_score`. Both written each run. |
| `justhodl-daily-report-v3` | Same — duplicate fields in `data/report.json` |
| `justhodl-intelligence` | Duplicate fields |
| `justhodl-pnl-tracker` | If it writes `khalid_strategy` PnL, duplicate as `ka_strategy` |
| `justhodl-asymmetric-scorer` | Reads regime — accept either field on input |
| `justhodl-investor-agents` | Already accepts macro context; update to read either field |
| `justhodl-morning-intelligence` | Update prompt + output to use new names; keep old as alias for 30 days |
| `justhodl-telegram-bot` | Add `/ka` alias to existing `/khalid` command |

**S3 dual-write:**
- Keep writing `data/khalid-metrics.json` (existing path)
- Also write to `data/ka-metrics.json` (new path, identical content)
- Same for `data/khalid-config.json` → `data/ka-config.json`
- Same for `data/khalid-analysis.json` → `data/ka-analysis.json`

**DynamoDB:**
- New writes use `signal_type='ka_index'`
- Existing items with `signal_type='khalid_index'` are NOT migrated (they're historical) — calibrator reads both via OR query
- `justhodl-calibrator` updated to merge both signal_type families when computing weights

**Strategy:** during this phase the system works with EITHER set of names. New name takes priority where a choice is required, but old name still works.

## Phase 3 — Frontend cuts over to new field names ✅ SHIPPED 60f3613 (2026-04-26)

**Files:** every page that currently reads `data.khalid_*` switches to `data.ka_*`. Since phase 2 dual-writes both, this is safe.

**Order matters:** frontend cuts over BEFORE Lambdas drop the old keys, otherwise the page goes blank.

**Rollback:** revert frontend commit; old keys still in S3 from dual-write.

## Phase 4 — Migrate Lambda function name (most disruptive single op)

`justhodl-khalid-metrics` → `justhodl-ka-metrics`

AWS Lambda doesn't support rename. The procedure:

1. **Create** new Lambda `justhodl-ka-metrics` from a copy of the old code
2. **Test invoke** the new one, verify output identical
3. **Move EventBridge** target from old → new
4. **Move Function URL** — actually, Function URLs are tied to the function name. The new Lambda gets a NEW Function URL. Update Cloudflare Worker `AGENT_LAMBDAS` map (we don't have one for khalid currently, but `/khalid/` page reads its Function URL directly — update that page to use the new URL).
5. **Update** `justhodl-khalid-metrics/source/lambda_function.py` in repo → moved to `justhodl-ka-metrics/source/lambda_function.py` via `git mv`
6. **Verify** new Lambda runs on schedule, writes to new + old S3 keys
7. **Wait 7 days** observing both work
8. **Delete** old Lambda `justhodl-khalid-metrics` (preserve via S3 backup of source)

**Rollback in this phase:** EventBridge target can be flipped back to old Lambda within seconds.

## Phase 5 — Frontend route rename

`/khalid/index.html` → `/ka/index.html`

1. **Copy** (don't move) `khalid/index.html` to `ka/index.html`
2. **Update internal links** in topbar, launcher, sidebar
3. **Add 301 redirect** at `khalid/index.html` → `/ka/` (HTML meta refresh, since GitHub Pages doesn't do real redirects):
   ```html
   <meta http-equiv="refresh" content="0; url=/ka/">
   ```
   This handles bookmarks, shared links, search engine results.
4. **Update sitemap** if any
5. After 30 days of no /khalid/ traffic in CloudWatch (CloudFront/Pages logs), delete the redirect stub

**Rollback:** delete the new route, restore links.

## Phase 6 — Drop old field names + S3 keys

Earliest after 30 days from Phase 2 cutover. Verify no consumer still reads old names.

1. **Lambdas** stop writing `khalid_*` fields
2. **S3** stop writing `data/khalid-*.json`
3. **Delete** stale `data/khalid-*.json` files from S3 (or leave them — they'll never be updated again)
4. **DynamoDB** legacy items remain (don't touch historical data, just stop creating new ones with old `signal_type`)

## Phase 7 — Strip Khalid from internal docs + ops history

Once production is fully migrated:
- Rename `khalid` references in `aws/ops/design/`, `aws/ops/audit/`, README files
- Some ops `history/` files mention Khalid as audit trail — leave (history is history)
- Rename `_partials/sidebar.html` references

## Phase 8 — Optional git history rewrite

If user wants the ENTIRE git log scrubbed of `Khalid` (currently 200+ commits mention it), that's a separate destructive operation:

```bash
git filter-repo --replace-text <(echo "Khalid==>KA")
git filter-repo --replace-text <(echo "khalid==>ka")
git push --force origin main
```

This is the same approach as proposed for the email purge. Side effects:
- All commit hashes change (anyone who cloned needs to re-clone)
- GitHub-cached blobs may persist 60-90 days
- CI may need re-bootstrapping

Recommend **only do Phase 8 after Phase 7 settles**, otherwise you'll do it twice.

## Total estimated effort

| Phase | Risk | Effort |
|---|---|---|
| 1. Frontend cosmetic | None | 30 min |
| 2. Backend dual-write | Low | 2 hours |
| 3. Frontend cutover | Low | 20 min |
| 4. Lambda rename | Medium | 1 hour + 7 day wait |
| 5. Route rename | Low | 30 min + 30 day wait |
| 6. Drop old keys | Low | 30 min |
| 7. Internal docs | None | 1 hour |
| 8. Git history rewrite | High | 30 min |

Total active work: ~5 hours coding + ~37 days of grace periods.

## Decision points needed from user

1. **New name** (Plumbing / Regime / JHX / custom?)
2. **Phase 8 yes/no** — rewrite git history once everything else settles?
3. **Telegram bot** — keep `/khalid` command working forever as alias, or drop after grace period?
4. **Trademark** — was "Khalid Index™" actually trademarked? If yes, the rename has legal implications.
