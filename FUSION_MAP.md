# JustHodl fusion map — Grok pass 2026-09-11

Fleet size: 333 Lambdas (SYSTEM_CATALOG.md). This is **not** a rewrite of every engine. Doctrine: warehouse-only joins, only fields a consumer actually needs, fail-soft if a feed is missing, never fabricate.

## Already fused (do not rebuild)

| Consumer | Inputs | Role |
|---|---|---|
| `justhodl-best-ideas` | 20 opportunity engines → `data/best-ideas.json` | Name-level multi-family confluence (PROTECTED SPECS — extended, not rewritten) |
| `justhodl-signal-board` | 7+ engines | Cross-asset signal store |
| `justhodl-conviction-engine` | opportunity stack | Conviction board |
| `justhodl-master-ranker` | name ranks | Cross-engine rank |
| `justhodl-stock-buying` | census + SMA + industry-boom + deal-scanner + FMP + base-rates | Flagship screener |
| `justhodl-risk-gate` / risk desk | stress + vol + plumbing | Size / risk-off |

## What each family should consume (needed fields only)

### Name-level opportunity engines
Need from **peers**, not a dump:
- `symbol` / `ticker`
- one score
- generated_at
- optional sector

**Join into Best Ideas (done this pass):**
- stealth-accumulation → SMART_MONEY
- options-flow unusual → FLOW (new family)
- squeeze-pretrigger → RISK (new family; crowding, not a quality vote)

**Haircut overlays (done this pass — not extra family hits):**
- `data/bond-vol.json` + `data/regime.json` → multiply conviction when risk-off / MOVE-like ≥ 130
- `data/beneish.json` manipulators + `data/sec-filings-intel.json` critical/high → 0.75 haircut + annotate

**Do not join (wrong grain or already consumed):**
- ETF-level flows onto a single ticker without a holdings map
- COT / eurodollar / auction crisis onto a name score
- Full 13F holdings blobs (use `most_bought` only)
- LLM briefs as numeric inputs

### Macro / liquidity engines
Should read:
- `data/regime.json` or bond-vol composite (one state)
- NFCI / plumbing / repo **from warehouse**, not a second FRED pull
- Each other only for the **canonical regime bit**, not full documents

### Crypto engines
Should share:
- CoinGecko global snapshot already in warehouse (stop extra `/global` calls)
- stablecoin-flow + funding + ETF arb as **overlays** on crypto-opportunities, not four unrelated ranks

### Squeeze stack
Need:
- FINRA short **volume** (live) + float (shares-float)
- Not Polygon `/stocks/v1/short-interest` (dead post-2018)
- Options unusual as confirmation only

### Risk / allocator
Need from opportunity engines: symbol + conviction_score + families_hit
Need from regime: one risk-off flag
Must **not** re-score names.

## Bugs found this pass

| Engine | Bug | Fix |
|---|---|---|
| `justhodl-stock-buying` | FMP still called `financialmodelingprep.com/api/v3/` (dead since 2025-08-31). Warm cache hid it until TTL. | v1.5.2 maps legacy path-style onto `/stable/?symbol=` |
| `justhodl-shadow-lab` | `/stable/` first, `/api/v3/` fallback — fallback always 403 | leave; fallback is wasted budget, not primary |
| `justhodl-apac-flows` | same v3 fallback after stable | leave this pass |
| Best Ideas | 20 engines, no FLOW/RISK, no regime temper, no forensic veto | SPECS + overlays, schema 1.1-grok-fusion |
| Fleet | 368 pending ops already in queue | **no new pending op this push** (would cancel queued run) |

## Known upstream holes (not code bugs)

- FINRA Gateway SI still waiting on Khalid creds (`KHALID_ACTIONS.md`)
- PatentsView key dark
- Polygon short-interest dead
- FMP `/api/v3` and `/api/v4` dead — any remaining primary v3 call is a bug

## Next passes (one engine family per push)

1. Verify Best Ideas harvest paths (`stealth-accumulation` / `options-flow` / `squeeze-pretrigger` list keys) via a later ops once the pending queue drains
2. Kill leftover FMP v3 fallbacks in shadow-lab + apac-flows
3. Wire stock-buying quality pillar to backlog/RPO + Beneish as modulators only
4. Crypto: one shared `data/crypto-global.json` consumer
5. Do not touch Perplexity lane (`justhodl-khalid*`, `khalid.html`)

## Proof

- Companion commit deploys `justhodl-best-ideas` + `justhodl-stock-buying` via `deploy-lambdas.yml`
- Proof of fusion = next `data/best-ideas.json` has `schema_version: 1.1-grok-fusion` and `overlays` object
- Proof of FMP fix = stock-buying warm keys refresh without 403 (watch `data/stock-buying.json` generated_at)
