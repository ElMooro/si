# INVEST — Why Leave SPX?

## The question, made quantitative

> Why should I own a stock or ETF if SPX gives the same return, or better, with a lot more safety? Are we expecting a sector to *significantly* outperform SPX, and why?

That is the only question this engine answers. It is not a stock screener that ranks names on fundamentals. It is a **gate**: SPX is the default position, and an industry — then a stock inside that industry — has to earn its way out of the default with evidence that starts upstream of price.

## The three tiers

**Tier 1 — CONFIRM.** A leading trade/commodity indicator (Chile/Peru copper, Korea semiconductor exports, Taiwan export orders, China credit impulse, global port/freight, grid buildout, lumber/housing) only counts as a real demand signal if **≥2 independently-sourced legs** corroborate it, with **≥60% of available legs** agreeing in direction. One noisy print is `TURNING` at best, never `CONFIRMED`. A leg with no live data is `INSUFFICIENT_DATA` for that leg, never a fabricated zero. Some legs are diagnostic-only (`voting=False`) rather than votes — see the Korea value/volume note below, which is the one place this doctrine had to get subtle.

**Tier 2 — GATE.** For every end-use industry a confirmed indicator points to, compute `excess_return_pp = industry_ER − SPX_ER` and `information_ratio = excess_return_pp / tracking_error`. **Both** have to clear a floor (`IR ≥ 0.40` and `excess_return ≥ 300bps/yr` by default) for the industry to pass. This is deliberately two-part: IR alone lets a tiny, low-vol edge through; raw excess return alone ignores that concentration carries risk SPX investors don't. Every number here is `pp_kind: estimated` and carries `ci` + `n_obs` — per the fleet's own `impact_mapper.py` contract, a bare estimated number without those is rejected at construction, not just frowned on.

**Tier 3 — RANK.** Only runs for industries that passed Tier 2. A stock has to beat its **own industry's median composite score by ≥8pp** (backlog growth, valuation discount to peers, contract catalysts, net share retirement, QoQ acceleration) to be picked over just buying the industry ETF. Cyclical names get an explicit peak-margin-trap flag (reused from `justhodl-opportunity-engine`'s cycle-awareness doctrine) — every industry this engine can surface is cyclical by construction, so a low P/E next to peak margins is treated as a trap, not a bargain.

## Khalid's worked example, traced end to end

> Chile copper exports pick up → what's it used in? → track global manufacturing/industrial production → Korea exports and port activity pick up → confirmed demand for memory → compare that industry's growth/P/E to SPX's → then find the stock with higher backlog, lower P/E, bigger contracts than the industry itself.

Mapped onto the tiers: `copper_demand_pulse` and `korea_semiconductor_exports` (Tier 1) → `semis_memory` industry, proxy `SMH` (Tier 2 — see the honest gap below) → stock ranking on backlog/PEG/catalysts within that industry (Tier 3). The Korea leg is Khalid's own remembered example almost verbatim — including the value/volume divergence (export **value** +52.3% vs port **volume** −4.3%) that `justhodl-boom-stage` proved was price-driven demand, not a plateau. `causal_graph.py` carries `korea_port_volume` as a **diagnostic, non-voting leg** for exactly this reason: a soft volume print next to a strong value print is the *signature* of this pattern, not evidence against it. An earlier version of this engine got that wrong — the unit tests (`test_tier1_diagnostic_leg_does_not_veto_a_confirmation`) exist because building the orchestration test against Khalid's own numbers caught it.

## What's new vs. what's reused (the duplication audit)

Before writing any code, the live repo (`github.com/ElMooro/si`, `SYSTEM_CATALOG.md`, and ~15 relevant lambda source files) was read directly. Nothing below duplicates a shipped engine:

| Piece | Status |
|---|---|
| Trade/commodity data ingestion (Korea/Taiwan/China exports, copper, port, freight, grid) | **Already live** — `canary-grid`, `divergence-engine-v2`, `boom-stage`, `portwatch`, `asia-leads`, `freight-pulse`, `grid-queue`. INVEST only reads their S3 output. |
| Sector-vs-SPY expected return (11 SPDR sectors, market-implied) | **Already live** — `justhodl-forward-returns` / `compass.html`. INVEST reads it as the single source of truth for any industry that maps to an SPDR sector; it does **not** recompute ER. |
| Factor→sector-ETF beta regression with n_obs≥8 gating | **Already live** — `justhodl-impact-graph`, 5 factors (`port_throughput_pulse`, `freight_composite_z`, `grid_executed_mw`, `dark_share_median`, `etf_net_flow_usd`). **This is the real gap**: none of its factors are commodity/trade-export data. Recommended next step (not done here, scoped deliberately): extend `impact-graph`'s `FACTOR_HISTORY`/`SECTOR_ETF` with the new legs this engine defines, so there's one beta engine fleet-wide instead of two. |
| Backlog, contract catalysts, "the FIVE" | **Already live** — `backlog-miner`, `justhodl-backlog` (XBRL), `justhodl-catalyst`, `justhodl-stock-buying`. INVEST reads them; it does not re-derive backlog or catalysts. |
| Cycle-awareness (peak-margin trap vs. early-cycle turn) | **Already live doctrine** — `justhodl-opportunity-engine` v2. Reused by name in `scoring.cycle_awareness_flag`. |
| Value-chain tier propagation ("who hasn't moved yet") | **Already live** — `justhodl-rotation-chain`. Different question (rotation timing *within* an already-known theme) from this engine's (*is there a new demand cycle at all*, evidenced by trade data before it's a known theme). Complementary, not overlapping — rotation-chain is a good Tier-3 cross-check once a theme graduates. |
| Company↔company supplier graphs | **Already live** — `supply-chain-graph`, `supply-chain-linkage`. Different axis (named company relationships vs. commodity→industry taxonomy). Good Tier-3 enhancement: once an industry is gated through, cross-reference its named suppliers. Not wired in v0.1 — flagged as an open item below. |
| OUTPERFORM/UNDERPERFORM-vs-SPY grading loop | **Already live** — `signal-logger` → `outcome-checker` → `calibrator`/`engine-trust`, same pattern already used for `attention_stealth` etc. INVEST emits a `grading_candidates[]` block in the exact shape that loop already consumes; it does **not** write to DynamoDB directly, because guessing `signal-logger`'s exact `put_item` schema against an unconfirmed contract is worse than flagging the wiring point. **Open item**, scoped and small: extend `signal-logger` to also read `data/invest.json`. |
| Commodity/leading-indicator → **end-use industry** taxonomy | **Genuinely new.** Nothing in the fleet named which industry a given trade print is actually telling you about. This is `causal_graph.py` — 7 leading indicators, 13 end-use industries, every edge sourced to a real live engine. |
| SPX opportunity-cost gate (IR + minimum excess return, two-part) | **Genuinely new.** `scoring.spx_opportunity_cost_gate`. |
| Stock-vs-own-industry-ETF composite ranking, gated by the industry passing first | **Genuinely new.** `scoring.stock_composite_score` + `vs_industry_etf_verdict`. |

## Honest gaps (do not paper over these)

- **The engine will report `INSUFFICIENT_DATA` for every indicator for roughly the first week after initial deploy — this is expected, not broken.** `confirm_indicator()` only counts a leg as available once it has both a live reading *and* ≥8 days of accrued history to compute a z-score against (mirrors the fleet's own `n_obs ≥ 8` floor). Verified live on 2026-08-15: `read_leg_value()` correctly resolved real numbers from S3 while `available_legs` stayed at 0 for lack of history — five diagnostic ops scripts (4721–4725) ruled out IAM, VPC, bucket policy, code staleness, and environment variables one at a time before landing on the actual, mundane, correct explanation. Once `data/invest/leg-history.json` has 8+ days of rows (one accrues per scheduled run, daily at 15:00 UTC), this resolves on its own.

- **`semis_memory` and other narrow thematic proxies (SMH, SOXX, LIT, ITB, IYT) have no `forward-returns` coverage today.** Tier 2 reports `INSUFFICIENT_DATA` for them rather than forking a second ER formula inside this engine. Recommended fix: extend `forward-returns`' `ASSETS` map to carry these 5 tickers — then INVEST gets them for free and the fleet keeps one ER source of truth. This is the single highest-leverage follow-up.
- **No macro semiconductor billings (SIA) index is live anywhere in the fleet** (confirmed during the audit — only a company-level engine touches it). Not claimed as a leg here.
- **Field names in `causal_graph.py`'s `Leg.source` strings were written from reading engine source code, not from a live invocation.** `ops_4716_invest_probe_fields.py` checks every one of them against live S3 before the main engine is trusted — run it first, per the fleet's own "census probe before code" standing discipline, and fix anything it flags.
- **`industry_boom_label` cross-walks (e.g. `"Industrial Machinery"` for grid/electrical) are best-guess** against `industry-boom.json`'s taxonomy strings — the same probe script checks these too.

## Deploy path: config.json + deploy-lambdas.yml, not a hand-rolled ops script

The first draft of this delivery hand-rolled a Lambda-creation ops script. That was wrong on two counts, both caught before shipping: AUTONOMY.md is explicit that deploy-lambdas.yml (triggered by `aws/lambdas/<fn>/{source/**,config.json}`) is the one deployer — "do not hand-roll deploys" — and it already bundles `aws/shared/*.py` into the zip automatically, which a hand-rolled `build_zip()` call would not have matched exactly. Second, the classic-EventBridge-cap problem is real but already solved fleet-wide: `config.json`'s `.eventbridge_scheduler` block (not the older `.schedule` block, which still calls classic `put_rule`) uses the existing `justhodl-scheduler-role` and the modern Scheduler API — no new IAM role needed. `justhodl-invest/config.json` uses that block directly, matching the pattern already live on `justhodl-52wk-quality-breakout` and others.

`ops_4717` was rewritten from a deployer into what AUTONOMY.md's own known-traps section says an ops script should actually do here: verify the function reached `State=='Active'` (a fresh function can report `LastUpdateStatus='Successful'` while still `Pending`), verify or self-heal the Scheduler entry (a documented trap: the declarative schedule doesn't always materialize on first create), then smoke-invoke and read back `data/invest.json` to confirm a real run, not just a 200.

## Files in this delivery

```
aws/lambdas/justhodl-invest/source/
  causal_graph.py     — the new taxonomy (7 indicators, 13 industries)
  scoring.py           — pure, AWS-free math (Tier 1/2/3) — 25 unit tests, all passing
  fleet_io.py           — defensive S3 reads (fleet:key:path convention) + history accrual
  lambda_function.py    — orchestration + handler
aws/lambdas/justhodl-invest/config.json  — deploy-lambdas.yml reads this; eventbridge_scheduler block, daily 15:00 UTC
aws/lambdas/justhodl-invest/tests/
  test_scoring.py        — pure-logic unit tests
  test_orchestration.py  — end-to-end wiring test against a fake fleet (monkeypatched, no AWS)
  conftest.py
aws/ops/pending/
  ops_4716_invest_probe_fields.py     — read-only field probe against live S3
  ops_4717_invest_verify_and_smoke.py — verifies the deploy-lambdas.yml deploy, self-heals the schedule, smoke-tests
invest.html               — new page, additive, reads data/invest.json through the existing data-proxy
```

All four Python source files: `py_compile` clean. All 25 tests: passing (`pytest aws/lambdas/justhodl-invest/tests/ -v`). Both ops scripts: **validated against the real, live `aws/ops/_preflight.py` pulled from the repo** — `PREFLIGHT PASS (9 files, 0 warnings)`. Nothing in this delivery has touched live AWS — this sandbox has no AWS network egress or credentials; deployment happens for real only once pushed through the pipeline below.

## Deploy — what actually ships this

This was pushed live from the same session that wrote it, in two pushes:

**Push 1 (normal commit, no `[skip-deploy]` — three workflows fire in parallel, none suppressed):**
`aws/lambdas/justhodl-invest/{source/**,tests/**,config.json}` + `invest.html` + this doc. `deploy-lambdas.yml` sees the lambda path and the `config.json` and deploys the function + wires the `eventbridge_scheduler` block; `pages.yml` sees `invest.html` and publishes it; `run-ops.yml` sees `ops_4716_invest_probe_fields.py` and runs the read-only field probe against live S3. `[skip-deploy]` was deliberately **not** used here — it would have suppressed `pages.yml` too (a documented trap that once cost six ops silently).

**Push 2, after reading push 1's results:** `ops_4717_invest_verify_and_smoke.py` — confirms the function is `Active`, confirms/heals the Scheduler entry, smoke-invokes, reads back `data/invest.json`.

If picking this up cold in a fresh session instead: bootstrap per the standing protocol, confirm `STATE.md`'s `next_free_ops_number` and `aws/ops/pending/` + `aws/ops/ran/` for collisions (never trust a remembered number), `python aws/ops/_preflight.py` the changed paths, then push in the same two-step order above.

## Open items (not done here, scoped on purpose)

1. Extend `forward-returns`' `ASSETS` to include SMH/SOXX/LIT/ITB/IYT — unblocks Tier 2 for the narrow thematic industries, including Khalid's own memory/semis worked example.
2. Extend `impact-graph`'s factor set with the new commodity/trade legs, so Tier 1→Tier 2 betas come from the fleet's one beta engine instead of INVEST's own lightweight IR gate.
3. Extend `signal-logger` to read `data/invest.json`'s `grading_candidates[]` — plugs INVEST into the existing `outcome-checker` → `calibrator` closed loop for free.
4. Wire `supply-chain-graph`/`supply-chain-linkage` into Tier 3 for gated-through industries — named-supplier confirmation on top of the backlog/catalyst/valuation composite.
5. Nav link to `/invest.html` from the other pages' headers — not touched here to avoid a wide, unrelated diff; `invest.html` itself is fully additive and live the moment it's pushed regardless.
