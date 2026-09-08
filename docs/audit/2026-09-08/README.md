# JustHodl institutional audit 2026-09-08 — engineering program

Source: `JustHodl-Audit-2026-09-08.md` (uploaded by Khalid; pinned to `34ddd51`, which was HEAD when
the program started, so every line reference in it applies verbatim). This file is the single
tracker: one row per finding, its owner boundary, the release it ships in, and the evidence.

Boundary doctrine: `justhodl-khalid`, `justhodl-khalid-risk`, `justhodl-engine-fusion` and their
pages are built by Perplexity and are **not edited here**; findings inside that boundary are
reported with the reproduction, not fixed.

## Releases

| Release | Scope | Ops | Status |
|---|---|---|---|
| **A** | Security containment (INST-01/02/03/04/05/06), capital authority (FR-01/02), fusion read-API readiness (D1–D4), release gates (INST-14), 7 page mislabels + `jh-wire.js` guard | 5219 containment, 5220 gate | shipped in this push — see ops reports |
| **A2** | Fleet credential sweep (`scripts/scrub_literal_keys.py`, 358 engines / 418 sites / 111 config env blocks / ~250 historical ops scripts) then provider-key **rotation** (Khalid, provider dashboards) | 5221+ | queued: batched pushes after the Release A gate is GREEN |
| **B** | Capital + risk maths: FR-03/04/05 (risk-sizer), FR-06/07/08/09 (risk-gate replay, deductions, monthly Sahm/truck, page schema) | — | next |
| **C** | Accounting + historical truth: INST-07/08 (portfolio-admin/snapshot), INST-09/10/11 (calibration-snapshotter/backtest-engine/research-backtest), INST-12 (outcome-checker), INST-13 (freshness monitor) | — | after B |
| **D** | Ownership + display contracts: producer contracts replacing heuristic wiring, per-output/per-page states in the engine directory, directory routes + script graph, generator atomicity, `jh-wire` typed drilldowns | — | after C |
| **E** | Curated donor→recipient contracts (34) with ablations; genuinely new production inputs (broker reconciliation, NBBO, borrow, WI quotes, point-in-time reference data) | — | after D |

## Findings

| ID | Sev | Finding | Boundary | Release | Status | Evidence / acceptance |
|---|---|---|---|---|---|---|
| INST-01 | P0 | Brain/Journal authorization by uid length; debug enumerates identities | Claude | A | **FIXED** | data-proxy v2.1.0: identity only from verified Supabase token or service secret; roles owner/user/service/anon; anonymous → 401; `/brain-debug`, `/brain-purge`, `?build/?dedup` role-gated; PIN retired as auth. `tests/worker-data-proxy.test.js` (12) + ops 5220 A |
| INST-02 | P0 | Anonymous userdata falls back into `u:<uid>` | Claude | A | **FIXED** | fallback removed; service-only `/admin/userdata-migrate` moves legacy blobs only when Supabase confirms the uid is not an account (ops 5219 dry+real) |
| INST-03 | P1 | Origin header grants ENTERPRISE, rate table fails open | Claude | A | **FIXED** | `api_auth.py`: Origin selects metered SITE tier by client IP; degraded in-process counter, never unlimited. `tests/deployment/test_api_auth.py` (5); ops 5220 B burst probe |
| INST-04 | P1 | Checkout trusts body user/plan | Claude | A | **FIXED** | verified buyer, `PRICE_PLAN_MAP` allowlist, pinned return host |
| INST-05 | P1 | Webhook acks failed persistence | Claude | A | **FIXED** | idempotent `stripe-evt:<id>` inbox, plan from live subscription items, 500 on failed durable write, unmapped prices never change plan |
| INST-06 | P1 | Literal credentials in source | Claude | A + A2 | **PARTIAL** | A: 5 audited sites + 3 shared modules + worker purge literal + wrangler key literals → `managed_secret`/SSM/Worker secrets; ops 5219 seeds canonical SSM params from live envs. A2: the audit undercounted — same 4 keys in 358 engines, a Telegram bot token in 87 places, Anthropic/NewsAPI/Census keys in config blocks; scrub tool validated (0 compile failures, 0 residuals on a scratch copy). **Rotation is Khalid's action after A2 lands.** Client-side FMP key in `journal.html` and other pages is a separate class (browser-exposed by design) → Release D proxying |
| INST-07 | P1 | Quantity/cost edits corrupt P&L, stop side never flips | Claude | C1 | **FIXED** (push pending A2) | read-validate-write: any partial edit recomputes cost_basis_total and the side from the sign; finite inputs only; absent positions refused (no upsert); conditional write detects concurrent edits. `aws/lambdas/justhodl-portfolio-admin/tests/run_tests.py` (3); ops 5224 |
| INST-08 | P1 | Missing price becomes cost | Claude | C1 | **FIXED** (push pending A2) | snapshot never substitutes cost for a mark: valuation_status PRICED/STALE_MARK/UNPRICED per position, market_value/P&L/stop_hit null when unpriced, basis always qty x unit cost, side from sign, summary P&L scoped to the priced sleeve with unpriced_positions + stops_not_evaluable published, mark provenance (provider, as-of) carried |
| INST-09 | P1 | Walk-forward weights backdated | Claude | C3 | **FIXED** (push pending A2) | snapshotter v2: immutable `calibration/versions/<snapshot_id>.json` + `calibration/index.json`, `available_at`/`calibrated_at`/`training_end_at`/model+code versions; backtest selects the newest snapshot with `available_at <= logged_at TIMESTAMP` (a Sunday snapshot never covers that week's Tuesday trade; legacy snapshots dated by their as_of). `aws/lambdas/justhodl-backtest-engine/tests/run_tests.py` (2) |
| INST-10 | P1 | NAV books full horizon on entry day | Claude | C3 (interim) / C4 | **PARTIAL** | the curve is now labelled `curve_semantics: signal_attribution`, `tradable_portfolio_nav: false`, `headline_eligible: false`, Sharpe labelled accordingly; backtest.html promise corrected. A daily-marked, capital-constrained ledger (positions, cash, overlaps, costs) is C4 |
| INST-11 | P1 | Research attribution joins current critique | Claude | C3 | **FIXED** (push pending A2) | critique joined only when its generated_at/available_at <= the call's decision time (else unknown); regime only from the entry snapshot (no latest-doc fallback); ensemble "alpha" claim gated on Welch t (|t|>=2), n>=20 per group and critique coverage>=50%, CI published. `aws/lambdas/justhodl-research-backtest/tests/run_tests.py` (3) |
| INST-12 | P1 | Outcome grading depends on job delay | Claude | C2 | **FIXED** (push pending A2) | checker-v4: every elapsed window graded at its own session close (never a live quote); asset + benchmark marks from one function (house warehouse `data/warm/polygon-full/grouped` first, Yahoo fallback) with provider/as_of provenance and `graded_at_session`; missing marks stay `_pending` with an attempts counter, UNSCOREABLE after 12 misses, never a zero excess return. `aws/lambdas/justhodl-outcome-checker/tests/run_tests.py` (3); ops 5225 |
| INST-13 | P1 | Freshness monitor marks empty artifacts healthy | Claude | C2 | **FIXED** (push pending A2) | v2.0: scoped bodies validated (EMPTY / INVALID / future timestamp / SOURCE_STALE = rewritten with old data), artifact_age_h vs source_age_h published, expected outputs from engine-manifest compared to actual keys (MISSING when previously seen, DECLARED_ABSENT otherwise), enumeration truncation reported, coverage block. `aws/lambdas/justhodl-fleet-freshness-monitor/tests/run_tests.py` (5); ops 5225 |
| INST-14 | P1 | Release gates cover a small subset | Claude | A | **FIXED** | deploy-workers: required test job (`tests/worker-*.test.js`, syntax check every worker); pages.yml runs `tests/*.test.js` + blocking literal gate; deploy-lambdas runs every changed engine's `tests/run_tests.py` (or pytest); `tests/deployment` runs every `test_*.py` |
| FR-01 | P1 | Katlin bypasses the authoritative risk artifact | Claude | A | **FIXED** | Katlin v2.3.0 consumes `data/khalid-risk.json`; effective cap = min(authority, desk, gate); `allows_new_entries=false` demotes PRIME/READY + empties basket; page shows authority / desk / gate separately. `aws/lambdas/justhodl-katlin/tests/run_tests.py` (7); ops 5220 D |
| FR-02 | P1 | Stale/missing risk → FULL_RISK; zero multiplier ignored | Claude | A | **FIXED** | missing/stale/invalid authority or gate → DATA_HOLD 0%; `0 <= sz` honoured; no legs → 0% not 25% |
| FR-03 | P1 | Risk-sizer breaches its 8% single-name cap | Claude | B1 | **FIXED** (push pending A2) | v2.0: cap applied after the quality tilt, drawdown and gate multipliers, cluster/gross scaling and rounding; `final_constraint_check` asserted. `aws/lambdas/justhodl-risk-sizer/tests/run_tests.py` (7); ops 5222 |
| FR-04 | P1 | Risk-sizer separate authority, ignores portfolio state | Claude | B1 | **FIXED** (push pending A2) | binds to `data/khalid-risk.json` (min cap, entry prohibition, missing/stale = HOLD), risk-gate sizing multiplier (zero honoured), nets the real book `portfolio/snapshot.json` (`portfolio/state.json` was never written by anyone), empty pipeline writes an explicit NO_IDEAS artifact; risk.html shows the authority + hold reasons |
| FR-05 | P1 | Zero NAV discards drawdown brake | Claude | B1 | **FIXED** (push pending A2) | `is not None` + finite/non-negative; <2 snapshots = UNKNOWN = hold; the page shows UNKNOWN (never 0.00%) and the BINDING trigger |
| FR-06 | P1 | Risk-gate replay reads today's feeds | Claude | B2 | **FIXED** (push pending A2) | v2.5: `compute_posture` is pure (no S3); the two overlays are read once in the handler and applied to the live composite only; `replay_*_fred_only` labels now true; thousands of serial S3 reads per run gone. `aws/lambdas/justhodl-risk-gate/tests/run_tests.py` (6); ops 5223 |
| FR-07 | P1 | Risk-gate live drops annotated deductions | Claude | B2 | **FIXED** (push pending A2) | `overlays[]` published with contribution/status/age/eligible; `composite_identity` asserts composite = weighted legs + overlays; stale/missing overlays never apply |
| FR-08 | P1 | Monthly Sahm/truck from daily forward-fill | Claude | B2 | **FIXED** (push pending A2) | native monthly series kept before forward-fill; Sahm = 3-mo avg minus min of the prior 12 3-mo avgs (official); truck YoY vs the observation 12 calendar months earlier; observation dates published; insufficient history = pending, never zero |
| FR-09 | P1/P2 | risk-gate.html schema mismatch | Claude | B2 | **FIXED** (push pending A2) | producer publishes engine_score / fleet_fused_score / state / drivers per leg, `fleet_context.inputs` dict, `schema_version risk-gate.v2.5`; page reads the real fields and gains the nine-indicator and overlays panels |
| FR-10 | P1 | Governed fusion rejects credit contract | **Perplexity** | — | reported | `fusion_engine.py:63-65` range-checks absent alternatives |
| FR-11 | P2 | Governed fusion always DEGRADED | **Perplexity** | — | reported | |
| FR-12 | P2 | Khalid Risk page hides fields | **Perplexity** | — | reported | |
| D1–D4 | P1/P2 | Fusion read API: empty docs healthy, list route throws, stale labels eligible, blocked rows returned | Claude | A | **FIXED** | `fusion_api.js` v1.1 readiness object, 503 no-store on invalid, `EXPIRED` labels, actionable-only opportunities. `tests/worker-fusion-api.test.js` (6); ops 5220 C |
| C-1 | P1 | 7 cards label upstream inputs as engine outputs | Claude | A | **FIXED** | repointed to each engine's real output (verified write sites); ops 5220 E |
| C-2 | P1 | Generic cards cannot show every field | Claude | A (honesty) / D (drilldowns) | **PARTIAL** | `jh-wire.js`: rows/columns shown vs available stated, hidden arrays/objects counted, raw-feed link; typed drilldowns + pagination in D |
| C-3 | P1 | WIRED status false positives | Claude | D | open | |
| C-4 | P1 | Directory misses directory routes / script graph | Claude | D | open | |
| C-5 | P1 | Manifest false negatives/positives | Claude | D | open | |
| C-6 | P1 | Wiring generator overwrites newer assignments | Claude | D | open (do not rerun) | |
| C-7 | P2 | `[null]` breaks the renderer | Claude | A | **FIXED** | null-first-row guard |
| Data | P1 | Liquidity spread proxy is daily range | Claude | E | open | |
| Home | P1 | 50% vs 75% competing authorities | Claude (Katlin side) | A | **FIXED** via FR-01 | homepage embed of Katlin now shows the authority cap |

## Needs Khalid

1. **Rotate** the FMP, Polygon, FRED, CoinMarketCap keys and the Telegram bot token in the provider
   dashboards **after** Release A2 lands (every consumer reads SSM by then). Then paste nothing here —
   run `ops_52xx_rotate_keys.py` which updates SSM + every Lambda env from the parameter values.
2. Confirm `raafouis@gmail.com` is the Supabase account you sign in with; ops 5219 binds it as the
   Brain/Journal owner and fails loud otherwise.
