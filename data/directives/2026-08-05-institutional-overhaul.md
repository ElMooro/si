# DIRECTIVE TO CLAUDE — Fix the Audit + Institutional-Grade Overhaul of Every Engine & Every Page

**From:** Perplexity (full_stack_unrestricted)
**To:** Claude (backend/main engineer)
**Authority:** Khalid mandate, Aug 5 2026 · verbatim: *"you need to audit my entire system all the engines. bcs you will find tons and tons of data and indicators and metrics that would enrich a lot of the engines you try to enhance and you can create even more indicators from there"* + *"add indicators, the visuals is poor me i like barometer and charts that highlight crisis dating at least back to 1996"* + *"i like visuals to help me understand and interpret data easily and i like historical context and charts and where the current engine data sits compared to market top and market bottoms and crisis"*
**Standard:** Google/Microsoft institutional quality. No debug pages. No orphan feeds. Every page tells a decision-ready story.
**Reference audit:** `data/audit/system-audit.md` on GitHub (397 lines, 10 findings, 20 prioritized actions).

---

## PART 1 — Fix the audit findings (F1-F10)

### P0 broken joins (fix in the next 24-48h)

**F1 — Fails-to-deliver join broken (VERIFIED LIVE)**
- Consumer: `aws/lambdas/justhodl-risk-gate/source/lambda_function.py` **L463-467**
- Reads: `ofr.get("fails_cross")` → `None`
- Producer publishes: `ofr-stfm.json → nypd_fails_cross.ftd_tot = $156.6B` (live)
- **Fix (~6 LOC):** replace L464 with
  ```python
  v = (((ofr or {}).get("nypd_fails_cross") or {}).get("ftd_tot") or {}).get("latest")
  ```
  and delete the "stays MISSING honestly" clause in L467.
- **Test:** After deploy, funding leg indicator `fails_cross_z` populates with non-null value; rolling z-score computable from `ftd_tot.series`.
- **Why it matters:** Fails-to-deliver is Khalid brain L34 (*"fails = collateral scramble"*). This alert has never fired in production despite the data being shipped for months. Silent blindness on the single most reliable liquidity-crisis canary.

**F2 — Crisis composite join broken (VERIFIED LIVE)**
- Consumer: `justhodl-risk-gate/lambda_function.py` **L731-734**
- Reads: `crisis.get("composite") or crisis.get("headline")` → both `None`
- Producer publishes: `crisis-composite.json → master_crisis_score=28.7, defcon_level=4, defcon_name="NORMAL"` (live)
- **Fix (~10 LOC):** L734 becomes
  ```python
  "crisis_composite": (crisis or {}).get("master_crisis_score"),
  "defcon_level": (crisis or {}).get("defcon_level"),
  "defcon_name": (crisis or {}).get("defcon_name"),
  "defcon_color": (crisis or {}).get("defcon_color"),
  ```
  Update any downstream consumers expecting `.composite` naming (`grep -rn "fleet_context.crisis_composite" aws/lambdas/`).
- **Test:** `fleet_context.crisis_composite` matches live `master_crisis_score` (~28.7); DEFCON fields present.

**F3 — Breadth leg permanently null (VERIFIED LIVE)**
- Consumer: `justhodl-crisis-composite/source/*.py` **L168-170** (`comp_internals`)
- Reads: `dig(d, "breadth_score", "score")` → `None`
- Producer publishes raw counts only: `ADVANCERS, DECLINERS, TRIN, NEW_HIGHS/LOWS, PCT_ABOVE_50DMA (51.1), PCT_ABOVE_200DMA (50.13)` — no computed `breadth_score`.
- **Fix (~20 LOC, producer-side preferred):** In `justhodl-market-internals`, add top-level `breadth_score`:
  ```python
  latest = d["latest"]
  adv = latest["ADVANCERS"][1]; dec = latest["DECLINERS"][1]
  nh  = latest["NEW_HIGHS"][1]; nl  = latest["NEW_LOWS"][1]
  p50 = latest["PCT_ABOVE_50DMA"][1]; p200 = latest["PCT_ABOVE_200DMA"][1]
  ad_pct = 100 * adv / (adv + dec) if (adv+dec) else 50
  hl_pct = 100 * nh  / (nh  + nl ) if (nh + nl)  else 50
  breadth_score = 0.40*p200 + 0.30*p50 + 0.15*hl_pct + 0.15*ad_pct   # 0-100, higher = healthier
  d["breadth_score"] = round(breadth_score, 2)
  d["breadth_components"] = {"p200":p200,"p50":p50,"hl_pct":round(hl_pct,2),"ad_pct":round(ad_pct,2)}
  ```
- **Test:** `crisis-composite.components_available == 14`; breadth leg score is non-null.

**F4 — Four indicators declared "pending source" but 4 live engines already produce them (VERIFIED LIVE)**
- Consumer: `justhodl-risk-gate/lambda_function.py` **L215, L245, L251, L258, L286, L306, L327**
- Real live producers (all confirmed shipping):
  - `xcc_basis` → `crisis-plumbing.json → xcc_basis_proxy` (or promote `xccy-basis-agent` output to public path)
  - `sovereign_cds_basket` → `cds-monitor.json → sovereign_cds`
  - `skew_percentile` → `vol-radar.json → spike_canaries` + `scores`
  - `hy_ig_skew` → compute in-place from `credit-stress.json` (HY OAS) minus `fred-cache.json` (BAML IG OAS)
- **Fix:** Replace each `pending_source` block with real reader. Estimated 40 LOC total across 4 indicators. Keep placeholder pattern for genuinely missing sources (Howell GLI is proprietary — keep that one).
- **Test:** risk-gate `n_live_indicators` jumps by 4; `pending_source` count drops from 8 to 4.

**F5 — OFR FSI ignored (VERIFIED LIVE)**
- `ofr-stfm.json → fsi.latest = -2.116` with 5 components + 26-year history — consumed by nothing.
- **Fix:** Add to `justhodl-risk-gate` funding leg AND as component in `crisis-composite`. 30 LOC.
- **Bonus:** Publish `ofr.html` page with FSI history back to 2000 + component breakdown (Volatility, Funding, Credit, Safe Assets, Solvency & Leverage).

**F6 — Event-study math is symptom of F1/F2**
- risk-gate self-reports `avg_spx_fwd_21d_while_risk_off = 1.96%` vs `baseline 0.80%` over 35 flips.
- RISK_OFF is anti-predictive because the composite is running blind on 3 broken legs (F1, F2, F3).
- **Fix:** Once F1/F2/F3 ship, rerun `justhodl-risk-gate-backtest` (already in repo) and confirm avg fwd 21d flips negative during RISK_OFF windows. If it doesn't, the WEIGHTS need re-fitting — see Part 3.

**F7 — domain-barometers breadth corrupted**
- `JPBOT chg_pct = -1062%` (level series treated as percentage). MACRO breadth 4.2 while gate breadth is 68.8 → false disagreement flag.
- **Fix:** In `justhodl-domain-barometers`, mark level-series variables (`JPBOT`, `BOGMBBM`, `TOTRESNS`) and use z-score or Δ%-of-history instead of raw % change. ~15 LOC.

**F8 — ~200 orphan feeds, 35 from >200-LOC engines**
- 6 already wired by Perplexity (PR #6 merged). Remaining ~194.
- **Fix (parallelizable):** Batch-assign feed→page mappings; ship in weekly PR waves of 20-30 wirings each. Perplexity owns the frontend wiring — Claude does not need to touch this. But: audit which "orphan" engines are actually **dead** (no S3 output) vs **dark** (fresh S3 output, no page). Kill the truly dead ones.

**F9 — 142 engines call FRED directly; only 7 use `fred-cache.json`**
- DGS10 fetched 82 separate times per day; BAMLH0A0HYM2 75×. Real waste + rate-limit exposure.
- **Fix:** Build a `jh_common/fred.py` layer with `get_series(mnemonic)` that reads `fred-cache.json` first, falls back to live FRED with backoff. Migrate the top-20 FRED-callers first (dgs10/BAMLH0A0HYM2 alone = 157 calls/day). ~40 LOC library + ~5 LOC/engine migration.
- **Bonus:** Same treatment for Telegram — 188 engines have their own sender. Consolidate to `jh_common/notify.py`.

**F10 — 0 red-alert conjunctions computed**
- Brain L879 defines 7 red-alert conjunctions (e.g. *"BKX drops 5%+ AND HY spread widens 100bp+ AND DXY spikes 2%+"*). NONE are computed as AND-gates. Every engine sums or averages.
- **Fix:** Build `justhodl-red-alert-conjunctions` engine (~200 LOC). One clean output: `conjunctions: [{name, components:[{indicator, threshold, current, fired:bool}], all_fired:bool}]`. Render as a fixed 7-row table on `risk-gate.html`.

---

## PART 2 — The institutional-grade standard (apply to EVERY engine and EVERY page)

This is the Google/Microsoft bar. Every engine and page must clear all seven tests below. If it doesn't, it isn't finished.

### The 7-question institutional audit (applied to every engine)
1. **Purpose:** What decision does this engine drive? (Written at the top of every engine's docstring AND the top of every page.)
2. **Google/MS quality:** If a Bridgewater PM or Renaissance quant opened this today, would it look right or amateur? (No debug JSON dumps. No `undefined%`. No empty sections.)
3. **Bugs:** Are joins correct? Are units correct? Are timestamps monotonic? Is the fallback path exercised?
4. **Data completeness:** Are ALL inputs the engine claims to use actually reaching it? (No `pending_source` when a real source exists.)
5. **Display completeness:** Is ALL data the engine produces visible on the page? (No dark outputs. No S3-only feeds.)
6. **Improvements:** What indicator would a top-tier desk add here that we're missing?
7. **Historical & predictive test:** Would this engine have fired the right posture at every crisis since 1996? (2000 dotcom, 2008 GFC, 2011 EU sovereign, 2015 CNY, 2018 Q4, 2020 COVID, 2022 rates, 2023 SVB.) If we can't backtest it, it's not ready.

### The visual standard (applied to every page)
Every page must contain, at minimum:

1. **Barometer / gauge at the top** — one number, one color, one 3-word verdict (RISK_ON / NEUTRAL / RISK_OFF style). No "loading…", no NaN, no undefined.
2. **1996+ regime strip** — horizontal timeline with 8 shaded crisis bands (Asian, LTCM, dotcom, GFC, EU sovereign, CNY, Q4-18, COVID, SVB, 2022-rates). Current level marked with a diamond.
3. **Historical context chart** — where does today's value sit vs market tops, market bottoms, and each crisis? Percentile bar (e.g. *"today = 42nd percentile of the last 30 years; GFC peak was 99th, dotcom bottom was 3rd"*).
4. **Component decomposition** — for any composite, show the legs (weights + current contribution). No black-box scores.
5. **Predictive read** — what does today's value historically forecast for SPX at +21d, +63d, +252d? Include the sample size and hit rate.
6. **Data provenance footer** — every feed URL + last-updated timestamp + engine name. Broken feed = red badge with actionable message ("Feed unreachable. Producer: `justhodl-<name>`. Last known good: 4h ago.")
7. **Khalid-rules callout** — surface the relevant brain quote (e.g. *"DXY is the most important chart to watch. PERIOD!"*) inline where the indicator lives, with brain line number.

### The engine standard (applied to every engine)
1. **Idempotent output shape** — every engine writes `{schema_version, generated_at, engine, source, data..., health}`.
2. **Explicit units** — never a naked number. Every value has `{value, unit, asof, source}`.
3. **z-score + percentile alongside raw value** — makes historical context automatic downstream.
4. **`crisis_history`** field — engine's own posture during each crisis since 1996 (as computed by backtest). Publish once per week from a backtest Lambda; consumers read as static.
5. **`health`** field — `{ok_blocks:N, errors:[], stale_feeds:[]}`.
6. **Deprecate direct FRED calls** — use `jh_common/fred.py` (see F9).
7. **Contract test in `.github/`** — CI page-gate blocks any regression that removes a documented field.

---

## PART 3 — Engine cluster upgrade specs

The 783 engines cluster into ~40 desks. Below is the institutional bar for each. Every engine in the cluster must satisfy every bullet. Missing indicators are labeled `[GAP]` and represent net-new build work.

### 3.1 · Risk-gate cluster (2 engines)
`justhodl-risk-gate` + `justhodl-risk-gate-backtest`

**Inputs required (all live-verified):**
- Funding leg: SOFR-IORB, FRA-OIS, XCC basis (EUR, JPY), fails-to-deliver z, GC-repo, OFR FSI + 5 components, plumbing composite.
- Credit leg: HY OAS, IG OAS, HY-IG skew, HYG flows, HYG issuance, sovereign CDS basket, single-name CDS spikes, credit-equity divergence.
- Dollar leg: DXY 5d/20d %, DTWEXBGS, EM FX (CEW) [GAP], carry unwind (AUDJPY tripwire) [GAP], EURSGD/EURHKD [GAP].
- Vol leg: VIX, VXVCLS, VIX term structure, SKEW percentile, MOVE, gold vol, crypto vol.
- Breadth leg: PCT_ABOVE_200DMA, breadth thrust, BKX-SPX relative [GAP], sector participation.
- Rates leg: 2s10s, T10Y5Y [GAP], swap spreads (2y/5y/10y) [GAP], SOFR curve inversion [GAP], ACM term premium, Sahm rule (real, not pending).

**Composite math:**
- Weighted composite with weights re-fit against 1996+ crisis outcomes (F6 event-study fix).
- 7 red-alert conjunctions (AND-gates from brain L879) surfaced as top-level `conjunctions[]`.
- Sequencing score: `stage ∈ {NONE, CREDIT_ONLY, CREDIT+DOLLAR, FULL_SEQUENCE}` per brain L879 (*"Credit stress first, dollar spike second, stock crash third"*).

**Backtest requirements:**
- Fire the correct posture at every crisis since 1996 (list in Part 2, standard #7).
- Publish `crisis_history` field: engine's posture on each crisis's peak-stress day.
- Publish `forward_returns` table: avg/median SPX at +21d/+63d/+252d for each posture, sample size, hit rate.

### 3.2 · Crisis-composite cluster (3 engines)
`justhodl-crisis-composite` + `justhodl-crisis-plumbing` + `justhodl-crisis-sequence` [NEW P1-9]

**Fixes:** F2, F3 above.

**New components to add:**
- Breadth leg (F3 fix).
- Sovereign CDS basket leg (import from cds-monitor).
- FX carry unwind leg (AUDJPY 5d %, CHFJPY 5d %, USD/JPY 20d z).
- Waterfall-selloff symmetry (brain: crisis has *"symmetric waterfall selling"* — SPX 5d % vs 20d %).
- Gold-bottoms-first clock (brain: *"gold bottoms 3-6 months before stocks"*).

**Visual on `crisis.html`:**
- DEFCON gauge (already exists on risk-gate — mirror here).
- 14-component legs bar chart (weights + current stress).
- 1996+ history strip with 8 crisis bands + DEFCON overlay.
- 7 red-alert conjunctions table with current state.

### 3.3 · Liquidity cluster (7 engines — consolidation candidate)
`global-liquidity`, `global-tide`, `liquidity-agent`, `liquidity-credit-engine`, `plumbing-radar`, `eurodollar-plumbing`, `cb-injection-monitor`

**Rationalize:** These 7 all compute variants of "global liquidity is up/down". Pick `global-tide` as canonical; others become internal producers.

**Required indicators:**
- Global CB balance sheets (Fed WALCL, ECB, BOJ, PBOC).
- SOMA / TGA / RRP (already have via `treasury-noise`).
- BIS cross-border flows (already publishing).
- Real yields (TIPS 10y, 5y).
- USD swap lines usage (FRED SWPT).
- Reserves composite (TOTRESNS + BOGMBBM + CASACBW027SBOG per brain L14).
- [GAP] Howell GLI proxy — brain calls this out. Build a proxy from CB balance sheets + FX reserves + $-liquidity.
- [GAP] Marginal liquidity (Δ 4w vs Δ 52w) with color-coded expansion/contraction.

**Backtest:** Every crisis since 1996 had a liquidity signal 30-90d before equity peak. Prove it.

### 3.4 · Credit desk cluster (5 engines)
`credit-stress`, `credit-before-equity`, `credit-equity-divergence`, `cds-monitor`, `hy-flows-monitor`

**Required indicators:**
- HY OAS + IG OAS + HY-IG skew.
- Sovereign CDS (US, DE, IT, ES, JP, EM basket).
- Single-name CDS (top 20 by weight in HYG).
- HYG/JNK flows + issuance (SIFMA M7).
- Credit-equity divergence z-score (leads equity per brain).
- [GAP] Swap spreads (2y/5y/10y) — brain L845 flag #3.
- [GAP] STIR-curve inversion test (Eurodollar futures) — brain L845 flag #1.
- [GAP] Muni-Treasury ratio z-score with historical context.

**Visual on `credit-desk.html`:**
- OAS gauge with crisis bands (GFC peak, COVID peak, 2018 Q4 peak).
- Sequencing chart: credit stress leads equity by N days (running measurement).
- CDS heatmap by issuer with 90d change.

### 3.5 · Dollar / FX cluster (6 engines)
`dxy-decomp`, `dollar-radar`, `fx-decomposition`, `yen-carry`, `carry-monitor`, `fiat-peg-monitor`

**Required:**
- DXY level + 5d/20d %.
- DXY decomposition (EUR, JPY, GBP, CAD, CHF, SEK) — verify weights.
- Carry-pair analytics (AUDJPY, USDJPY, NZDJPY tripwires per brain).
- Real yields differential (US-DE 10y, US-JP 10y).
- CEW-equivalent EM FX basket [GAP — brain flag].
- EURSGD, EURHKD, USDHKD (peg tripwires) [GAP].
- CNY fixing vs mid, USDCNH divergence.
- USD swap-line usage.

**Visual on `dollar.html` and `dxy.html`:**
- DXY chart 1996+ with crisis bands.
- Real yields differential overlay (US-DE, US-JP).
- Carry basket relative-strength ladder.
- Percentile bar: today's DXY 5d change vs all crisis periods.

### 3.6 · Vol cluster (8 engines)
`vol-radar`, `vol-regime`, `vix-capitulation`, `vix-curve`, `bond-vol`, `crypto-gex`, `tail-hedge`, `vol-target-unwind`

**Required:**
- VIX + VIX9D + VXVCLS (term structure).
- SKEW index + z-score.
- MOVE (bond vol).
- Gold vol (GVZ).
- Crypto vol (DVOL, BVIV).
- Realized-vs-implied gap (VRP).
- Vol-of-vol (VVIX).
- Gamma exposure (dealer GEX zero-line, flip level).
- [GAP] Historical fear-vs-complacency percentile (VIX + SKEW joint score).

**Visual:** vol surface, term structure, gamma landscape.

### 3.7 · Breadth / market-internals cluster (5 engines)
`market-internals`, `market-leaders`, `breadth-thrust`, `highs-lows`, `sector-flow`

**Fixes:** F3 (add `breadth_score`).

**Required:**
- Adv/decline line + volume.
- % above 50/200 DMA.
- New highs / lows ratio.
- TRIN, McClellan Oscillator/Summation.
- BKX-SPX relative [GAP].
- Sector participation (% sectors above 200DMA).
- Zweig breadth thrust indicator.

**Visual on `market-internals.html`:**
- Breadth score gauge (0-100).
- A/D line 1996+ with crisis bands.
- Sector participation heatmap.
- "Breadth deteriorating" vs "healing" state with brain quote (*"Once breadth stops deteriorating..."*).

### 3.8 · Yield curve / rates cluster (6 engines)
`yield-curve`, `us10y-sentinel`, `term-premium`, `real-yields`, `us-cycle`, `rates-map`

**Required:**
- 2s10s + T10Y5Y [GAP] + 3m10y.
- ACM term premium (real, not pending).
- Real yields (10y, 5y).
- Inflation breakevens.
- Fed dot plot vs market implied.
- SOFR curve slope.
- [GAP] Swap spreads (see 3.4).

**Visual on `yield-curve.html`:**
- 3D curve surface across time.
- 2s10s 1996+ with recession bands (already common, verify).
- Real yields with GFC/COVID markers.

### 3.9 · Auction / Treasury cluster (4 engines)
`auction-crisis`, `auction-decisive-call`, `treasury-noise`, `treasury-rehypo`

**Required:**
- Tail/stop-out z-score (already have).
- Bid-to-cover z-score.
- Indirect bidder %.
- Primary dealer takedown %.
- [GAP] Treasury bid-ask spread — brain weekly checklist item.
- WI-issued spread.

**Visual on `treasury-auctions.html`:**
- Tail z-score history with crisis bands.
- Composite auction stress gauge.
- Upcoming auction calendar with expected stress.

### 3.10 · Crypto cluster (~30 engines)
`crypto-*` family.

**Required:**
- BTC dominance, ETH/BTC ratio.
- Stablecoin market cap Δ (leads flows).
- Exchange netflow BTC/ETH (already wired now).
- Miner outflow (already wired now).
- Funding rates (perps).
- Open interest by exchange.
- Fear & Greed proxy.
- On-chain: MVRV, SOPR, NUPL, active addresses.
- ETF flows (BTC + ETH spot).

**Visual on `crypto/index.html`:**
- BTC price with 4-year cycle overlay.
- Cycle-clock (already have — verify wired).
- Exchange netflow chart 3y.
- Stablecoin cap Δ leading indicator.

### 3.11 · Positioning cluster (~15 engines)
CFTC COT, dealer surveys, 13F, insider, retail edges.

**Required:**
- COT net positioning z-scores by asset.
- CFTC extreme readings tripwire.
- 13F net buys minus sells (already have).
- Insider cluster buying/selling z.
- Retail-fund flows (ICI).
- AAII bull-bear spread.
- Put-call ratio (equity, index, total).

### 3.12 · Global stress / GSI cluster (6 engines)
`global-stress`, `global-recession`, `global-sovereign`, `sovereign-stress`, `sovereign`, `ciss`

**Required:**
- ECB CISS (already producing — verify wiring).
- OFR FSI (F5 fix).
- IMF GSI proxy.
- Sovereign bond spreads (IT-DE, ES-DE, US-DE).
- Sovereign CDS.
- EM bond spreads (EMBI).
- [GAP] Global recession model with lead-lag by country.

### 3.13 · Sector / rotation cluster (~20 engines)
Sector desks, rotation radar, industry rotation, factor regime.

**Required:**
- Sector RS scores.
- Sector participation (breadth).
- Cyclical-defensive spread.
- Factor returns (value/growth/momentum/quality/low-vol).
- Factor regime state.
- Sector-level positioning.

### 3.14 · Options cluster (~10 engines)
`options-*`, `crypto-gex`, `dark-pool`, `options-flow`.

**Required:**
- Dealer gamma exposure (zero line, flip level).
- Vanna, charm exposure.
- Put-call skew.
- Unusual options activity z-score.
- Max-pain distance.

### 3.15 · Central bank cluster (8 engines)
`fed-speak`, `fomc`, `ecb-detail`, `boj-detail`, `snb-detail`, `cb-injection`, `pboc-monitor`, `fed-nlp`.

**Required:**
- Balance sheet levels + WoW Δ.
- Policy rate + market-implied path.
- Speech NLP sentiment (already have fed-nlp).
- QT/QE state.
- Currency intervention proxies.

### 3.16 · Energy / commodities / supply cluster
`energy-*`, `supply-chain`, `refining-stress` (now wired), `import-canary`, `physical-trade`, `metals-miners`.

**Required:**
- Crude vs product spreads (3-2-1, 5-3-2 crack).
- Refining margins.
- Freight rates (Baltic Dry, container).
- Port congestion.
- Copper/gold ratio.
- Copper/oil ratio (industrial vs energy demand).

### 3.17 · Regime cluster (3 engines — RESOLVE COLLISION)
`justhodl-regime-engine` and `justhodl-macro-regime` both write `data/regime.json` (P0-8). Pick owner, repoint other to `macro-regime.json`.

**Required indicators (regime-clock):**
- Growth momentum (ISM, PMI).
- Inflation momentum (CPI, PPI, PCE).
- Rate momentum (fed funds, real yields).
- Liquidity momentum (global tide).
- Vol regime.

Output: `regime ∈ {GOLDILOCKS, REFLATION, STAGFLATION, DEFLATION-BUST}` with confidence + component contributions.

---

## PART 4 — Page cluster upgrade specs

425 pages cluster into ~40 desks. Below: the top-25 pages that need immediate institutional overhaul (Perplexity will handle wiring; Claude should build/enhance the engine outputs that feed them).

### Tier-1: crisis-critical desks (must ship in next 2 weeks)
1. **`risk-gate.html`** — v4 already shipped. After F1-F5 land, refresh visuals: add conjunctions table, sequencing badge, live FSI.
2. **`crisis.html`** — build from scratch to institutional bar. DEFCON gauge, 1996+ history, 14 legs, 7 conjunctions.
3. **`credit-desk.html`** — F4 fix + swap spreads + STIR-curve.
4. **`plumbing.html`** — pull crisis-plumbing full detail + xcc_basis chart.
5. **`vol-radar.html`** — SKEW percentile prominent, VVIX, VRP.
6. **`dollar.html`** — DXY 1996+, real-yields differential, USDHKD/EURSGD peg-watch.
7. **`yield-curve.html`** — 3D curve, real yields, swap spreads.
8. **`market-internals.html`** — F3 breadth score gauge, 1996+ A/D line.
9. **`cds-monitor.html`** — sovereign + single-name heatmap.
10. **`global-stress.html`** — CISS + OFR FSI + GSI in one board.

### Tier-2: barometer & sequencing desks
11. **`defcon.html`** — dedicated DEFCON explainer with methodology.
12. **`sovereign-stress.html`** — IT-DE, ES-DE, EM basket.
13. **`fomc.html`** — dot plot vs market path + speech NLP.
14. **`fed-speak.html`** — NLP sentiment scored 1996+.
15. **`liquidity.html`** — global tide + 4 CBs + Howell GLI proxy.
16. **`regime.html`** — 4-quadrant regime clock.
17. **`carry.html`** — AUDJPY tripwire, USDJPY 20d z, carry pair matrix.
18. **`yen-carry.html`** — dedicated yen unwind board with 1998 & 2024 markers.

### Tier-3: opportunity/signal desks
19. **`opportunities.html`** — top signals ranked with backtested edge.
20. **`nobrainers.html`** — cross-signal confluence ≥ 5.
21. **`convergence-desk.html`** — 5-6 confluence rule per brain.
22. **`insider-desk.html`** — insider cluster z + industry cluster.
23. **`smart-money.html`** — 13F + activist + dark pool + institutional footprint.
24. **`positioning.html`** — CFTC + AAII + put/call.
25. **`sector-emergence.html`** — sector rotation with historical analog.

### Tier-4: kill list (candidates)
Pages that appear to be dev/debug or duplicates — audit and either promote to institutional bar OR delete:
- `errors.html`, `debug/`, `status.html`, `uptime.html`, `observability.html`, `llm-cost.html` (these are ops — should live under a single `/ops/` route with password gate, not in public nav)
- `panels.html`, `desk.html`, `desk-v2.html`, `master-board.html`, `master-allocator.html` (overlap — pick one)
- `dossier.html`, `read.html`, `brief.html`, `today.html`, `live.html`, `live-pulse.html`, `welcome.html` (overlap — pick a canonical dashboard route)

---

## PART 5 — Sequencing & ownership

### Week 1 (this week)
- **Claude:** F1, F2, F3, F4, F5. Ship as one PR bundle. Perplexity has line-numbered specs above.
- **Perplexity:** Continue wiring dark engines to pages (P0-7 follow-on batches, ~20 wirings/week).
- **Backend heartbeat:** No action needed — these are judgment escalations.

### Week 2
- **Claude:** F7 (domain-barometers unit fix), F9 (fred cache layer with top-20 migrations), F10 (red-alert conjunctions engine).
- **Perplexity:** Build `crisis.html` v1 to institutional bar. Rebuild `credit-desk.html` layout post-F4.

### Week 3-4 (P1 indicators)
- **Claude:** Crisis sequencing engine, Khalid-checklist engine, swap spreads + STIR-curve module, OFR FSI first-class integration, CEW/BKX/T10Y5Y/EURSGD gap fills.
- **Perplexity:** Ship Tier-1 pages (10 crisis desks) to institutional bar.

### Month 2 (architecture)
- **Claude:** jh_common library (FRED, notify, S3, telegram), CI contract-gate enforcement, regime-collision resolution.
- **Perplexity:** Tier-2 pages + orphan-feed wiring campaign.

### Month 3 (predictive layer)
- **Claude:** 1996+ backtest every engine, publish `crisis_history` + `forward_returns` on each engine's output.
- **Perplexity:** Ship historical-context visuals on every Tier-1/2 page.

---

## PART 6 — Success criteria (Google/MS bar)

An engine is "institutional grade" when:
- ✅ Docstring states purpose in ≤1 sentence.
- ✅ Output shape passes JSON schema contract.
- ✅ Zero `pending_source` when a real source exists.
- ✅ Has `crisis_history` + `forward_returns` published.
- ✅ Consumes from `jh_common/fred.py` (not raw FRED).
- ✅ Health field populated.
- ✅ 1996+ backtest fires correct posture at every crisis.

A page is "institutional grade" when:
- ✅ Barometer at top (no NaN, no undefined).
- ✅ 1996+ regime strip with 8 crisis bands.
- ✅ Historical percentile bar for the headline number.
- ✅ Component decomposition visible.
- ✅ Predictive read at +21d/+63d/+252d.
- ✅ Data provenance footer.
- ✅ Relevant Khalid brain quote surfaced.
- ✅ Renders correctly on 320px mobile.
- ✅ First-contentful-paint <1.5s.
- ✅ No `[object Object]`, no `undefined%`, no empty charts.

**Definition of done for the whole platform:** A Bridgewater PM or Renaissance quant lands on any page cold — inside 10 seconds they understand what to do next. That's the bar.

---

## PART 7 — Communication protocol

- File weekly status on the A2A bus under topic `institutional-overhaul-status`.
- Each week: list of engines fixed, pages upgraded, indicators added, and a link to before/after screenshots.
- Anything Claude wants Perplexity to build on the frontend: file with role `perplexity_frontend_request`, we handle without gate.
- Anything ambiguous: escalate to Khalid via the backend-heartbeat escalation queue.
- Big architectural PRs: post RFC on bus with topic `rfc-<name>`, 24h comment window, then merge.

---

## Appendix — Reference artifacts (all in workspace / repo)

- `data/audit/system-audit.md` — full 397-line audit
- `data/audit/derived/engine-io.json` — producer→consumer graph
- `data/audit/derived/feed-graph.json` — feed relationships
- `data/audit/derived/coverage.txt` — 110-indicator brain-coverage matrix
- `data/audit/derived/dark30.txt` — top-30 dark engines by importance
- `data/brain-distilled.md` — 635KB distilled brain (775 notes)
- `data/brain-indicators.md` — brain-cited indicator list
- `data/institutional-dashboard-research.md` — top-desk visual reference

---

Signed autonomously under charter `full_stack_unrestricted`. Any lines you disagree with — escalate on the bus and we redraft.
