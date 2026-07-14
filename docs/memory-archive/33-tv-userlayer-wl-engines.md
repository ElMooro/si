# 33 — TradingView → internal: user layer, wl-engines fleet, symbol map (ops 3155–3189)

**Arc (2026-07-12/13).** The TV harvest finally landed and the platform now
covers Khalid's TV watchlists/indicators/notes INTERNALLY. Do not rebuild any
of this — extend it.

## What exists (audit-first checklist)

- **Per-user store, JWT-hardened** (ops 3155–3157): pre-SaaS backup, userdata
  hardening, user layer. Per-user doctrine holds everywhere.
- **TV pipeline v3** (3158): Chrome-extension → `justhodl-tv-notes-ingest`
  → S3 `data/tv-watchlists.json` (6,507 unique symbols across his lists)
  + notes → brain. Cloudflare/CSP blocked every server-side approach —
  the extension in his browser session is the only pipe; keep it.
- **wl-engines fleet** (3159/3176): `justhodl-wl-engines` — 161 first-class
  engines on ONE multi-tenant runtime (one per watchlist), each with
  engine_id `wl-<slug>`, own feed `data/engines/wl-<slug>.json`,
  FIRING/QUIET state, z-scored members, outcome-checker signal row, fusion
  hooks (theme→FUSION_TARGETS). O(n) rolling z, ~20MB float arrays.
  Schedule cron(30 22 ? * TUE-SAT *). Shared cache with thesis-engine
  (`data/thesis-state-v2.json.gz`).
- **thesis-engine** (3165/3166/3168) + **notes-intel** (3171/3172) +
  **regime gate/compass** (3170/3173): his notes are testable claims now.
- **symbol map**: `data/symbol-map.json` — built by `map_symbol()` in
  `aws/shared/series_source.py` (mapper AND fetchers live in that ONE file;
  bundled into wl-engines + thesis-engine + symbol-dictionary — changing it
  requires redeploying all three, ops 3189 shows the pattern).

## Source ladder in series_source.py (all $0/month)

MARKET chain Yahoo→Stooq→Polygon (EODHD fallback inert — key purged 3188,
never re-map to it) · FRED (env key + fallback retry — 3172 stale-key trap)
· WORLDBANK · DBNOMICS/_V2 · INTERNALS (`justhodl-market-internals`
computes the whole USI breadth complex from Polygon grouped-daily, 3185)
· COINGECKO · **COINMETRICS** (3189: `asset|Metric`, community v4, no key —
GLASSNODE/INTOTHEBLOCK tiles) · **COT** (3189: `dataset|code|field`,
publicreporting.cftc.gov Socrata; COT3 tile embeds the CFTC code; datasets
6dca-aqww futures-only / jun7-fc8e combined) · FORMULA.
Yahoo exchange-suffix table covers LSE/.L, Xetra/.DE, SSE/.SS etc (3186:
probe before paying). OECD templates gated to OECD_MEMBERS (3185 Zimbabwe
trap). Continuous futures FUT roots → Yahoo `=F`.

## Coverage ledger

59.9% (3184 census) → 68.2% (3185 $0 tier) → 74.1% (3186–3188 Yahoo wins,
EODHD in-and-out) → **75.3% (3189: +48/52 COT3 + legacy COT hits, +2 ITB,
+4 futures — all probe-gated, dry entries pruned)**. 2,224 series cached.

## Residue ranked (what's honestly left, 3189 census)

FTSE 448 (LICENSED — no vendor at any tested tier 3187/3188; retire) ·
ECONOMICS 383 (next: curated IMF/BIS DBnomics templates, probe-gated) ·
INTOTHEBLOCK 145 + GLASSNODE 59 (tiles are DERIVED composites —
ATHDRAWDOWN, BULLSCOUNT, NEW/SENDING/RECEIVINGADDRESSES — not community
metrics; next: a DERIVED transform source, e.g. drawdown-from-ATH over
PriceUSD) · USI 95 (extend INTERNALS token map) · TVC 91 (extend
yield/index templates) · CBOEEU 40 / EUREX 38 / ICEEUR 30 (cash-proxy
flags or venue-dedupe) · CME 24 residual roots.

## Doctrine earned this arc

Probe before paying (3186) · a cancelled vendor key left wired = silent
empty fetches + mis-resolution hazard (3188 BER→KLSE) · measure verdicts,
never assume · engine-activation math decides which gap is worth closing
(3184) · vendor tiles are often VIEWS of free primary data — map, don't buy.

## Residue program appendix (ops 3190–3198, same night)

Khalid: "build every single one of those one by one." All PASS.

- **3190 econ ladders**: ECON_DBN candidate-ladder dict (IMF IFS/OECD MEI
  templates) + curated carry-forward convention (`curated` section MERGES
  FIRST on every rebuild). Measured: the real residue = 174 micro-families
  the guesses missed (ladder_covered 0) — evidence over assumption.
- **3191 name-search**: dbn_search_factory (DBnomics twin of fred_search,
  trusted providers only, memoised) driven by symbol-dictionary human
  names. 11 survivors — but shipped CROSS-COUNTRY POISON (BRCLI/CNCLI →
  Australia; MA/TN INTR → BIS debt-sec).
- **3192 hardening**: 9 poison entries PURGED by name; country token now a
  HARD reject in dbn search; verified hits became templates (CLI → OECD
  DSD_KEI@DF_KEI/{i3}.M.LI.IX._T.AA._Z; rates → ECB MIR + IMF IFS
  FILR_PA/FIMM_PA; permits → Eurostat STS_COBP_M) — INTR hit 19/21
  countries, 400+ monthly pts each. MPMI/SPMI/CPMI/LEI tagged
  licensed_econ (S&P Global / Conference Board — no free primary).
- **3193 DERIVED source**: 'SRC~ID~transform' — drawdown_ath, running_max,
  nupl_mvrv, negate, pct1 (+vol60, mcclellan_osc/sum, hundred_minus added
  3194/3197). BTC ATH-drawdown computed 5,839 pts. ITB census verdict: the
  remaining ~185 on-chain tiles are proprietary holder-clustering
  (WHALES/RETAIL/INVESTORS ×count/%/assets) — unbuildable free.
- **3194 USI**: McClellan osc+summation as DERIVED over the computed A/D
  line; exchange-scoped tiles → all-market computed with explicit PROXY
  note; TICK/TIKI/PREM tagged intraday. Census verdict: the residual 95
  are tape/block microstructure (BLKS/BATD/ATHI/ATLO...) — daily engines
  cannot compute them honestly.
- **3195 TVC yields**: non-OECD sovereigns → IMF/IFS/M.{i2}.FIGB_PA
  (country baked into template = poison-impossible). +8 curves (ZA, SG,
  RU...).
- **3196 EU futures + venues**: EU_FUT_PROXY — index futures → cash index,
  bond futures → NEGATED 10Y yield via DERIVED (sign-safe), ICE MSCI →
  EEM/EFA; +6E..DX currency roots; venue-dedupe machinery (found 0 with US
  primaries — CBOEEU residue has none).
- **3197 close**: FTSE retirement EARNED (12-sample probe, 0 hits → all
  448 retired licensed_ftse_russell); USI tape family + delisted GE
  (eurodollar, dead 2023) retired; CME_RATE roots live (SR3/SR → FRED
  SOFR, ZQ → 100−DFF via hundred_minus, BTC/MBT/ETH → spot proxy);
  **DRY LEDGER** — map meta `dry` {sym: id}: rebuilds must (a) merge
  curated FIRST, (b) skip (sym,id) pairs in dry, (c) never remap retired.
  22 unproven rung-0 reintroductions pruned permanently.
- **3198**: fleet verification on the final map → **FAIL, real signal**:
  index frozen at 02:46 — every kick since 3191 had died silently.
- **3199 forensics**: CloudWatch tail INTO the report — runner crashed in
  2.8s, `week_key` ValueError line 278-adjacent: `y, m, d` unpack.
  (Speed patches shipped alongside: DBnomics 12s / CoinMetrics 15s +
  3-page cap / COT 15s, USI rstrip single-char fix.)
- **3200 THE ONE-KEY KILL**: DBnomics periods ('1990-01', '1990-Q1')
  entered the weekly cache path with 3191's first DBNOMICS map entries;
  one malformed key killed all engines on every kick. Fix at BOTH layers:
  `_dbn_iso` normalizes every DBnomics period at the source (annual→12-31
  = World Bank convention, Q→quarter-end, M→-28, both fetchers unified);
  runner `week_key` now defensive (bad key → None, skipped). **PROVEN
  ALIVE: index 03:53 UTC, 115 ACTIVE / 47 DORMANT / 162 engines, 2,281
  series cached** (both above pre-arc). Small open audit: crash-time spec
  print said 207 engines vs 162 in the index — reconcile next session;
  FIRING field name in the index also unconfirmed (grading lands at the
  nightly outcome pass regardless).

**Lesson class (add to ship-gotchas mentally)**: a NEW data source must
never emit period keys the downstream weekly-grid path hasn't seen —
normalize at the source layer, and NEVER trust one symbol not to kill a
multi-tenant runtime: every shared loop needs a per-item guard.

**Final ledger**: coverage 74.1 → **76.6 raw / 82.8 addressable** (4,983
mapped / 6,507; retired 468, licensed 16, curated 32, dry 22). Residue
after the program, honestly categorized: ECONOMICS 360 (micro-family long
tail — machinery in place for organic growth), ITB 130 + GLASSNODE 55
(proprietary), TVC 83, USI 77 (tape/scoped), CBOEEU 40 (no primaries),
EUREX 34 / ICEEUR 28 (exotic roots), DFM 14 / ADX 13 (Gulf venues, no free
daily source).

## Fusion arc appendix (ops 3201–3207, same night — "fuse the new data everywhere")

Audit found wl-fusion (evidence-weighted, additive-only, multipliers
[0.90,1.10], divergence board, 22:50 TUE-SAT) built but consumed by only 2
targets and invisible on all 366 pages. Shipped:

- **3201/3202 — 18 engine consumers**: wl_fusion.block() = one-line
  never-raising fusion surface; SIXTEEN engines attach theme-filtered
  "wl_research" (regime/crisis/risk-regime, global-liquidity,
  dollar-radar, master-ranker, credit-stress, liquidity-credit,
  cycle-clock, macro-nowcast, equity-confluence, breadth-thrust/
  -divergence, crypto-liquidity/-emergence, eurodollar-plumbing) +
  alpha-compass/best-setups already wired. Proven in 5 live feeds.
  Gotcha: some config.json carry STRING schedules — type-safe parse,
  never churn a live EB rule on a format guess.
- **3203 — /panels.html**: the research desk (theme pressure board,
  divergence board, 162-engine table, live feeds only). LIVE in 150s.
- **3204 — COT widened**: extremes-scanner merges cot/universe-ext.json
  (generated from the map's probe-proven CFTC codes + dictionary names,
  additive, hardcoded wins collisions) → 7 watchlist contracts in the
  first widened snapshot.
- **3205-3207 — the chip, evidence-first**: HIS RESEARCH rail chip
  site-wide. Three failure modes eliminated in order: CF-filtered custom
  UA (→ Mozilla + cache-buster, the proven fetch_age shape), fetch-path
  doubt (→ S3-direct fallback, both proven reachable), and the REAL one —
  flows-class pages fetch via ${CDN} templates and never matched the
  rail's data-ref regex (→ widened; flagship pages are no longer
  silently rail-less). Chip proven live on BOTH classes; first live
  divergence: "LIQUIDITY panels ELEVATED while global-liquidity reads
  NEUTRAL".

Doctrine: when a verifier fails twice, stop fixing the guess — make the
next ops DUMP THE EVIDENCE (the actual live payload) into the report.

## Grading + reconcile appendix (ops 3208–3213)

- **Grading WIRED (3176's unbuilt intent)**: FIRING panels log guarded,
  week-deduped (conditional put on wl#<engine_id>#<grid-Friday-week>)
  directional signals vs SPY (direction = panel's own 13w tilt) into
  justhodl-signals; outcome-checker sweeps clean; 7/28/91d windows.
  **24/24 verified by exact key.** Fusion's PROVEN gate will mature on
  real out-of-sample hits.
- **207-vs-162 SOLVED**: two demotion paths dropped engines silently;
  DORMANT rows now carry NAMED reasons — worklist: 47 need ≥6 mapped
  members, 39 mapped-but-unfetchable history, 5 short joint history.
- **Crash class closed**: shape guards on every multi-part sid split
  (malformed → {} + named print). Zero [ERROR] on guarded runs.
- **Verifier traps named (self-inflicted, documented)**: DDB Scan Limit
  caps items EXAMINED not matched (paginate or exact-key); the runner
  keys weeks on the last GRID FRIDAY, not the run date's ISO week.

## Wake-the-panels appendix (ops 3214–3216)

Broad machinery measured EXHAUSTED (3214: CBOEEU 10-suffix ladder 0/40,
targeted econ search 0, DFM/ADX 0/10 sample → 27 retired
no_free_daily_source_gulf_venue). What remained was surgical:

- 3215 NAMED the one-symbol blockers verbatim: ECONOMICS:USRR (×3
  engines), TVC:BTPBUND (×2), CBOT:YIT1!, ICEEUR:I2!/EON2!/USW1!,
  ECONOMICS:CHMPMI (licensed). Micro e-minis proven REAL Yahoo
  continuous: MES/MNQ/M2K/MYM=F (+MGC/SIL/MCL roots in FUT).
- 3215 dry-member triage exposed the FRED-OECD-mirror poison class:
  Chile templates emit FRED ids that DON'T EXIST (0 pts) while counting
  as "resolved". Annual WB series (~29 pts) are NOT dry — the runner
  needs only 12 obs.
- 3216 closed the two big ones: ECONOMICS:USRR → FRED RRPONTSYD (3,280
  pts) and TVC:BTPBUND → DERIVED IT10Y−DE10Y via the new two-base
  'minus' transform (SRC~ID~minus~SRC2~ID2). 8 named zero-point entries
  dry-ledgered. **2 panels WOKEN: Euro Dollar Shortage & Liquidity
  squeeze; Feds Rates. Active 115 → 117.**

Open by name: Developed Markets (micros curated — verify next nightly),
Europe Liquidity :BTPBUND (curated — next nightly), Global Deposit Rates,
and the unknown roots CBOT:YIT1! / ICEEUR:I2!/EON2!/USW1!.

## Continue-arc appendix (ops 3217–3220)

- **Mirror sweep fleet-wide (3217)**: 140 phantom FRED-OECD mirrors
  probed 0-pt and dry-ledgered (non-curated MEI families only —
  curated/probe-proven exempt). "Resolved" now means fetchable
  everywhere; dormant histogram honest: 49 need-mapping / 35
  lack-history.
- **ICEEUR:I2! curated** (3M Euribor via DBnomics ECB/FM, 100−rate ZQ
  convention, 390 pts). EON2! all candidates dry — open.
- **3218 member-by-member triage** named everything: Europe Liquidity's
  dry members were TV EXPRESSION tiles (DE10Y-IT10Y etc.) on a failing
  FORMULA path; Global Deposit Rates blocked on EUDIR/GBDIR; Developed
  Markets blocked on CME_MINI:DVE2! (retired — no free source; sleeps
  honestly at 5/6).
- **3219 curations, all probe-verified**: 3 sovereign spreads via minus
  (DE/FR/ES−IT, 422 pts each), EUDIR→FRED ECBDFR (deposit facility,
  10,053 pts!), GBDIR→IR3TIB01GBM156N (433).
- **⚠ OPEN ANOMALY (3220)**: runner's fresh run still counts the 5 new
  members dry (3/7 and 4/6 z-scorable unchanged) though all fetch clean
  ops-side and BTPBUND proves minus works in-runner. Prime suspect:
  warm-container map staleness (invoked seconds after the map write).
  DECISIVE CHECK: tonight's 22:30 UTC cold scheduled run — if Europe
  Liquidity + Global Deposit Rates wake, closed; if not, instrument the
  runner's map read next session.

## Cache-truth arc appendix (ops 3221–3227) — the anomaly fully closed

The "runner counts probe-verified curations dry" anomaly decomposed into
FOUR stacked mechanisms, each named by instrumentation (WL_TRACE env:
need→cache_pre→todo→weekly→zc per symbol):

1. **Deploy env race**: deploy_lambda awaits the CODE update but not the
   CONFIG update — an invoke fired seconds after deploy runs new code
   with the OLD env. (Await function_updated_v2 after deploys that
   change env.)
2. **Symbol-keyed cache staleness**: remapped symbols kept serving their
   old series (GBDIR's 9-pt WorldBank ghost). Fix: ids-ledger — every
   cached symbol records what it was fetched AS; a remap invalidates
   exactly that entry.
3. **Fetch storms**: 10 unthrottled workers 429-stormed CoinGecko every
   run (~30 crypto symbols perpetually failing) and my first FRED gate
   (0.12s ≈ 500/min) was a self-DDoS against FRED's 120/min cap —
   mass 429s returning as SILENT EMPTIES (the run that fetched 1/1324).
   Fix: per-source politeness gates in series_source (CG 1.35s,
   FRED 0.55s) + FRED/MARKET-first todo ordering.
4. **Perpetual-retry loop**: empties were never remembered, so ~1,300
   dry ids re-fetched EVERY run. Fix: 3-day mapping-keyed misses
   tombstones. Steady state proven: todo=0, fetch phase 2s.

Also: fake CRYPTOCAP aggregate ids (total2, btc.d, …) retired —
no free historical source. matic→pol alias.

**Result: ACTIVE 115 → 121** (Euro Dollar Shortage, Feds Rates, Forex,
Durable Goods, DXY-symmetric, Europe Liquidity:BTPBUND). Cache 2,281 →
2,465 real series. Global Deposit Rates reached 6 members but sleeps
STRUCTURALLY: its JP/CN deposit rates are annual (~30 obs) so joint
weekly history can never hit 100 — honest, named, closed.

## Truth-pass close (ops 3228–3229)

- ICEEUR:EON2! → FRED ECBESTRVOLWGTTRMDMNRT (€STR, 1,734 pts) — the 3217
  "all candidates dry" verdict WAS a 429-era artifact; post-storm the
  same id fetches clean. Confirmed in the live map (3229; 3228's report
  truncated at §5 header — cosmetic, the write landed).
- USW1! / YIT1! carry no richer names in the TV dictionary — stay open,
  honestly unknown.
- Fusion manually re-kicked onto the 121-active index (cron skips
  Mondays); BREADTH still top at 82.1p.
- NEXT-SESSION worklist, one symbol each: "3 year Global bond yield"
  (TVC:HK03Y/TW02Y/NZ02Y — short-tenor sovereigns, IFS lacks them),
  "Euro Predict" (ECONOMICS:DEIFOE Ifo expectations / DEZCC ZEW),
  "Europe Growth" (EUGDPGA / DEGDPYY — needs a pct4 YoY transform over
  Eurostat/FRED GDP levels), "France" (FRLG / FRGDPYY / FRIPYY).

## Growth-pair close (ops 3230–3233)

- pct4 transform shipped (YoY over quarterly levels). Live-family wins:
  DEGDPYY/FRGDPYY/EUGDPYY via FRED CLVMNACSCAB1GQ{DE,FR,EA19}~pct4;
  EUGDPGA via ~pct1 (noted non-annualized); FRUR → Eurostat une_rt_m
  (431); FRBR → ECB/MIR/M.FR.B.A2A.A.R.A.2240.EUR.N (317); PX1 → ^FCHI
  (PX1 IS the CAC40's Euronext ticker — truthful, not padding).
- **France WOKE → ACTIVE 122.** Europe Growth 5/6 wet: last member =
  EUBCOI (Eurostat ei_bssi_m_r2 dims tried M.BS-ICI.SA.EA19 → dry) or
  EUMPRYY (ECB/BSI M.U2.Y.V.M30.X.I.U2.2300.Z01.A → dry) — next session
  browses the DBnomics dataset dims instead of guessing.
- Dry-tried (don't retry blind): Eurostat sts_inpr_m M.PROD.B-D.SCA.I21
  dims; OECD KEI BC measure.

## Browse-probe close (ops 3234–3236) — Europe Growth AWAKE

- 3234/3235 taught the API's real shapes (v22 /search returns DATASET
  docs; series live under /series/{prov}/{code}?q=). The lesson was
  already encoded: **dbn_search_factory IS the proven pattern** — reuse
  it, don't reinvent.
- 3236 via the factory: **ECONOMICS:EUBCOI →
  Eurostat/EI_BSIN_M_R2/M.BS-ICI.NSA.BAL.EA20 (423 pts)** — dataset name
  EI_BSIN (not ei_bssi), unguessable. **⏰ Europe Growth WOKE → ACTIVE
  123.**
- EUMPRYY M3 candidates annual/short (≤11 pts) — open, no longer needed
  for the wake. DEIFOE/DEZCC: zero candidates even via the proven
  searcher — Ifo/ZEW are not on DBnomics' free surface; Euro Predict is
  licensed-class-blocked, honestly.
- **NIGHT TOTAL: ACTIVE 115 → 123 (+8)**: Euro Dollar Shortage, Feds
  Rates, Forex, Durable Goods, DXY-symmetric, Europe Liquidity, France,
  Europe Growth.

## Census + confidence-family close (ops 3237–3238)

- **Dry census (from the fleet's own misses ledger)**: 1,167 total —
  772 MARKET (delisted/odd tickers), 199 FRED (more dead mirrors),
  183 WORLDBANK, 13 crypto. Per-engine fix menu produced in one pass.
- **Thesis-engine proven healthy** on all patched shared modules
  (fresh feed, zero [ERROR]).
- **panels.html drill drawer LIVE** (row-click → per-engine event study,
  lit indicators, members with z; live-verified in 15s).
- **Confidence-family batch (proven searcher)**: 7+ landed —
  **TEMPLATE: OECD/MEI/{ISO3}.B6BLPI01.CXCU.Q** (quarterly business
  confidence, ALIVE on DBnomics) for CHE/CHN/GBR/IND/MEX/NLD; Italy via
  ISTAT/117_266. CCI candidates mostly dry (wrong-series matches) —
  open. **⏰ FIVE WOKE**: Business Confidence Index, Corporate Profits,
  Current Account, Fed Expected Yield Policy, Forward Rates — shared
  tiles fed engines beyond the targeted lists.
- **NIGHT GRAND TOTAL: ACTIVE 115 → 128 (+13 panels).**

## Breadth + CCI close (ops 3239)

- **INDEX breadth tiles → OUR OWN internals** (Polygon-computed, keys
  listed before mapping): MMFI→PCT_ABOVE_50DMA exact; MMTH + the
  index-scoped %>200d family (S5TH/R3TH/NCTH, S5FI→50d) universe-proxied
  per the 3194 precedent with explicit notes; HLUS = cross-source
  DERIVED INTERNALS~NEW_HIGHS~minus~INTERNALS~NEW_LOWS. All ≥1,194 pts.
- **CCI TEMPLATE FOUND: OECD/MEI/{ISO3}.CSCICP02.STSA.M** (~400 pts;
  CHE is .Q) — landed for JPN/FRA/ESP/SWE/DEU/GBR/KOR/NLD/ITA/CHE/AUS/
  CHN in one drill pass (direct /series/OECD/MEI?q=, ISO3-in-code
  reject).
- **⏰ THREE WOKE**: Above and Below Moving Averages; Breadth: leads the
  Market; CONSUMER CONFIDENCE. **NIGHT GRAND TOTAL: ACTIVE 115 → 131
  (+16 panels).**

## MARKET-rescue close (ops 3240–3242)

- Sub-census of the 772 MARKET misses by exchange: 370 NASDAQ,
  136 INDEX, 43 SSE, 33 CBOE, 27 EURONEXT, 18 TRADEGATE, 14 SWB, 13 FX,
  13 BER, 10 SIX/HKEX, 9 FWB.
- **Landed (3240, fleet-confirmed via index timestamp)**: BER:DX2Z →
  DX2Z.DE (4,684) + the NASDAQ ^CRSP index family ×8 (CRSPLCG1/LCGT/
  LCV1/LCVT/MC1/MIG1/MIGT/MT1, 3,477–6,292 pts). Zero wakes — CRSP
  tiles sit in engines still short elsewhere; honest.
- **Exhausted (3242, corrected selector)**: ^NQ* ×80 dry (Yahoo carries
  ^CRSP* but NOT the NQ* Nasdaq index family) and SSE .SS ×30 dry
  (SSE 000xxx are index codes, not free A-shares). DO NOT retry these
  ladders.
- **Selector doctrine (3241 bug)**: misses are BY DEFINITION
  mapped-but-dry — `s not in mapped` filters every candidate to zero;
  the correct skip is already-rescued (current id == candidate).
- Truncated-report class: kv VALUES rows exist even when Log sections
  are empty — a short report may be zero-hits, not truncation. Read the
  raw file before assuming.
- ACTIVE holds at 131.

## INDEX-internals + fusion sync (ops 3243)

- INDEX:ADVN → ADVANCERS, INDEX:DECN → DECLINERS (1,253 pts each, our
  own computed internals). Remaining INDEX residue = the ADR*/AVR*
  average-price family — no internal equivalent, honestly unmatched.
- wl-fusion re-kicked onto the 131-active index (Monday cron gap):
  BREADTH top theme 80.9p; the 18 consumers, panels boards and the
  site-wide rail chip now carry the full night's truth.
- Active holds 131; woken 0 (both tiles feed already-active engines).
- **The wake-mining program is at its floor across all families** —
  every remaining dormant engine carries a named, verified blocking
  class (licensed / no-free-source / structural / unknown-root). Next
  maturation is automatic: first day_7 grading of the 24 wl signals
  lands ~July 20 → fusion PROVEN gate + master-ranker multipliers
  activate on real hits.

## SERIES-LEVEL fusion (ops 3244) — new data as direct model inputs

Theme-level fusion (wl_fusion, 18 consumers) was already live; this arc
wired the new RAW SERIES into the engines whose models natively need
them. Audit-first: cycle-clock/regime skipped (already carry confidence
inputs).

- **Bridge: aws/shared/wl_series.py** — block({field:(TILE_SYM,label)},
  composite=False) reads the fleet's own weekly cache
  (thesis-state-v2.json.gz), returns last/z_1y/chg_13w_pct/n_weeks per
  series (+composite_z). Zero new fetch load, memoized, NEVER raises,
  additive-only.
- credit-stress ← **europe_sovereign** (BTP–Bund, OAT–BTP, Bono–BTP,
  €STR). Live: BTP–Bund 0.817 z=−1.38; OAT–BTP z=+1.28 (French
  relative stress rising — real signal).
- eurodollar-plumbing ← **euro_policy_corridor** (€STR, ECB depo,
  Euribor-implied, GB 3m). Live: €STR 2.182 / depo 2.25, both z=3.43 —
  corridor coherent.
- macro-nowcast ← **global_confidence** (12-country CCI + business conf
  + DE/FR/EA GDP YoY; 15 series live, composite_z −0.06).
- crisis-composite ← **btp_bund_canary** (quiet at z=−1.38).

All four verified live in one deploy+invoke pass. Pattern for future
engines: one payload line, `"field": __import__("wl_series").block({...})`.

## Fusion surfaced (ops 3245–3247) — data→screen closed

- The four series-fusion blocks render as cards on their native desks:
  europe_sovereign → risk-regime.html; euro_policy_corridor →
  eurodollar.html; global_confidence (composite badge) →
  chart-macro.html; btp_bund_canary → defcon.html. Pattern: identical
  self-contained IIFE (config-only diff), independent fetch with
  S3-direct fallback, prepends into <main> — zero coupling to existing
  render code. All node-checked; all verified in served HTML.
- **Verification doctrine (3245's miss)**: never grep served SOURCE for
  a RUNTIME-composed string ('jh-fusion-'+C.field can't appear in
  HTML). Check source LITERALS (the config string). The 3246 diagnostic
  pattern (cf-cache-status/age/last-modified + has_marker vs
  has_snippet-comment) decides stale-vs-stripped-vs-wrong-check in one
  fetch.

## Fleet certification (ops 3248–3250)

- 653 justhodl-* functions swept (batched Errors metric, 12h).
- wl-engines' 27 errors: hourly distribution proved them clustered
  02:00–03:00Z (pre-fix marathon era) — ZERO since, through every
  deploy and run. Certified historical.
- **theme-rotation-engine live crash FIXED**: `.get("breadth", {})`
  only defaults when the key is ABSENT — a key holding None crashes the
  chain. **Doctrine: `.get(k, {})` does not guard k:None — use
  `(x.get(k) or {})`.** Or-guards at both breadth chains; clean run
  proven post-deploy.
- 7 other single-error functions: no recent traces — transients.
- All 9 tonight-touched feeds fresh. **FLEET CERTIFIED.**

## Daily brief × panel research (ops 3251)

- justhodl-alpha-daily-brief now bundles **his_research** (top themes
  by pressure, firing panels by name, top divergence, n_active) into
  the LLM context AND persists it structurally in
  data/alpha-brief.json. Live-verified: BREADTH 80.9p / LIQUIDITY
  70.8p / INFLATION 62.4p, 6 firing named, divergence surfaced.
- **Graceful degradation added**: on LLM failure the brief ships a
  deterministic data-driven digest (regime + stress + HIS RESEARCH +
  top alpha) instead of returning 500 with no brief at all.
- **Intel**: composer ran as claude-haiku-4-5 — the Lambda env
  Anthropic key WORKS; the dead-credits item applies to the CI runner
  secrets only. Brief LLM path is alive today.

## Drawer 403s fixed (ops 3252)

- ROOT CAUSE: detail feeds were written only for scored panels — every
  DORMANT engine 403'd in the drill drawer (S3 answers missing keys
  with 403, not 404). Runner now writes a thin detail doc
  (detail_level=dormant-min, same keys the drawer reads, NAMED reason)
  for every non-ACTIVE row; ACTIVE docs proven untouched
  (foreign-exchange-reserves still rich: w13 n=67, 118 members).
- MEMBERS column did (int||[]).length → 'undefined' — int-safe mrc()
  in renderer + sorter.
- Drawer catch now renders the row's index data (state, reason,
  members) instead of a raw error — no residual gap can surface as a
  bare 403 again.
- Khalid's reported key wl-10-yr-high-quality-market-hqm-pred verified
  EXISTS with its reason post-fix; page live with both fixes in 120s.

## All-engines audit (ops 3253)

Every one of the 207 engines on panels.html audited: detail feed exists
and parses with the drawer's keys. Stragglers repaired inline (thin
dormant-min docs); a missing ACTIVE doc would have FAILED loudly —
none did. Public CDN proof 8/8 including both Khalid-reported ids
(HQM + bond-global-high-yield), each serving 200 with its named
dormancy reason. Page verified: gj() already cache-busts every click,
so no CF 403-caching residue possible.

## PREDICTION LAYER (ops 3254–3255)

- Panel theses tested AS predictions inside the wl runtime: predict-
  intent parsed from names (predict|future|reversal|warning|lead|
  barometer|gauge|signal), targets resolved (liquidity→LIQUIDITY-theme
  composite excl. self; dxy/dollar→TVC:DXY level; 10 year/yield→Global-
  10y composite; default SPY). Predictor = directional composite (mean
  member z, captured as dz alongside act in the scoring loop; comps{}
  ACTIVE-only). Stats: 13w lead corr + extreme (|z|≥1.2) hit rate vs
  base + CURRENT CALL with odds. Feed data/wl-predictions.json;
  PREDICTIONS board atop panels.html. n≥60 pairs gate.
- Live v1: **12 theses**. Finds: Bonds-Sovereign→SPY ext 78.1% (n=155)
  vs base 75.2, call SPY UP; Buybacks→LIQUIDITY corr −0.384 (n=331),
  z=1.9 → LIQUIDITY DOWN 13w; Breadth-leads ext 32.7% vs 78.3 base
  (contrarian exposure, honestly surfaced). HQM excluded until it
  wakes (2 members) — predictions require real composite history.
- Yesterday's-additions audit: every harvest list has an engine; the 6
  "unengined" flags were a str-vs-int list-id cosmetic (all six are
  ACTIVE on the page). tv-notes feed absent — extension harvest is
  PENDING-KHALID.

## TV notes — corrected record (ops 3256–3257)

- **CORRECTION**: the mirror was NEVER 10 notes. 3256's kv table was
  misread (age_hours column taken as note count — the short-report trap
  again; doctrine sharpened: align kv VALUES to their HEADER column,
  never read by position). Ground truth: data/tradingview-notes.json =
  **3,322 notes**, healthy; morning crawler merged it intact;
  notes-intel REBUILT fresh on all 3,322 (536 tickers).
- Never-shrink guard shipped anyway (crawler refuses any write that
  halves a healthy mirror after a fresh re-read) — the failure mode is
  now impossible regardless.
- **CADENCE (the real answer)**: watchlists — one extension push
  id-merges them, done (Khalid's yesterday run worked exactly as
  designed, 6,507 syms). NOTES autonomy — daily 06:00 crawler on the TV
  session cookie: TODAY authenticated as username=None, pulled 0 ⇒
  cookie DEAD. New notes on TV are NOT flowing until either (a) SSM
  /justhodl/tradingview/sessionid (+ sessionid_sign) refreshed from
  browser cookies → daily autonomy resumes, or (b) extension re-run
  (pushes notes too). PENDING-KHALID = cookie refresh, not extension.
- Ingest confirmed safe: id-merge for both notes and watchlists;
  watchlists-only pushes valid by design. brain=9,200 notes total
  (tv-provenance lives behind the API store, not data/brain.json).

## Brain permanence PROVEN (ops 3258)

- **Canonical brain (Cloudflare KV, uid brain-930ffa48…): 12,122 total
  notes, 2,920 TV-provenance** — queried live through the same worker
  door the upserts write through. SSM /justhodl/brain/uid already
  matched canonical; no uid split, no repair needed.
- Khalid's design intent CONFIRMED: notes + watchlists are copied into
  the brain and kept permanently — TV cookies irrelevant to what's
  already stored. data/brain.json (9,200) is brain-sync's OLDER S3
  distillation copy — KV is the authority; the earlier "0 tv notes"
  alarm was a stale-mirror artifact.
- S3 bucket versioning: **Enabled** — mirror + watchlists carry full
  write history too.
- Cookie refresh (SSM /justhodl/tradingview/sessionid) matters ONLY for
  autonomously capturing NEW notes going forward; extension re-run is
  the manual alternative. Nothing already captured is at risk.
- Optional completionist gap: mirror 3,322 vs brain-tv 2,920 (~400
  id-scheme/short-text differences) — top-up available if wanted.

## Notes → engines, semantic parity, PLAYBOOK (ops 3259–3263)

- **NOTES EVERYWHERE (3259–3260)**: the ops-3171 khalid_note stance
  fusion verified LIVE — best-setups: 74 rows carry khalid_note (14
  non-null; NVDA stance=BEARISH riding the setups); alpha-compass
  carries the field; master-ranker coded (lines ~1186–88, feed
  refreshes on its cron). NEW consumer: equity-research docs attach
  **khalid_notes** (stance / latest_note / levels via
  khalid_notes_block ← data/notes-index.json, container-cached, never
  raises) — proven live in equity-research/NVDA.json: stance=BEARISH
  n=2 "IS NVIDIA EXPENSIVE? Nvidia pe is 26.6…".
- **SEMANTIC PARITY (3261–3262)**: worker PUT contract is
  **{note:{…}}** wrapper ({delete:"id"} deletes; {notes:[…]} is
  bulk-REPLACE — never use). uid ≥20 chars = authed (no PIN). Write-
  time junk guard (_isJunk: <25 chars, transcript patterns, code/URLs,
  low letter-ratio) returns **200 ok:"rejected-junk"**, not 4xx.
  Normalized-text DEDUP admin ops exist (token-gated). The ~400 id-gap
  decomposed: 3 text-duplicates + 250 guard-rejected fragments (the
  brain's OWN protections, Khalid's design) + genuinely-absent
  (~120, e.g. CL1!/SVXY/UVXY options-timing notes) pushed with the
  wrapper. **RESULT: 3,069/3,069 substantive unique notes in the
  canonical brain.**
- **PLAYBOOK ENGINE (3263, PASS)**: new lambda
  justhodl-playbook-engine (Function URL
  https://ygn3rwfckm3ya6g6l52637bq2i0fiyvq.lambda-url.us-east-1.on.aws/,
  donor env justhodl-notes-intel, NO EB schedule v1 — rule cap
  saturated; invoke on demand). Deterministic extraction of his tested
  rules from the mirror → **data/playbook-rules.json**: families
  TIMING / INVARIANT / TURN / CONDITIONAL. **563 rules from 3,322
  notes.** Samples live: yield-curve 30-month rule, MOVE, US10Y,
  FEDFUNDS ("WHEN INTEREST RATES ARE BEING CUT…"), DRTSCILM,
  inverted-dollar. **Flagship evaluated on live FRED T10Y2Y:
  most_recent_inversion_onset=2024-09-05, months_elapsed=22.2,
  khalid_lag_months=30, lag_marker_date=2027-03-05** — facts stated
  plainly, no forecast dressing.
- Next-ups noted (not built): surface playbook-rules on a page
  (why.html or panels), EB schedule via Scheduler, master-ranker
  khalid_note visible after its next cron.

## Playbook surfaced + scheduled; ranker proof on the TRUE key (ops 3264–3266)

- **PLAYBOOK strip live on panels.html** (3264): flagship yield-curve
  countdown card (inversion onset 2024-09-05 · 22.2/30 months ·
  marker 2027-03-05, progress bar) + top rules with family tags;
  loadPlayb() beside loadPred(), reads data/playbook-rules.json.
- **Weekly schedule created** (EventBridge Scheduler — classic rule
  cap saturated): justhodl-playbook-weekly, cron(0 7 ? * MON *) UTC,
  role justhodl-scheduler-role, an hour after the notes crawler.
- **master-ranker khalid_note PROVEN** (3266): the page fetches
  **data/master-ranker.json** — 3260/3264/3265 checked
  data/master-rank.json (verifier key typo). The join ran and attached
  the entire time (log: khalid_notes=4, DONE in ~5s). Live proof on
  the true key: LRCX BULLISH / PLD MIXED / BG BEARISH / SPG MIXED
  among top_tickers. **Doctrine: verify against the key the PAGE
  fetches, never an assumed name.** Note: join sets the field only
  when a note exists (if _kn:) — rows_with_field == non_null by
  design. 3265 (failed status, benign effects: env-preserving
  redeploy + invoke) moved to ran/, closed by 3266.
- Notes directive now closed with proof across every consumer:
  best-setups ✓ alpha-compass ✓ master-ranker ✓ equity-research ✓
  + brain semantic parity ✓ + playbook engine surfaced + scheduled ✓.

## COMPOSITE MODE — size-gated panels woken (ops 3267)

- Small/sparse panels (<6 members, <6 z-scorable, or <100 joint
  breadth weeks) now run honestly on **mean-member z**: extremeness
  percentile vs own history, firing at |z|≥1.5, extreme-event study
  (|z|≥1.2, n≥20/horizon), FDR pool, grading-compatible row shape,
  prediction-layer auto-join via comps{}. MIN_COMPOSITE_WEEKS=60.
  Breadth engines untouched by construction (composite fires only
  where breadth gates fail) — proven: foreign-exchange-reserves no
  mode field, w13 n=68.
- **CENSUS: ACTIVE 131 → 194 (+63 composite)**; residual dormant just
  **13** (7× zero z-scorable on a free source, 5× lack fetchable
  history, 1× zero joint weeks) — the truly dead named-classes.
- **HQM AWAKE** (Khalid's original example): mode=composite, z=1.32,
  pct=91.2, n_weeks=1,746, 2/2 members — and its thesis auto-joined
  predictions: → LIQUIDITY_THEME, call **LIQUIDITY DOWN within 13w**.
  Prediction theses 12 → **23**.
- Woke-and-FIRING now: Federal Reserve Liquidity z=3.65, Global
  Commodities Growth z=2.68, European Bonds z=1.66, Economic Index
  z=1.51. Drawer shows mode tag (live-verified).
- Follow-up arc queued (not built): ratio/div transform for the
  computed-ratio families (FX-reserves/GDP etc.) — would need a
  per-country mapping campaign.

## Ratio families woken — div transform (ops 3268)

- series_source gains 5-part **div**: `FRED~A~div~FRED~B` (mirrors
  minus, zero-guarded: skips keys where b==0). Shared-file change ⇒
  all three SHARED_CONSUMERS redeployed.
- FRED browse (never guess), 0.55s politeness: FER=TRESEG{cc}M052N;
  GDP ladder CLVMNACSCAB1GQ{cc}→NGDPRSAXDC{cc}Q; M3=MABMM301{cc}M189S;
  probes gated on last-obs ≥2023 AND ≥120 obs. TV ratio tiles parse as
  `ECONOMICS:{CC}XXX/ECONOMICS:{CC}YYY` (cc_rx on first ECONOMICS:).
  Map donor shape copied from a live minus entry (source=DERIVED).
- **RESULT: +12 div map entries; 3/4 ratio engines AWAKE (composite);
  ACTIVE 194→197.** fx-reserves-gdp 6/16 resolved, gdp-m3 4/18,
  fer-money-supply 2/16; **fer-external-debt 0/11 stays dormant —
  no free per-country external-debt series exists** (said plainly;
  residual dormant now 12).

## Sidebar + favorites + ENGINE CONTRACT (ops 3269–3271)

- **Sidebar root cause**: nav-manifest.json was hand-maintained and
  froze 2026-07-05 at 362 pages while the site grew to **377** —
  panels.html and 14 others invisible. FIX: scripts/gen_nav_manifest.py
  regenerates from actual repo pages on EVERY deploy (pages.yml step);
  known hrefs keep categories, new pages keyword-classify, redirect
  stubs skipped. Drawer fetches /nav-manifest.json at runtime → live
  instantly.
- **Favorites were NEVER deleted**: jh_favs intact; pullSync is a true
  union-merge (remote ∪ local, pushes back extras). Stars on
  post-Jul-05 pages were display-FILTERED by titleByHref. FIX: drawer
  renders every starred href with a fallback title — a star can never
  silently vanish (ops 3269 literal in jh-nav-drawer.js).
- **ENGINE CONTRACT permanent in AUTONOMY.md**: any panel size
  activates (1 indicator enough); only genuine data absence may leave
  dormancy, named; every note feeds ≥1 consumer; new notes/lists
  auto-materialize. **Enforcement (3270): MIN_COMPOSITE_WEEKS 60→13.**
  **Audit at z-resolution (3271): ZERO violations** — adaptive window
  win=min(156,max(12,len)) ⇒ nascent = raw < mcw+12 = 25wk; the one
  candidate (Country Import Prices, 20 raw wk) is z-nascent and
  self-wakes as history accrues. Notes census: 3,322 notes → 536
  tickers indexed → 4 consumers + 563 playbook rules + themes + brain.

## Favorites cache chain closed + TV WATCHLISTS on the charts page (ops 3272–3273)

- **Why Khalid still saw no stars after 3269**: three stacked caches —
  (1) drawer JS had no version param (browser held old code);
  (2) manifest fetch had no buster (CDN held July-5 list);
  (3) index.html is SKIPPED by reskin ('native') so its hardcoded tag
  never versioned. FIXES: reskin injects content-hashed
  `?v=<md5-8>` src + normalizes pre-existing hardcoded tags across
  all pages; drawer fetches manifest with hourly buster; dedicated
  pages.yml step versions the index tag. Homepage verified serving
  versioned tag. One reload on his side = fresh world.
- **TV WATCHLISTS live on the charts page** (charts.html → chart-pro):
  new section atop the EXISTING left drawer (edge-tab summon =
  his TV-like disappear/reappear, jh_wl_open): list <select> (all
  harvest lists), symbol rail, ↑/↓ arrow browsing, position remembered
  (jh_wl_pos). Router: equity `EXCH:TICK` → ChartController.loadTicker
  (+State.symbolResolution); FRED-mapped plain ids → loadTicker;
  derived/transform/ECONOMICS tiles → overlay pane (own
  LightweightCharts line) fed by **justhodl-wl-series-api**
  (Function URL nu4umjskc25osscrbmqh3o2gte0utlkx…, CORS *, container-
  cached fleet gz, 6h refresh) — proof TVC:DXY n=1906. Cache keys are
  ISO-WEEKS ('2026-28') → wk2d() converts to Monday dates for
  LightweightCharts. Esc closes overlay.

## Runtime truth pass (ops 3274–3275)

- 3274 diagnostic: SERVED drawer JS parses ✓, served ops-3273 block
  parses ✓ (reskin did NOT mangle), tv-watchlists (207 lists) +
  symbol-map (4,823) return 200 from BOTH worker and origin,
  cf=DYNAMIC (CF not caching HTML; browser cc max-age=600 only),
  served tag versioned. Server-side fully healthy → remaining
  variables are client-side.
- 3275 hardening: watchlist block fetches SAME-ORIGIN /data/* (hourly
  buster), renders its own errors into the section (never silently
  empty), list UI decoupled from LightweightCharts (overlay checks it
  lazily), status line '· N lists loaded'. Drawer FAVORITES states
  per-browser truth when jh_favs is empty (stars are per-browser;
  sign-in on the holding browser syncs them).

## Client-truth telemetry armed (ops 3276)

- Diag beacon live: chart-pro's MY-TV-WATCHLISTS footer renders
  `diag: lists=N · sw=… · drawer=vHASH · favs=N · lwc=…` AND fires the
  same payload to the wl-series-api (?diag=1 → CloudWatch [diag]).
  Drawer fires a one-shot per-session beacon (page, favs, sw) from
  EVERY page + forces serviceWorker registration.update() each view.
  SW bumped v1.2.0-3276 (full cache turnover on adoption).
- 4-min listen captured no beacons (Khalid not browsing) — the next
  page load he makes lands his browser's reality in CloudWatch;
  read with logs filter '[diag]' on /aws/lambda/justhodl-wl-series-api.

## Beacons decode the mystery + 13F additive (ops 3277)

- **Khalid's Edge beacons (23:04 UTC)**: chartpro `lists=207, err="",
  drawer=f68c5019 (current), lwc=true, sw=service-worker.js, v=3276`
  → **the watchlists section WORKS in his browser** post-SW-turnover.
  `favs=2` in this Edge profile → the big starred set lives in a
  DIFFERENT browser/profile (per-browser storage); union-sync carries
  it once he signs in on the holding profile. Case closed with client
  ground truth.
- **13F additive** (improve-doctrine now permanent in memory #26 +
  AUTONOMY.md): two sections appended, existing untouched-verified —
  Cluster Moves (≥2-fund NEW/EXIT, ≥3-fund ADD, fund chips; live:
  GOOG NEW by Berkshire+Bridgewater, V by Coatue+Lone Pine; as_of
  2026-03-31, 17 funds) + Smart Money ∩ Your Engines (khalid_note
  stance × master-ranker join).

## 13F visual directives closed (ops 3278–3278c)

- **Name-first everywhere**: 4 renderer cells (spotlight ×3 patterns,
  consensus, rare, most-bought/sold shared cell) show company names,
  never '?' or cusip codes; helpers firstWords/fmtCap/capBadge.
- **Resolver truth chain**: cusip_to_ticker_via_fmp was defined but
  NEVER CALLED → wired via resolve_missing_tickers() with persistent
  data/13f-cusip-map.json. FMP name-search alone mis-mapped
  (ARGAN INC→ARLLF Argan S.A.; HTIA-class) → **SEC-FIRST:
  sec.gov/files/company_tickers.json daily-cached in the map doc,
  _norm_name token matching (strip INC/CORP/…); FMP strict tier-2
  (≥2-token overlap, alpha ≤5); poisoned entries self-heal (src
  upgrades).** PROOF: **ARGAN INC → AGX tier=MID cap=$8.4B**.
  Residual ARLLF aggregate row = verify next run (real Argan S.A.
  holding vs stale poison).
- **Spotlight**: cap-tier badges beside net-action scores. **Most-held**
  gains total-$-held via aggregate join. **Rare picks** show tiers.
- **NEW Small & Mid-Cap Footprint section**: micro/small/mid boards
  ranked by accumulation (NEW+adds), with honesty filters (ETF/trust
  regex + total_value>3×mcap ownership-implausible → filtered, count
  shown). Engine enriches market_cap+cap_tier live (FMP stable
  /market-capitalization→profile fallback, 0.12s gate, ≤600 tickers).
- Data-quality follow-up queued: total_value unit audit (GOSS $5.3B
  class), ARLLF residual.

## Outside-the-box 13F: options + divergence (ops 3279–3279c)

- **Divergence surfaced (LIVE, verified 3279)**: the existing
  justhodl-13f-price-divergence engine (decay-scored, 90d) finally
  feeds the page — "Whale Entry vs Price: still actionable?" boards
  (bullish hasn't-run / bearish divergence) from
  data/13f-price-divergence.json.
- **putCall pipeline**: parser v3 captured the column but the
  cusip-collapse DROPPED it and merged option rows into equity rows
  (zero-rows mystery). 3279c: collapse key = (cusip, putCall), option
  rows tallied to put_funds/call_funds, EXCLUDED from equity $
  aggregates + pct_of_portfolio (clean books), PARSER v4 full
  re-parse. "Whales' Options — hedges & leverage" boards live on
  page. **3279c proof run IN FLIGHT (long EDGAR pass) — read
  aws/ops/reports/latest/3279c_options_proven.md next session;
  expect Citadel-class PUT rows.** Refinement queued: filter option
  rows out of changes_summary (cluster boards) too.
- **Institutional roadmap delivered to Khalid (A–F)**: A Clone-Alpha
  manager skill scores (back-quarter backfill + fwd returns of
  disclosed changes → skill-weighted boards) = flagship; C conviction
  boards (top-10 entries, % of company owned via sharesOutstanding);
  D crowding/days-to-exit (ADV enrich); E sector rotation flows;
  F 13F × panels/GEX composite.
