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
