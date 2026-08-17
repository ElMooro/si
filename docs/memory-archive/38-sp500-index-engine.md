# 38 — justhodl-sp500: The Index As A Stock (ops 4808, 2026-08-17)

**Origin (Khalid):** "engine called sp500 giving all sp500 metrics as a whole —
p/e, forward p/e, yield, everything — as if the index were a single stock, so
I can compare any stock against it when deciding to buy."

## What shipped (v1.0.3, ops 4808 GREEN)
- Lambda `justhodl-sp500` (py3.12, 1024MB/300s), env `FRED_API_KEY` healed
  from donor `dollar-strength-agent` (NO justhodl- prefix — the prefixed name
  does not exist; ops 4807 never tripped this because its target env was
  already populated).
- Scheduler `justhodl-sp500-daily` cron(45 21 ? * MON-FRI *) UTC — 30 min
  after justhodl-spx-ma refreshes `spx-ma/member-closes.json`.
- Outputs: `data/sp500.json` (full doc) + `data/sp500-history.json`
  (merge-on-write daily headline ledger, permanent).
- Page `sp500.html` (nav-linked from invest.html): headline cards, six metric
  tables (agg · member median · p25–p75 · p10–p90 · n), macro-cross strip,
  and the COMPARE box — client-side stock vs index (agg + median + percentile
  + CHEAP/IN LINE/RICH at ±10% vs median) built from the `members` block.
- Compare mode server-side too: invoke `{"compare":"NVDA"}` → 12 rows.

## The math (index-as-a-stock)
- Ratio-of-sums everywhere: index P/E = Σ(repriced cap) / Σ(NI), with per-
  member NI = cap_census × earnings_yield_pct/100 (SIGN-PRESERVING → losers
  in the denominator, exactly like the real index; pe>0 fallback for null ey).
- Components backed out per member from the census matrix snapshot:
  rev = cap/ps · book = cap/pb · EV = ev_sales×rev · ebitda = EV/ev_ebitda ·
  fcf = cap×fcfy · div/buyback null→0 (index treatment, counted in diag).
- Daily reprice from the member-closes ledger: s = px_now/px_census;
  cap_now = cap×s; EV_now = EV + cap(s−1). 493/495 repriced day one.
- **Forward block: matrix cols `pe_fwd` / `ps_fwd` / `ev_ebitda_fwd`** (the
  fundamental-graphs derived NTM ratios). NI_ntm = cap/pe_fwd etc. Raw
  `est_*` columns are EXCLUDED from `_lv` by census `MX_EXCLUDE_PRE =
  ("px_","rsi_","vol","est_")` — cols_missing proved it; the derived ratios
  are the only in-matrix consensus carrier. NTM growth = pe_ttm/pe_fwd − 1.
- **Component-correct quality aggs** (means are poisoned by negative-equity
  mega-caps — cap-weighted ROE read 205.9 raw, 65.2 winsorized, both wrong):
  ROE = ΣNI/ΣBook×100 (≡ pb_agg/pe_agg), net margin = ΣNI/ΣRev, FCF margin =
  ΣFCF/ΣRev. Other quality/growth/balance = cap-weighted mean winsorized
  p2–p98 (dists always from raw member cols).
- Macro cross via FRED (DGS10/DGS2/CPIAUCSL): ERP ttm+fwd, Rule of 20
  (pe + CPI YoY), div_yield − 10Y, 2s10s.
- Every metric = {agg, median, p10/p25/p75/p90, n, cap_cov_pct, unit,
  method}. `members` block: 15-field array per ticker (MEMBER_FIELDS legend)
  powers page-side percentiles.

## Day-one real numbers (2026-08-17)
SPX 7,786.01 · cap $70.87T · 495 members · P/E 24.73 (median 23.45) ·
**fwd P/E 20.37** (median 18.34, NTM growth 21.4%) · EY 4.04% (fwd 4.91%) ·
div 1.07% · buyback 1.23% · FCF yield 3.31% · P/S 3.55 · P/B 5.28 ·
EV/EBITDA 16.28 · **ROE 21.3% (ΣNI/ΣBook)** · 10Y 4.63 · CPI 3.54 →
ERP −0.59% (fwd +0.28%) · Rule of 20 = 28.3. AAPL compare: P/E 32.5 → 71.1
pct RICH · fwd 30.7 → 84.4 RICH · P/B 38.9 → 92.5 RICH.

## Gotchas burned (4 red cycles → green)
1. Donor name: `justhodl-dollar-strength-agent` ≠ real `dollar-strength-agent`.
   Heal is now multi-donor (+ justhodl-blackswan-watch) with per-donor
   ClientError catch.
2. `est_*` absent from matrix (MX_EXCLUDE_PRE) → forward None. Fix = derived
   `*_fwd` ratio cols. est_eps_avg × shares was a dead end (also excluded).
3. **UnboundLocalError cap_c**: patch inserted the forward block BEFORE the
   components section that defines cap_c. py_compile can't catch it; the
   async Event invoke hid the traceback (doc "never refreshed within 8 min").
   → LOCAL HARNESS IS THE GATE: stub boto3 via sys.modules, synthetic matrix
   + ledger, assert aggregate identities against ground truth BEFORE push.
   It caught the crash in 5 s and validated the fix exactly.
4. Harness date trap: census generated_at AFTER all ledger dates → ci==li →
   scale 1.0; a ×1.05 "truth" then reads as an engine bug. Census date must
   sit between ledger dates in synthetic fixtures.
5. Cosmetic: ops kv() reused metric names for compare rows → Data table
   collision (pe_ttm shows AAPL string). Band log is authoritative.

## Verify pattern (reusable)
Settle by zip-marker (MARKER "sp500 v1.0.3") → Event-invoke → poll as_of ≤8m
→ truth bands (members 400–520, pe 12–45, fwd 10–pe×1.25, ey 1.5–9, dy
0.5–4, roe 5–40, erp −6..8 warn) → compare smoke (12 rows, percentiles).

Next ops: 4809.

## v1.1.0 (ops 4809 GREEN, same day) — expanded better-buy compare
41-field member block (was 15) + 38-metric grouped compare + FIVE-PILLAR
percentile score (valuation .30 / quality .25 / growth .25 / balance .10 /
momentum .10; low-better metrics inverted; per-pillar min-n gates) →
composite 0-100 + verdict tiers (≥65 STRONG CANDIDATE / 55 MODEST EDGE /
45 NO CLEAR EDGE — SPX default / <45 PREFER SPX) + tags. New fields all
matrix-real: ps_fwd 99.2% cov · ev_ebitda_fwd 95.4% · roic/roa/altman/
shareholder-yield 100% · ntm_growth 94.3% · mom_6m/mom_12_1 ~100% (census
moms block IS in the matrix). Momentum rows: member-median only (no index
agg). Page: verdict band + pillar bars + grouped table; ?t=NVDA deep-link.
Day-one: NVDA 62.4 MODEST EDGE (higher quality/faster growth/stronger
momentum, richer) · AAPL 54.0 NO CLEAR EDGE. Nav FORCE-pinned "Research &
Tools" (ops 4809). Next ops 4810.

## v1.2.0 (ops 4810 GREEN) — per-sector fair fight
`sectors` block: 11 GICS ratio-of-sums aggregates (pe/fpe/ntm/ps/ev_ebitda/
roe=ΣNI/ΣBook/net_margin/div+fcf yields/weight/n; AGG gained min_k param —
the k>=50 gate would have nulled every sector). compare() adds
`sector_context` (7 rows vs sector agg + fwd-P/E percentile-in-sector via
_sec_idx). Page: sector table, sector line in verdict band, history
sparklines (unlock at 5 banked days). Day-one truth: weights 100.1%, Tech
34.1%/89 · Comm Svcs pe 17.8 fpe 22.3 (NTM −19.9% — consensus decline,
honest) · Fin 15.5/15.0 · Energy fpe 12.4. Killer read: NVDA fwd P/E =
84th pctile in the INDEX but 48th pctile IN TECH (mid-pack for its
sector); AAPL 77.5th in Tech (rich even for its sector). QUEUED: day-two
re-read after first scheduled run (Mon 21:45 UTC — ledger accretion,
history chart unlock path). Next ops 4811.

## SPX-BEATERS sibling engine (ops 4811 GREEN, 2026-08-17)
justhodl-spx-beaters v1.0.0 → data/spx-beaters.json + spx-beaters/
weekly-closes.json ledger, Scheduler justhodl-spx-beaters-weekly cron(0 13
? * SAT *), page spx-beaters.html (pinned Portfolio & Execution) + teaser
section on sp500.html. Weekly scan: ALL caps (universe 5,239) + ALL ETFs +
asset classes. Own factor: full-market weekly-close momentum ledger
(Polygon grouped-daily, one call/Friday, ≤30 fetches/run, 53w target —
day-one bootstrapped 30w in a single invoke, ~20s to fresh doc). Legs:
mom .30 (cross-sectional 12-1 .6 + 6m .4 pctile) / fleet .25 (stock-
buying tier+score, best-setups rank, master-ranker, invest tier-3) /
flows .15 (13F net$ 6,778 tickers + congress-alpha) / industry .15 (boom
league pctile, 131 inds) / quality .15 (sp500 five-pillar composite,
S&P members only). ETF: mom .35 / compass ER-vs-SPY .35 / rotation .30
(trend gate + rank + RRG). Rules: ≥2 legs, mom+industry-alone never
lists (industry is group evidence), score≥55, weights renormalize over
available legs, macro (risk-gate sizing + rotation regime + ERP/R20)
ships as CONTEXT never a multiplier (brain). Day-one: 1,507 qualifiers
(377/388/434/308 by cap, 21 eq-ETF, 2 commodity), 77 listed, 0 contract
violations. Tops: SPCX 94.9 (6m +539%, 13F +$7.65B) · IWD 92.0 (RRG
LEADING + gate PASS) · USO 91.9. Honest partials: 12-1 pending 30/53w
(completes next 1-2 Saturdays), etf_bond/crypto buckets empty (census
class coverage — candidates for classification widening). G0 gates
verified every live feed contract pre-invoke (universe.stocks/compass.
assets/rotation.assets/boom.league/stock-buying.top/13f.t/sp500.members).
Next ops 4812.

## ops 4812–4813: ledger complete + forensics + v1.0.1
Invoke-to-complete finished the momentum ledger SAME DAY (round 1:
+23 fetches → 53/53w, ~20s). 12-1 LIVE market-wide; SPY 12-1 = +15.5%
(the bar); 71/78 listed rows carry 12-1. FORENSICS: **SPCX = SpaceX**
(universe row: "Space Exploration Technologies Corp.", NASDAQ, $108.74,
mcap $1.42T, cap_bucket "mega") — the +539%/6m +402%/12-1 with 13F
+$7.65B is a REAL post-IPO rocket, not an artifact; the ops "looks like
a FUND" warn was the substring 'spac' matching "SPACe" (heuristic
false-positive, engine correct). Bond/crypto zeros = HONEST: all 12
candidates (TLT/IEF/LQD/HYG/EMB/MUB/SHY/TIP/BIL + BTC/ETH/IBIT) hold
ledger closes and scored < bar. Latent bug fixed in v1.0.1: compass
pseudo-tickers BTC/ETH could bind momentum to unrelated grouped-daily
equities → MOM_ALIAS {BTC→IBIT, ETH→ETHA} (why[] cites "via IBIT");
mega→large explicit. League tops with 12-1: SPCX 94.7 · APGE 93.2
(12-1 +259%) · QTTB micro (12-1 +664%) · XLE 92.9 (12-1 +34.8%) · USO
91.2. Remaining time-gated: sp500 day-two Mon 21:45 UTC · beaters first
SCHEDULED Saturday run (Aug 22). Next ops 4814.

## v1.1.0 COMEBACK wing (ops 4814 GREEN) — Khalid: "good quality stocks
beaten up bad, attractively cheap vs the sp500, higher chance to come
back and beat it"
New `comeback` bucket in spx-beaters (weekly, same doc/page). Pool: all
ledgered stocks ≥30% below 52w high (weekly closes, ≥40w). STILL-FALLING
SKIP unless comeback-screener CONFIRMED/EARLY_TURN (8w base ≥ −3%
otherwise). Legs (renorm, ≥3 required): quality .30 (S&P: sp500 quality
pillar ≥55 — spx_composites now returns per-pillar; broad: stock-buying
score ≥55) — **QUALITY REQUIRED**, cheap-alone never lists · cheap .25
(valuation pillar; why cites fwd P/E vs index agg; broad: PEG) · accum
.20 (13F net>0 INTO weakness + congress) · stabilize .15 (8w base,
CONFIRMED→0.85) · revisions .10 (eps-revision-velocity>0). HARD TRAP
EXCLUDES (master-ranker redflag pattern): beneish red_flags +
earnings-quality avoid + insider-sell clusters + share-flows
SBC_WASH/BUYBACK_BLUFF + comeback-screener DILUTION boards. Min 60.
Ops 4814 cross-checks listed∩live-trap-sets == NONE every run. Day-one
LIVE (all S&P scope): PTC 75.8 (−30%, qual 67, fwd P/E 13.6 vs 20.4,
8w +31%) · GDDY 72.4 (−36%, fwd 10.8, −47% vs index) · IT 72.1 (8w
+42%) · INTU 70.1 (−52%, qual 71) · BR 67.9 · BSX 66.3 (−52%). Page:
comeback section w/ red dd chips + guards banner; sp500 teaser includes
comeback pick. Next ops 4815.

## v1.2.0–1.2.1 (ops 4815 GREEN) — institutional metrics + empirical
odds + anchored AI verdicts (Khalid: "all HF/institutional metrics,
odds of beating SPX, AI short answer: buy? odds? how long? downside %")
INST BLOCK per row (ledger-derived): 52w realized vol (ann), max
drawdown, Sharpe-momentum (12-1/vol), 26w RS-consistency (% weeks
beating SPY) — auto why-lines when notable. **EMPIRICAL BASE RATES from
our own 53w ledger** (single 26w cohort, 6m-return formation quintiles,
n=4,457): Q1 32.5% beat SPY / −24.3pp median · Q5 41.7% / −7.0pp ·
comeback cohort (dd≤−30 + base) n=200: 31.5%. SPY 26w +13.9%. TRUTH:
median stock LOSES to SPY in every quintile (breadth) but Q5>Q1
persistence held. Each row: odds_base_26w_pct from its live quintile
(comeback rows use comeback cohort). **AI VERDICT** (top-6/bucket,
weekly cache spx-beaters/ai-cache.json): claude-haiku-4-5 direct API
(ANTHROPIC_KEY donor justhodl-watchlist-debate; pattern copied from
watchlist-debate), strict-JSON, deterministic server clamps: odds ∈
[base±12] ∩ [2,98] · downside ∈ [min(60,½vol) .. min(95,max(dd,vol))]
· horizon 13–52w · BUY requires odds≥55. Rules fallback when LLM
unavailable (mode tag on row). v1.2.1: downside caps (349% bug — vol
anchor uncapped on +539% movers), history_weeks (short listings like
SHAZ honestly skip odds/AI). ⚠️**Anthropic billing STILL DOWN**
(new_calls=0, all-rules fallback; heals itself when Khalid tops up
credits — zero code change). Day-1 verdicts: SPCX WATCH 53%/26w/dwn60 ·
APGE WATCH 52 · XLE WATCH 47/dwn12 · PTC comeback WATCH 36/39w/dwn38.
Page: AI line per card + inst chips + base-rate table + anchor
methodology. Next ops 4816.
