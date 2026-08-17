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
