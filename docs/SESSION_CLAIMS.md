# SESSION CLAIMS — parallel-session coordination (est. ops 4830)

Two-plus Claude sessions build concurrently. ops-number collisions are
caught by preflight, but WORKSTREAM collisions (same feature built
twice: 4819 odds chips, 4827 CSLT probe) waste runs. Protocol:

1. BEFORE starting a workstream: `git pull`, read this file.
2. Claim = add a row (workstream, ops range guess, timestamp UTC).
   Session tag MUST carry a unique nonce (e.g. S-A#k7q2) -- two
   sessions reused the same tag on 2026-08-17.
   commit + push IMMEDIATELY (tiny commit, [skip-deploy] fine).
3. Release = move the row to Done with the final ops numbers.
4. If your intended work is already claimed: pick the next free
   workstream from the queue instead. Never build a claimed row.
5. Stale claims (>6h, no matching ops activity in git log) may be
   taken over — note the takeover.

## Active claims
| workstream | ops | session | claimed (UTC) |
|---|---|---|---|
| DEEP-IMPROVEMENT probe: H.4.1 weekly custody/foreign-RRP ids + TreasuryDirect auctions + FiscalData MSPD + ECB SDW BOP + CSLT equity-tx titles | 4863 | S-fable-A | 2026-08-17 22:5x |
| IMF BOP worldwide layer: structure probe -> multi-country portfolio+ST-other liabilities wire -> macro hot-money composite (+BIS v2 probe folded in) | 4843-4846 | S-A#k7q2 | 2026-08-17 17:4x |

## Done (this arc)
| workstream | ops | session |
|---|---|---|
| base-rates spine (Fusion 1) | 4818 | S-fable-A |
| odds chips consumers (Fusion 1) | 4819-4820 | S-B |
| plumbing composite + risk-gate v2.4 (Fusion 2) | 4821-4823 | S-fable-A |
| foreign-flows engine v1.0 (TIC/CSLT) | 4824-4826 | S-fable-A |
| CSLT official/private + countries v1.1 | 4827-4829 | S-B |
| capital-flow.html TIC card + verify | 4830-4831 | S-fable-A |
| global-flows engine (Peru live) + capital-flow TOP restructure | 4832-4836 | S-A#k7q2 |
| micro-probes + Taiwan CBC+TWSE hot-money wire + generic world card + throttle fix | 4837-4842 | S-B |

## Roadmap: capital-flows engine deep improvement (2026-08-17 review)
WAVE 2A -- keyless, immediate, highest value:
- Fed H.4.1 WEEKLY official layer: Treasury custody for foreign officials
  + foreign-official reverse-repo pool -> weekly_official block in
  foreign-flows (weekly cadence vs monthly TIC = leading read on the
  official exit) -> becomes the risk-gate dollar-leg input
- CSLT per-country EQUITY net-tx + valchg (who bought the +181B June)
- Derived analytics (zero new data): china+belgium Euroclear-adjusted
  composite row; 3m-vs-12m acceleration flags per country/destination;
  buyer-concentration (custody-center share, top-5 share)
- Issuance-adjusted absorption: foreign Treasury buying / net marketable
  issuance (FiscalData MSPD) + TreasuryDirect auction indirect-bidder %
- TPEx OTC -> hot-money taiwan v1.1 (endpoints proven 4858)
- Japan MOF weekly securities flows BOTH directions (foreigners->Japan
  AND Japan->foreign bonds, the lifer carry channel), cp932 parse
WAVE 2B -- keyless, second: TIC B-tables banking channel (eurodollar
  doctrine); ECB SDW euro-area BOP portfolio liab (SDMX keyless);
  country-ETF flow fusion from etf-true-flows (EWJ/EWT/EWY/MCHI/EWZ/
  INDA... = real-time per-country appetite, internal join, zero API)
WAVE 2C -- needs Khalid: Korea BOK+KRX, Chile BCCh, Turkey EVDS key
WAVE 2D -- scrape-class, careful: India NSDL FPI, Indonesia bond
  ownership, ChinaBond foreign CGB holdings, Thailand SET retry
  w/ browser headers; NOTE: HKEX stopped daily northbound flow
  disclosure Aug 2024 -- do not fake a dead feed
ENGINE BRAIN (no new data): official-demand composite (TIC monthly
  anchor + H.4.1 weekly pulse); signal event-grading via signals-ledger
  (safe_haven z<-1.5 episodes vs fwd SPX/UST, beaters-style base
  rates); revision-aware banks (keep first_print per month, publish
  revision-bias per series); flowsxplumbing concordance block

## Open queue (expansion wave 2 -- from probe 4858, endpoints verbatim in its report)
- TPEx OTC daily foreign net -> hot-money taiwan v1.1 (openapi
  tpex_3insti_daily_trading JSON 862KB + en 3itrade_hedge_result.php
  both HTTP 200; sums listed+OTC; ROC-year dates need conversion)
- Japan MOF weekly securities flows -> global-flows japan (CSV alive
  254KB Shift-JIS at /policy/international_policy/reference/
  itn_transactions_in_securities/week.csv; cp932 parse micro-probe)
- foreign-flows v1.3: per-country EQUITY net-tx/valchg decomposition
  (series exist in CSLT release, title-probe like 4858) + Luxembourg
  strict-title probe (dup 10308 burn) + official/private per country
- Thailand SET investor-type: 403 on plain GET -- retry with browser
  headers/referer; Brazil BCB olinda: PEC path 404 -- probe service
  list for the FX-flow (fluxo cambial) resource
- BEA ITA quarterly layer (portfolio liab/assets, FDI, NIIP): needs
  BEA_KEY donor discovery across the fleet first

## Open queue (unclaimed)
- catalyst-chain v1.1: S3 freshness gate (RPO/deferred as-of must be
  <200d old -- AMZN chained on a 2020 as-of day-one, honest bug),
  UNKNOWN-direction coverage expansion, chain grading via signals
  ledger, catalyst.html card
- risk-gate dollar-leg input from foreign-flows (official-outflow z +
  safe-haven spike, STRESS-ONLY) — **UN-GATED 2026-08-17 21:30**: June
  cycle verified (new_release fired, data month 2026-06-01)
- global-flows key-gated: Korea (BOK ECOS + KRX keys) and Chile
  (BCCh token) -- BLOCKED ON KHALID
- Fusion 5/6 (sector triangle + mispriced-boom) — wave 2, after Sat
