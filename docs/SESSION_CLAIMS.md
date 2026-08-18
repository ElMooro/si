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
| H.4.1 weekly official layer: justhodl-official-pulse (RRP proven + custody runtime-resolver) + dollar_leg composite + page card (+risk-gate wire if leg structure trivial) | 4864-4866 | S-fable-A | 2026-08-17 23:0x |
| IMF BOP worldwide layer: structure probe -> multi-country portfolio+ST-other liabilities wire -> macro hot-money composite (+BIS v2 probe folded in) | 4843-4846 | S-A#k7q2 | 2026-08-17 17:4x |

### SHARED-SURFACE RULE (foreign-flows.html) -- 2026-08-17 23:4x
The page script is now: helpers block FIRST (PROXY/fN/cls/zs/acc/
jget/S_), then one async IIFE where EVERY section is wrapped in
S_("name",fn) or try/catch. Any session editing this page MUST keep
that structure and run `node --check` on the extracted script as a
local push gate. Burn on record: a rebase re-ordered declarations
(const acc used before init -> TDZ) and one throw blanked the whole
desk for the user.

### Collision note 2026-08-17 23:1x (S-fable-A2)
Two sessions ran under the same id S-fable-A; this one is now
**S-fable-A2**. H.4.1 lane CEDED to the earlier claimant (4864-4866).
HANDOFF for that lane from my failed resolver probe (report 'ops 4864
-- custody resolver probe.md'): ALL 10 weekly FRED custody candidates
(WMTSEC/WMTSECL/WSEFINT/WSEFINTL/...) end 2012-11-07 -- FRED dropped
the custody memo family entirely; WLRRAFOIAL is the ONLY current
weekly official series. Recommend: official-pulse degrades honestly to
RRP-only + queue a Fed Data-Download-Program (federalreserve.gov
/datadownload, rel=H41, csv) direct probe for the custody memo item.
My duplicate ops_4864_*.py removed from pending (failed, inert).

## Done (this arc)
| provider-window sentinel v1.0.0 (weekly FRED-vs-bank diff, WINDOWED alerting) | 4850 | S-fable-A2 |
| catalyst-chain v1.0.0 (4-stage event->filing->street machine; 60 chains, 30 unpriced) | 4852-4853 | S-fable-A2 |
| hot-money engine split + three dedicated desks (foreign-flows/global-flows/hot-money pages) | 4854-4857 | S-fable-A2 |
| foreign-flows v1.2-v1.3 (21-country treasury matrix; hist_10y; all six official/private families) | 4858-4863 | S-fable-A2 |
| foreign-flows v1.4 (per-country equity decomposition + Euroclear china+belgium composite) | 4868 | S-fable-A2 |
| foreign-flows v1.5 (absorption 12.0% + auction tape + accel flags) | 4869-4871 | S-fable-A2 |
| HOTFIX foreign-flows.html: cross-session TDZ blanked the desk; helpers-first + 10 armored sections, node-gated | 4872 | S-fable-A2 |
| hot-money v1.1.0: TPEx OTC leg (-4.66bn vs listed +45.45 -> combined +40.79 identity-checked) | 4873-4874 | S-fable-A2 |
| global-flows v1.3.0: Japan MOF weekly (1,127 weeks banked, both directions; +1,629bn JPY carry bid) | 4875-4876 | S-fable-A2 |
| gf page hotfix: unit suffix bound to declared unit + JP flag | 4877 | S-fable-A2 |
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
- Fed H.4.1 WEEKLY official layer: WLRRAFOIAL (foreign-official RRP,
  PROVEN live 2026-08-12=$357.4B) + custody series (WMTSECL stale 2012;
  wire op must resolve the CURRENT sibling empirically: search Weekly
  candidates incl RESPP*-prefixed, pick last-obs>=2026-07) -> weekly_official block in
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
