# 37 — MacroMicro Decision + Asia-Leads Arc (ops 3582–84, 2026-07-20)

## The decision
Khalid asked whether to integrate the paid MacroMicro API and "reverse-engineer" their data sources. **Verdict: NO subscription.** Their API is a 2-endpoint wrapper (`/v1/stats` + `/v1/stats/series/{id}`) over primaries the platform already ingests directly (FRED/BLS/BEA/Census/EIA/ECB/NYFed/OFR/CFTC/13F/CDS/CB balance sheets ≈ 90% overlap). Their proprietary MM composites are their IP and unnecessary — the house composite layer (197 active engines, graded families, closed-loop) is deeper. The honest version of "reverse-engineer" = **coverage-gap analysis → source the missing leads from free primaries.**

## Probe (ops 3582, runner-side — sandbox egress can't reach these)
- **HIT**: DBnomics provider **NBS** dataset **A_A0L08** "Social Financing and Its Composition" — China TSF at the primary source.
- **HIT**: FRED releases/dates works (but see gotcha) · KR exports XTEXVA01KRM667N · TW exports VALEXPTWM052N (updated same-day).
- **MISS**: Taiwan export ORDERS not on FRED/DBnomics; dmz26.moea.gov.tw DNS-fails from runner. KR 20-day flash: unipass.customs.go.kr times out; needs free BoK ECOS key. China monthly TSF: not on DBnomics (NBS mirror = ANNUAL `A_` datasets only).

## The audit that saved 3 rebuilds (ops 3584, Khalid: "make sure they aren't already built")
1. **justhodl-econ-calendar EXISTED** (no config.json → invisible to CI): FMP economic calendar **with consensus + surprise tape** + `econ-calendar.html` — strictly better than my FRED releases/dates block. → my v1.0 `us_calendar` block **dropped**; existing engine wired + freshness-gated instead (was `upcoming=0` → re-invoked → **80 upcoming, next major: Initial Jobless Claims**; resurrect-if-unscheduled branch included).
2. **justhodl-china-liquidity EXISTED**, docstring literally: *"The textbook measure uses Total Social Financing, which is not on FRED; this engine uses… a proxy."* → the NBS find is **its own missing input**. Upgraded china-liquidity v2 with `tsf` block (annual composition, 8 series, per-series YoY) alongside the proxy; `NBS_TSF_MONTHLY` env hook ready for when a monthly source lands (PBoC-direct queued).
3. **MI already had the econ_calendar feed**; only `asia_leads` was added (zip-marker verified — MI is never invoked, LLM burn).
4. **macro-leads.html was ALREADY the "MacroMicro-style signals, without the paywall" desk** — the natural home. +2 additive cards: `asia-pulse`, `next-prints`.

## What shipped (all PASS_ALL)
- **justhodl-asia-leads v1.1** (lean): KR + TW export YoY/3m from FRED, frequency-labeled, 24m history. **LIVE: KR +47.96% YoY (Apr) · TW +48.33% YoY (May) → SEMIS DEMAND HOT** card. Scheduler daily 10:20 UTC. Deployed via ops helper (dodges the config-stomping workflow).
- **china-liquidity v2**: real NBS TSF annual composition live (8 series) next to the money-acceleration proxy; explicit provenance note.
- macro-leads.html cards served; MI feed marker verified.

## Gotchas
- FRED `releases/dates` with `include_release_dates_with_no_data=true` lists some releases **daily** (FOMC Press Release spam) — for a usable calendar use the FMP-based econ-calendar engine.
- NBS on DBnomics = **annual-only** (`A_` codes). Monthly Aggregate Financing needs PBoC-direct (queued). v1.0 shipped a "3m change" on annual data — the frequency-label bug class; always tag `frequency` per series.
- macro-leads.html patch anchor: `</script>` appears ×4 — anchor on the unique `function el(id){…}` definition.
- Failed patcher rep = file untouched (write-at-end pattern) → but MULTI-file patchers with per-rep writes leave partial state; this one left card divs without renderer → re-run idempotently (guard markers).

## PENDING-KHALID
Free BoK **ECOS key** (Korea 20-day flash — the true nowcast) · Taiwan **MOEA export-orders** endpoint discovery · **PBoC monthly TSF** source.
