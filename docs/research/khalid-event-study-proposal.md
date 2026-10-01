# Khalid forward-return event study — proposal, not a frozen protocol

No executable-strategy claim, threshold change, acquisition or production wiring
is authorized by this document. Status: definitions and point-in-time evidence
unresolved. First freeze a versioned specification before looking at outcomes.

## Definitions to freeze

| Criterion | Existing convention | Study decision still needed |
| --- | --- | --- |
| Asset scope | Browser current SP500 members, hard-coded Nasdaq list, curated ETF/metal/bond/crypto instruments | Historical security identities, membership and asset-specific calendars; metal/bond ETF proxies are not spot metals/direct bonds. |
| SP500 decline / resilience | No explicit required browser/backend gate found | Benchmark vehicle, trailing decline horizon/threshold, aligned observation instant, and absolute versus relative resilience. Must use information available at entry, not future SP500 returns. |
| ETF inflows | Legacy text/score clues; native provider-flow projection is informational only | Exact fund-level measure, positive threshold, 1/5/21 reporting-observation window and verified first availability. Other team owns source semantics. No constituent trade inference from holdings. |
| Tight range / Bollinger width | Browser 20-bar Bollinger width in bottom 20% of approximately 252 bars; recent 10-bar median range <=75% of earlier 40-bar range | Freeze these as existing-browser definitions or separately preregister alternatives. |
| Capitulation | From index i−90 through i−3: return <=−3%, volume >=1.7x preceding 20-bar median, and (close−low)/(high−low) >=0.25 for a nonzero bar range | The close must be at least 25% of the bar's range above its low: low 100, high 110, close 103 passes. Fix lookback inclusivity, source volume units and venue coverage. Volume is asset-relative; the return shock is not volatility-normalized. Any asset-normalized shock variant is exploratory. |
| Drawdown >=50% | Close versus trailing 504-bar high | Confirm anchor; require full warm-up and comparable split adjustments. 504 crypto days and 504 equity sessions are different lengths. |
| Support on '3m' | Browser within −0.5%/+3.5% of trailing 63-bar low; Katlin thesis also uses calendar-quarter bars | User meaning unresolved. Name 63-session and calendar-quarter variants separately; neither may silently stand for the other. Do not select the winning definition and call it confirmatory. |
| Oversold RSI | Browser RSI(14) <=45; <=30 labelled deeply oversold | Freeze existing reset convention or obtain intended cutoff; keep alternatives separate. |
| Exclude biotech/smallcaps | No explicit exclusion found in inspected scorer | Point-in-time industry taxonomy, cap threshold and missing-classification policy. Today's cap cannot exclude historical smallcaps. |
| Tech/ETFs/momentum preference | Browser has additional optional momentum checks | Decide whether preference means stratified reporting, eligibility or weighting. Initial event study should not invent position weights. |

Existing code also imposes below-250-day, volume dry-up, flat average, higher-low,
valuation and sector gates; backend adds catalyst, dilution and reward/risk.
Keep an **existing-code replication** cohort separate from the **requested
criteria** cohort. Do not silently add or remove gates to obtain more events.

## Proposed evaluation once definitions and evidence are frozen

1. Freeze rule version/hash, universe definition, decision clock, warm-up,
   missing-data policy, all variants and hypotheses in a trials ledger before
   computing outcomes. Reserve a chronological final holdout; choose training,
   validation and test dates without reading their returns.
2. Join source records by instrument identity, effective date and verified
   availability at the decision instant. Retain corporate-action basis and
   delisted observations. Absent historical flow/holdings/classification evidence
   blocks the complete-criteria cohort; price-only results are a separate subset.
3. Candidate horizons are 21/63/126 sessions, inherited from existing research,
   pending protocol freeze. Show asset and aligned benchmark forward returns,
   distributions and close-based adverse excursions; they are event outcomes,
   not fills, strategy NAV, Sharpe or portfolio drawdown.
4. Purge training labels whose endpoints or true availability meet/cross the
   next fold boundary for each horizon. Apply the same rule between validation
   and final test. In walk-forward refits, train only on then-matured evidence;
   embargo/repeated-event rules must be explicit. Never infer availability.
5. Report funnel count after every gate, missing/rejected counts, unique assets
   and dates, overlap clusters, period/asset strata and cluster-aware uncertainty.
   Do not count overlapping asset-days as independent trials. Publish every
   preregistered variant; multiplicity adjustment and effective sample method
   require review. Zero/few events are valid findings, not a reason to loosen gates.
6. Event returns are gross observations. An execution simulation requires a
   separate decision on entry timing, exit/stop/target, holding limit, re-entry,
   overlapping positions, capital and weights. Costs then need commissions,
   spreads, slippage, impact/participation limits, dividends/actions and
   borrow/funding where relevant. Do not infer costs or executable fills from
   OHLCV, nor present current constituents as a historical investable universe.

## Evidence panel proposal

An additive research section on `khalid.html` should eventually show specification
version, definition/availability blockers, fold boundaries, gate funnel, sample
size/dependence, uncertainty, gross-versus-cost assumptions and source links.
It must distinguish replication, exploratory variants and untouched holdout;
missing evidence must not produce an accuracy badge, recommendation or gate vote.
Coordinate with the owners of provider-flow labels and holdings coverage/UI.
No page or scorer changes are included in the current offline boundary PR.
