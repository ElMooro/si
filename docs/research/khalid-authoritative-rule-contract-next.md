# Next bounded slice: one Khalid qualification contract

Proposal after the separate Katlin diagnostic correction; no implementation or
live rule change in this draft. Fleet-wide page/output coverage is owned elsewhere.

## Problem and scope

`aws/lambdas/justhodl-khalid/source/scoring.py` qualifies backend candidates while
`jh-khalid-sniper.js:score` independently calculates different requirements for
`khalid.html` and chart contexts. The browser adds a 504-bar drawdown and 63-bar
support definition; backend uses different location, supply, catalyst and
reward/risk gates. Selecting either wholesale would silently change the strategy.

The next draft should add a public, versioned **backend qualification evidence**
contract to existing Khalid output, and render that contract on the existing page.
Keep existing production action/risk fields and thresholds unchanged in this first
slice. Separate `existing_backend_rules` from `requested_strategy_definition` so
requested but unresolved/unimplemented criteria cannot appear as passed.

Each criterion needs a stable ID, exact definition/version, applicability,
`PASS`/`FAIL`/`UNAVAILABLE`/`UNRESOLVED` state, measured value/unit, evaluation clock,
source/effective/availability clocks, public provenance reference and reason.
The engine publishes the complete ordered checklist and overall qualification.
Missing fields or unknown versions block a browser qualification badge. No local
recalculation, truthy evidence strings, score fallback or fabricated timestamp.

`khalid.html` renders backend qualification and coverage, including failed/open
criteria. Chart/individual-symbol callers must show unavailable when the engine
has no matching identity/revision; they must not run a second recommendation
scorer. Retain any standalone technical chart observations only as explicitly
separate measurements with no qualification authority. Inventory both consumers
before retiring the independent sniper badge path.

## Definitions requiring explicit resolution

- Preserve requested stocks/ETFs/metals/crypto scope, resilience during SP500
  declines, genuine recent/current ETF inflows, tight range/Bollinger compression,
  normalized capitulation volume, >=50% drawdown, support, oversold RSI,
  biotech/smallcap exclusions and tech/ETF/momentum preference.
- '3m' remains unresolved: browser 63 native bars and quarterly thesis context are
  different, especially for crypto. Do not choose the backtest winner.
- Freeze resilience benchmark/horizon/decline rule, drawdown anchor, RSI cutoff,
  Bollinger lookback/threshold, climax shock/volume baselines, sector/cap taxonomy,
  preferred versus required gates and flow publication lag/window before claiming
  a complete requested-strategy match.
- Entry/exit, holding period, re-entry, position overlap and costs are still
  unspecified; event studies cannot become an executable portfolio backtest.

Coordinate flow criterion definitions and producer references with the legacy
proxy-semantics owner. The current native provider-flow projection is informational
only: no silent promotion into action/risk votes, no holdings-to-trade inference,
and no OHLCV proxy presented as measured ETF creations/redemptions.

## Proposed review and acceptance

1. Trace current backend action fields and preserve them byte-for-byte on frozen
   fixtures while adding qualification evidence; tests must fail on contradictory
   or missing criteria, revisions and source clocks.
2. Test the page with contradictory old browser inputs: only engine-qualified
   state can determine the new badge/checklist. Verify every public contract field
   is rendered or has a documented display reason, with keyboard/mobile behavior.
3. Keep full-strategy historical testing unavailable until retained inputs and
   definitions qualify. Display the separate technical pilot as exploratory only;
   repeated event counts, omitted gates and missing costs stay visible.
4. Review the exact single-contract/page draft independently before release. Any
   subsequent promotion of newly defined gates into live selections is a separate
   explicit delta, not an incidental consequence of UI reconciliation.

This does not expose private Brain/account inputs, alter unrelated pages or
combine holdings UI, flow semantic repairs and capital-policy changes in one PR.
