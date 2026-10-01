# Tape-truth observation qualification — draft

The old producer labels any retained CVD ledger LIVE. A failed refresh can publish
that old ledger under a new generated_at and emit GENUINE_UP with conviction. The
industry-case join previously copied only call/conviction, and three public pages
presented those verdicts without the source dates. Recent bar proxies also cannot
establish aggressor direction, participant identity or intent.

This slice withdraws those calls rather than substituting a new classifier.
`tape-truth-observations.v2` retains the existing observations and calculations;
root status is RESEARCH, calculation-availability labels become OBSERVATIONS,
and verdict call/conviction are null. `tape-truth-qualification.v1` always marks
calls/sizing ineligible and freshness UNKNOWN. No authoritative observation SLA
has been established; no TTL, exchange calendar or delayed-feed duration is
invented. A malformed, timezone-naive or future clock is explicitly classified,
never promoted to freshness. Date-only source keys remain dates, not fabricated
midnight/closing timestamps.

## Provenance and scope

- CVD source date: original requested-session ledger key. Underlying minute bar
  times remain unverified; the existing session-selection math is unchanged.
- FINRA source date: retained filename/ledger date, including fallback days.
  Short-sale volume is not net selling, short interest or participant intent.
- GEX timestamp: original response `timestamp` if present, otherwise missing.
  This does not establish individual option/OI observation times or feed delay.
  Empty-chain responses retain a supplied timestamp too. Arithmetic and the
  dealer-position assumption are unchanged, not newly validated.
- Publication: tape-truth generated_at is separate from all three source clocks.
- `cases[t].tape` uses `tape-truth-projection.v1`, retains the original source
  publication and full observation legs, and records projection time separately.
  Unknown/legacy source contracts or invalid publication clocks are unavailable.
  A new projection never refreshes its retained observations.
- Shared Python helper: only tape-truth and industry-case import it. No I/O.
- Shared JS helper: only tape-truth.html, industry-case.html and why.html use it.
  The three pages withhold old/unknown contracts. why changes are restricted to
  IC_/TT_; they subscribe to the existing ticker bus, including switches while
  the original single fetch is still pending. The bus and other modules remain
  byte-identical. Unrelated industry context remains available.
- Tape-truth config changes only its unsupported Description copy; schedule,
  handler, runtime, timeout and memory declarations are unchanged.

## Consumer boundary

Exact repository references resolve to these two producers, the narrow helpers
and these three pages. Actual output packets abstain in the existing Strategist
extractor and Causality scalar extractor. No protected decision/risk/order consumer
was established; external consumers remain unverified. Generic public narrative
readers may encounter the artifact, so unsupported calls are removed at source.
Industry-case's llm_case function and its supplied facts remain unchanged: tape
was not part of that prompt. Tests compare the actual intercepted prompt inputs.
No provider requests, subscriptions, schedules, manual invocations or private
content are introduced or used by this work.

## Reproducible acceptance

`python3 -B aws/lambdas/justhodl-tape-truth/tests/run_tests.py` executes actual
producer/build and industry projection code with stub clients and intercepted
storage/HTTP/LLM calls. Both complete original producers are frozen byte-for-byte.
Fresh, failed-refresh, missing-key and FINRA fallback scenarios preserve fetch
calls, every existing observation value and ledger writes. The stale-ledger
counterexample reproduces old GENUINE_UP and verifies withdrawal. Missing,
malformed, naive and future clocks, old/unknown contracts, malformed known-contract symbol maps and observation
legs, old-source/new-wrapper
projections, unrelated industry results, prompts and capital abstention are checked.

`node --test tests/tape-truth-qualification.test.js` runs the actual generated
packets through all three renderers, including the original why ticker bus, a
switch while loads are pending, and legacy clearing. Numeric zero stays zero;
null, booleans, strings and nonfinite numbers are not formatted as observations.

`node tests/tape-truth-browser.cjs` exercises 1440/390 tape layout, the original
industry Enter control, both why modules and ticker switching, and legacy
withholding. Every request is intercepted. The why harness contains the exact
scoped modules and original bus, not the unrelated full research application.

Known pre-existing limitation: industry-case's league section calls undefined
`cls`, caught by its existing section wrapper. This unrelated defect is retained.
No claim is made that all industry UI works. CVD zero/missing handling, retained
baseline windows, session calendar, divergence math, and GEX assumptions/crossover
math are unchanged and remain separate work. Source values retained by this
contract are research observations, not certified measurements.

Full repository gates and independent review must bind to the final committed
head before release. This document does not authorize release. Runtime receipts,
publication and live-browser acceptance remain future checks; legacy packets will
stay unavailable after a future page deployment until compatible publication.
