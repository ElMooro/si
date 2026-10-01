# Industry-case publication and governed narrative repair (draft)

Ops 6412 returned the August 18 object's metadata, empty/unknown metrics and no matching direct bindings in its bounded scopes. It did not establish global trigger absence. This repair prepares the producer and its existing page before any recurring activation. Config, schedules, shared router/cost policies and other production pages are unchanged.

## Resulting behavior

`industry-case-publication.v1` carries original source `generated_at`/`as_of` values and the weekly ledger's complete original date sequence, with MISSING/INVALID/FUTURE/VALID clock syntax states. VALID means a well-formed non-future clock, never fresh. No authoritative SLA was found for these inputs; freshness remains UNKNOWN and current-condition eligibility false. Newly generated output time remains `generated_at`; aggregate observation `as_of` is null because no common authoritative observation date exists. Available calculations use COMPUTED or PARTIAL, not LIVE. A missing/thin primary spine remains MISSING. Unread optional sources are explicitly distinguished when the primary prevents computation.

Finite measured arithmetic, rankings, cohort membership, concentration and return calculations retain the predecessor behavior. Invalid shapes/booleans/nonfinite values are unavailable, with discarded-row/invalid-value counts; independent usable source legs survive. Finite arithmetic overflow is unavailable rather than JSON Infinity. Existing source keys and output destination are unchanged. Existing legacy packets retain their measured league, member table and cases, labelled with their original packet date and unknown source qualification. The page shows new source clocks/qualification, distinguishes output generation time, and retains ticker/industry links, sorting and Enter navigation. Dated earnings/heat wording no longer asserts current conditions. Narratives and descriptive text are escaped.

## Governed optional narratives and cost

The direct urllib/Anthropic/key path is removed. Narratives default OFF (`INDUSTRY_CASE_NARRATIVES` must explicitly equal `governed` to opt in); event payloads cannot enable them. The default yields deterministic recorded-cohort explanations and zero model calls, even on duplicate events. Enabling this environment flag is not part of this draft or an authorized recurring activation.

Optional calls require importable existing router/cost modules and affirmative existing mode/budget/engine-cap checks. They call only `llm_router.complete(tier='bulk', max_tokens=160, contains_proprietary=False, on_demand=False)`, which performs its own admission. No fallback bypass, alternate provider or shared-policy edit is added. Router denial/error/missing dependencies return a deterministic explanation without exposing exceptions. Names are capped at 120 characters, serialized fact text at 700, final prompt at 1100 and returned narrative at 420 characters. At most six router entries occur per invocation; the existing bulk router selects Anthropic Haiku and returns empty on failure rather than adding a provider.

Optional work requires at least 60 seconds of reported Lambda time remaining and a 30-second monotonic admission window, rechecked after cost admission. These are execution-budget reserves, not data-age policies. They stop new optional calls; they cannot cancel an in-flight shared-router/SDK call. No hard wall-time guarantee is claimed for enabled narratives. The default-off path avoids that optional latency.

Default incremental model cost is zero. If separately enabled, the declared cadence represents 20–23 weekday invocations/month and up to 120–138 router entries (132 in October 2026), before duplicates/retries and cache/admission effects. Actual token prices/usage and live governance settings are unverified; no hard dollar ceiling is established. Shared per-engine caps retain their existing fail-open/cache/concurrency limitations; this patch does not strengthen or weaken that policy.

## Activation blockers and rollback plan

There is no durable idempotency store, conditional publication, period claim or cross-container lock. Duplicate events can repeat the deterministic write; if narratives are separately enabled they can repeat router admission. Tests make this limit explicit. Safe cross-invocation protection requires a separately reviewed state/IAM or equivalent design; this draft adds none. Do not enable recurring execution or paid narratives until trigger ownership, duplicate/retry/concurrency behavior, enforceable cost controls and runtime time budgets are approved and proven.

Freshness remains unknown until an authoritative contract supplies observation semantics/SLA; no invented age threshold is introduced. Deployment must verify the packaged local publication helper and existing router/cost dependencies. Acceptance must use the natural scheduled publication and ordinary public source path; no invocation or access-route bypass is authorized here. Any later activation needs a reviewed exact target/cadence/retry change and captured prior settings. Rollback would disable only that new binding or restore its exact predecessor; optional narratives can return to the default-off mode. Never re-label an old packet fresh.

## Offline verification

- Actual producer replay against the frozen predecessor compares every industry and non-narrative case measurement.
- Old populated inputs, missing/invalid/future clocks, missing and malformed source legs, nonfinite/overflow cases, duplicate events, router denial/error/unavailability, prompt/output limits, six-call cap and time-budget exhaustion are tested without AWS/provider clients.
- The existing tape-truth test harness changes only industry fixture import resolution and assertions whose original frozen direct-LLM contract is intentionally superseded here; tape producer arithmetic and production files remain untouched.
- Actual page tests and intercepted Chromium cover legacy and repaired packets, original dates, unknown qualification, deterministic explanations, sorting and navigation at widths 1440 and 390.
- No change to global model policy, infrastructure, Lambda config, source schedules, provider inputs, ledgers or capital behavior. No real invocation, provider call or deployment was performed.
