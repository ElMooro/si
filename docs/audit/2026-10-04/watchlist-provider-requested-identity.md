# Watchlist provider routes retain the original request — 2026-10-04

The post-release source `db1cd68852824c84a72fa7c0ea70b569db0bdacb`
translates 55 provider mappings before `openSym` on mouse, Enter/Space,
advanced-table and Arrow navigation. Consequently `chartRoute` receives the
provider target as the request. For example, selecting `ECONOMICS:USGDPYY`
labels `FRED:A191RO1Q156NBEA` as the requested instrument and calls the relation
exact, losing the original ID and substitution qualification. The private card
also receives the replacement ID. Independent execution of the actual four
callbacks reproduces all 220 combinations. No actual user storage was read.

Earlier Pages37205726600, pinned to `3dac613520fa9e037b7610cf75f8e81a2457e019`,
failed the frontend/worker behavioral step; deploy and self-heal were skipped.
The follow-up Pages37206577739, pinned to `db1cd688`, completed on attempt1 at
13:48:14 UTC: build/deploy SUCCESS, self-heal SKIPPED. These are GitHub runner
metadata facts, without Actions log or artifact-body access. The successful
publication does not validate the original-request attribution.

The correction restores the original `openSym` arguments and resolves
`providerRest(requested) || chartSymbol(requested)` only inside the existing
noncanonical route qualification. Exactly five runtime substitutions preserve
the original requested ID, requested namespace hint and card while retaining
the provider handoff/resolved target. Provider aliases remain possible proxies
or source substitutions with equivalence explicitly unverified. Full canonical
FRED/World Bank requests retain priority over mappings and country heuristics.
The complete 55-entry providerRest and 461-entry extraChart functions,
fredId/chartSymbol, native chart, catalog, store, quotes, rail, observations/cache,
page and all other claim rows remain unchanged. No volume work is included.

The transition ledger appends five reversible edits and preserves every prior
entry and original fixture byte. The identity guard reverses all recorded
post-coverage edits before reversing the six historical coverage edits, retaining
its exact-offset assertions and original helper comparison. Tests execute the
actual source callbacks and native handler. Browser fixtures intercept every
request and invent all packets, maps and stores. They cover all55 targets through
mouse, Enter, Arrow and advanced-table controls at1440/390, plus conflicting maps,
canonical identifiers, late responses, loading and attribution expiry. Storage
assertions preserve fixture members/order/favorites/flags and UI state after each
navigation. Advanced-view activation is an explicit synthetic fixture action.

No service, endpoint, polling, cadence or paid integration is added; routing
uses the same targets and existing bounded transport as db1cd688. No production
request is made. Actual provider equivalence, runtime symbol-map contents,
returned instrument/venue/currency and live edge/nav/control acceptance remain
unverified under the retained HTTP403 STOP. Schema2 storage authority, compatible
adapter and recovery originals remain required; old-renderer-only rollback to
legacy authority is unsafe. No actual missing-list/loss/recovery, TradingView
parity or complete-system-green claim is made.

Candidate validation and exact independent review are recorded in the external
commit-bound receipts; publication of this correction requires the normal
reviewed PR/Actions lane and fresh current-main/owner reconciliation. The db1cd688
runner publication above is separate from any later correction publication.

Parent validation passed: 3,714 full Node tests, 581 targeted identity tests,
500 actual-browser cases (250 each at1440/390, including220 provider-control
combinations each), 600 syntax graphs with zero errors and36 pages/143 wiring
declarations without missing or stale entries. Browser external network requests
were zero. One unit fixture initially returned a row for every closest selector,
wrongly imitating a grip click; it now models the actual selectors and the failed
receipt is retained. No source assertion was relaxed.
