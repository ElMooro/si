# Watchlist chart identity guard — draft handoff

This draft repairs the watchlist click boundary on current main
`ad565594a579537a0b0e5eff5bcb417c50999442`. It is not merged or deployed.
Shared chart/data/catalog ownership remains `S-codex#symdir1001a`; the narrow
watchlist guard is claimed by `S-shopiz#wl1004c8`. Only that claim row changes.

## Verified defect and behavior

Post-PR91 routing passed substitutes to native chart Enter before native evidence
could retain the requested instrument. Examples: both NASDAQ:AAPL and NYSE:AAPL
became AAPL; BINANCE:BTCUSDT/BTCUSDC became BTC-USD; OANDA:EURUSD became EURUSD=X;
CME_MINI:ES1! became ES=F. A synthetic map could override FRED:DGS10 with DGS2.
Direct World Bank canonical IDs were blocked without a map hit. A country-code
heuristic changed explicit FRED suffixes before packet identity validation.
These are source/local-fixture findings. The actual production symbol map has
not been fetched, and no actual user-storage contents or losses are established.

The guard keeps requested ID, candidate and accepted handoff separate. An
unverified candidate is displayed as such and is never sent to the native handler.
Rejected clicks explicitly say the chart is unchanged. Canonical FRED and World
Bank IDs retain their full suffix, indicator and country even when a synthetic
map disagrees. Other exact catalog-qualified warehouse IDs remain supported.
Ordinary bare IDs require an unchanged native ticker and fallback ticker;
ambiguous currency, futures-shaped and bare BTC/ETH requests remain unavailable.
Explicit unchanged identifiers such as BTC-USD, GC=F and EURUSD=X are supported;
this is unchanged routing, not independent certification of provider equivalence.

The bounded provider-prefix aliases DGS2, DGS5, DGS10, DGS30 and T10Y2Y require
catalog agreement on FRED:+the identical series ID. Every alias is exercised
through actual native Enter/klines behavior: the handoff is the original bare
request, observation requested_id remains that request, and chart_alias retains
the canonical FRED ID. Mismatched packets produce no plotted bars. The native
alias evidence still says provider identity is independently unverified. Neither
catalog agreement nor that alias label establishes broader equivalence. US10Y,
DFF, SOFR and CPIAUCSL bare name/suffix matches are not generalized into aliases.
Their canonical prefixed IDs remain available through exact scalar routing.

Clicks no longer wait for the candidate map. A late map cannot replay an old
selection. The existing single map request per page is retained; no new endpoint,
poll, retry cadence, paid service, recurring cost or quote-refresh policy is added.
Rejected handoffs can reduce chart requests. Quote, storage, migrations, recovery,
notes, members, order, favorites and flags are unchanged by this repair.

## Reproducible qualification

All browser contexts are fresh invented fixtures at fixture.identity.test or the
existing fixture.watchlist.test. Every request is intercepted with a supplied
synthetic response or aborted. No production browser, provider API or actual user
storage is accessed. Source fixture inversion preserves its assertions and all
historical transition bytes; the new guard transitions are explicitly tagged.

Commands:

```
node --test --test-concurrency=4 tests/*.test.js
node tests/watchlist-identity-browser.cjs /tmp/watchlist-identity-guard-browser
node tests/watchlist-integrated-browser.cjs /tmp/watchlist-identity-guard-integrated-browser
node tests/watchlist-renderer-failure-browser.cjs /tmp/watchlist-identity-guard-failure-browser
node tests/watchlist-transaction-browser.cjs /tmp/watchlist-identity-guard-transaction-browser
python3 scripts/check_page_scripts.py
python3 scripts/gen_engine_wiring.py --check
python3 aws/ops/_preflight.py <all six changed paths>
python3 scripts/check_secrets.py
python3 scripts/check_staged_batch.py --inventory /tmp/watchlist-identity-guard-inventory.json
```

Local results: 3,194 Node checks (including 88 existing watchlist checks and 61
new checks), all passed. The new checks revisit the original 25 exact-function
counterexamples (19 routing cases plus six unchanged quote guards). The identity
browser has 68 passing handoff cases across 1440/390: the original 19 browser
counterexamples at both widths plus alias, mismatch, explicit-identity and
late-map controls. Existing editing/keyboard/scroll/mobile flows pass at both
widths; ten renderer failure cases and nineteen transaction/recovery cases pass.
Syntax: 600 public graphs, zero errors. Wiring: 36 pages / 143 references, no
missing or stale entries. Preflight, secrets and exact staged inventory are
required again on the frozen candidate before publishing the draft.

The browser JSON retains source SHA256s, original requested IDs, native submits,
chart evidence, mismatch diagnostics and intercepted transport records. Local
logs/screenshots/receipts are retained under /tmp/watchlist-identity-guard*;
independent exact-head review and the final commit/tree belong to the parent
handoff receipt and draft PR. No output claims live acceptance or TradingView
parity.

## Parent decision and recovery boundaries

Previously automatic venue, stablecoin, FX, continuous-futures and economic-name
substitutions will now show unavailable unless a future authoritative contract
qualifies them. Parent/current owner must review that handoff implication against
fresh main before any merge or deployment. Shared resolver/data adapters were
not edited or claimed. Independent exact-head review is mandatory for the draft.

PR91 merge 0ebefcefe42e781e108558050f0c4eb560beb971 and its release/recovery receipt
remain unchanged. Retain schema2 canonical data and the reviewed compatible
store/renderer adapter. Reverting to the old renderer after canonical adoption
can restore stale legacy authority and is unsafe. This guard changes no store.

Live acceptance remains HOLD at the previously retained HTTP403 denial. No
alternate host, route, tool, browser, model or credentials were used to retry it.
No merge, workflow dispatch, deployment or actual-user migration is authorized by
this draft handoff. Unrelated PR78, CloudWatch, catalog, volume, PR88 CI and the
1069 publisher issues are outside this repair and remain with their owners.
