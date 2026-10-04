# Watchlist chart handoff: preserve coverage and expose substitutions

This revision supersedes the unreleased broad-withholding candidate described in
`watchlist-identity-guard.md`. It starts from current main
`9ed5177576cbaf8b53e0c00f8fc17e133b65411b`, incorporating PR94's separate Pages
repair and the chart owner's 461 additional routes. The prior candidate and its
HOLD/CLEAR receipts remain historical evidence; they do not qualify this revision.

The 461 targets are preserved exactly: 379 economic, 25 FX_IDC and 57 foreign
listing/index mappings. `extraChart`, `fredId` and `chartSymbol` remain byte
identical to current main. These mappings establish routing, not economic,
provider, venue or currency equivalence. The runtime symbol-map file is absent
from this checkout and has not been fetched from production. Its actual contents
remain unverified.

A watchlist click retains the original requested ID separately from the submitted
native input, resolved route, primary lookup, possible fallback and resulting
chart frame. For example, TVC:GOLD still opens GC=F and explicitly identifies a
possible proxy. LSE:IEMD still opens IEMD.L; a listing suffix does not certify GBP.
NASDAQ:AAPL retains the qualified original input and native AAPL lookup.
BINANCE:BTCUSDT, BTCUSDC and ETHBTC retain their literal native primary lookup;
the existing possible Yahoo fallback/supplemental lookup (including BTC-USD) is
disclosed. The native daily crypto path can merge Yahoo-only historical dates
even after primary success; retained primary bars do not certify a single pair
or currency for that composite history. This behavior is explicitly qualified. These
routes do not establish returned packet instrument/venue/currency truth.

Canonical FRED and World Bank IDs outrank conflicting symbol-map entries and
country-code heuristics. The five native same-ID FRED aliases retain their
original input; other exact catalog FRED prefixes are submitted explicitly to
avoid a bare-ID market fallback. A conflicting MARKET security suggestion such
as AAPL to MSFT is ignored in favor of the supported original/static route.
DATA/DESK browse-only items and malformed canonical IDs do not enter a market
fallback. No complete supported asset class is withheld.

An additive chart status line and watchlist detail identify the request,
substitution, literal namespace/pair hints and unverified returned identity. The
chart status remains visible when the watchlist closes, qualifies loading and
retained prior frames, reports unavailable scalar observations, and expires when
another selection owns the chart. `jhChartEvidence.symbol` is a chart-frame label,
not independently verified packet instrument metadata. Its source label is
inherited evidence, not a provider-equivalence certificate.

There is no new polling, paid endpoint, service or cadence. The existing single
startup symbol-map fetch uses the existing body-inclusive request helper with a
10-second deadline. It adds no requests on success; late map completion never
replays a prior click. Quote cache/request limits and the shared native fallback
transport remain unchanged. A newly retained literal primary (for example
BTCUSDT instead of a USD proxy) may fail before the existing fallback succeeds:
that adds failed calls within the unchanged, finite native waterfall (up to nine
market URL attempts, ten for index-prefixed routes), not a background refresh
loop. Native daily crypto and some Polygon paths can make a supplementary
Yahoo lookup after primary success; these existing requests are unchanged. No measured paid-cost estimate is available from
invented packets; no new paid integration or recurring work is introduced. The
status observer performs no network requests.

Schema2 canonical storage, reviewed adapter, migration/recovery receipts and
original legacy stores are unchanged. No actual user lists, members, order,
favorites, flags or browser storage were read or edited. Verification uses fresh
Chromium contexts and invented stores, maps and packets; all requests are
intercepted. Preserve canonical schema2 data and compatible adapter on rollback;
an old-renderer-only rollback after adoption remains unsafe.

Qualification requires all current-main targets through the real native
Enter/klines handler at 1440 and 390 pixels, including canonical/mismatch,
conflicting-map, proxy-fallback, loading, late-response and selection-change
cases. Existing editing, renderer-failure, IndexedDB/recovery suites and the full
Node suite remain gates, along with page scripts, wiring, preflight, secrets,
complete staged inventory and independent exact-head review. Gate receipts bind
source hashes; GitHub Actions publication is separate from live acceptance.

The earlier production build-manifest HTTP403 remains a hard stop. No production
map, asset, page, navigation or browser requests are retried through another
route/tool/credential. Runner publication can be qualified via GitHub Actions;
edge markers, served navigation and production click acceptance remain blocked.
No TradingView feature or data parity is claimed.

## Source gates before independent review

The finalized source passed 3,658 full Node tests and 613 targeted watchlist tests.
Existing isolated desktop/mobile editing checks, ten renderer-failure cases and
19 IndexedDB/recovery cases passed. Page scripts parsed 600 public graphs; wiring
checked 36 pages and 143 declarations without missing/stale entries. Preflight
passed all seven changed paths. The full real-chart browser run passed 1,028
handoffs at 1440/390, including all 461 current-main routes at each width; every
request was intercepted and fixture watchlist data remained unchanged. Both
status surfaces, failed scalar identities, Yahoo-only supplemental dates, late
responses and away/direct-return expiry passed. Independent exact-head review
remains required before publication; its result is attached to PR93.
