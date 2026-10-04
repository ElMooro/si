# Watchlist correctness repair, first release

Owner trace: S-shopiz#wl1004c8, explicitly assigned by Khalid on Oct3
23:59:57 UTC. Baseline main af793e5713034fdf8a543f437ed1b1577d633a11.
Chart/catalog/volume owner S-codex#symdir1001a retains unrelated work.
No actual user browser storage was read, imported or altered; no user data loss
has been established. Tests use invented local storage and intercepted requests.

## Corrected behavior

Saved custom lists override the catalog by stable ID. Full saved membership and
order are visible, including same-count replacements and lists over 120 rows.
Sorting is a temporary view, keyboard accessible, with aria-sort; section order
is preserved. Native Favorite, Compare, Notes, Financials, Alert and Cycle Flag
menus remain available. Arrow navigation stays inside focused watchlist rows.
The existing toolbar List control opens the pane on mobile. Search and paste
controls remain visible. New Add/search, paste and favorite saves retain supplied
qualified IDs; old saved members are never silently rewritten.

Displayed prices are explicitly daily aggregate closes, with both bar dates,
source, age and completion uncertainty. They are not called live, Last or EOD.
Packets reject missing identity/time/source, numeric coercion, invalid ordering,
future timestamps and overflow. A one-bar packet has no invented change.
Qualified venues, FRED, currencies, futures, crypto and known native economic
aliases remain visible with an unavailable reason when the existing resolver
and Worker contract cannot establish the exact instrument. The /ohlc ticker
echo does not prove venue or currency. No provider contract was fabricated.

Quotes use at most three in-flight slots, a ten-second deadline covering the
response body, five-minute cache expiry and a thirty-second failure backoff.
There is no timer polling. Visible rows load on demand, expired values are reused
on visibility/manual refresh, and obsolete queued list work is discarded. An
immediate refresh does not create another request. One visible eligible symbol
can make at most one successful request per five-minute cache window; a failing
symbol can retry at most once per thirty seconds only upon another explicit
reuse event. The prior permanent qSet lockout is removed, so prolonged manual
reuse can increase requests; no service, paid API or recurring schedule is added.
Native duplicate lastPx requests are suppressed when the enhancer owns the pane.

Malformed saved stores and failed writes are reported without recording a
successful native edit. Write/readback checks cannot make localStorage atomic
across old open tabs; this release makes no such guarantee.

## Migration activation remains HOLD

Unsafe automatic migration and its mutating apply/recovery paths are removed.
Review import only reads validated originals, checks for a changed snapshot,
and offers a JSON download containing the original strings and proposed plan.
It never changes the destination or advances a ledger. The preview uses stable
source IDs, preserves destination names/order/removals, records deleted mappings,
and refuses ambiguous legacy name mappings. Original Chart Pro stores remain.

Activation requires a coordinated transaction protocol for every native,
Chart Pro and legacy-tab writer. Importer-only Web Locks and check-then-set
localStorage cannot exclude concurrent writers; independently reproduced
synthetic interleaving lost a newly created list. Shipping that importer would
violate preservation of user intent. Quota, destination and ledger failures
cannot mutate storage while this HOLD is in effect. No owner browser migration
was used for verification. Preview/download is intentionally the first safe
repair, not a claim that automatic migration is complete.

## Reproducible acceptance

- `node --test tests/*.test.js`
- `CHROMIUM_EXECUTABLE_PATH=/usr/bin/chromium node tests/watchlist-correctness-browser.cjs /tmp/watchlist-browser`
- `CHROMIUM_EXECUTABLE_PATH=/usr/bin/chromium node tests/watchlist-quote-lifecycle-browser.cjs /tmp/watchlist-lifecycle`
- `python3 scripts/check_page_scripts.py`
- `python3 scripts/gen_engine_wiring.py --check`
- `python3 aws/ops/_preflight.py chart.html jh-chart-engine.js jh-chart-tvrail.js jh-chart-tvwatch.js`
- `python3 scripts/check_secrets.py`
- `python3 scripts/check_staged_batch.py --inventory /tmp/watchlist-inventory.json`

Browser fixtures cover 1440/390 widths, actual clicks, mobile reopen, keyboard,
scroll, paste/Add and full saved rows. Lifecycle fixtures deliberately ignore
AbortSignal, then prove hard deadlines release stalled slots, HTTP429/500 and
malformed JSON retry, cache/visibility/manual reuse, late same-ID responses,
list replacement, flag removal and resolver-body timeout. Synthetic Node
fixtures cover identity/numeric/time guards and preview rename/delete/collision/
removal/legacy/malformed/concurrent-read cases. Native source and historical
preservation helpers have exact SHA-bound inverse changes: every unrelated
original byte is reconstructed, and an unrelated mutation is a failing negative
control. Existing assertions are retained.

Release needs independent review of the exact committed head, all required
gates, merge/deploy evidence and served build manifest/asset/navigation checks.
Any access denial is a release blocker without alternate-route probing.
This work does not claim TradingView parity. Section CRUD, drag/multiselect,
preferences and transactional import activation remain separate follow-up work.
Existing 500GB retention, schedules, engines, IAM, workflow configuration and
PR88 publisher/preflight exceptions are untouched.
