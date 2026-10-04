# Watchlist correctness and local migration transaction

Owner: S-shopiz#wl1004c8. Khalid explicitly assigned watchlist quote/migration
repairs on Oct3 23:59:57 UTC and subsequently authorized local watchlist writer
coordination. Baseline main af793e5713034fdf8a543f437ed1b1577d633a11.
Chart/catalog/volume owner S-codex#symdir1001a retains unrelated work.
PR90 is the isolated review candidate. No actual user browser storage was read,
imported or altered, and no user data loss has been established. All fixtures
are invented local stores; all browser requests are intercepted.

## Quote and UI behavior

Saved custom lists override catalog lists by stable ID. Complete membership and
order remain visible, including same-count replacements and lists over 120 rows.
Sorting is a temporary keyboard-accessible view with aria-sort; section order
is preserved. Native Favorite, Compare, Notes, Financials, Alert and Cycle Flag
menus remain available. Watchlist arrows stay within focused rows. Existing
List, search and paste controls remain usable on mobile. New Add/search/paste
and favorite writes retain supplied qualified IDs; old saved members are never
silently rewritten. Rows show a readable ticker and a wrapping full identity.
Favorites Add updates the actual favorite array; its automatic name is explained
instead of creating an invisible custom list. Cross-tab deletion repairs both
the selected value and subsequent action target.

Quotes are daily aggregate closes with both bar dates, source, age and completion
uncertainty. They are not described as live, Last or EOD. Packet guards reject
missing identity/time/source, coercion, invalid ordering, future timestamps and
overflow. One bar has no invented change. Qualified venues, FRED, currencies,
futures, crypto and known native aliases remain visible with unavailable reasons
when the existing resolver/Worker contract cannot establish the exact instrument.
The /ohlc ticker echo does not prove venue/currency. Row alerts use only a matching
finite aggregate quote, never another active chart's last bar.

Quotes retain three slots, ten-second body-inclusive deadlines, five-minute
cache expiry and thirty-second failure backoff. There is no polling. Visible
rows load on demand; manual/visibility reuse refreshes expired data and discards
obsolete queued work. A visible eligible symbol can make at most one successful
request per five-minute window; failures can retry at most every thirty seconds
only upon another reuse event. This restores requests after the old permanent
qSet lockout, so repeated manual reuse can increase requests. Native duplicate
lastPx requests are suppressed. No paid API, service or recurring cadence is added.

## Writer and reader inventory

| Location | Watchlist role | Coordination boundary |
| --- | --- | --- |
| engine saveWatchlist | Sole Supercharts save gateway | Awaits one canonical IndexedDB readwrite transaction; no legacy-write fallback |
| engine New / Paste | New destination list | Captures revision before prompt/composition, uses a collision-resistant fresh ID |
| engine listMenu / persistCustom | Rename, Duplicate, Delete, Add | Captures revision before prompt/confirm/composition; reads authoritative model |
| engine addToList | Toolbar, list menu, search/Add/More, row context | Same awaited gateway, qualified identity preserved |
| engine toggleFav / openCtx flag | Favorite, Cycle Flag/removal | Same gateway, latest authoritative model after completion |
| engine loadJSON/init/loadLists | Destination lists/favorites/flags reads | Canonical readiness and model bridge, catalog bytes unchanged |
| tvwatch findList/flagsFromStore | Visible saved model and flags | Canonical bridge and change notifications |
| chart-pro WatchlistManager.saveLocal | Legacy source lists/flags/favorites writer | Observed read-only input; saveCustom/saveFlags/syncToCloud/cloud restoration unchanged |
| Older Supercharts tabs | Legacy destination/old import-ledger writers | Cannot overwrite the canonical database; divergence retains both versions and blocks replacement edits |
| jh-nav-drawer getFavs/setFavs | jh_favs page-href favorites | Excluded from instrument import, unchanged |
| wl-lens | Reads Chart Pro source stores | Unchanged; canonical destination remains a one-way local import |
| Metric/indicator favorites, layout/pin | Separate domains | Excluded and untouched |

Only chart.html, tvrail, tvwatch, native watchlist seams and the new local store
module change production behavior. No Chart Pro source, navigation, catalog,
volume, cloud communication, engine, workflow or account-permission changes.
SHA-bound inverse fixtures reconstruct every unrelated original engine byte and
all historical preservation assertions; an unrelated mutation is a failing
negative control.

## Atomic authority and import behavior

The first candidate removed the unsafe automatic migrator and offered only a
read-only preview. The expanded candidate restores explicit reviewed Apply.
Startup remains nonmutating: it reads destination/canonical state but does not
import Chart Pro or overwrite any original store.

The local IndexedDB authority contains lists, flags, favorites, ledger, revision,
exact bootstrap originals and the accepted source snapshot. An explicit native
edit first adopts destination data only, in the same transaction as that edit.
Every accepted import also retains an immutable receipt with its reviewed raw
originals, ledger and resulting model. Active model and receipt commit together.
Success requires verified put requests and transaction oncomplete; a put success
alone is insufficient. Quota, abort, blocked/unavailable storage and malformed
records do not fall back to legacy writes or record a successful save.
Readonly reload/export also have ten-second deadlines. An incomplete receipt
export explicitly reports `complete:false` and `importReadError`, with a visible
download warning. Late completion cannot clear a read timeout.

Stable source IDs map independently of display names. Source order is appended
without reversal; destination renames, colors, order and removals are preserved.
Deleting an imported destination records a tombstone in its native edit transaction.
Consumed members/favorites/flags are cumulative, so removed values are not
reimported. Source deletions/renames do not silently delete/rename a destination.
Unsupported source flags and nonboolean favorite values reject review without
consuming identities; a corrected source remains eligible on the next review.
Ambiguous legacy name mappings remain nonmutating and require a decision.
Navigation jh_favs is retained in backup but never imported as instruments.

All updated destination writers share the database transaction and expected
revision. Revision is captured before a blocking prompt/confirm or replacement
is composed. Competing tabs cannot replace a first winner with stale intent;
notifications are an optimization, and lower delayed revisions do not regress
cache. Failed native edits reload the latest model and visibly report failure.
Startup reads members and their revision together after catalog network awaits,
so another tab's edits during a slow fetch cannot be paired with a newer token.

Legacy localStorage namespaces and their old ledger are never written or deleted.
The original Chart Pro stores, original Supercharts stores, current legacy data,
canonical data and accepted import snapshots remain exportable. Canonical edits
are visible in updated Supercharts; old cached Supercharts tabs keep their legacy
view until reload. This does not add bidirectional Chart Pro/cloud synchronization.

## Precise legacy concurrency boundary

IndexedDB cannot atomically compare localStorage. Apply checks its reviewed raw
source/destination snapshot and canonical revision inside its transaction, then
performs a final synchronous legacy comparison before the put. An observed
change rejects the import without advancing the active ledger/receipt.

An old writer can race after that last comparison. The accepted reviewed
snapshot remains retained, the newer legacy data is untouched, and later
divergence is reported. This is not a cross-store CAS guarantee. Destination
divergence freezes replacement edits and exports both versions for explicit
reconciliation; source divergence invalidates pending Apply and offers another
review, while independent canonical edits remain available.

Automatic continuous migration is deliberately withheld. Choosing it while
uncoordinated old localStorage writers remain requires a behavior decision;
default startup is nonmutating preservation, with explicit reviewed import
available. No silent reconciliation or guessed name mapping is performed.

## Reproducible acceptance

- `node --test tests/*.test.js`
- `CHROMIUM_EXECUTABLE_PATH=/usr/bin/chromium node tests/watchlist-correctness-browser.cjs /tmp/watchlist-browser-phase2`
- `CHROMIUM_EXECUTABLE_PATH=/usr/bin/chromium node tests/watchlist-quote-lifecycle-browser.cjs /tmp/watchlist-lifecycle-phase2`
- `CHROMIUM_EXECUTABLE_PATH=/usr/bin/chromium node tests/watchlist-transaction-browser.cjs /tmp/watchlist-transaction`
- `python3 scripts/check_page_scripts.py`
- `python3 scripts/gen_engine_wiring.py --check`
- `python3 aws/ops/_preflight.py chart.html jh-chart-engine.js jh-chart-tvrail.js jh-chart-tvwatch.js jh-watchlist-store.js`
- `python3 scripts/check_secrets.py`
- `python3 scripts/check_staged_batch.py --inventory /tmp/watchlist-phase2-inventory.json`

Real browser cases include 1440/390 clicks, scroll/keyboard/sorting, qualified
Add/More/paste, native duplicate/delete/flag, stale rename prompts against another
tab's member edit, zero-write preview and successful canonical Apply. Browser
tests also delay catalog startup while a second tab edits, then use native Add,
and delete the selected list across tabs before Add/Rename on the survivor.
Transaction cases use real IndexedDB, including competing adoption/edits, source/destination
divergence, quota, abort after put success, malformed records/bootstrap, blocked
upgrade/open timeout, reload/crash recovery and delayed revisions. Quote lifecycle
fixtures deliberately ignore AbortSignal, then prove deadlines release slots,
HTTP429/500/malformed JSON retries, cache/visibility reuse and late same-ID safety.

## Release boundary

The sole production HTTP check was a PRE-RELEASE BASELINE GET using normal
urllib access to https://justhodl.ai/build-manifest.json. It returned HTTP403
before any candidate was deployed. Its body was not used as source evidence.
No nav request, asset/browser retry, alternate host/proxy/tool route or credential
bypass followed. This is an access denial, not proof of a missing/broken source,
site outage, denied GitHub merge permission or failed candidate deployment.

Served manifest/unique edge marker, served navigation and relevant live clicks
remain unverified because of that retained boundary. GitHub draft PR creation,
branch pushes and description updates were authorized and succeeded. Exact-head
review and required gates remain mandatory before merge; deployment may only use
the existing GitHub Actions runner lane. No local AWS credentials or API access.

Existing 500GB retention, schedules, money engines, communications, IAM and
workflow settings are untouched. PR88 publisher/preflight exceptions remain
unchanged. TradingView parity is not claimed. Section CRUD, drag/multiselect,
preferences and automatic import/reconciliation policy are separate decisions.

## Concurrent integration hold

The final main refresh advanced to
`78f26ed545810f5a42a37992f14d8353317d86ed` (00:57:41 UTC). It replaced
`jh-chart-tvwatch.js` with the 77,306-byte `tvwatch-layout-3` UI, including
columns, table/advanced views, section/reorder, import/export, notes and alerts.
This local candidate and its acceptance results remain bound to the earlier
`af793e571` renderer. They do not qualify that new UI or an integrated release.
No replacement, auto-resolved rebase, dependent push or merge was performed.

The new owner adds independent legacy list/flag/favorite writers through
`write`, `saveCustom`, `setFlag`, `toggleFav` and import/rename/duplicate/clear/
removal/color handlers. Membership intent also lives in `jh-tv-watch-ui` via
`extra`, `hide`, `order` and section entries; simply adopting raw destination
lists omits this intent. New toolbar hooks use `__jhTvWatch2` and override the
native controls. Extended-hours and 420-day requests require new qualification.

The next coordinated adapter must preserve the new renderer, every UI section,
notes/alerts, raw preference snapshot and layout controls; preserve existing
membership/order/hide/extra/section semantics; fence each of its destination
writers with a captured revision; and attach the qualified quote lifecycle to
the new daily/extended/table display. Its own full browser flows and failure
cases then need fresh exact-head review. Replacing either file wholesale would
erase authorized concurrent work. Current owner coordination and this expanded
adapter are the integration blocker; GitHub write authority and the retained
HTTP403 served-proof boundary are separate facts.
