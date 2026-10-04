# Current watchlist integration and qualification

Owner: S-shopiz#wl1004c8. Baseline: current main
78f26ed545810f5a42a37992f14d8353317d86ed, tvwatch-layout-3.
Khalid explicitly assigned watchlist corrections at Oct3 23:59:57 UTC. Parent
subsequently instructed coordinated local integration against the newer renderer.
This branch preserves the prior candidate and PR90; it does not replace newer
main with the earlier renderer. No production deployment is authorized until
parent release conditions are reconciled. One ordinary production manifest GET
returned403; it was stopped without retry, alternate host, route or credential.
No actual user browser, watchlist members, flags, order or storage was accessed.
Every browser fixture is invented and all transport is intercepted.

## Authoritative saved model and writer coordination

Lists, instrument favorites, flags and the complete jh-tv-watch-ui record share
one canonical IndexedDB record and transaction. UI fields include membership
extra/hide/order, sections, collapse, alias, list color and starred lists alongside
columns, width, sort, table, advanced grouping, summary and active selection.
Compound actions commit their model and UI together. The representation is
retained; existing raw source and destination stores remain intact.

The current renderer's captured synchronous draft stages all watchlist mutations.
One saveBatch validates the captured revision and awaits transaction completion.
A failed write retains import/input text and does not show success. Further model
actions during a pending save are rejected without changing their displayed target.
Prompt/confirm, file chooser/read, drag and resize capture their target/revision
before waiting. Competing accepted edits win; stale actions cannot replace them.
Read-only Add presentation remains available with malformed storage; submission
retains entered text and refuses adoption. Malformed originals can be exported.

| Writer | Adapter boundary |
| --- | --- |
| tvwatch write/saveUi | Require captured action; batch model and UI |
| List selection, section/order/drag/sort/resize, columns/table/advanced | Same UI transaction |
| Import as new/append/file, create/duplicate/rename/delete/clear | Same captured batch; stable fresh IDs |
| Rows/keyboard flag, remove, favorite; Favorites Add/removal | Actual model array/map and UI staged together |
| Native Search/More and Add | Delegate to current renderer active target; retain qualified identity |
| Native engine saveWatchlist and remaining menus | Await canonical save with pre-composition revision |
| Chart Pro and older tabs | Read-only legacy inputs; divergence retains both versions and freezes replacement edits |
| Private notes/alerts, navigation favorites, chart settings | Existing separate domains retained |

Renderer ownership markers prevent native rebinding of list, query, Add and menu
controls. Saved lists override catalog lists by ID in both lookup and dropdown;
different IDs with the same name remain separate. Equal-count member replacements
and order changes repaint. Dropdowns retain all lists instead of truncating at60.
Own-key and null-prototype maps preserve constructor/toString/__proto__ list IDs,
raw members, flags and migration source IDs. Empty or reserved custom IDs (favorites/flag:*) produce an explicit read-only conflict, preserve originals and entered Add text, and reject adoption rather than silently rename or alias them. No catalog bytes or source members
are rewritten. Focused row controls Delete/reorder; chart focus cannot delete a
watchlist row. Inherited rail grid/menu styles are corrected only inside watchlist
controls; horizontal columns and mobile scrolling remain available.

## Migration and schema compatibility

Startup reads and validates but does not import or adopt data. Review/Apply is an
explicit user action. Stable source-ID mappings, cumulative consumed identities,
and destination tombstones preserve renames/deletions/member/favorite/flag removals
across repeated review and source changes. Name-based ambiguous legacy mappings
are rejected. Original Chart Pro namespaces are never deleted or overwritten.
Active model, import ledger and immutable reviewed-import receipt commit together;
record/receipt quota, abort or timeout does not advance a success ledger.

Schema2 adds the complete UI key without changing the existing database layout.
Schema1 is read compatibly without write: the UI projection is bound into the
expected token as exact legacy raw text. The first explicit accepted action promotes
atomically, retaining prior originals, source snapshot, model, ledger and receipts,
and attaching the exact UI original/baseline. A real browser fixture executes the
preserved original schema1 module, then the new module. Reloaded UI changes reject
an older token; old schema1 writers reject schema2 rather than dropping UI.
Older localStorage writers can race after the last comparison. Their originals
remain intact; subsequent edits report divergence and retain both versions. This
is not a cross-store atomic compare-and-swap. Notifications optimize refresh;
transaction revision checks establish the concurrency boundary.

## Honest aggregate quotes and request impact

Rows and the card label daily aggregate Close with both bar dates, source,
bar age and cache-expiry state, and unverified completion/venue/currency. They do not claim live or
EOD. Existing resolver identity must agree with the exact bare equity endpoint
contract. Qualified venues, economic series, currency/futures/crypto identities
and native aliases remain visible with an unavailable reason. Namespace stripping
is not used for measurements. Packet validation rejects missing/conflicting
identity/source/interval, invalid chronological timestamps, future bars, null,
boolean, blank, nonfinite closes and delta/percentage overflow. One bar has no
invented change. Optional measurements remain unavailable unless measured.

The existing12-day normal and420-day table/advanced requests remain. Three slots,
a body-inclusive10-second deadline, five-minute successful cache, and30-second
failure backoff bound reuse. There is no polling or new recurring cadence. Visible
on-demand rows, manual Refresh and visibility reuse can refresh expired data.
View toggles retain request ownership, upgrade rich history after completion, and
preserve queued visible work. Late responses cannot overwrite timed-out cache.
Native duplicate aggregate requests are suppressed when this renderer owns quotes.
The existing Ext column/view remains with explicit uncertainty; unverified minute
requests are withheld. No new paid API, service or recurring cost is introduced.
Compared with the old permanent lockout, eligible repeated manual reuse can add
at most one successful request per five-minute cache window or a failed retry per
30-second backoff. Table upgrade can add one420-day request after a12-day response.
This estimates requests, not an unmeasured provider bill.

Daily-derived1W/1M/3M/1Y use5/21/63/252 preceding bars and are explained in the UI.
YTD needs a prior-year bar, average volume needs10 measured bars, and a52-week
range needs365-day coverage. Advanced rows also carry their own raw identity/date/source/bar-age/cache-expiry/uncertainty qualifier. Both advanced toggles persist closing. Private notes retain their original independent onchange save during pending/failed watchlist edits; chart/compare/financial More routing retains its baseline payload, with raw Add-only identity carried separately. Private note/alert controls remain. No TradingView
feature or market-data parity is claimed; backend identity/session/freshness
verification and access acceptance remain separate from UI features.

## Scope, preservation and release boundary

Only chart script references, tvrail import controls, the current watchlist
renderer, native watchlist persistence/target seams, new store/quote modules,
synthetic tests and audit/claim files change. SHA-bound inverse fixtures recover
every baseline engine/chart/helper byte and the newer77306-byte renderer exactly.
A negative control changing an unrelated notes key fails the hash check. Existing
historical behavior assertions stay enabled. Engine money/scoring behavior,
chart/catalog/volume ownership, AWS/worker/workflows/IAM/security, retention,
schedules and communications remain unchanged. PR88/shared publisher-preflight
exceptions and pending PR78/catalog/volume decisions are outside this branch.

Validation results are recorded in the final evidence companion. Production
merge/deployment and served-nav/edge-marker/live-click acceptance remain withheld.
The existing403 is not an invitation to try another route. Rollback requires a
coordinated reviewed renderer/store change retaining schema2 authority and backup;
an old renderer cannot be substituted over an adopted schema2 model safely.
Prior local candidate8fe0eaa05145e4ab4a5e9e02816daf4dea5f5b61 remains at
/workspace/watchlist-repair and its report is watchlist-prior-candidate.md.
