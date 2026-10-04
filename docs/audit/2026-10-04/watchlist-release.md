# Watchlist source release; live acceptance on hold

Owner: S-shopiz#wl1004c8. This receipt records the completed source release.
It supplements the historical [integration qualification](watchlist-integration.md)
and [prior candidate record](watchlist-prior-candidate.md); their original holds
and evidence remain unchanged. The current claim remains active for live acceptance.

## Published source and runner evidence

| Evidence | Exact result |
| --- | --- |
| [PR91](https://github.com/ElMooro/si/pull/91) | Normal merge at 2026-10-04 03:12:07 UTC; `0ebefcefe42e781e108558050f0c4eb560beb971` |
| Independently reviewed candidate | `684e2b2d583331fee62461b22d70a5e04efc1d20` |
| Candidate and merged tree | `a56c13dca5633dace4abcbb1fc87498a9cb3d2fd`; exact equality verified |
| Pre-release main and first merge parent | `78f26ed545810f5a42a37992f14d8353317d86ed`; second parent is the reviewed candidate |
| [Automatic Pages run 37173386896](https://github.com/ElMooro/si/actions/runs/37173386896) | Push event for the merge SHA; attempt 1 completed SUCCESS at 2026-10-04 03:17:12 UTC |
| [Pages build](https://github.com/ElMooro/si/actions/runs/37173386896/job/111350829494) | SUCCESS, including behavioral, syntax, wiring, offline-build, public-artifact and manifest-stamping gates |
| [Pages deployment](https://github.com/ElMooro/si/actions/runs/37173386896/job/111351545701) | SUCCESS; deploy-pages step succeeded, failure assertion skipped, self-heal skipped |
| GitHub Pages deployment | `6836277501`, `github-pages`, success status `19232769668` for the merge SHA |
| Uploaded Pages artifact | `11292805807`, 7,334,214 bytes; `sha256:d40418c1dfc3407eeb6404ce08fe1f88959a2f7d27ca359043f74ab84381ab05` |
| [Post-merge page gate 37173386915](https://github.com/ElMooro/si/actions/runs/37173386915) | SUCCESS for the merge SHA |

These results establish the source merge and runner publication. They do not
establish which bytes the production edge served or whether production controls work.
The release contract requires served evidence before live acceptance claims;
it did not require a successful served baseline before this authorized source release.

Independent exact-head review cleared the candidate and then its source-release
and recovery contract. Local verification passed 3,133 Node tests; 88 targeted
tests; invented real-Chromium chart flows at 1440 and 390 pixels; 10 renderer
failure scenarios; and 20 IndexedDB migration/recovery cases. Independent repeats
passed with zero external requests. Source gates passed 600 syntax graphs,
wiring for 36 pages and 143 dependencies, the complete 26-file staged inventory,
and a 16,112-file secrets scan with zero findings. These are source/synthetic
results, not production-browser results. CodeRabbit's draft-skipped status is
not counted as independent review.

## Live acceptance and remaining integration gaps

Live acceptance is **HOLD / UNVERIFIED** at the retained HTTP403 from the earlier
ordinary production build-manifest request. It was not retried. No alternate
route, host, credential or access setting was used. There were **no production
clicks** and no actual user watchlist/storage reads or mutations. Served manifest,
exact asset bytes, navigation, the unique source marker, desktop/mobile controls,
keyboard behavior and scrolling remain unverified in production.

The current integrated implementation leaves these visible limits:

- Quotes are daily aggregate Close with dates, source, bar age, cache state and
  completion/venue/currency uncertainty. Verified live/session data and qualified
  venue, FRED, currency, futures, crypto and alias measurements remain unavailable
  where the existing resolver and endpoint contract cannot establish exact identity.
- The Ext column/view remains available, but extended-session measurements are
  unavailable until their identity and completion contract is verified. EPS,
  dividends, market cap and earnings dates are also unavailable in the table.
- Malformed storage, ambiguous migration mappings, empty/reserved saved-list IDs
  and changed legacy destinations retain originals and block replacement edits.
  The UI provides explicit review and preservation/backup; an automatic conflict
  reconciliation workflow is not qualified.

Existing list/edit/import/export, sections, ordering, favorites/flags, columns,
table/advanced views, notes/alerts and native navigation remain in the integrated
renderer and were exercised synthetically. This receipt makes no TradingView
feature or market-data parity claim and starts no broader implementation phase.

## Recovery constraint

**Retain schema2 canonical data and the reviewed schema2-compatible store and
renderer adapter. Do not perform an old-renderer revert after adoption.** The
older renderer can expose stale legacy localStorage authority even though the
canonical record still contains accepted edits. Recovery must preserve or restore
compatible authority pinned to reviewed candidate
`684e2b2d583331fee62461b22d70a5e04efc1d20`, retaining accepted model/UI, revision,
ledger, immutable import receipts, originals and complete backup. Do not restore
legacy originals over subsequent accepted user intent.

The synthetic 20-case recovery run rejected the actual older schema1 writer
without mutation, then reinstated and reloaded the reviewed compatible store
with the model/UI/ledger/receipts/originals and backup exactly unchanged. The
independent repeat produced the same report SHA256:
`070bf220be93d1354981061b75f3f3df1fbcf24de3f213d7a5ce433f7d793d1b`.
This qualifies downgrade fencing and compatible recovery; a plain old-main
renderer revert is not a qualified rollback.

## Preservation and this bookkeeping publication

[PR90](https://github.com/ElMooro/si/pull/90) remains an unchanged open draft at
`57aecc2e5ef1742d6367c64938952f908314fb1a`. Earlier candidate commits, holds,
audit documents and local evidence remain retained. Only the owner's claim and
this new receipt are changed by this bookkeeping batch. Runtime code, tests,
other claims, workflows, schedules, AWS/security settings and user data are unchanged.
Publication uses `[skip-deploy] [skip-ops]`; neither another deployment nor a
workflow dispatch is part of this task.
