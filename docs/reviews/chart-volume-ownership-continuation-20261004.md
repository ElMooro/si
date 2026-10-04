# Reported-volume preservation and public repaint containment

This draft proposal is reconciled with main `ba7cb884dcf0fbdd20af191a191f45f861620c3e`. It removes known quantity substitution and guards the known public repaint paths. It does not certify provider units, upstream missingness, expected historical coverage or live release acceptance. No merge, production push, deployment, Actions dispatch, schedule change or provider request is authorized by this proposal.

Only three executable production files change: the data-proxy Worker, `jh-chart-bbfix.js` and `jh-chart-engine.js`. Navigation, layout, watchlist/provider work, complete structure/distribution modules, acquisition/request order, indicator calculations, scalar diagnostics and native setter recovery are retained. New tests use invented data only. Complete predecessor sources make preservation comparisons inspectable.

## Runtime behavior

The BB helper classified quantities from volume/close magnitude, multiplied other rows by close and median-scaled classified rows using later observations. Those stages mutated shared cache/replay rows without verified units. Both rewrite stages are removed. The retained 250ms helper uses a private owner API instead of unconditionally repainting public `lastBars`; the existing setter guard and BB preparation/recovery remain.

Worker `extendCryptoDaily` replaced primary overlap quantities when null, zero or outside a ratio range against a supplementary provider. That unsupported substitution is removed. The primary owns overlap OHLC and its null, zero and finite quantities; supplementary reported quantities on uncovered dates remain unchanged. Existing request/fallback order, source/count metadata, dates and invalid-value decoding are retained. No quantity-to-currency inference or inferred inverse factors are introduced.

`repaintOwnedBars` requires private current-array identity, existing WeakMap source evidence, private symbol/timeframe, load generation, chart and expected completion sequence. It checks these before/after preparation and after the original canonical painter. Capturing expected sequence before paint prevents adopting a synchronously reentrant newer paint. Completion uses the captured private frame rather than stale public globals. Unknown public evidence and stale/current-array mismatches are refused before paint, BB preparation or range writes.

Both public `window.paint` publications now use this API, containing direct calls, structure's second 400ms callback, distribution dynamic-helper onload and distribution toggle. Repository public consumers pass current `lastBars`; acquisition/replay retain private canonical painting. A small canonical entry guard refuses known mismatched market symbol/timeframe before incrementing `paintSeq`, so an invalid concurrent call cannot cancel a valid awaiting paint. Fresh matching acquisition/replay arrays need not equal current `lastBars`.

Private unidentified legacy input remains accepted and unqualified. Original scalar observation clearing/rendering, including unknown/mismatched/empty diagnostics, is retained. Non-scalar catalog arrays carrying observation metadata retain their original behavior. Replay full copies/prefixes preserve their parent's existing WeakMap evidence and shared original rows; unknown parents receive no invented identity.

A fresh source context reloads original input and clears frontend RAM corruption. Existing corrupted quantities are not reverse-engineered; upstream manufactured zeros cannot be reconstructed by this patch.

## Ownership and latest-main reconciliation

The current `S-codex#symdir1001a` row reserves AI advisory-policy native acceptance/read-only 6473; chart accounting originals are recorded accepted. An older historical chart-owner narrative does not establish active work on these repair functions. The October 4 watchlist release receipt independently records that scope distinction. Both older volume branch heads are ancestors of main; the relevant pending/batch/parts scan has no matching repair payload. Read-only workflow metadata at inspection reports no queued, in-progress or waiting run. These observations do not guarantee future branch stability.

Open draft PR90 at exact head `57aecc2e5ef1742d6367c64938952f908314fb1a` shares engine/helper filenames. Independent complete-function comparisons find `identifyBars`, `paint`, `startReplay`, `klines`, `load`, `loadTicker` and `tickLive` identical to the repair baseline. Its inspected changes concern watchlist/search/storage/UI functions. This supports a separate bounded draft, not merging competing complete-engine preservation contracts. Reconcile both contracts before merge if either branch changes shared targets.

Own claim `S-codex#volown1004q2` names this draft on `codex/volume-repaint-owner-containment`. Removing that one row restores every other claim byte; no other owner's claim or workspace changes.

Validation first used main `98667899404c9bfad0509b056f694166a6899f75`, including published PR96 and docs PR97. Main advanced through `781bba855` to `ba7cb884d` with a FRED watchlist mapping. All four upstream changed files are retained byte-for-byte: watchlist module, source-transition record, existing identity test and applied-patcher receipt. Three repair baselines, four guard entrypoints and existing claims are unchanged between those mains. The new integrated checkout preserves earlier candidates and evidence.

## Strict preservation contract

The original substitution-only removal exposed two Node and one actual Python preservation failures. They compared complete source against an accepted Worker containing the unsupported substitution; they did not demonstrate changed supported OHLC/date/request behavior. Their assertions and old failed logs remain retained.

| Original guard | Failed boundary |
| --- | --- |
| `worker-crypto-source.test.js:15` | Complete source equality against the recorded after-function, which included substitution. |
| `worker-symsearch-cache.test.js:108` through original JS Worker helper | Required complete Worker hash `5a04e13f...` failed before unrelated-handler comparisons. |
| Actual `test_crypto_source_attribution.py` through original Python helper | Same complete-Worker hash failed before snapshot/handler comparisons. Running/importing the file alone executes no test functions. |

Independent review accepted runtime transitions before preservation hooks changed. The new manifest retains exactly one complete Worker function transition and eight engine sites: four prior timer/replay sites plus four separately reviewed public-publication/known-market-entry sites. Exact candidate hash, unique after-sites, textual inversion and complete retained-baseline equality are all required. Inversion only compares source; it never restores quantities or executes the removed writer.

The accepted engine reverses to every byte of prior accepted `bae06914...` and current-main baseline `4ece6b9e...`. All 405 other existing functions remain unchanged. Four narrowly recorded hooks precede every original assertion in the current-source reader, JS Worker helper, Python Worker helper and watchlist source helper. Original assertion bodies, acceptance hashes, historical transitions and all 1514 historical fixtures remain byte-identical to integrated main. Eleven helper/entrypoint mutation controls and seven engine controls were actually rejected.

| Runtime file | SHA256 |
| --- | --- |
| Worker | `9c381d12e99e440e4e6ee23135e2cad598cd36b9018cfa3f95b8f6a7b87fb835` |
| BB helper | `f7c0487d50e0ba50fbe78e12402bb0a60393d0648b2270d2f4a95ce6ad599b63` |
| Engine | `564e150db0635ef05d9ba7872b39b36b83101fb8031bdea1c938713e0e8a6b74` |

## Executed acceptance and limits

| Head and scope | Actual result |
| --- | --- |
| Unchanged986 main | 3714/3714 Node |
| 986 candidate | 3818/3818 Node |
| Unchanged latest ba7 main | 3849/3849 Node |
| Latest integrated proposal | 3953/3953 Node; 0 failed, 0 skipped |
| New tests | 104: 67 earlier containment cases plus 37 independently authored public-path cases |
| Actual unchanged Python attribution/snapshot functions | 4/4 on both reconciled heads |
| Root source/syntax/secrets/wiring/boundary/evidence gates | 15/15 on both reconciled heads; staged secrets checked separately |

The 37 independent executable cases use complete canonical paint, original scalar functions and complete structure/distribution modules. Each path covers stale timeframe/symbol, unknown evidence, noncurrent array and matching ownership. Four complete prior-source controls reproduce 5m-to30m mislabels. Fresh acquisition, append, interior correction, replay prefix/full, null/zero/nonfinite/string/boolean/raw spikes, invalid-call concurrency and original scalar/catalog branches are covered.

Independent native Chromium passes at 1440/390. Four actual public paths run while private 30m acquisition is held: five API wakeups per width produce zero stale sequence/quote/raw writes; matching public repaint completes. Raw/cache/replay aliases, five prefixes, repeated paints, append/correction, fresh reload, pending symbol/timeframe, calendar-await and same-array supersession, and actual pinned Lightweight Charts setter recovery are exercised. All 3324 native BB points across 394 non-flat windows pass an independent rational-input population-variance reference within 64 machine epsilons; max band difference about 2.84e-13. All 13 actually served selected-graph files match the latest integrated checkout. The incoming tvwatch behavior is outside this selected browser graph; full source/built graph checks use its integrated bytes. Full optional-page loading and wall-clock production cadence remain unqualified.

Two failed new native-fixture iterations are retained: structure's callback had exhausted before fixture control, then the extra held30m test warmed a synthetic cache required cold by a later case. Corrections affected only the new fixture: execute complete unchanged structure cold; isolate only that invented cache entry before the later cold test. Runtime and assertions did not change to conceal failures. Browser requests are intercepted/aborted and service workers blocked; actual provider requests/page errors are zero.

Thirteen offline build steps pass on the 986 candidate. The final committed proposal's separate diagnostic build receipt binds its actual source head and checks 600 built graphs. No diagnostic build is a live release receipt. The absent `ci/dash_budget.txt` leaves measured 819 dead dashes unverified against a budget; no setting is created to manufacture a pass.

The broader deployment static runner was actually attempted with external connections and unexpected real service CLIs refused. It stops at discovery because `boto3` is absent; no full deployment-suite pass is claimed. The unchanged synthetic shell deployment suite fails at its snapshot offline-regression child, independently diagnosed as the same missing dependency. No assertions are skipped/replaced; no AWS request occurs.

## Production acceptance remains open

Reported numeric quantities are preserved at the accepted packet boundary; units remain unknown where source evidence is absent. The unchanged `USD volume` label is unqualified. No provider comparability, upstream missingness repair or universal indicator qualification is claimed. Existing Katlin/SymDir null coercion, source/unit loss, daily filtering/resampling/`stripMixInBars` and expected-constituent coverage remain outside scope. No private/history packet harvest, backend missingness edit, money decision, private Brain access, workflow/security/settings/control change, deletion or new cost is included.

The retained live HTTP 403/raw-log stop is honored without retries or alternate routes. Production remains HOLD for live acceptance, unit/coverage qualification and incomplete environmental gates; historical owner presence alone is not the hold. Minimal coordination is to review this exact draft with any concurrent complete-engine contract, resolve dependency/ratchet gates in the authorized environment, and qualify original units/coverage and exact served Worker/Pages bytes before an authorized merge/deploy. Other claims remain untouched.
