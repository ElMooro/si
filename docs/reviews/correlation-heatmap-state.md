# Correlation heatmap view-state repair — draft

The delegated managed-browser observation was PRIOR → DELTA → LATEST retaining the prior-only unavailable message, while the detector reported zero aligned dates and needed 312. This draft reproduces that failure locally against the complete predecessor page. No fresh production feed was read during this repair.

Two frontend paths caused it: every absent matrix selected the same prior-only message, and `loadData()` returned early for `warming_up` before replacing the heatmap. The repair renders the current selected mode before that return, replaces the whole table on each selection/refresh, and uses distinct latest/delta/prior unavailable messages. Refresh preserves the selected view. The prior matrix remains unavailable because the current producer does not publish it; no matrix is reconstructed and no acquisition/API action is added.

Scope: `correlation.html`, `renderHeatmap`, its call placement in `loadData`, and existing heatmap controls/accessibility. Buttons wrap on mobile and expose `aria-pressed`; the existing scroll region gains keyboard focus and a visible focus outline. Missing/invalid cells remain unavailable, real numeric zero is retained, and valid values use the unchanged existing color functions. Empty-state source messages and table labels/tooltips are escaped.

## Source and preservation

- Existing source: `data/correlation-breaks.json`, original URL unchanged.
- Producer: `aws/lambdas/justhodl-correlation-breaks/source/lambda_function.py`; warming packet has `status`, `message`, `n_dates_aligned`, `instruments`, `labels`, `generated_at`; normal output publishes `latest_matrix` and `delta_matrix` but no prior matrix.
- Complete predecessor from main `5c797c053`: `tests/fixtures/correlation-heatmap-before.html.txt`, SHA-256 `4f4e02065df102b2582c7a6966b5756f62b3246b431997c343ff3f4a32fb17a3`.
- Regression comparison verifies all other inline functions, the feed URL, non-heatmap narrative markup, and every navigation/asset URL remain byte-identical. Source packet values are not mutated. Browser checks retain backend interpretation and `generated_at` metadata.
- No backend calculations, classification prose, history thresholds, units, navigation, source acquisition, private Brain, mutation controls, workflows, or schedules change. Broader correlation narrative/unit issues and other panels' pre-existing lifecycle behavior are separate work.

## Validation

- Seven focused tests pass, including a failure reproduction against the complete predecessor, initial warming state, repeated view switching, valid matrices, null/zero/invalid cells, missing/empty/malformed matrix shapes, valid → warming → valid refresh, escaped source text, and source/metadata preservation.
- Full frontend/worker suite: 2,296 tests passed at the reviewed base.
- Intercepted Chromium: 1440px and 390px; repeated pointer/Enter/Space activation, Tab focus, correct pressed state, no page overflow, populated/empty refresh, preserved metadata, and no extra feed fetch on view selection. All requests fulfilled locally or aborted. Screenshots: `/tmp/correlation-empty-{1440,390}.png` and `/tmp/correlation-valid-{1440,390}.png`.
- Required source gates passed: secrets scan; Brain public boundaries (15); page-script regressions (6); current 599-page syntax; sovereign site assets (3); wiring registry (143 attachments/36 pages); forbidden literals; offline boundary (6); whitespace check.
- Local offline artifact build runs guarded bakers, workspace/section generation, reskin, asset stamps, SEO and access-contract embedding before final built-page syntax and manifest checks. This is local build evidence, not a deployment receipt.

No edits to PR25. Draft only: independent review before merge/deployment. After a separately approved deployment, check exact public page/build-manifest bytes and repeat PRIOR → DELTA → LATEST against the actual detector packet at desktop/mobile widths, verifying source timestamp and data qualification. This draft does not claim a live fix or complete rendered-field parity.

Rebase verification: rebased onto `c0f2565eb`; only session-claim context conflicted, and both claims were retained. Repair bytes remained unchanged. Full rebased frontend suite passed 2,299 tests (three new upstream tests), both browser viewports passed again, and the seven focused tests additionally verify the frozen predecessor SHA-256.

## Independent-review refresh ordering and failure follow-up

Review of `12d6f2af850b0c80a6e1cf8e83fc565fba944605` found two additional synthetic stale-state paths: delayed old JSON could overwrite a newer warming packet, and failed refreshes retained numeric heatmap cells and the old selectable packet.

Each refresh now takes a monotonically increasing local request generation. The renderer checks it after both fetch and JSON resolution, and before handling any failure. Superseded successes and errors cannot publish data, timestamps, banners or heatmap state. An active request failure clears `CURRENT_DATA`, redraws the selected heatmap as unavailable, and marks current timestamps unavailable. Mode switches still update their selected state after failure but cannot access the discarded packet. A later successful refresh clears the old failure banner. This changes only response/heatmap lifecycle, not backend calculations or signal classifications.

Four new deterministic regression tests failed on the reviewed predecessor and pass with the repair: delayed JSON after newer warming success; stale network/JSON failures after newer valid/warming success; active network/HTTP/JSON failure followed by repeated switching and recovery; and latest failure followed by old success. Intercepted browser coverage at 1440/390 additionally exercises delayed JSON, failure-switching and recovery. Preservation checks exclude only the reviewed request lifecycle and heatmap code; all other calculation/classification renderers, source URLs and navigation remain unchanged. These follow-up findings are synthetic, not newly proven production failures.

Follow-up validation after rebase onto `eed39c07f`: 11 focused regressions, 2,304 full frontend/worker tests, both intercepted Chromium viewports, source/privacy/wiring/offline gates, and final 599-page built syntax passed. The final diff retains current main claim wording plus this workstream only. Compiled access scope stays 599 routes / 898 engines; no live verification or deployment occurred.
