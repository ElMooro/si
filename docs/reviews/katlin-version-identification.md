# Version identification after the Katlin permission repairs

This metadata-only follow-up identifies the reviewed, deployed behavior from PR52 (required funding hold), PR56 (regime/credit/volatility abstention) and PR54 (current permission required for actionable Katlin alerts).

The user supplied an engine-version bump requirement for this follow-up. The checked repository instructions (`CLAUDE.md`, `AUTONOMY.md`, `DEPLOY_LANE.md`) require deployment verification and unique live identification; no blanket engine-version bump text or `.agents/skills` files were found in this checkout. The supplied user requirement is authoritative.

Katlin's existing `VERSION` changes from 2.5.1 to **2.5.2**. Existing daily and permission-refresh publications already carry it in `version`. Output schema stays **1.1**. No decision, weight, threshold, clock, data-source criticality, basket, research field or policy changes.

Alert-router gets its first explicit **ENGINE_VERSION = 1.0.0**, emitted only as additive `engine_version` in its next normal history publication. This starts an engine implementation version series; it does not reinterpret the existing history `version` (default 1.0) or generic webhook-envelope `version` (1.1), which are format identifiers and remain unchanged. Alert selection, permission checks, transport payloads, destinations, timing, dedup state and historical events are unchanged. The existing guarded public-history projection is not modified or bypassed.

Tests exercise Katlin daily/refresh output versions and schema preservation, plus the actual alert handler with offline allowed/blocked/duplicate cases. An AST comparison binds the handler change to its single history-identification assignment; it cannot hide policy changes. No live test sends or manual invokes are used.

## Explicitly excluded policy

The released PR56 removes only regime-composite, credit-stress and vol-regime risk votes. **Auction and raw-gate local fallback votes remain outside PR56, PR54 and this follow-up.** The auction fallback remains 45; an unknown nonempty raw-gate posture can still contribute 50 locally. The separate required raw-gate contract hold remains binding. Neither their eligibility policies nor these fallback values are changed here. The accurate scope is regime/credit/volatility abstention, not all possible unqualified inputs.

Volatility's Katlin mapping remains unresolved; every volatility state abstains rather than receiving an invented calibrated risk score. Producer-native measurements remain research context.

## Reviewed release

[PR57](https://github.com/ElMooro/si/pull/57): implementation `07a13de0f15cdfe59f2a0fe994462199db7bc74d`, accepted integration `bfbb51851dbe4e8b73dfa75424b2553303909fa4`, exact merged release `4432045e324ab32b22b1c0068775909f49f56e54`. A concurrent other-lane acceptance report made the final merge tree differ from the integration tree; a further independent review accepted the exact merged head before dispatch. All source/shared/workflow and seven owned files remained identical. No blockers found.

Passed: 1,018 deployment static checks plus 15 candidate-shell checks; 15 Katlin native tests plus tail (5), FedWatch (5), cycle (13), shipping (5), OOS (6) and optional-vote (6); router native (12), sector (5) and focused (9) (including allowed/blocked/duplicate history identification); 13 relevant frontend tests; 15 public-boundary tests; 6 current-main monitor tests; source/config/secrets/wiring/stub/compile and `_preflight.py` (2 files, 0 warnings). GitHub guard run 36885677554 passed. Initial staged tree `a69720f835cb99ddd7241985b77fb1b3127fdcc9` and integration tree `6af30dd8fa2afcbfa2b6123bd35bd43e3e22d25e` matched their commits.

Pinned release workflows: [Katlin 36885982234](https://github.com/ElMooro/si/actions/runs/36885982234), request `katlin-version-pr57-4432045e`; [initial router attempt 36886084252](https://github.com/ElMooro/si/actions/runs/36886084252), request `alert-version-pr57-4432045e`. Each targets only its named engine. The second dispatch was queued after Katlin started to avoid replacing a pending run.

The parent-managed 15:22:39 natural UI acceptance recorded in `katlin-reviewed-vote-alert-release.md` verifies PR56 behavior, not these later version identifiers. New identification should be checked on the next ordinary publication; no raw Katlin retry, guarded-history workaround, live test message or manual invocation is performed here.

Katlin receipt observed at **2026-10-01T15:43:19Z**, matching release commit/run and complete native source inventory, `verified=true`: CodeSha256 `rmnfoD2e15IkfrNzOYCXq/i7bBQbSNxhLnO/CQr6GGo=`; `lambda_function.py` 213791 bytes, SHA256 `c1ce0b54682992f0b44ecdec4bb605dd5e9b35ef313ca0bb567cb98c2a805f70`. As with PR56, this receipt proves code staging before the governed validation/promotion completes; workflow success is recorded separately.


Katlin workflow completed successfully at **2026-10-01T15:46:52Z**; job 110449279852 reports its deployment step completed at 15:46:46. The merge-triggered run 36885854264 skipped deployment.

Initial router run 36886084252 was cancelled while pending at 15:45:41 by GitHub workflow concurrency after another lane's newer push. It did not deploy. The other lane's running workflow was left untouched; its changes do not overlap either engine, shared modules or the deployment workflow. The first replacement request (`alert-version-pr57-4432045e-requeue1`) returned HTTP 500 from the existing workflow-dispatch endpoint. Recent workflow-dispatch listings showed no run created with that correlation ID; no alternate route or credential was used.


After a second listing still showed no run from the HTTP 500 request, the same authorized workflow-dispatch path accepted replacement run **[36887298076](https://github.com/ElMooro/si/actions/runs/36887298076)**, request `alert-version-pr57-4432045e-requeue2`. It pins `expected_sha=4432045e324ab32b22b1c0068775909f49f56e54`. GitHub's triggering `head_sha` is newer main `8b4e8692dbfdb773faff9427faa9c293c5fb2276`; it is not the selected source commit. The reviewed workflow, scripts, shared modules and both engines were unchanged between those commits. No other lane's run was cancelled or modified by this task.

Replacement router workflow **36887298076 completed successfully at 2026-10-01T15:53:28Z**; job 110453771769 reports its deployment step completed at 15:53:22. Its receipt timestamp is **15:53:14 UTC**, `verified=true`, with commit `4432045e324ab32b22b1c0068775909f49f56e54` and run 36887298076. CodeSha256 `OOXh+C3Bz989uMkS5we42xUubuD1/kyfDcZAG7TSqJE=`. Complete native source inventory matched: `lambda_function.py`, 46830 bytes, SHA256 `6a30b07d80bd46dc260a016957695c17d6b93978859baab87ce8bf987d556382`; unchanged `cot_context.py`, 2475 bytes, SHA256 `8940ba298d2a8263ca4beacfa077668fa1dcfbca9add65d09eccee6f8cf1675c`.

Both version releases are complete at the workflow/receipt level. Public receipt links: [Katlin](https://justhodl.ai/data/ops/releases/justhodl-katlin.json), [alert-router](https://justhodl.ai/data/ops/releases/justhodl-alert-router.json). This task did not inspect numbered Lambda versions or retry previously denied artifact/log downloads. New identifiers in natural output remain for normal UI/publication acceptance; no HTTP 403 Katlin retry or HTTP 503 guarded-history read was performed during this follow-up. The parent-provided PR56 natural proof remains separately and narrowly recorded.
