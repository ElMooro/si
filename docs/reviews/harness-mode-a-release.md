# PR30 release receipt and remaining acceptance

Implementation accepted at `868eb509bec7cc1b7a6c281a08819eaeb09bbebc`.
Refresh `3ee132fb21e93a3efd8c8fc33bd59013bc1ff6dc` resolved only the claims
conflict against current main; all 16 non-coordination files retained their accepted
bytes. Squash release: `c0f2565eb0d66b9463b7b225dcefc9f35d95adf8`, exactly
17 intended changed files. No meaningful implementation conflict occurred.

[Lambda release run](https://github.com/ElMooro/si/actions/runs/36820409050)
and [Pages release run](https://github.com/ElMooro/si/actions/runs/36820409017)
both succeeded. Refreshed local gates passed 2,292 frontend tests, 9 withdrawal
regressions, existing Ask Desk checks, 8 mocked browser scenarios, 1,018 deployment
checks plus 15 shell checks, targeted source/config checks, preflight, 599 page
graphs, wiring, secret scan and diff checks. No private or paid QA request occurred.

## Package evidence

Public `data/ops/releases/<function>.json` receipts all name the release commit,
report `verified=true`, and match the accepted `lambda_function.py` source hashes:

| Function | Source SHA-256 | Receipt UTC |
| --- | --- | --- |
| justhodl-alpha-decay | `7ca24e7db7779b984f5739e0c019632a3c05ec9b763c082416192acacb56dd5d` | 2026-10-01 05:37:57 |
| justhodl-ask-desk | `cbb38ffd039bb2a2e6c3bd4b6a32e54a0cdb4444da78e89d10928a17215c9b62` | 2026-10-01 05:38:28 |
| justhodl-backtest-harness | `809e29c0b0470c741672c11e007171faf1f440b50774bfbd1d50eeaaa8b92ec0` | 2026-10-01 05:38:46 |

Deployment logs confirm exactly those three targets and native/built CodeSha256
equality for each. The shared boundary's repository hash is
`dd3cae640caa850921dacdd825b04780793094a858d4905b79348d43e25d7ede`.
The standard packager includes shared modules, as verified by the pre-release ZIP
checks; the receipt's source inventory covers handler files, not a separate
per-member shared-module hash. No ALL deploy or manual producer invoke was used.

## Edge and consumer evidence

Normal TLS HTTPS fetches returned the new page and the release's build manifest.
Cloudflare adds one analytics beacon script after the build: removing only that
script and its newline gives the exact manifest hash for `backtests.html`:
`7f7d8c9ee9eb553fa01593f20ea2441c420ea5eee7913fdcbc33363dbd35dce0`.
Repository HTML differs from the built page as expected through the existing SEO,
palette, asset-stamping and page-contract build steps.

The **actual HTTPS edge renderer**, replayed locally with the **current public
legacy harness artifact**, renders eight BLOCKED rows and zero PASS rows even
though the artifact contains one PASS. The source-hash-matched Ask Desk reader,
executed locally with current public harness and alpha-decay artifacts and stubbed
S3, returns BLOCKED/zero qualified rules for both and forwards no Mode A rule or
DEPLOYABLE comparison. This is deterministic consumer verification, not a live
Ask Desk/LLM invocation. Existing paired replays prove alpha-decay history/live
health and Mode B/meta-labeler preservation.

Actual live Chromium navigation failed with `ERR_CERT_AUTHORITY_INVALID`; no TLS
bypass was used. Managed-browser acceptance by the parent remains pending. The
8 invented-fixture browser scenarios passed at both 1440 and 390 pixels before
release; those are not presented as live screenshots.

## Natural publication and pending checks

At verification, the harness still served v1.1.1 generated 2026-09-30 22:04:41Z
with one PASS, and alpha-decay still served v1.0 generated 2026-09-30 13:45:13Z
with one DEPLOYABLE row. Neither yet published the new blocked contract.
Deployment changes code, not existing artifacts; natural publication acceptance
therefore remains **PENDING**. No on-demand QA run was forced.

* Alpha-decay: the deployment guard twice verified the **existing classic**
  `justhodl-alpha-decay-daily` binding and its unchanged `cron(45 13 * * ? *)`
  cadence, 13:45 UTC daily. The existing pipeline idempotently reapplied it;
  there was no retiming, new binding, or schedule-policy edit. A much older
  repository snapshot also lists an `alpha-decay-sched` Scheduler binding; its
  current state was not independently inventoried by this release.
* Harness: repository schedule inventory dated 2026-08-01 lists daily 21:20 UTC
  (`backtest-harness-weekly`, despite its name) and 22:00 UTC
  (`justhodl-backtest-harness-daily`). The latest artifact is consistent with
  the 22:00 run. The config does not declare an active schedule block and the
  deploy did not rewrite its bindings. Current live cadence/enabled state is
  **not independently verified**; do not infer it from stale inventory.
* Ask Desk: request-driven; no scheduled qualification publication was added.
  Guard behavior is source/replay verified, not exercised through a paid model.

After natural runs, verify harness v1.2.0 / alpha-decay v1.1.0 publish
`backtest-harness-mode-a-withdrawal.v1`, BLOCKED and zero qualified/pass counts;
all selected configs/OOS metrics must remain withheld, and alpha comparisons empty.
No strategy-validity, fresh-availability, automatic-promotion or capital claim is
made. Meta-labeler correction still requires its separate approval.

Machine-readable evidence: [harness-mode-a-release-evidence.json](harness-mode-a-release-evidence.json).
