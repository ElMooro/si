# Katlin required funding fail-closed release

PR: https://github.com/ElMooro/si/pull/52

- Accepted implementation: `be7b83a4b252c56e71d3d0459aaf96ed16352917`.
- Independently accepted integration: `32d29f275e1a57710683cb3525720f4e937b4834`, preserving main `398821002` and identical implementation/test bytes.
- Released merge: `fd118f31096c71b8f05df23a615cc17c044ba63d`.
- Pinned single-target workflow: https://github.com/ElMooro/si/actions/runs/36876111985 (`function=justhodl-katlin`, request `katlin-funding-pr52-fd118f310`).
- Merge-triggered run 36876084659 skipped deployment. No ops dispatch, manual live invocation, schedule/cadence change, or trading action requested.

The repair removes the funding risk vote for unusable required funding, forces DATA_HOLD / entries false / cap zero, and bounds expiry by the existing 36-hour funding freshness window. Valid producer states retain exact 20/50/85 mappings. Frozen-clock complete-output comparison against main passed for all three valid states.

Integration gates: 1,018 deployment tests; 2,632 frontend tests; 15 native plus 34 nested boundary tests; public-boundary 15 tests; source/config, secrets, wiring and stub guards. Independent review: no blocking findings. Explicit producer points 0–13 and invalid upper-bound behavior independently checked; persistent boundary-specific assertions suggested as nonblocking.

Baseline public natural packet at 2026-10-01T14:22:39.847073+00:00 had UNQUALIFIED funding incorrectly shown as GREEN/20. It was already DATA_HOLD / entries false / cap zero due to a separate raw risk-gate failure. Therefore a blocked post-release packet alone cannot establish the funding correction: verify the funding vote is absent and its specific hold reason appears. Research observation time was 2026-10-01T04:12:50Z.

Repository cadence: permission refresh rate(15 minutes), daily research 04:10 UTC Tuesday–Saturday, weekly backtest Sunday 09:30 UTC. Public refresh timing is observation evidence, not a complete live Scheduler inventory.


## Deployment proof

Run 36876111985 completed successfully at 2026-10-01T14:32:13Z. GitHub job metadata reports successful exact checkout, preflight, alias protection, and deployment (deployment step finished 14:32:08Z).

Public receipt: https://justhodl.ai/data/ops/releases/justhodl-katlin.json

Receipt verified: commit `fd118f31096c71b8f05df23a615cc17c044ba63d`, run `36876111985`, workflow `deploy-lambdas.yml`, `verified=true`. Package CodeSha256 `4jfjjTSVm3Zf2Y0vhkXIdSSPdd6t2jGU4s4XmZfa/u8=` matches its ZIP hex digest. Complete native source inventory matches committed bytes: `lambda_function.py`, 212697 bytes, SHA256 `d7a0ea50ce0ec4cadf1eb15517fa25ae38c48fb36be52b6a3d186a22972b6b9a`.

Receipt timestamp 14:28:36Z precedes candidate promotion: the receipt alone proves uploaded code, while successful workflow metadata establishes completion of its promotion step. Numbered-version artifact and detailed schedule proof could not be independently inspected: `gh run download 36876111985` for `lambda-release-evidence-fd118f31096c71b8f05df23a615cc17c044ba63d` returned HTTP 403 Forbidden from GitHub artifact blob storage, and `gh run view 36876111985 --log` returned HTTP 403 Forbidden from results-receiver.actions.githubusercontent.com. Stopped those downloads without alternate credentials/routes. No complete live Scheduler inventory or numbered alias-version claim is made.


## Natural publication acceptance

Public `data/katlin.json` naturally refreshed at **2026-10-01T14:37:38.959625+00:00**, observed at 14:38:36 UTC after the completed deployment. Funding risk votes: **0**. Required funding hold reason present; posture **DATA_HOLD**, entries **false**, exposure cap **0**. Core and barbell baskets are empty; cash **100%**. Permission expires at 14:37:41.595029 UTC, so the held packet grants no continuing permission. Research time remains **2026-10-01T04:12:50Z**, research status FRESH: permission refresh did not renew research observations.

The separate raw risk-gate contract hold remains. An entry-eligible live outcome was not observed; valid-state behavior and funding expiry limits were verified offline only. The consecutive 14:22:39 and 14:37:38 refreshes support the declared 15-minute natural cadence; they are not a complete live Scheduler inventory. Daily full-research publication under this revision and weekly backtest are not yet observed.

Captured public packet: 2016583 bytes; SHA256 `4dd341746af8a6092684b38072d63f57e7dff4911afa6414c6f4a626619dcac9`.

Funding input capture: bond-warroom generated `2026-10-01T14:01:11+00:00`, state `UNQUALIFIED`, SHA256 `d2fe687972de5541c1c566a791abad0e639dcb849578e3a88d7626f02e3d671f`. Katlin source-health clock: `2026-10-01T14:01:11+00:00`.
