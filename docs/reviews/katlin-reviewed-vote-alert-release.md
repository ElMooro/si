# Katlin vote and alert release — 2026-10-01

User approved correcting producer-withheld votes, the volatility-state mismatch, and actionable alerts ignoring blocked permission at 14:57 UTC. Two separately reviewed repairs preserve the prior required-funding fix, raw-gate holds, measured research and optional-source criticality. No volatility thresholds were invented: its Katlin risk mapping remains unresolved and abstains. No trades, manual Lambda invocation, test notifications, schedules, IAM, secrets, private Brain or new paid API changes were requested.

## Accepted source and release commits

| Repair | PR | Accepted implementation | Accepted integration | Released merge |
|---|---|---|---|---|
| Regime/credit/volatility vote qualification | [56](https://github.com/ElMooro/si/pull/56) | `b82d35eb9150d2f14a119686f532582928d786ac` | `bf0dd79e469d0e6c7d9d8595528f434def2bb949` | `c0df1ea930fddc984359f5f576526c4879955367` |
| Actionable Katlin alert permission | [54](https://github.com/ElMooro/si/pull/54) | `b331bf25e4ecffdb957d288666733d4d4370c7f3` | `c48d8a9eae78a7161b3366822a1e56bd7740dbce` | `e58b840623d3e3c1e163a00c9e220f0c9987eb60` |

Independent reviewers found no blocking issues at each exact head. Integration preserved each repair byte-for-byte; each final merge tree matched its accepted integration tree. Merge titles used `[skip-deploy] [skip-ops]`; explicit dispatches pin a single function and source commit.

Vote gates: 1,018 deployment static checks, 15 candidate-shell checks, 2,650 frontend tests, 15 native funding/authority tests plus tail (5), FedWatch (5), cycle (13), shipping (5), OOS (6) and optional-vote (6) tests with matrices; source/config/secrets/wiring/stub/15 public-boundary tests and compile. Alert gates: 1,062 deployment checks plus 10 subtests, 7 focused tests plus 61 subtests, native alert suites, compile/stub/inventory and GitHub guard. No tests sent live messages or accessed AWS.

The optional-vote fixture now stays SELECTIVE/cap 65 when withheld inputs are added; before the repair, adding regime or credit individually expanded it to FULL_RISK/cap 100. The actual daily and refresh paths preserve research clocks and packets while abstaining. The alert repair also returned zero events offline for the captured 14:37:38 DATA_HOLD packet despite a retained actionable-tier row; this is offline consumer verification, not natural live execution proof.

## Deployment evidence

- Katlin workflow: [36882424842](https://github.com/ElMooro/si/actions/runs/36882424842), request `katlin-votes-pr56-c0df1ea930`, only `justhodl-katlin`.
- Alert workflow: [36882886329](https://github.com/ElMooro/si/actions/runs/36882886329), request `katlin-alert-pr54-e58b840623`, only `justhodl-alert-router`.

Katlin public receipt matched commit/run/source bytes, verified=true, CodeSha256 `HSnYfb+u1OtOeFejQOAZyMazcr2OzTnlaSYgaFm4vrg=`. Native lambda source: 213796 bytes, SHA256 `b808fb6b78555ede18d97e98985410dc1629eaafad4e8c7509b19aba9c00e3ff`. Receipt timestamp 15:16:09 UTC proves code staging; it precedes governed candidate validation/promotion, so workflow completion is separate evidence.

Katlin workflow completed successfully at **15:19:47 UTC**; job 110437440127 reports the deployment step successful at 15:19:43. This establishes completion of the standard candidate-validation/promotion workflow, separate from the earlier receipt. Merge-triggered run 36882398172 skipped deployment; alert merge-triggered run 36882864776 was cancelled while queued, and only the pinned alert dispatch proceeds.

Alert workflow completed successfully at **15:22:36 UTC**; job 110440501012 reports the deployment step successful at 15:22:31. Its public receipt matched commit/run and the complete native source inventory, verified=true, CodeSha256 `ij7D7+XV0dfJ1evc8t5AMi2DENaVB5c78I5EKc184j0=`. Lambda source: 46691 bytes, SHA256 `3da98a56f427fd7b1c7932d86d2712435c860b62d6c4d9568d97953750d48792`; unchanged `cot_context.py`: 2475 bytes, SHA256 `8940ba298d2a8263ca4beacfa077668fa1dcfbca9add65d09eccee6f8cf1675c`. Receipt timestamp: 15:22:24 UTC.

Public receipts: [Katlin](https://justhodl.ai/data/ops/releases/justhodl-katlin.json), [alert-router](https://justhodl.ai/data/ops/releases/justhodl-alert-router.json).

## Natural acceptance and access limits

Reading `https://justhodl.ai/data/katlin.json` for natural publication acceptance returned HTTP 403 Forbidden. That read stopped without another route or credential attempt. No post-release natural Katlin packet is claimed. A separate read of `https://justhodl.ai/data/alert-history.json` returned HTTP 503 Service Unavailable before the alert release; that is an availability failure, not runtime acceptance. A post-deployment retry returned the same HTTP 503. No natural post-release alert run or suppression outcome is claimed. Both session claims remain explicitly limited to verification handoff; implementation and reviewed releases are complete.

Previously denied GitHub numbered-artifact/log downloads were not retried: HTTP 403 at artifact blob storage and results-receiver.actions.githubusercontent.com. Workflow/job metadata and public release receipts are separate available evidence. No complete live Scheduler inventory or numbered alias-version inspection is claimed. Katlin's standard governed workflow validates a numbered candidate read-only; the unconfigured alert-router deployment does not manually invoke its handler. Standard existing workflow behavior was used without new operations or configuration changes.


## Parent-managed natural UI acceptance

The parent subsequently reported normal Katlin UI acceptance at 1178×755 for the natural packet generated **2026-10-01T15:22:39.025797 UTC**. Research time remained **04:12:50 UTC**. The table had six legs, omitting regime, credit and volatility; all three abstention reasons were visible. Their `research_context` and `source_health` entries showed UNQUALIFIED_RESEARCH, decision eligibility false and zero investment votes. Source dates and parsed-input hash caveats remained visible. Credit retained September 29 observations; volatility retained **CONCERNED/46**, explicitly marking its Katlin mapping unresolved.

DATA_HOLD, cap 0, entries false and 100% cash remained. Expiry **15:22:42.450669 UTC** correctly produced the expired-permission warning. Raw-gate 50 and auction 45 remained outside the patch. This is bounded, parent-observed natural UI evidence for PR56, not a new read through the executor's denied route and not evidence for every possible volatility state. Other volatility states remain offline-test evidence. It does not verify the later version-identification follow-up or natural alert-router suppression. Alert-history acceptance remains unavailable.
