# Sunday schedule reconciliation — 2026-09-12

**Compiler attachment is verified. IMF drain completion remains open.**

The authorized repair covers only the six `brief-compiler-*` declarations.
The manifest remains authoritative. Their cron expressions, UTC timezone,
compiler target and per-mode inputs are preserved; their transport changes
from absent classic EventBridge rules to EventBridge Scheduler. No
`events.put_rule` is called. The reconciler's explicit
`attach-brief-compiler` action creates missing schedules, checks existing
ones, and refuses conflicting definitions. It never invokes broad enforce.

The remaining drift is a review queue, not authorization to change schedules.
The 5447 inventory found 309 differences. Six are the absent compiler rules;
the remaining **303 findings cover 299 distinct schedule keys**.

| Drift class | Before compiler attachment | Left for review | Next action |
| --- | ---: | ---: | --- |
| UNDECLARED | 226 | 226 | Compare live target, payload, cadence, delivery settings and last successful output with the producer's current contract and deployment history. Add legitimate live schedules to the manifest only after that review. Propose disabling a retired or duplicate wire separately, with its exact name and rollback. |
| TARGET_DRIFT | 33 | 33 | Compare target ARN, qualifier, input JSON semantics, input path and delivery settings. The current detector compares serialized inputs, so classify formatting-only differences separately from real routing differences. Preserve the known working target until intent is established. |
| EXPR_DRIFT | 28 | 28 | Reconcile the observed cadence with the producer's freshness requirement, upstream publication time and UTC/timezone semantics. Document the chosen expression per schedule before changing it. |
| STATE_DRIFT | 15 | 15 | Determine whether each enabled/disabled difference reflects an intentional pause, replacement schedule or missing retirement record. Preserve live state during review. |
| MISSING | 7 | 1 | The one remaining key is `default/portwatch-sched`, declared `cron(20 11 * * ? *)`. Inspect its producer, alternate triggers and deployment history before proposing creation or removal of the declaration. |
| DUPLICATE_TARGET | 0 | 0 | No duplicate-target repair is indicated by this inventory. |

Drift is a count of differences, not failed jobs. A schedule can contribute
more than one difference. An undeclared live schedule can be healthy; a
declared-missing schedule can be obsolete. Neither class justifies automatic
fleet-wide enforcement.

For the next review batch, produce an exact per-schedule change list with
current state, intended state, evidence, affected output key, verification and
rollback. Start with the one remaining missing declaration and the 33 target
differences because they concern delivery. Keep all 299 live schedule keys
unchanged until that list has been reviewed within a new implementation scope.

The checked-in legacy manifest and the deployed S3 manifest already differ
outside this compiler repair. Do not upload the entire legacy file over S3.
Op 5448 applies only the six reviewed records, uses an ETag-conditional write,
backs up the prior S3 manifest under `config/ops/5448/`, and compares every
unrelated declaration and live schedule before and after.

## Evidence and boundaries

- Op 5446: GREEN; import health `HEALTHY`, FRED `COMPLETE`, dead-lanes `OK`.
  `data/import-health.json` LastModified `2026-09-12T14:35:06Z`.
- Op 5447: [inventory](../../../aws/ops/reports/5447_sunday_inventory.json),
  including the full drift list, S3 timestamps and deployed package hashes.
- Reconciler release: `92cf24d423a5666009aaa5f3e727858291586827`,
  [Deploy Lambdas run 34700110142](https://github.com/ElMooro/si/actions/runs/34700110142),
  GREEN, 10 behavioural tests. Live code hash
  `JVk7XpNiRnUALYZovbtwRd/3WcnXTJbACLcnTBdc/6c=`.
- Op 5448: **GREEN**, [run 34700544888](https://github.com/ElMooro/si/actions/runs/34700544888).
  [Attachment and output proof](../../../aws/ops/reports/5448_brief_compiler_attached.json)
  was committed in `9221174`. The six enabled schedules were created at
  `2026-09-12T14:54:15Z`. Every expression, timezone, mode input and target
  matched the reviewed manifest. All **950 other live schedules** and all
  unrelated manifest declarations were identical before and after. Drift
  fell **309 → 303**, with zero general enforcement actions.
  The first attempt stopped before manifest/schedule changes because the
  existing shared execution role had reached IAM's inline-policy size quota.
  Commit `8fbb849e49a35028c710fa8b447e8dd76c4dfddf` fixes forward the same
  operation using a managed policy scoped to the six schedule ARNs and
  `iam:PassRole` for the dedicated compiler execution role only. Existing
  shared-role policies are not rewritten. The dedicated Scheduler role can
  invoke only `justhodl-brief-compiler` and trusts Scheduler only from this
  account's default schedule group.
- FRED counters retain distinct meanings: crawler-imported **282,141**;
  catalog stored-series **277,599**. No counter or banner rewrite is needed.
  The catalog was last modified `2026-09-12T13:55:08Z` in the inventory.
- CATALYST stays missing. An event confluence count is not a signed CATALYST
  signal. The event adapter registry and shadow setting are unchanged.
- OFR's bulk repo/NYFed/MMF datasets are fresh; the sampled per-ID
  `REPO-TRI_AR_TOT-P.json.gz` was last modified `2026-08-06T13:31:12Z`.
  Refreshing those per-ID files from bulk warehouse blobs is deferred.
  The working OFR hot join is unchanged.
- IMF: op 5449 confirms a recent lease write but **no drain progress**. See
  the execution evidence below. Another kick is allowed only after >48 hours
  without a state write; none was sent in this repair.

## Compiler output proof — op 5448

One synchronous `{"mode":"all"}` invocation completed successfully. The
deployed compiler package matched the reviewed `lambda_function.py`,
`brief_compiler.py` and `brief_contract.py`, including the `by_ticker` parser.

| Live S3 key | LastModified, 2026-09-12 UTC | Verified contents |
| --- | --- | --- |
| `config/schedule-manifest.json` | 14:54:16 | Six compiler Scheduler declarations; all other declarations preserved |
| `data/schedule-drift.json` | 14:54:32 | 303 remaining findings; six schedules verified; zero general enforcement |
| `data/plumbing-brief.json` | 14:54:53 | LIVE, source `justhodl-brief-compiler` |
| `data/market-tape-brief.json` | 14:54:53 | LIVE, source `justhodl-brief-compiler`; uncapped ETF tape |
| `data/official-stats-brief.json` | 14:54:53 | LIVE, source `justhodl-brief-compiler` |
| `data/positioning-brief.json` | 14:54:56 | LIVE, source `justhodl-brief-compiler`; accumulating/distributing/flat **3200/1832/32 of 5064**, uncapped |
| `data/event-brief.json` | 14:54:56 | LIVE, source `justhodl-brief-compiler`; confluence **387** from the newer warehouse source, previously 385 |
| `data/verdict.json` | 14:54:57 | Coverage **0.9231**, only missing family **CATALYST**, shadow **true**; existing `jh-fusion-projection` writer retained |
| `data/import-health.json` | 14:45:06 | HEALTHY; FRED COMPLETE; dead-lanes OK across 14 lanes |
| `data/warm/imf-full/_state/state.json` | 14:48:18 | DRAIN; 218 banked, three queued; lease advanced but no banked/queue progress since the earlier snapshot |

The event count changed because the warehouse input changed, not because this
operation fetched Finviz. No OFR, Polygon or Finviz HTTP request was made.

## Final health and IMF inspection — op 5449

[Run 34700850104](https://github.com/ElMooro/si/actions/runs/34700850104) is
GREEN for the **read-only inspection**, committed in `fbd43eb`. That does not
mark the underlying IMF drain successful. The full evidence is in
[5449_imf_progress_proof.json](../../../aws/ops/reports/5449_imf_progress_proof.json).

At `2026-09-12T15:00:03Z`:

- `data/import-health.json`, LastModified **14:55:06Z**: **HEALTHY**;
  FRED **COMPLETE**, crawler-imported **282,141**; dead-lanes **OK**, all 14
  lanes have recent state writes. The FRED queue cursor equals its queue total;
  it is not a remaining backlog.
- `data/provider-catalog.json`, LastModified **14:55:18Z**: FRED stored-series
  **277,599**. Both counters remain intact and the shipped banner is unchanged.
- `data/event-brief.json`, LastModified **14:54:56Z**: **LIVE**, compiler source,
  confluence **387**. A read of `data/finviz-signals.json` found no explicitly
  named signed-count candidate. This is a bounded inspection of that source,
  not a claim about every warehouse object. No CATALYST adapter was registered.
- `data/warm/imf-full/_state/state.json`, LastModified **14:48:18Z**:
  **DRAIN**, **218** banked, **3** queued, **2** failure records. The queued
  flows are `PIP`, `IMTS`, `IMTS_2026_MAY_VINTAGE`, with attempt counters
  **3/0/0**. The lease was active until **15:02:16Z**; internal `as_of` remains
  `2026-09-02T02:43:15Z`.
- CloudWatch records the earlier IMF execution ending at **14:47:13Z** with
  status **timeout** and duration **850,000 ms**. Since op 5447, LastModified
  advanced but banked count, queue length and internal `as_of` did not. The
  healthy dead-lanes indicator measures recent writes, not successful drain
  advancement. **IMF drain completion is therefore not proven.**

No extra IMF Event invocation was sent: state age was **0.196 hours**, below
the user's **>48-hour** kick threshold. No new Lambda was created. The
timeout/progress discrepancy is the remaining operational issue; any proposed
change to the IMF downloader should be reviewed separately from this six-wire
schedule repair. In particular, do not treat repeated lease writes as completed
drains or drain unrelated queues to clear a status indicator.

Schedule installation plus an explicit `{"mode":"all"}` invocation proves
wiring and compiler execution. It does not claim observation of a later
natural cron tick. Those should be checked after their scheduled UTC times.
