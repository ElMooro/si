# Exact history-expiry containment

Khalid authorized disabling only `ops6541-bound-noncurrent-versions` at
2026-10-17 02:52:14 UTC. Ops6544 changes only that rule's Status from Enabled
to Disabled in `justhodl-dashboard-live`. It preserves all other rules, ordering,
filters, actions, dates and the transition-default setting. Unknown configuration,
missing/duplicate/changed target, or detected drift aborts before mutation.

The prior ops6541 lifecycle section is now read-only, preventing its rerun from
recreating/re-enabling this exact rule. No other part of that old operation is
executed by this containment. Source owners' other changes remain intact.

Use the existing direct Actions lane with
`STAGED/ops_6544_disable_6541_history_expiry.py`, then separately dispatch
`STAGED/verify_6544_history_expiry.py` for read-only verification. Do not execute
these scripts or construct AWS clients locally. The CLI refuses outside Actions.
No workflow, IAM, security, schedule, budget, engine, trading, object, storage-class
or location change is included. No purge, fleet restore or automatic rollback.

Only GetBucketLifecycleConfiguration and at most one PutBucketLifecycleConfiguration
are used, with expected owner, 3s connect/7s read timeouts, zero SDK retries,
12 reads maximum and a 180s process alarm. A PUT with an uncertain response is
never repeated; only readback follows. Read-only verification never issues PUT.
Two exact postreads are required; job success alone is not completion evidence.

S3 lifecycle PUT has no If-Match/ETag condition. The deploying owner must serialize
writers and check current serial/direct runner activity first. Two fresh complete
reads detect observed drift immediately before PUT, but are not atomic CAS and
cannot eliminate the final read-to-write race. Any observed conflict stops this
operation without a stale rollback or repeated mutation.

Full before/after configuration stays in memory. Public evidence includes only
the known target before/after, exact-other-fields-preserved result, transition
default, allowlisted expiration scopes, source/run metadata and fixed failure codes.
No raw SDK errors, object inventories, private values, billing or credential reads
are published. Other enabled expiration rules are inspected but never changed;
The containment publishes only the user-named empty, `data/` and `system-events/`
prefixes and named rule IDs. The separate read-only verifier additionally permits
exact matches of fixed public literals from ops5027, ops5024, ops5164, ops5166,
ops5306 and the 2026-04-25 archive lifecycle source. Source candidates are not
evidence that a rule is installed: only a fresh live exact match identifies it.
Other identifiers/prefixes are withheld, including otherwise plain alphanumeric
text. Tag/size-qualified scopes are explicitly qualified.

Disabling this rule preserves remaining versions after S3 propagation; it cannot
recover versions already permanently deleted. Other expiration rules may remain
active, so this is not a claim that all history is protected. Existing retention
and storage charges outside this one rule remain unchanged.

Validation: `PYTHONDONTWRITEBYTECODE=1 python -m unittest discover -s tests -p test_retention_6544.py -v`.

## Completed containment

PR114 merged as `48449c14ae7d3563b752e8bfc57b0c8caf7831a3` after independent
exact-head review. Direct runner [38997304708](https://github.com/ElMooro/si/actions/runs/38997304708)
recorded Enabled to Disabled at 2026-10-17 03:03:52 UTC: one PUT, four GETs,
two exact postreads, every other rule/field and transition-default unchanged.
Its immutable public receipt is commit `1afe7863e45f4a501673a96631bda191bacf9399`,
`aws/ops/reports/latest/ops_6544_disable_6541_history_expiry.md`.

Separate [read-only runner 38997377347](https://github.com/ElMooro/si/actions/runs/38997377347)
confirmed Disabled at 03:05:04 UTC with zero PUTs and four GETs. Its receipt is
commit `9a80b0fc7cad23ef5c6a3ddb6796069d99f360ee`, same directory,
`ops_6544_disable_6541_history_expiry_readback.md`. Independent release review
validated both pinned runs and report commits. Enabled one-day noncurrent
expiration remained on `data/` and `system-events/`; two other rule scopes were
withheld. The supplemental verifier identifies only exact public-source matches.
No further lifecycle write is authorized or needed for that inspection.

## Remaining live expiration scopes

PR115 merged as `c8dd9f035b78f663f2a7b7777002ad42cb543cc6`; its tree exactly
matches independently reviewed head `40d0bd72b9065068c9b227061abf7a7f131f5319`.
[Read-only runner 38997768205](https://github.com/ElMooro/si/actions/runs/38997768205)
confirmed the following at 2026-10-17 03:11:06 UTC with zero PUTs, four GETs,
target still Disabled and every other lifecycle field/header unchanged.
[Immutable public receipt](https://github.com/ElMooro/si/blob/dd565e7f5f497f31debd1e22554408bb1c63c381/aws/ops/reports/latest/ops_6544_disable_6541_history_expiry_readback.md).

| Zero-based rule index | Exact live prefix | Exact public rule ID | Enabled expiration |
|---|---|---|---|
| 2 | WITHHELD | WITHHELD | Current-object expiration: 30 days |
| 7 | `data/providers/` | `ops5027-purge-dead-versions-providers` | Noncurrent: 1 day; expired delete markers |
| 8 | `data/` | `ops5164-purge-dead-versions-data` | Noncurrent: 1 day; expired delete markers |
| 9 | `system-events/` | `ops6520-purge-dead-versions-system-events` | Noncurrent: 1 day; expired delete markers |

None of these four scopes is tag/size-qualified. `data/providers/` is also under
the enabled `data/` rule. These rules were inspected and preserved, never disabled.
Index 2 did not match any finite public literal; its exact scope and overlap remain
unidentified in public evidence. No more candidate guessing or lifecycle writes
were performed. This incomplete scope identification does not block the verified
exact-rule containment, but blocks an all-history-protection conclusion.

The source and supplemental read-only projection passed independent exact-head
reviews; the final synthetic suite passed 18 tests, preflight had zero warnings,
secret scanning had zero findings, and both commit inventories matched their
reviewed trees. No user-object access, engine invocation or fleet restoration was
needed. The only AWS mutation across all three runs was the one authorized
lifecycle Status change in the first run. Previously deleted versions remain
unrecovered; no savings or total-cost-cap result is claimed.
