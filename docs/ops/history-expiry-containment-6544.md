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
default, sanitized expiration scopes, source/run metadata and fixed failure codes.
No raw SDK errors, object inventories, private values, billing or credential reads
are published. Other enabled expiration rules are inspected but never changed;
tag/size-qualified or unsafe-text scopes are explicitly qualified/withheld.

Disabling this rule preserves remaining versions after S3 propagation; it cannot
recover versions already permanently deleted. Other expiration rules may remain
active, so this is not a claim that all history is protected. Existing retention
and storage charges outside this one rule remain unchanged.

Validation: `PYTHONDONTWRITEBYTECODE=1 python -m unittest discover -s tests -p test_retention_6544.py -v`.
