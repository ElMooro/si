# Provider catalog: full prefix LIST pages

The production change is one literal in the existing complete provider-prefix
walk: `MaxKeys` 400 to 1000. It retains every prefix, full opaque-token traversal,
duplicate prefix/hot records, stable size ordering, failure propagation, manifests,
provider pages, shards, SQLite FTS rows, counters, consumers and publication order.
Root discovery keeps its delimiter and existing limits. Manifest-counted Eurostat
and ECB series stores remain exact manifest reads rather than object scans.
No configuration, schedule, source freshness, storage or retention change.

The reviewed predecessor is handler SHA-256
`ec8f82b9010454f8708da80279a61330f10685aba22385addee7d1a603e25684`,
identical at source commits `31233b15f6b7b8928523d9b4553b16623edd1ce0`
and `600179075a54b5983d1a3e012582915b0da2dc0e`. The regression reconstructs
that entire predecessor from the candidate and asserts its exact hash before
running both complete handlers with invented S3 and real SQLite/gzip.

## Offline evidence

Run `python3 aws/lambdas/justhodl-provider-catalog/tests/run_tests.py`.
Optional reproducible measurements: append
`--benchmark-output /tmp/provider-list-benchmark.json`.

Every output body, key and upload option is byte-identical under a fixed clock
and gzip timestamp. SQLite rows, handler result, HEAD/GET sequence and unrelated
LIST requests also match. Cases cover 0/1/399/400/401/999/1000/1001/5001 objects,
short pages, truncated empty pages, a final empty page, independent opaque token
chains, nested/overlapping prefixes, equal-size order, hot duplicates, missing
hot markers, derived counters and second-page failure without final publication.

One local Python/tracemalloc run on invented full-handler inventories:

| Prefix objects | LIST calls 400 / 1000 | Python peak bytes 400 / 1000 | Seconds 400 / 1000 | Identical published bytes |
|---|---|---|---|---|
| 5,001 | 13 / 6 | 3,661,352 / 3,543,979 | 1.69 / 1.97 | 889,229 |
| 25,001 | 63 / 26 | 11,276,687 / 11,214,287 | 11.19 / 9.18 | 4,061,227 |

Fixture timing includes fake response construction, local SQLite and compression;
Python peaks exclude native SQLite allocations and input fixture construction.
These are neither Lambda memory/runtime proof nor measured billing savings.
For unchanged inventories with full responses, requests per prefix become
`max(1, ceil(objects/1000))` instead of `max(1, ceil(objects/400))`.
Short responses may require more requests; pagination remains exhaustive.

## Native release acceptance and rollback

Ops 6437 is staged for explicit direct Actions dispatch after review/merge.
It makes at most nine read-only AWS calls and one signed-package HTTPS GET in
120 seconds. It reads exactly one function, its named hourly rule (at most 100
target rows, no pagination), one public release receipt and two public output
documents (2 MiB each), with HEAD of those two outputs plus the exact validated
index key. Package download is bounded to 64 MiB and each compared source member
to 8 MiB. Retries are disabled. It stops on denial or any bound/control mismatch.
It performs no invocation, resource mutation, billing/metric/log query or S3 LIST.
Raw payloads, environment, signed URLs and SDK errors never enter the report.

Before targeted deployment, require predecessor source, matching reachable
helpers, receipt and all declared controls, including already active tracing and
standard DLQ. Retain that sanitized report as prior-state evidence. Native
acceptance compares the actual ZIP hash, complete tracked source/reachable helper
bytes, readiness and receipt; the operator checks the receipt's exact release
commit. Compare the hourly schedule-control fingerprint before and after.
The fingerprint excludes target payload contents; the normal deployment lane
preserves them through its existing tested scheduling transaction.

After normal targeted Actions deployment, wait for the existing hourly run.
Require a common catalog/search generation after deployment, matching search
counts/coverage joins, and both output/index modification times after deployment.
No manual producer invocation. This establishes natural publication and metadata
contracts; complete same-input equivalence comes from the offline regressions.
It does not establish the current monthly cost attribution or a budget cap.

Rollback is a reviewed source-only reversal of this literal to 400, preserving
the same configuration/schedule and deploying only this function through the
normal lane. Recheck exact receipt, controls and subsequent natural publication.
