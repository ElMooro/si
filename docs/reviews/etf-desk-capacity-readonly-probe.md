# Draft ops6410: exact desk deployment and observed capacity evidence

**Draft only. Do not dispatch until independent review accepts the exact head.**
PR26 remains draft and unmerged at `71034757e4c4b6db0c15780f0decc6f7238bbfe3`.
This probe does not activate anything. Ops6409 is reserved by the symbol-directory
work; ops6410 was checked against current main's staged/pending/ran inventory and
claims before reservation.

The staged script is `aws/ops/staged/ops_6410_etf_desk_capacity_readonly.py`.
It uses the existing `ops_report.report` context and **`r.kv(...)`**, with sanitized
failure tokens. It creates clients only inside `main()` after checking the exact
GitHub repository, direct-dispatch workflow name, workflow-dispatch event and a
full source SHA. Importing it or running it outside that runner creates no clients.
There are no workflow edits, pending-queue files, or automatic execution triggers.

## Exact read scope

| Read API | Exact resource / request | Maximum calls |
|---|---|---:|
| Lambda GetFunctionConfiguration | `justhodl-etf-global-desk`, us-east-1, account 857687956942, full unqualified ARN; before and after to detect drift | 2 |
| S3 HeadObject | `justhodl-dashboard-live/data/ops/releases/justhodl-etf-global-desk.json` and `data/etf-desk-research.json` only | 2 |
| S3 GetObject | Exact release receipt only, conditional `IfMatch` from its HEAD | 1 |
| CloudWatch GetMetricStatistics | `AWS/Lambda`, only `FunctionName=justhodl-etf-global-desk`; Duration Average/Maximum/SampleCount and Invocations/Errors/Throttles Sum | 4 |
| CloudWatch Logs FilterLogEvents | `/aws/lambda/justhodl-etf-global-desk`; exact `"REPORT RequestId:"` filter, 20 events/page, at most 3 pages | 3 |
| **Total SDK operation ceiling** | No automatic retries (`total_max_attempts=1`) | **12** |

The fixed window starts at the current UTC hour minus 47 hours and ends at the
observation time: at most 48 hours, including the current partial hour. Metrics
use 3,600-second periods, with at most 48 returned points per metric (192 total).
Missing datapoints remain unavailable, not zero. A measured Sum=0 remains zero.
Duration with zero samples does not produce an observed duration/headroom value.

Runtime projection includes only State, LastUpdateStatus, CodeSha256, MemorySize,
Timeout and normalized LastModified. No environment, update-reason text, ARN,
request identity, or other configuration fields are reported. SDK configuration
responses inherently contain other fields in memory; they are not serialized or
saved. The operation validates the exact function identity before projection.

Receipt HEAD must be at most **65,536 bytes**. Body reading is bounded to that
length plus one overflow sentinel, verifies metadata/length, closes the stream,
rejects duplicate JSON keys/non-finite constants, and projects typed known fields
only. It verifies the expected PR23 merge commit
`d00f52a256815d6798988f23abec68afd0e15d4d`, receipt/live CodeSha256 agreement, ZIP
hash consistency, the receipt verified flag, and the unchanged known native source
hash. It reports only the three explicitly named native/model/store source hash
entries. Missing shared-module entries remain null/unverified; a receipt that
omits them is not relabeled as full source-inventory proof. A changed runtime or
mismatched receipt fails the probe after its safe report is recorded.

The desk output is **HEAD-only**: byte length, ETag and LastModified. No output
body, fund rows, raw/prior originals, replay manifests, account data or Brain data
are fetched. S3 listing and arbitrary keys/buckets are prohibited. Object
LastModified is not labeled a producer generation date or semantic freshness.

## REPORT privacy and interpretation

`FilterLogEvents` receives only the exact group's filtered REPORT-shaped records.
SDK results briefly contain message text and event identifiers in memory. The
probe neither prints nor stores them. It accepts only a full bounded standard
REPORT line (at most 4,096 characters), discards its request ID without capturing
it, and projects numeric timestamp, duration, billed duration, memory size, max
memory used, and optional init duration. It emits no raw message, request ID,
stream name, event ID, error-type text, or arbitrary returned field. Lines with
prefix/suffix payloads, malformed numeric values or impossible memory values are
ignored. The maximum output is **60 numeric records**. Empty pages, repeated
pagination tokens and the three-page cap never become a claim of complete logs;
`pagination_complete` states the observed result.

This is conventional REPORT-format evidence, not cryptographic platform-message
attestation. Report timestamps ending after LastModified are counted descriptively;
they do **not** prove that those invocations ran the current code. Metrics and
older REPORT rows can span earlier releases or memory configurations. Arithmetic
`Timeout - observed max Duration` and `MemorySize - observed max memory` is emitted
only with stable before/after configuration and available measurements. Sampled
memory maxima do not certify a full-window peak when pagination is incomplete.

**`enabled_supplement_capacity_established` and
`new_supplement_network_cost_measured` always remain false.** Historical baseline
telemetry cannot establish the extra supplement's write/readback latency, retry
behavior, cache pressure, or enabled end-to-end margin. No new invocation is
created to obtain a post-release sample.

## Bounds, permissions and failure behavior

Each SDK call has a **3-second connection timeout and 5-second read timeout** with
one total attempt. A **90-second admission deadline** is checked before/after every
API and after receipt-body reading. This stops further calls after the deadline;
it is not a hard process watchdog for an in-flight transport read. No polling,
sleep, unbounded paginator, query execution, resource discovery, or fallback on
access denial is used. At most 12 SDK calls, four metric requests, three filtered
log pages, two headers and one small receipt body are read. The server-side log
scan volume within that one group/window is not measured; no monetary cost or
strict scanned-byte bound is asserted. Existing runner credential setup and GitHub
report publication are outside these probe-call counts.

The wrapper validates both the allowed operation and exact parameters **before**
accessing an SDK method. No Invoke, Put, Update, Create, Delete, schedule, StartQuery,
S3 listing, vendor or notification APIs are permitted. The only writes by this
script are the existing reporter's local Markdown/step summary. The existing direct
workflow later commits that report; no production objects or configuration change.
Repeated observations are idempotent reads (telemetry may naturally advance).

API denials, absence, receipt precondition races, invalid contracts and exhausted
bounds stop without fallback or raw error details. Exceptions are caught within
the report context and converted to allowlisted reason tokens/SystemExit(1), so
SDK messages, request IDs and payload-bearing tracebacks cannot enter the report.

No IAM changes are proposed. The existing runner needs read permissions for the
exact Lambda, exact two S3 objects, GetMetricStatistics and FilterLogEvents on the
exact log group. Broader IAM permissions do not broaden the code's allowlist.

## Offline validation and future dispatch boundary

Run `python -m pytest -q tests/ops/test_etf_desk_capacity_readonly.py` with the test
runtime installed. Invented-only tests block mutations/unreviewed targets before
SDK access, exercise all API/page/body/time caps, reject tampered receipt fields,
check missing/zero telemetry and configuration drift, preserve incomplete-page
semantics, pin expected source hashes, and verify real report/stdout/step-summary
redaction on successful and failing runs. They mock client construction and issue
zero AWS calls.

Independent review must approve the exact probe head before any production run.
Prefer merging the approved probe first, then using the existing direct workflow
from main after verifying the approved script bytes are still present. Do not
dispatch an unmerged branch casually: the existing workflow commits its checked-out
report back to main. The report records the actual GitHub source SHA and script
SHA-256 for review. No dispatch command was executed during preparation.

Preparation validation: 45 focused probe/ETF-phase/ops-queue/report-commit tests
passed (51 subtests), including 16 probe tests. Preflight passed with zero warnings.
All tests use invented clients/evidence; no production probe was executed.
