# History snapshotter newest-50 selection: hold

The bounded heap candidate is behavior-equivalent in the offline checks, but
the present evidence does not justify a production release. It retains at
most 50 valid string timestamps per feed, reducing large synthetic allocation
peaks, while increasing CPU on ordered scans. Actual scan order, feed counts,
memory pressure and billed duration are unverified. Preserve the candidate as
a draft; do not merge or deploy it on this evidence.

## Revalidated source and scope

Current main was fetched from `ElMooro/si` and rechecked at
`31b50acda4d33a127cff0ffb59a588a18e4005c6`. Its handler is identical to the
complete predecessor fixture retained from claim commit
`c37b9cf823f163901afec36cfc332f3f4d71fef8`, SHA-256
`fe1f2ee892f23c8fc39c3d901a2bc908654da76798c04417757563b19680fe73`.
The audit's unbounded `_all_sks` accumulation and final descending sort still
exist. The independent cutoff is **101 scan pages**: the loop stops when
`pages > 100`, after processing that page. It is unchanged.

`DEPLOY_LANE.md`, `CLAUDE.md`, relevant `AUTONOMY.md` rules, `STATE.md`,
`docs/SESSION_CLAIMS.md`, open PRs and deployment source were inspected.
No AGENTS.md or local `.agents/skills` files were present. The new claim
`S-codex#hs501002p8` covers only this selection and verification; the SDMX
workstream, provider-catalog ownership and PR66 work are untouched.

The production candidate adds `heapq` and replaces only timestamp append
with bounded selection for strings. Non-string malformed `S` values continue
through the original append/sort path. Existing comparisons still reject mixed
incompatible values. No timestamp parsing, deduplication or coercion is added.
Counts, first/last strings, first-encountered latest hash on ties, tied timestamp
multiplicity, descending timestamp order, stable feed order, missing/malformed
handling, output schema and visible failures remain as before. An AST check
requires every other production function and top-level statement to match.

`audit.html` consumes `data/history-index.json`, including all index and feed
fields and the newest-50 pill list. `history-api.handle_index` loads and returns
the same index. `weights.html` instead consumes
`calibration/history-index.json`; that separate index is not written by this
helper. All consumer source is unchanged.

## Offline evidence

Run `python3 aws/lambdas/justhodl-history-snapshotter/tests/run_tests.py`:

- 303 index cases and 318 complete handler fixtures compare the exact full
  predecessor with the candidate, including return values, exception
  type/message, stdout, ordered fake AWS calls, complete output bytes and
  all write options.
- Cases cover boundary lengths through 10,000, paginated/empty-page scans,
  order and equal-count feeds, duplicate/tied/Unicode/invalid-date strings,
  missing and malformed scalar/container inputs, empty feeds, the 101-page
  cutoff, source/query/table/scan/write failures, deduplication, compression,
  binary bodies, large fake archive writes and heartbeat failures.
- Fixed clocks hold SK/TTL, hourly gating, JSON duration and gzip header
  timestamps equal. Minute-five and crossing-minute cases are included.
  The candidate's heap is instrumented to verify its 50-element bound.
- Four successful output pairs pass through the actual history API function
  and actual audit page `load`/`selectFeed` functions with invented DOM/fetch;
  resulting response bytes and rendered values agree. No archive route is
  called. The existing history API suite also passes its 12 privacy/route cases.

Selected source/config validation, ops preflight, secret scan, stub guard,
engine-wiring check and the 15 public-brain-boundary tests pass. The full deployment
gate passes 1,035 static tests and 15 validated-candidate shell checks. Dependencies were
installed only in `/tmp/history-test-venv`; no gate was weakened and no unrelated
source was repaired. Fixtures are synthetic, never production originals.

## Runtime and memory measurements

`history-newest50-equivalence.json` retains the native comparison verdict.
`history-newest50-benchmark.json` retains every benchmark sample. Reproduce with:

```sh
python3 aws/lambdas/justhodl-history-snapshotter/tests/benchmark.py --json /tmp/history-newest50.json
```

Python 3.12.14; nine interleaved CPU samples after warmup and three traced
allocation samples for each of 24 size/order/feed combinations. The complete
index builder runs, including aggregation, final sorting and serialization;
fake scan/S3 have zero latency. Compilation and fixture ownership are excluded.
Prebuilt inputs retain timestamp strings in both versions: traced peaks measure
list references/sort workspace conservatively, not freed string bodies, process
RSS, real page allocations or Lambda billed memory.

| Synthetic rows / feeds / order | Predecessor CPU | Heap CPU | Predecessor traced peak | Heap traced peak |
|---|---:|---:|---:|---:|
| 50 / 1 / ascending | 0.095 ms | 0.103 ms | 4,450 B | 4,866 B |
| 5,000 / 1 / ascending | 3.775 ms | 4.802 ms | 43,000 B | 5,066 B |
| 50,000 / 1 / ascending | 32.626 ms | 43.347 ms | 445,496 B | 5,072 B |
| 50,000 / 1 / shuffled | 72.119 ms | 57.681 ms | 644,984 B | 5,072 B |
| 200,000 / 1 / shuffled | 443.708 ms | 373.795 ms | 2,424,680 B | 5,078 B |
| 45,000 / 45 / ascending | 30.840 ms | 38.460 ms | 411,907 B | 161,279 B |
| 45,000 / 45 / shuffled | 81.019 ms | 79.240 ms | 415,027 B | 160,767 B |

The heap does extra per-row work and loses Python's efficient sort on already
ordered runs. Descending and small-input cases also regress; ties have mixed
results. An exploratory selection-only comparison with a sorted bounded list
also showed ordered-input CPU regressions; that alternative was not installed
in production source and is not a validated candidate. DynamoDB scan order is
not established here, and shuffled cases cannot be assumed representative.

A normal public `justhodl.ai/data/history-index.json` read failed with HTTPError;
no alternate archive/private route, direct AWS call or access bypass was used.
Thus the chosen sizes span synthetic scales rather than verified live counts.
There is no evidence that saving these allocations reduces provisioned memory,
failures or billed duration. AWS billing, metrics, private payloads and retained
data were not read. **No measured dollar savings are claimed.**

The helper runs after the existing `minute < 5` gate. It has no elapsed-time
cutoff, only the page cutoff, but shares the whole handler's existing Lambda
timeout with snapshotting and publication. CPU regressions could consume more
of that budget; faster/slower completion also changes real `duration_s` and
`generated_at`. Fixed-clock fixtures prove equality for matching inputs and
completion outcomes, not byte identity between independent real timed runs or
unchanged failure probability near a timeout. No time-budget setting changes.

## Deployment reconciliation, controls and rollback

The source-only candidate changes no config, scans, retries, archive writes,
storage/retention, protected approximately 500 GB of data, five intentional user
schedules, IAM/security, cadence, concurrency, timeout, engine communication,
paid-service usage or risk/trading behavior. Its only main commits concern
coordination. No producer invocation, AWS mutation, deletion or deployment is
performed for this held candidate.

Static deployment inspection matters even for this small change. The current
lane merges the existing environment with empty configured overrides. Legacy
`memory_mb` and `timeout_s` metadata do not activate canonical `memory` or
`timeout` update flags; live settings are preserved. Runtime/role/handler updates
require absent explicit opt-ins, and there is no schedule-write block for this
function. Description is reapplied. The standard lane also unconditionally
reconciles X-Ray `Active` and the default DLQ. Their existing live values must
already match before any future release; a tiny source diff alone cannot prove
unchanged controls or authorize activating tracing.

No runner probe is justified after the benefit gate held. Exact live package,
release receipt, unchanged-control proof and natural post-release output are
therefore **not obtained**, and no release run/package/receipt is claimed.
Before reconsidering release, establish representative scan order/counts and
meaningful benefit using authorized bounded technical evidence, obtain an
independent exact-head review, repeat all applicable engine/consumer/preflight/
secret/deployment gates, and capture a privacy-preserving runner baseline.
That baseline must qualify the exact predecessor package and all relevant live
runtime/env/storage/concurrency/tracing/DLQ controls and bindings, including the
five excluded monitoring schedules. Require a matching post-release package,
commit-bound receipt and unchanged-control projection through normal Actions.
Observe natural output without invoking a producer or inferring helper
execution from publication alone.

Rollback, if a future qualified release occurs, restores only this source's
predecessor import/append code through a reviewed commit and the same Actions
lane, followed by exact package/receipt/control verification. All data, storage,
retention and schedules remain untouched. The present draft needs no production
rollback because it has not been released.
