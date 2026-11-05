# Draft only: ETF desk supplement enablement capacity review

**Do not merge or activate this proposal yet. Current enabled end-to-end Lambda
capacity is not established.** This draft changes only the desk Lambda's
`environment.ETF_OWNERSHIP_SUMMARY_ENABLED` to the exact string `true`; it does not
change code, memory, timeout, credentials/inheritance, schedules, acquisition,
canonical qualification, UI, or decision gates. Only the existing desk's next
natural acquisition would opt into retained v2 inputs. The draft is not deployed.

The disabled implementation was independently accepted at PR23 head
`5899e09a8c02c6795c505dd68d495e43e5cfbe2b` and merged as
`d00f52a256815d6798988f23abec68afd0e15d4d`. The merge tree
`0e5a69a1ef0a1eddd3627fb690fd7384765b2449` exactly matches the checked current-main
candidate. On that tree: 78 desk native tests, 1,008 deployment static tests,
15 mocked candidate shell checks, preflight, source/config validation, secret
scan and eight-file staged inventory all passed. Deployment evidence is tracked
separately in `desk-supplement-enablement-capacity.json`; code delivery is distinct
from activation and from a new qualified publication. No activation, native
invocation, direct AWS access, or diagnostic probe was performed by this task.

## Disabled-slice delivery status

[Deploy run 36812617926](https://github.com/ElMooro/si/actions/runs/36812617926)
completed successfully at exact merge SHA `d00f52a256815d6798988f23abec68afd0e15d4d`.
The canonical HTTPS receipt read returned 403. The ordinary download of the
workflow's 366-byte release artifact also returned 403, so its contents are not
known. No alternate origin, access bypass or AWS probe was attempted.

**Exact deployed receipt/source hashes remain unverified.** The JSON evidence
records expected reviewed source hashes, clearly separate from runtime proof.
The workflow result proves its completion; it does not replace readable receipt
acceptance. No activation change was made; the main desk config still omits the
switch. The live environment was not independently read, and activated coverage
or a new summary publication is not claimed.

## Accessible evidence and its limits

The normal-TLS public desk read in the implementation task returned HTTP 403.
There was no alternate origin, denied-source bypass, private source fetch, or new
probe. A complete actual production-sized retained scenario is therefore not
available here. Supplied live counts (22,809 current, 25,252 prior rows) guide one
synthetic workload only; the actual fund distribution and identities are not
claimed to have been replayed.

Committed historical report
[`ops_5986_etf_desk_native_acceptance.md`](../../aws/ops/reports/latest/ops_5986_etf_desk_native_acceptance.md)
records a completed September 21 desk execution at **159.62316 seconds / 942 MiB
peak**, with 4,096 MiB configured. Current repo config is 900 seconds / 4,096 MiB.
The old arithmetic margins are 740.37684 seconds and 3,154 MiB, but these are not
current capacity measurements. The old execution predates current canonical
summary replay and this supplement. Ops6381 records the once-daily phase change
with zero native invocations; its 2-second operation duration is not producer
runtime. Neither report provides current S3 latency distributions or cache churn.

## Offline stress measurements

Reproduce in separate processes to keep peak RSS meaningful:

```sh
python tests/benchmark_desk_supplement_capacity.py
python tests/benchmark_desk_supplement_capacity.py --record-cap
```

Both cases use invented disjoint exact identities, all 16 funds qualified, the
actual `OwnershipSummary` and bounded `ArtifactWriter`, and in-memory storage.
They exercise both writing with verified readbacks and a second replay without
PUTs. They create no cloud clients, vendor requests, or invocations. They are
stronger publication-volume cases than the original overlapping-identity examples.

| Synthetic case | Records | Pages + manifest | Payload | PUTs / immediate GET readbacks | Accumulation + comparison | Finish + in-memory writer | Replay + comparison | Peak process RSS |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Supplied aggregate counts, disjoint identities | 70,870 | 356 + 1 | 26,147,918 B | 357 / 357 | 1.208 s | 0.522 s | 1.500 s | 350.7 MiB |
| 100,000-record cap, 33,333 current / 33,334 prior | 100,000 | 501 + 1 | 36,891,265 B | 502 / 502 | 1.301 s | 0.700 s | 1.971 s | 482.1 MiB |

These timings include synthetic comparison work the production supplement already
has available. Process RSS includes the test storage, retained artifacts and
replay; it is neither incremental Lambda RSS nor a worst-case memory guarantee.
The fixture fits the cache and replay issues zero backing-store GETs; the real
canonical-plus-desk working set has not been measured. Local timings omit network
latency and SDK retries and cannot certify the live 900-second budget.

## Bounded publication cost and unbounded-by-size timing risk

The existing reducer preflights all summary payloads before emission and caps:
100,000 records, 300,000 observations, 48 MiB total, 768 pages plus one manifest,
256 KiB per page and 512 KiB per manifest. Overflow returns unavailable and leaves
the base desk intact; it never emits a prefix labeled complete. This is the hard
publication-volume ceiling, not the smaller synthetic case result.

For one successful enabled compilation the supplement adds at most **769 immutable
PUT attempts and 769 immediate verified GET readbacks**, excluding SDK retries.
At 30 natural daily publications that is at most **23,070 PUTs and 23,070 immediate
readbacks**, and at most **1,509,949,440 bytes (1.40625 GiB)** of new retained summary
payload if every object is unique. Content addressing can reduce newly stored
bytes, but conditional existing-object PUT attempts still occur. Existing
input/output/run object counts remain unchanged; their payload sizes increase.
No new provider requests, invocations, schedules, or holdings shards are added.
No monetary price/billing or consumer traffic estimate is asserted.

Retained replay adds no PUTs, but may require up to another read of each summary
object if evicted, and can force displaced baseline objects to be re-read.
Therefore immediate summary GETs are not a bound on total execution GETs. The desk
cache is 768 MiB; its current full working set remains unknown.

The writer has six workers and at most twelve pending operations. At 769 objects,
a simple no-skew model takes 129 waves of PUT-plus-readback work. If a complete
pair takes 0.1 / 1 / 5 seconds, that alone is approximately 12.9 / 129 / 645 seconds.
These are sensitivity arithmetic, not measured latency or guarantees; object
sizes, queue skew, bandwidth, retries and other compilation/replay work matter.
The handler config uses a 5-second connect timeout, 20-second read timeout and
`max_attempts=2` retry setting. Slow calls can consume the Lambda budget even while
all byte/count bounds pass. The collection deadline nominally reserves 360 seconds,
but compilation and retained replay have no independent wall-time admission check.

There is consequently **no established current time, memory, or network margin**.
The historical margin minus a local synthetic timing/RSS number is not an
acceptance test. Enabling now would accept an unmeasured risk of an unavailable or
timed-out natural publication; this draft does not make that decision.

## Review requirement and rollback

Keep the PR draft pending independent acceptance of current capacity evidence or
explicit acceptance of the remaining uncertainty. Suitable evidence would cover
an authorized complete retained current scenario, current native duration/peak
memory, publication object/byte counts, PUT/readback latency including retries,
and full canonical-plus-desk replay/cache behavior. No probe, invocation or source
access to obtain that evidence is authorized or performed in this draft.

If separately accepted and enabled, validate the subsequent ordinary publication's
v2 input, exact 16-extra inventory, qualification exclusions and replay without
manually invoking the producer or changing cadence. New code being present is
not proof the supplement was activated or published.

Rollback must set the desk environment value explicitly to **`false`** in the
normal reviewed config lane. Merely deleting the key is insufficient because
deployment merges the function's existing environment. False affects only new
acquisitions: v2 recovery/replay retains its policy and original expiry; immutable
evidence is not deleted. Previously published supplements remain historical until
the next normal publication, with their original validity deadlines.
