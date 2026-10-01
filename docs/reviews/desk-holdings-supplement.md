# Draft: extra-only ETF desk holdings supplement

This draft adds `extra_holdings_summary` to versioned desk output, using the existing
`etf_holdings_model.OwnershipSummary` reducer. It feeds the current/prior snapshots
already reconstructed by `extra_holdings()`, reuses their retained snapshot and
comparison references, and creates no duplicate holdings shards.

The current catalog intersection is 100 canonical overlaps plus these 16 extras:
BKLN, BND, ECH, EFNL, EPU, FALN, FXE, FXY, IEFA, MOAT, PPLT, QQQM, RSP, SGOV, USHY,
VCIT. The allowlist prevents expansion beyond these funds; canonical overlaps are
excluded even if catalog membership changes. Duplicate configured tickers and
missing/extra collection inventory fail closed. The supplement is explicitly
labeled `additional_desk_funds_only`; its denominator is the extra inventory, not
300 canonical funds or 116 desk funds. It does not combine the canonical summary.

## Activation and replay

Only an exact `ETF_OWNERSHIP_SUMMARY_ENABLED=true` on **new desk acquisition** opts
into `etf-desk-inputs.v2` with `desk-extra-qualified-membership.v1`. Absent, false,
or malformed values retain v1 / output version 2.0.0. The current desk config does
not set this variable, and this PR deliberately leaves config unchanged: independent
review must authorize a separate config-only enablement before normal publication
can include the supplement. The draft itself neither deploys nor enables it.

V1 replay retains old bytes. V2 replay/recovery uses retained policy and source
clocks regardless of the current environment switch. Reviewed predecessor desk
model/store SHA-256 pins permit exact reconstruction, never executing retained
compiler source. The new gzip fixture was produced from the initial main
`0035bf47da741a2916a0e34df173934e5ae297aa` model/store, using invented fixture data
only; it contains no live provider or account evidence. Both its old output and
the older existing desk golden fixture replay byte-for-byte.

The canonical reducer, identity rules, acquisition code, UI, decision/gate logic,
workflows and schedules are unchanged. Zero vendor requests, AWS invocations,
notifications or deployments were performed for this task. The only attempted
live data read was normal-TLS public HTTPS, which returned 403; no bypass was used.

## Meaning and limits

Partial/missing/identity-ambiguous snapshots remain excluded from qualified counts.
Raw observations and known-presence lower bounds remain separate from qualified
counts; lower bounds are never proof of absence. Date cohorts stay separate and
same-date revisions never become dated membership changes. Source expiry is
exclusive. Zero remains zero and null remains unavailable. No trade, daily-change,
unit, corporate-action or currency qualification is added.

The supplied Sept 30 observations (22,809 current and 25,252 prior extra rows;
only MOAT qualified, with Sept 29/Aug 31 dates and 45 matched / 10 current-only /
11 prior-only identities) were not independently reverified here. Tests and cost
measurements use explicitly synthetic data. The implementation does not hard-code
MOAT as qualified or weaken rules to qualify any other fund.

## Cost evidence

Reproduce the offline measurements with:

```sh
python tests/benchmark_desk_holdings_supplement.py
python tests/benchmark_desk_holdings_supplement.py --all-qualified
python tests/benchmark_desk_holdings_supplement.py --producer old
python tests/benchmark_desk_holdings_supplement.py --producer new
```

`desk-holdings-supplement-benchmarks.json` retains measured results. The pre-edit
reducer benchmark matches only the supplied aggregate row counts; all identity
distributions, per-fund sizes and clocks are invented. One-qualified case: 122
pages + one manifest, 8,999,102 bytes, 0.76 seconds, 87.9 MiB peak process RSS.
All-qualified case: 242 pages + one manifest, 17,735,633 bytes, 0.97 seconds,
138.4 MiB RSS. These are cases, not worst-case guarantees or live Lambda timings.

The small complete producer fixture changes 32 to 34 PUT attempts, adds 3,874
retained bytes and 396 root bytes, and changes compile time from 0.043 to 0.052
seconds. Explicit replay emits no PUTs. Each extra immutable object adds a PUT
attempt and verified readback on compilation; unchanged immutable content can be
reused but attempts still incur requests. Existing input/output/run objects keep
their object counts; compiler bytes change once. Network latency is unmeasured.

At 30 daily publications, the measured row-count cases imply 3,690–7,290 extra
summary PUT attempts and at most 270–532 MB of new summary payload if every object
changes. Additional vendor requests, schedules and invocations are zero. While
the desk switch is absent, actual incremental publication cost is zero. This is
bounded publication work, not an acquisition expansion. No cloud price or billing
estimate is claimed.

The unchanged reducer has hard caps: 100,000 records, 300,000 observations,
48 MiB total, 768 pages plus one manifest, 256 KiB/page, 512 KiB/manifest.
It preflights bytes before any summary emission. Overflow retains the base desk
and returns an unavailable supplement without a partially published summary.

## Validation and rollback

Validation includes full desk native tests, focused canonical holdings and
independent acceptance tests, old/new exact-byte replay, compiler/artifact tamper
rejection, strict brake transitions/retries/recovery, all reducer overflow bounds,
exact inventory/no-overlap assertions, deployment static and mocked shell gates,
source/config validation, secret scan, preflight and staged inventory checks.
The desk Lambda's existing test glob includes the new supplement tests.

Rollback before activation: close the draft or revert its isolated changes.
After a separately reviewed activation: set only the desk's
`ETF_OWNERSHIP_SUMMARY_ENABLED` to `false`, using the normal reviewed config lane.
New natural runs omit the supplement without changing collection or cadence;
retained v2 replay/recovery stays available. An already published immutable summary
remains dated and subject to its own expiry until replaced by the next ordinary
publication. Do not delete retained artifacts or revert to a compiler incapable of
replaying v2. No operational rollback was executed.
