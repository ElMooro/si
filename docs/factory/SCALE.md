# SCALE — the factory at millions to billions of agents

Khalid's direction (2026-09-13): the learning must be fast and sharp now, and the same design must hold when
there are millions or billions of agents. This document is the North Star every factory ship is measured
against. It is deliberately short: invariants, not features.

## 1. What an agent is

An agent is a **record**, never a process: `{alias, rank, co, task, adapter_id, evidence pointers}`. Agents cost
bytes. Compute is a fixed pool of slots that executes *tasks drawn from queues*; a billion agents and eight
agents draw from the same pool. Nothing per-agent is ever provisioned (no Lambda, no schedule, no role, no
endpoint). This is already the doctrine (`compute_inflight_cap_owned_by_factory_gateway`); it is the rule that
makes "billions" a counter instead of a bill.

## 2. One champion, many adapters

Weights are the scarce thing. There is **one base model per domain** (coder, forecaster) staged once in the
owned bucket (`factory/models/base/<id>/<revision>/`, hashed, licensed), and **LoRA adapters per generation**
(`factory/champions/gen-N/`). A squad of a million recruits shares one adapter; a colonel's desk may own a
specialised adapter. Serving cost scales with distinct adapters loaded, not with agents. Adapters are promoted
only by the shared factory contract: held-out exam, forward wall, evidence — never by the trainer.

## 3. Evidence is the only currency

Every unit of learning is a `factory-evidence.v1` object: claim, warehouse keys with hashes, holdout untouched,
falsifier, grade window, checker id. Grades are written only by graders. Ranks move only on warehouse-graded
windows. Training rows exist only as keep-only-passes derived from graded evidence. **No evidence, no learn.**
At scale this is also the anti-collapse rule: a billion agents cannot vote themselves smarter; they can only
submit checkable claims that the same checkers grade.

## 4. Checkers scale, models do not

Learning speed = graded examples per day. Checkers are unprivileged, network-less, horizontally scalable
(runner containers, processing jobs); they are the part of the system that gets more machines when demand
grows. Models get a burst budget. So the throughput plan is: curriculum with executable tests (code),
anonymized drills with official prints (markets), guest salons with envelopes (both) — thousands of graded
attempts a day at near-zero GPU cost, then one capped LoRA burst per generation on the passes.

## 5. Storage shape at scale

- Evidence, verdicts, discipline events: append-only, content-addressed ids, **sharded ledgers by day and
  hash prefix** (`.../YYYY/MM/DD/<xx>/…` and jsonl shards) — never one object per agent per tick.
- Counters (agents, cards, spend, graded/day): a single small state object with ETag, or a DynamoDB counter
  table when writers exceed one; readers never list to count.
- Ranks: partitioned by squad once cards exceed the state cap (512 KB); the supervisor row stays in the
  authoritative state; squads roll up.
- Retention: runtime snapshots expire (3 days); evidence never expires; reading receipts expire (90 days).

## 6. Ownership of the training lane

Weights, recipe, data and image are Khalid's: `factory/models/base/*` (weights + manifest), `factory/training/`
(recipe, pinned bundle in S3), `justhodl/factory-train` (image mirrored into the owned ECR, digest-pinned).
The hub is a catalog, not a dependency. `gear_b_own.own_spec()` builds the launch spec from these only and
refuses non-permissive licenses, unhashed files and unpinned images. Cost stays inside the same guard:
priced live, spot, capped per day and per season.

## 7. Invariants (a ship that breaks one is wrong)

1. Agents are records; compute is a fixed pool; nothing is provisioned per agent.
2. One base per domain; adapters per generation; promotion only by the shared contract.
3. No evidence, no learn; graders are not students; teachers are off the runtime path.
4. Real data only; missing stays missing; no fabricated votes, prints or neutrals.
5. Every spend is priced live and capped; no always-on GPU; no per-agent vendor calls.
6. Private stays private: Brain, exams, holdouts, manifests; the public feed carries aliases and digests.
7. Proof is a receipt or an S3 object, never a green workflow.

## 8. The next three levers, in order

1. **Trace bursts** (`factory/training/generate.py`): batch inference of the champion over the curriculum on a
   spot GPU, traces to the unprivileged verifier — this is where graded examples/day jumps from single digits
   to hundreds.
2. **Drill runner** for markets: replay a method card on anonymized weeks (train blocks) with the same
   labels the wall uses; holdout blocks stay untouched.
3. **Squad adapters**: once a squad's held-out pass rate beats the champion twice, it earns its own adapter
   line; the roster cap stops being 48 and becomes a per-squad cap.
