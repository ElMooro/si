# JustHodl Compound Factory — Gear B (weights) and the outside-world curriculum

Owner: Khalid. Region: `us-east-1`. Shipped by Claude, 2026-09-13 (Ship 2, ops 5522).
Read `GEAR_A.md` first; nothing there changes. This document covers what was deliberately
unavailable there: **weight training** and a **general code checker**, both bounded.

## Decision recorded

Khalid (chat, 2026-09-13): up to **$600/month** to train the student; he wants to see it coding,
doing tasks inside and outside the system, analyzing, and building models. That decision becomes
two owner artifacts written by ops 5522 on the runner (never by an engine):

- cost-guard `ai/policy.json` → `daily_budget_usd = 20.00` ($600 / 30). Every SageMaker create is
  still priced from the live Price List against it.
- private `factory/control/gearb.json` (create-if-absent): `enabled`, the coder card, `ml.g5.2xlarge`
  spot, `max_runtime_s = 10800`, `daily_budget_usd = 20`, `season_cap_usd = 600`, `min_sft_rows = 1500`,
  `max_jobs_per_day = 1`, LoRA hyperparameters, and the approval line. A change is a new op, not an edit.

Nobody can be paid to "train" the student, and the student is not distilled from any hosted model.
Its teachers are checkers: tests that pass, prints that land, bars that follow.

## Objective, unchanged

Two numbers only: the held-out score goes up; the cost per passed trace goes down. Compute is
earned inside the caps, never taken: the student cannot read or edit `gearb.json`, the policy,
IAM, schedules or endpoints.

## The refusal chain (what must be true before a dollar is spent)

`gear_b.tick` runs from the existing hourly `justhodl-ai` inventory (no new schedule). In order:

1. `factory/control/gearb.json` present, `enabled`, approved, daily ≤ policy, instances allowed.
2. No Gear B job running.
3. `factory/holdout/manifest.json` frozen (else: refuse with the key name).
4. Dataset build: rows from `factory/traces/code/*` (Gear A graded passes), `factory/skillbook/*`,
   and `factory/curriculum/code/verified/*` (runner-verified rows). Each row must carry a source, a
   checker, and a license in the allow-list (`own`, MIT, Apache-2.0, BSD, CC-BY-4.0, …); rows inside
   the holdout (HumanEval ids, holdout source shas) are excluded; exact and prompt-level duplicates
   dropped; no family above `max_family_share`. Anything under `factory/traces/_reject/` is never read.
5. Floor: fewer than `min_sft_rows` verified rows → status `waiting_for_traces: N of 1500`, nothing written.
6. Dataset written once per generation to `factory/gearb/datasets/gen-N/` (create-if-absent) with
   sha256 digests and an `eligibility_digest`.
7. Launch: hub card must publish a training recipe; live price required; the job's on-demand
   ceiling (price × MaxRuntime) must fit today's remaining Gear B budget, the season cap, the
   cost-guard projection, and `max_jobs_per_day`. Then exactly one managed-spot training job with
   `MaxRuntimeInSeconds`, tags `justhodl-ai-managed` + `jh-factory=gearb-gen-N`, and the digests in
   its environment. The job record `factory/gearb/jobs/<job>.json` is written *before* the create.
8. Completion → `factory/gearb/candidates/gen-N.json` with `exam.status = pending_exam`.
   Promotion uses `factory_core.promotion_decision` against the base model's own exam; until the
   weights exam lane exists (below) no candidate is promoted and `factory/gearb/champion.json` stays
   at generation 0. No endpoint is ever created.

## Outside-world coding curriculum (the school is not the si repo)

`.github/workflows/factory-code-exam.yml` (03:20 UTC daily, dispatchable) has three jobs:

| Job | Credentials | Network | Does |
|---|---|---|---|
| fetch | AWS (freeze only) | yes | pulls MBPP (google-research, CC-BY-4.0; train+validation ids) and APPS (codeparrot/apps, MIT; introductory) as candidate rows with license + citation; on `freeze_holdout=true` also freezes the HumanEval exam (MIT, **exam only**) and the holdout manifest |
| verify | none | **none** (`docker run --network none`, read-only FS, 1 CPU, 1 GB) | executes every candidate solution against its tests with a timeout; only passes survive |
| write | AWS | yes | writes passes create-if-absent to `factory/curriculum/code/verified/` with `verified_by=owner_runner`, the checker id and run id |

Nothing from `ElMooro/si`, from Khalid, or from a paid model enters the lake. HumanEval task ids are
frozen inside the holdout manifest so `gear_b.curate` can exclude them forever.

## Holdout + anonymized drills (`scripts/factory_holdout.py`)

Any open-weight model has read about March 2020, SVB and the yen-carry unwind, so those weeks
are a leak-check, not the exam. The manifest freezes Khalid's blocks (holdout: covid-2020, svb-2023,
yen-carry-2024; train: hikes-2022, gilt-ldi-2022, tariff-2025) once. Drills are 20-session windows
of SPY/QQQ/IWM/TLT/GLD bars normalized to 100 with no dates, tickers or calendar features; labels
are the season's own `labels_from_prices` over the following five sessions (the student never
authors a label). A train window overlapping a holdout block raises before anything is written.
Provenance (dates, symbol) lives only in the private `factory/holdout/provenance.json`.
BTC drills wait for the Coinbase minute bank (season 2). **Promotion is decided by the forward
wall only.**

## What Khalid will see, and when

- After ops 5522: `data/ai.json → gear_b` (status `armed`, budget, holdout frozen, dataset
  `waiting_for_traces: 0 of 1500`). The ai.html card for this block is the next page ship; the
  block is already public and aggregate-only.
- Within a day: the exam workflow's `write` job report (`written: N`); the hourly status shows the
  verified-row count climbing toward the floor. MBPP alone is ~460 rows; APPS introductory adds up
  to 600 per run, so the floor is typically reached in 2–3 days.
- Then: one spot LoRA job per day at most (3h cap ≈ $4.55 on-demand ceiling at the current
  ml.g5.2xlarge price; spot bills less), `factory/gearb/candidates/gen-1.json` when it completes.
- Not yet: promotion, and the student *using* the weights. That needs the weights exam lane — a
  batch transform (a job, not an endpoint) over the frozen HumanEval prompts, outputs executed by
  the same network-less verify job, scored against the base model. That is Ship 3, and it is the
  first moment the student can "code" beyond the finite Gear A family.

## Not done here, on purpose

Weights exam + promotion (Ship 3); DPO pairs from `_reject` (needs the exam lane); the student
running tasks with the promoted weights (needs promotion); BTC drills; the ai.html Gear B card.

## Verification

- `python tests/factory/test_gear_b.py` (17), plus the existing factory suites and the
  `justhodl-ai` harness (23/23), deployment gates (305), configs and secrets clean, every workflow parses.
- ops 5522 proves: receipt sha-match, policy budget, coder card with a recipe, control written
  once, pre-freeze refusal on the holdout, workflow dispatched, post-freeze refusal on the floor,
  `data/ai.json.gear_b` free of ARNs and job names. It launches nothing.

## Addendum 2026-09-17 -- the exam launches, grades and decides itself

- Root cause of six dead training jobs (gen1..gen7, 2026-09-15/16): the recipe ran a fixed 400 optimizer steps whatever the
  data. With 570 rows (~84 packed sequences) that is ~70 epochs at ~47 s/step on ml.g5.2xlarge, ~5 h inside a 3 h cap: every
  job was stopped at ~50% (`MaxWaitTimeExceeded`) with nothing saved, and the hourly tick relaunched it up to 3x/day.
- `factory/training/train_qlora.py` now plans `max_steps` from the data (packed sequences x epochs / (2x8); the pin's
  `max_steps` is only a ceiling) and stops on `time_budget_s` (launcher passes `max_runtime_s - 1200`; recipe default 9600)
  with the adapter saved and `stopped_by=time_budget` in the manifest. Pin `epochs` = 3. The plan is written to the
  manifest (`plan`). TRAP still holds: re-pin the bundle after any change under `factory/training/*`.
- `gear_b.tick` after `poll_jobs`: `decide_pending()` applies `factory_core.promotion_decision` to any `exam_running`
  candidate whose evaluation exists (`factory/gearb/decisions/gen-N.json`, champion at `factory/gearb/champion.json`,
  `release_status awaiting_owner_release`), then `examine_pending()` takes the newest `pending_exam` candidate, extracts
  the adapter to `factory/champions/gen-N/adapter/` (create-if-absent manifest), and launches ONE frozen-holdout exam job
  (`jh-exam-genN-*`, prompts only, greedy, `exam_max_runtime_s`, cost-guard priced) recorded under `factory/bursts/jobs/`
  with `launched_by gear_b.examine_pending`. While an exam is in flight the tick refuses to launch training (one spot
  instance). `factory-exam.yml` runs on a schedule (:09/:39) and grades the oldest Completed exam job with no evaluation.
- The scoreboard's `voice` is `offline (deterministic desk read, no calls)` whenever the read carries `fallback/empty`;
  `read_path` and `calls_this_read` are published so ai.html can say the stances are gate-mapped, not an AI view.
