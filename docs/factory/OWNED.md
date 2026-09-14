# OWNED — what the factory keeps inside Khalid's system

Companion to SCALE.md. A capability is "owned" when weights, code, data and the checker that grades it all sit
in Khalid's S3/ECR/repo and keep working when every teacher is gone. Each row names the checker, because a
capability without one only produces fluency.

## Owned now (2026-09-13)

| Piece | Where | Checker |
|---|---|---|
| Base weights, hashed + licensed | `factory/models/base/<id>/<revision>/` + `manifest.json` (staged by `factory/training/stage_base_weights.py`; Apache-2.0 only) | `gear_b_own.validate_base_manifest` refuses unhashed/unlicensed |
| Training recipe (QLoRA) | `factory/training/train_qlora.py`, content-addressed bundle in S3, `factory/training/current.json` pin | promotion decided by the exam job, never by the trainer |
| Trace bursts + exams | `factory/training/generate.py` (K samples/task at T; holdout refused in burst mode; exam greedy) | unprivileged verifier (runner, network-less) |
| Training image | pinned AWS DLC by tag; `factory-training-image.yml` mirrors it into ECR `justhodl/factory-train` and re-pins by digest | `validate_pin` refuses non-ECR / unpinned when `require_digest` |
| Launch path | `gear_b_own.own_spec` / `burst_spec` → the existing Gear B refusal chain (priced, spot, capped) | cost guard + budget check |
| Evidence contract | `factory_evidence.py` (`factory-evidence.v1`, reading receipts) | graders only write grades |
| Chain of command | `factory_discipline.py` in the student tick | warehouse-graded 21-day windows |
| Official prints | `scripts/factory_official_prints.py` + Sat/Sun cron | grader `grade_market` |
| Code curriculum + holdout freeze | `scripts/factory_oss_curriculum.py`, `scripts/factory_holdout.py`, `factory-code-exam.yml` (Gear B lane) | executable tests in a network-less container |

## Own next, in order (each is a lever on graded examples per day)

1. **First trace burst → verifier → generation 1.** Weights staged (ops 5530) → `burst_spec` on the curriculum →
   verifier keeps passes → `gear_b.curate` reaches its 1,500-row floor → first capped LoRA burst → exam job
   (`generate.py --mode exam`) → promotion by contract. This closes the loop; everything after it compounds.
2. **Drill runner (markets).** Replay `regime_rule` / `method_card` claims on anonymized train-block weeks with
   the wall's own labels; holdout blocks untouched. Turns 13 graded weeks a season into hundreds, and makes
   creative market hypotheses checkable the day they are written.
3. **Exam bank of our own defects.** The `si` repo is the exam hall: every real bug fixed on `main` becomes a
   frozen case (`factory/exams/jh-defects/`), graded like the identity family. The student is examined on the
   system it will one day maintain — without ever training on it.
4. **Proved-lesson retrieval.** Embed proved evidence (existing MiniLM pipeline, CPU) so every new task is
   answered with the lessons that already survived their falsifiers. Retrieval, not training; no GPU.
5. **Forecast teacher on the wall.** `chronos-bolt-base` (CPU serverless) posts Monday forecasts as
   `guest-chronos-bolt` with an envelope; the student learns from a foundation model only through the grade.
6. **Squad adapters.** A squad that beats the champion twice on held-out cases earns its own LoRA line; the
   roster cap becomes per-squad (SCALE.md §8).
7. **Cost-per-pass on the scoreboard.** Season-tagged spend / passed traces — the number that must fall.

## Deliberately not owned

Proprietary model APIs (voice only, never grader or lesson source); the Brain constitution (private, engines
read enums through `consume_brain`); anything that would train on Khalid's notes, the `si` source, CC-BY-SA text
or ungraded reading; per-agent compute of any kind.

## Review fixes (2026-09-14, from ChatGPT's source review)

Substantiated and fixed in this lane: `model_source` survived `load_control` only by accident (now explicit);
the builder's `instruction/context/response` rows are accepted by the owned trainer through `template.json`
(both contracts, tested); the verifier's success is a one-time nonce delivered over a pipe and printed only after
the tests complete — candidate output cannot certify itself — plus a forbidden-token refusal (process control,
interpreter introspection, network); the trace-verify holdout check fails closed; ranks require ≥8 new graded
tasks since the last promotion; evidence is read newest-first and no verdict is applied when it is incomplete.
Substantiated, not fixed here (other lane's files, noted for them): APPS wrapper indentation, APPS `__run`
interface, diversity ceiling math, job double-counting in the budget, APPS source hash by id, persistent
curriculum cursor. Interface honesty items ("LEARNED THIS TURN" → read/practiced/passed/retained; the coding chat
does not call the model; report "success" vs RED) are queued for the desk.

## The engine's page and chat (2026-09-14)

The engine sits on `https://justhodl.ai/ai.html` (factory pane: chat box, chain of command, wall, exams). In the chat:
`where do you stand / what did you learn / status` returns a self-report assembled from objects only
(`factory_status.status_text`: base manifest, gearb control, champion, bursts + verifier summaries, verified row
count, training jobs, wall, ranks, health) — never an LLM, never a guess. `task: ...` (owner only) files an
immutable card under `factory/queue/tasks/`; with `tests: <asserts>` the task is gradable and rides the next
burst as a prompt the verifier grades; inside tasks map to lane actions (burst / verify / exam / status / spawn);
outside tasks without tests are answered from reading receipts and marked ungraded. APPS is a second graded
family again: stdio programs run per test case through a preamble (no re-indentation), source hashed by content,
and the dataset is loaded from the Hub's parquet conversion when the script-backed load is refused.

## Audit fixes (2026-09-14, 22-finding external audit at 7c60fd2)

Fixed in this lane: F01/F02/F21 — verifier v2: the supervisor is the judge (it parses the asserts, the candidate
process only evaluates left-hand expressions and returns reprs; expected values never enter the candidate process;
stdio tasks run one real subprocess per case with bytes and exit status; empty/zero-case suites refused; failures
kept as rows). **Residual, stated plainly:** a candidate that already knows the expected values (MBPP prompts show
them) and forges the runner's result line over the raw descriptor before the runner writes could still pass; the
static screen refuses the plain forms, an obfuscated form cannot be screened. Hidden tests (MBPP+/EvalPlus-style
extra cases, fresh private exams) are the real close and are next. F03 — spend admission on every provider attempt
regardless of caching; an unreadable meter admits nothing (llm_cost fails closed). F04 — the GLM/xAI dispatch bug
(NameError) fixed; xai routed through its own branch. F05 — only the owner's own turns leave the box. F06 — spawn
needs an explicit imperative, never a question or a negation. F07 — a requested adapter generation must be loaded or
the burst refuses. F08 — one ledger row per job identity. F09 — an unknown job state blocks launches. F10 — owner
results replay idempotently, conflicts raise. F14 — status reads factory/gearb/champion.json. F16 — states kept
separate; the inference claim is scoped to the owned model. F20 — the trainer re-hashes consumed base bytes. F22 —
an owner task trains only when the owner writes "trainable". Gear B accepts self-trace rows only from the v2
checker; rows judged by the retired checker stay on disk as history and are excluded from curation (re-verify
bursts 0 and 1 under v2 to regenerate them). Not yet: F11/F12/F17/F18/F19 (family ceiling math, dedup by quality,
one token contract with the chat template, new-information gate for generations, numeric generation sort/pagination).

## The owned model answers the chat (2026-09-14)

`factory_inference.py` + `ops 5551`: a SageMaker **asynchronous** endpoint (`jh-owned-coder-async`) serves the staged
Qwen2.5-Coder-7B-Instruct on the LMI/vLLM inference container, with application-autoscaling 0..1 — nothing runs while
idle, a queued request wakes it (cold start of a few minutes), it scales back to zero after 15 idle minutes; priced
per hour only while up (`ml.g5.xlarge`), under the same cost guard. In the chat, ordinary and coding questions are
submitted to the owned model; the answer lands in the log on the next poll, labelled `owned:<model>@<revision>`;
`status` stays behind an explicit request; when the control is absent or disabled the route says so and never
substitutes a canned answer. Context sent to the model = the owner's own turns and the model's own prior answers.

## Re-audit fixes (2026-09-14, second external audit at ebdaec6c)

Gate 2 (verification + data flow): verifier **v3** — the suite is transformed in order (`assert` → `__report(i, value)` in
place, so stateful tests keep their order), the child streams a typed value per case over a captured descriptor and exits
through a captured `_exit`; the supervisor decodes exact builtin types (no `repr`, no candidate `__eq__`), compares with
Python equality (`2.0 == 2` passes), and requires exactly one record per case in order plus a terminal marker — extra or
missing lines are protocol violations. The three re-audit bypasses fail in obfuscated forms (custom `__repr__`,
`__main__` mutation, frame walking); unsupported suites are refused rather than thinned (function and stdio). Hidden
tests are root-only inside the container (`chmod 700 /work`). **Residual (unchanged in nature):** a candidate that
already knows the expected values and forges the entire protocol before any runner code runs still passes; only hidden
tests close that. The writer now carries the verifier's checker/judge/cases unchanged, records every failed attempt,
replays owner results idempotently (conflicts raise), and consent is an explicit affirmation that negation overrides;
the curator accepts self-traces only from a v3+ supervisor judge with a bound receipt. `tests/factory/test_learning_seam.py`
proves the seam on a mixed batch. Gate 1 (chat): the object at `InputLocation` is the serving payload byte-for-byte,
metadata is a separate object, request state lives in `factory/inference/pending/` with an idempotency key and a
delivery marker, terminal states include failed/expired/malformed/truncated, and the desk polls the chat after a
visibility change. Reports: a `fail()` now makes the report status `failure`. Meter: an unreadable spend meter is a
refusal, never a cached zero. Spawn honours an explicit count. Still open: A15 (skillbook receipts), A16/A18/A20, and the
serving dollar cap (instance-hours) beyond the autoscaling bound.
