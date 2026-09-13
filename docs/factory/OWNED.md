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
