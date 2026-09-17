"""ops 5587 -- exam result probe (Claude, 2026-09-17). READ-ONLY. Direct lane only (STAGED never runs on the serial lane).

Context: factory-exam.yml's scheduled runs had failed on every tick since 2026-09-17 ~11:00 UTC -- the resolve step
appended a free-text line to $GITHUB_OUTPUT and the runner rejected it ("Invalid format"). Commit 5f8d4c4 fixed it;
the verifying dispatch (run 35229787292) then pulled, graded and wrote an evaluation, i.e. a completed exam job HAD
been waiting. This probe prints what that evaluation says without inventing a number:

  LEARNING (coding) = candidate exam score - base exam score (base.json, 82.3% = 135/164)

and the surrounding state: every result under factory/exams/code/results/, the Gear B decisions/champion, the
training/exam jobs' SageMaker status + billable seconds, and what data/ai.json currently publishes. No launches,
no writes, no control edits. RED only when the results prefix cannot be read at all.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

REGION, PRI, PUB = "us-east-1", "justhodl-ai-857687956942", "justhodl-dashboard-live"
RESULTS = "factory/exams/code/results/"
BASE_KEY = RESULTS + "base.json"
NOW = datetime.now(timezone.utc)


def _get_json(s3, bucket, key):
    try:
        return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    except Exception as e:  # noqa: BLE001
        return {"_error": str(e)[:160]}


def _list(s3, bucket, prefix):
    keys, token = [], None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
        if token:
            kw["ContinuationToken"] = token
        page = s3.list_objects_v2(**kw)
        keys.extend((o["Key"], o["LastModified"]) for o in page.get("Contents", []))
        token = page.get("NextContinuationToken")
        if not token:
            return keys


def _pct(x):
    try:
        return "%.1f%%" % (100.0 * float(x))
    except Exception:  # noqa: BLE001
        return "n/a"


def main():
    cfg = Config(read_timeout=60, retries={"max_attempts": 3})
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    sm = boto3.client("sagemaker", region_name=REGION, config=cfg)
    with report("5587_exam_result_probe") as r:
        r.heading("ops 5587 -- exam result probe (read-only)")

        r.section("1. Exam evaluations on disk (factory/exams/code/results/)")
        try:
            keys = sorted(_list(s3, PRI, RESULTS), key=lambda kv: kv[1])
        except Exception as e:  # noqa: BLE001
            r.fail("cannot list %s: %s" % (RESULTS, e))
            sys.exit(1)
        base = _get_json(s3, PRI, BASE_KEY)
        base_score = base.get("score") if isinstance(base, dict) else None
        r.kv(evaluations=len(keys), base_score=_pct(base_score), base_passed="%s/%s" % (base.get("passed"), base.get("n")),
             base_evaluation=base.get("evaluation_id"))
        newest = None
        for key, lm in keys:
            d = _get_json(s3, PRI, key)
            if "_error" in d:
                r.warn("%s: %s" % (key, d["_error"]))
                continue
            r.log("%s  gen=%s score=%s passed=%s/%s critical=%s missing=%s burst=%s at=%s (LastModified %s)" % (
                key.rsplit("/", 1)[-1], d.get("generation"), _pct(d.get("score")), d.get("passed"), d.get("n"),
                d.get("critical_failures"), d.get("missing_completions"), d.get("burst"), d.get("at"),
                lm.strftime("%m-%d %H:%M UTC")))
            if key != BASE_KEY and str(d.get("generation") or "") not in ("gen-0", "base"):
                newest = (key, lm, d)

        r.section("2. LEARNING (coding) from the newest candidate evaluation")
        if not newest:
            r.warn("no candidate (gen-N) evaluation exists yet -- only the base; the grader's verifying run must have "
                   "graded a gen-0/base re-run or a candidate whose record it could not attribute")
        else:
            key, lm, d = newest
            cand = d.get("score")
            delta = (float(cand) - float(base_score)) if (cand is not None and base_score is not None) else None
            r.kv(candidate_generation=d.get("generation"), candidate_score=_pct(cand), candidate_passed="%s/%s" % (d.get("passed"), d.get("n")),
                 critical_failures=d.get("critical_failures"), missing_completions=d.get("missing_completions"),
                 base_score=_pct(base_score), LEARNING=("%+.1f pts" % (100.0 * delta)) if delta is not None else "n/a",
                 exam_job=d.get("burst"), graded_at=d.get("at"))
            if delta is None:
                r.warn("LEARNING not computable (candidate or base score missing)")
            elif d.get("critical_failures"):
                r.warn("evaluation carries critical_failures=%s -- treat the score as provisional until the grader verdict is clean" % d.get("critical_failures"))
            else:
                (r.ok if delta > 0 else r.warn)("LEARNING = %s (candidate %s - base %s)" % (("%+.1f pts" % (100.0 * delta)), _pct(cand), _pct(base_score)))

        r.section("3. Gear B decisions / champion")
        for key, lm in sorted(_list(s3, PRI, "factory/gearb/decisions/"), key=lambda kv: kv[1]):
            d = _get_json(s3, PRI, key)
            r.log("%s  %s  (LastModified %s)" % (key.rsplit("/", 1)[-1], json.dumps({k: d.get(k) for k in ("generation", "decision", "verdict", "promote", "reason", "candidate_score", "base_score", "learning", "decided_at") if k in d}, default=str)[:400], lm.strftime("%m-%d %H:%M UTC")))
        champ = _get_json(s3, PRI, "factory/gearb/champion.json")
        r.log("champion.json: %s" % json.dumps({k: champ.get(k) for k in ("generation", "release_status", "score", "learning", "decided_at", "_error") if k in champ}, default=str)[:400])

        r.section("4. Training / exam jobs (SageMaker) since 2026-09-16")
        try:
            jobs = sm.list_training_jobs(NameContains="jh-", CreationTimeAfter=datetime(2026, 9, 16, tzinfo=timezone.utc), MaxResults=50, SortBy="CreationTime", SortOrder="Descending").get("TrainingJobSummaries", [])
        except Exception as e:  # noqa: BLE001
            jobs = []
            r.warn("list_training_jobs failed: %s" % str(e)[:160])
        for j in jobs:
            name, st = j["TrainingJobName"], j["TrainingJobStatus"]
            billable, secondary, reason = "?", "", ""
            try:
                dj = sm.describe_training_job(TrainingJobName=name)
                billable = dj.get("BillableTimeInSeconds", 0)
                secondary = dj.get("SecondaryStatus", "")
                reason = (dj.get("FailureReason") or "")[:120]
            except Exception as e:  # noqa: BLE001
                reason = "describe failed: %s" % str(e)[:80]
            r.log("%s  %s/%s  billable=%ss  created=%s  %s" % (name, st, secondary, billable, j["CreationTime"].strftime("%m-%d %H:%M"), reason))

        r.section("5. What data/ai.json publishes right now")
        ai = _get_json(s3, PUB, "data/ai.json")
        gb = ai.get("gear_b") if isinstance(ai, dict) else None
        sb = ai.get("scoreboard") if isinstance(ai, dict) else None
        r.kv(ai_generated_at=ai.get("generated_at") if isinstance(ai, dict) else ai, gear_b=json.dumps(gb, default=str)[:600] if gb else gb)
        if isinstance(sb, dict):
            r.kv(scoreboard=json.dumps({k: sb.get(k) for k in ("learning", "learning_coding", "coding", "voice", "read_path", "base_score", "candidate_score", "generation") if k in sb}, default=str)[:500])
        r.ok("probe complete (no writes, no launches)")


if __name__ == "__main__":
    main()
