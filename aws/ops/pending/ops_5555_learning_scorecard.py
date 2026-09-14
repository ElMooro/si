"""ops 5555 -- learning scorecard (read-only): is it learning, and how much? (Claude, 2026-09-14)

Definition held to: LEARNING = a measured gain on tasks it has never seen (the frozen holdout exam), produced from
evidence it generated itself and that an independent judge accepted. Everything else is supply. The scorecard reports
each stage from objects, then two percentages that cannot be confused:
  supply_readiness_pct  = eligible training rows (through the REAL curator: receipts resolved) / Gear B floor (1500)
  learning_pct          = exam delta of the promoted champion vs the base on the frozen holdout, or 0 while no adapter
                          has been trained and examined. Rows, bursts and receipts never move this number.
Launches nothing.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import boto3

HERE = Path(__file__).resolve()
REPO = HERE.parents[3]
sys.path.insert(0, str(REPO / "aws" / "lambdas" / "justhodl-ai" / "source"))
sys.path.insert(0, str(REPO / "aws" / "shared"))
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

REGION, PRI, PUB = "us-east-1", "justhodl-ai-857687956942", "justhodl-dashboard-live"
FLOOR = 1500


def _get_json(s3, bucket, key):
    try:
        return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    except Exception:  # noqa: BLE001
        return None


def _list(s3, bucket, prefix, cap=20000):
    keys, token = [], None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
        if token:
            kw["ContinuationToken"] = token
        r = s3.list_objects_v2(**kw)
        keys += [o["Key"] for o in r.get("Contents", [])]
        token = r.get("NextContinuationToken")
        if not r.get("IsTruncated") or len(keys) >= cap:
            return keys, bool(r.get("IsTruncated"))


def main() -> int:
    s3 = boto3.client("s3", region_name=REGION)
    sm = boto3.client("sagemaker", region_name=REGION)
    import gear_b as gb
    with report("ops_5555_learning_scorecard") as R:
        R.heading("ops 5555 -- learning scorecard: supply (rows/receipts) vs learning (exam delta), from objects only")
        # 1. supply: verified rows by checker generation, eligible through the real curator (receipt resolution)
        keys, truncated = _list(s3, PRI, gb.CURRICULUM_VERIFIED_PREFIX)
        by_checker, eligible, tasks_eligible, families = {}, 0, set(), {}
        for k in keys:
            doc = _get_json(s3, PRI, k)
            if not isinstance(doc, dict):
                continue
            c = str(doc.get("checker") or "none")
            by_checker[c] = by_checker.get(c, 0) + 1
            row = gb._row_from_verified(k, doc, s3, PRI)
            if row:
                eligible += 1
                tasks_eligible.add(doc.get("task_id"))
                fam = row.get("family") or "?"
                families[fam] = families.get(fam, 0) + 1
        R.ok("verified rows on disk: %d (listing truncated=%s); by checker: %s" % (len(keys), truncated, json.dumps(by_checker)))
        R.ok("ELIGIBLE through the curator (receipt resolved, hashes bound, supervisor judge, cases>=1): %d rows over %d distinct tasks; families %s" % (
            eligible, len(tasks_eligible), json.dumps(families)))
        supply_pct = round(min(100.0, 100.0 * eligible / FLOOR), 1)
        # 2. bursts + attempts
        bursts = [k for k in _list(s3, PRI, "factory/bursts/jobs/")[0]]
        attempts = sum(1 for b in bursts for _ in [0] for k in _list(s3, PRI, "factory/bursts/%s/attempts/" % b.split("/")[-1].replace(".json", ""), cap=5000)[0])
        R.ok("bursts launched: %d; failed-attempt records on disk: %d" % (len(bursts), attempts))
        # 3. training + champion
        jobs = gb._job_records(s3, PRI)
        champ = _get_json(s3, PRI, gb.CHAMPION_KEY)
        R.ok("Gear B training jobs (deduplicated): %d; champion: %s" % (len(jobs), json.dumps({k: (champ or {}).get(k) for k in ("generation", "adapter", "promoted_at")}) if champ else "none (base model)"))
        # 4. exam: baseline + candidates
        exam_prompts = _get_json(s3, PRI, gb.EXAM_PROMPTS_KEY)
        exam_results = _list(s3, PRI, "factory/exams/code/results/", cap=200)[0]
        base_result = candidate_result = None
        for k in exam_results:
            d = _get_json(s3, PRI, k) or {}
            if d.get("generation") in (0, "base", "gen-0"):
                base_result = d
            elif d.get("promoted"):
                candidate_result = d
        R.ok("frozen holdout exam: prompts %s; exam result objects: %d; base exam: %s; promoted candidate exam: %s" % (
            "present" if exam_prompts else "MISSING", len(exam_results),
            json.dumps({k: base_result.get(k) for k in ("pass_rate", "tasks", "at")}) if base_result else "not run yet",
            json.dumps({k: candidate_result.get(k) for k in ("pass_rate", "tasks", "at")}) if candidate_result else "none"))
        learning_pct = 0.0
        if base_result and candidate_result and base_result.get("pass_rate") is not None:
            learning_pct = round(100.0 * (float(candidate_result["pass_rate"]) - float(base_result["pass_rate"])), 1)
        # 5. serving
        try:
            ep = sm.describe_endpoint(EndpointName="jh-owned-coder-async")
            serving = ep.get("EndpointStatus")
        except Exception:  # noqa: BLE001
            serving = "absent"
        ctrl = _get_json(s3, PRI, "factory/control/inference.json") or {}
        R.ok("owned serving endpoint: %s; chat control enabled=%s" % (serving, ctrl.get("enabled")))
        stages = [("weights staged", bool(_get_json(s3, PRI, "factory/models/base/qwen2-5-coder-7b-instruct/manifest.json"))),
                  ("bursts produced candidates", len(bursts) > 0), ("independent judge admitted rows", eligible > 0),
                  ("supply floor reached", eligible >= FLOOR), ("base exam measured", base_result is not None),
                  ("adapter trained", len(jobs) > 0), ("candidate examined and promoted", candidate_result is not None)]
        done = sum(1 for _, ok in stages if ok)
        R.ok("pipeline stages: %s -> %d/%d" % (", ".join("%s=%s" % (n, "yes" if ok else "no") for n, ok in stages), done, len(stages)))
        R.kv(supply_readiness_pct=supply_pct, learning_pct=learning_pct, eligible_rows=eligible, floor=FLOOR, stages_done="%d/%d" % (done, len(stages)))
        R.ok("SUPPLY READINESS %.1f%% (%d/%d eligible rows) | LEARNING %.1f%% (exam delta; 0 until an adapter is trained and examined)" % (supply_pct, eligible, FLOOR, learning_pct))
        R.ok("GREEN -- scorecard recorded")
        return 0


if __name__ == "__main__":
    if main() != 0:
        sys.exit(1)
    sys.exit(0)
