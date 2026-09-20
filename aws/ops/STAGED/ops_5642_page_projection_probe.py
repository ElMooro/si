"""ops 5642 -- what ai.html shows now (Claude, 2026-09-17). READ-ONLY: data/ai.json market_exam + scoreboard + market_read."""
import json, sys
from pathlib import Path
import boto3
HERE = Path(__file__).resolve(); sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402


def main():
    s3 = boto3.client("s3", region_name="us-east-1")
    with report("5642_page_projection_probe") as r:
        r.heading("ops 5642 -- data/ai.json as the page reads it")
        ai = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/ai.json")["Body"].read())
        ex = ai.get("market_exam") or {}
        for split in ("holdout", "train"):
            e = ex.get(split) or {}
            r.log("%s: run=%s n=%s model=%s baselines=%s" % (split, e.get("run_id"), (e.get("model_scores") or {}).get("n"), json.dumps(e.get("model_scores")), json.dumps(e.get("baselines"))))
        rx = ai.get("reasoning_exam") or {}
        r.log("reasoning: run=%s families=%s" % (rx.get("run_id"), json.dumps(rx.get("families"))))
        ce = ai.get("coding_exam") or {}
        r.log("coding: base=%s learning_pts=%s verdict=%s" % (ce.get("base_score"), ce.get("learning_pts"), str(ce.get("verdict"))[:160]))
        gb = ai.get("gear_b") or {}
        r.log("gear_b: " + json.dumps({k: gb.get(k) for k in ("status", "dataset", "latest_job", "generation", "jobs_today", "champion", "last_decision") if k in gb}, default=str)[:700])
        ce2 = ai.get("coding_exam") or {}
        r.log("coding candidates: " + json.dumps(ce2.get("candidates"), default=str)[:500])
        sb, mr = ai.get("scoreboard") or {}, ai.get("market_read") or {}
        r.kv(generated_at=ai.get("generated_at"), voice=str(sb.get("voice"))[:120], read_path=sb.get("read_path"), calls_made=sb.get("calls_made"), calls_this_read=sb.get("calls_this_read"),
             stances=json.dumps(mr.get("stances")), decision_status=mr.get("decision_status"), n_blockers=mr.get("n_blockers"), calls_ledgered_while_advisory=mr.get("calls_ledgered_while_advisory"))
        (r.ok if ex else r.warn)("market_exam %s on the page projection" % ("present" if ex else "ABSENT -- inventory tick has not run since the exam"))


if __name__ == "__main__":
    main()
