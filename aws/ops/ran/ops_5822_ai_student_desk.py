#!/usr/bin/env python3
"""ops 5822 — fill empty student_wall and publish a 4KB public desk for ai.html."""
import json
import sys
from datetime import datetime, timezone

import boto3

BUCKET = "justhodl-dashboard-live"


def main():
    try:
        s3 = boto3.client("s3", region_name="us-east-1")
        ai = json.loads(s3.get_object(Bucket=BUCKET, Key="data/ai.json")["Body"].read())
        sb = ai.get("scoreboard") or {}
        mr = ai.get("market_read") or {}
        ce = ai.get("coding_exam") or {}
        me = ((ai.get("market_exam") or {}).get("holdout") or {})
        ms = me.get("model_scores") or {}
        base = (me.get("baselines") or {}).get("prior") or {}
        beats_prior = None
        if ms.get("score") is not None and base.get("score") is not None:
            beats_prior = float(ms["score"]) > float(base["score"])
        wall = {
            "engine": "justhodl-ai-student-wall",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "source": "composed from data/ai.json when wall file missing",
            "voice": sb.get("voice") or mr.get("voice"),
            "decision_status": mr.get("decision_status") or "ADVISORY_ONLY",
            "understanding_score": sb.get("understanding_score"),
            "coding_exam": {"score": ce.get("base_score"), "passed": ce.get("base_passed"), "verdict": ce.get("verdict")},
            "market_exam_holdout": {
                "score": ms.get("score"),
                "direction_acc": ms.get("direction_acc"),
                "prior_score": base.get("score"),
                "beats_prior": beats_prior,
                "n_drills": me.get("n_drills"),
            },
            "calls": {"made": sb.get("calls_made"), "graded": sb.get("calls_graded"), "hit_rate_by_window": sb.get("hit_rate_by_window")},
            "stances": mr.get("stances"),
            "blockers": mr.get("n_blockers"),
            "pipeline": (ai.get("pipeline") or {}).get("status"),
            "can_do": [
                "Study Brain notes and sit coding/market/reasoning exams",
                "Publish an advisory market read with stances",
                "Ledger calls only while ADVISORY_ONLY — they do not size risk",
            ],
            "cannot_do_yet": [
                "Beat the prior baseline on the frozen market holdout",
                "Grade the 11 open calls — hit_rate is null until outcomes exist",
                "Promote a champion above base Qwen weights",
                "Write production code to main without a human apply-lane",
            ],
            "next_lesson": (
                "Market holdout score is at or below the prior baseline. "
                "Do not promote weights. Grade ledgered calls. Keep voice advisory."
            ),
        }
        body = json.dumps(wall).encode()
        for key in ("data/ai/wall/student-latest.json", "data/ai-student-desk.json"):
            s3.put_object(Bucket=BUCKET, Key=key, Body=body, ContentType="application/json",
                          CacheControl="no-cache")
            print("wrote", key, len(body))
        print(json.dumps({k: wall[k] for k in ("decision_status", "understanding_score", "market_exam_holdout", "calls")}))
        return 0
    except Exception as e:
        print("FAIL", type(e).__name__, e)
        sys.exit(1)


if __name__ == "__main__":
    sys.exit(main() or 0)
