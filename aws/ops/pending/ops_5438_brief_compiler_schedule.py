"""ops_5438 -- prove compiler, patch schedule-manifest, invoke reconciler.
Does NOT call events.put_rule.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import boto3

HERE = Path(__file__).resolve()
REPO = HERE.parents[3]
sys.path.insert(0, str(REPO / "aws" / "shared"))
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402
from brief_compiler import MODES, run  # noqa: E402

B = "justhodl-dashboard-live"
MANIFEST = "config/schedule-manifest.json"
FN = "justhodl-brief-compiler"
ARN = "arn:aws:lambda:us-east-1:857687956942:function:" + FN

RULES = [
    ("brief-compiler-plumbing-6h", "cron(20 */6 * * ? *)", "plumbing"),
    ("brief-compiler-market-tape-6h", "cron(25 */6 * * ? *)", "market_tape"),
    ("brief-compiler-official-stats-daily", "cron(40 1 * * ? *)", "official_stats"),
    ("brief-compiler-positioning-daily", "cron(50 1 * * ? *)", "positioning"),
    ("brief-compiler-event-6h", "cron(35 */6 * * ? *)", "event"),
    ("brief-compiler-verdict-3h", "cron(10 */3 * * ? *)", "verdict"),
]


def main():
    with report("ops_5438_brief_compiler_schedule") as R:
        R.heading("ops 5438 brief compiler schedule")
        s3 = boto3.client("s3", region_name="us-east-1")
        lam = boto3.client("lambda", region_name="us-east-1")

        for mode in MODES:
            doc = run(s3, mode, source="ops_5438")
            if mode == "verdict":
                R.ok("verdict coverage=%s missing=%s" % (doc.get("coverage"), doc.get("missing_families")))
            else:
                R.ok("%s status=%s %s" % (mode, doc.get("status"), (doc.get("why") or "")[:120]))

        obj = s3.get_object(Bucket=B, Key=MANIFEST)
        man = json.loads(obj["Body"].read())
        existing = {r.get("name"): i for i, r in enumerate(man.get("rules") or [])}
        added = []
        for name, expr, mode in RULES:
            rule = {
                "kind": "events",
                "name": name,
                "expr": expr,
                "state": "ENABLED",
                "targets": [{
                    "id": "1",
                    "arn": ARN,
                    "input": json.dumps({"mode": mode}),
                    "path": None,
                }],
            }
            if name in existing:
                man["rules"][existing[name]] = rule
            else:
                man["rules"].append(rule)
                added.append(name)
        man["generated_at"] = __import__("datetime").datetime.now(
            __import__("datetime").timezone.utc).isoformat()
        man["source"] = "ops_5438 brief-compiler ticks"
        s3.put_object(
            Bucket=B, Key=MANIFEST,
            Body=json.dumps(man, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        R.ok("manifest rules=%d added=%s" % (len(man["rules"]), added))

        try:
            inv = lam.invoke(FunctionName="justhodl-schedule-reconciler",
                             InvocationType="RequestResponse")
            payload = json.loads(inv["Payload"].read() or b"{}")
            R.ok("reconciler drift=%s" % payload.get("drift_count", payload))
        except Exception as e:
            R.warn("reconciler invoke: %s" % str(e)[:180])

        try:
            inv = lam.invoke(
                FunctionName=FN,
                InvocationType="RequestResponse",
                Payload=json.dumps({"mode": "verdict"}).encode("utf-8"),
            )
            payload = json.loads(inv["Payload"].read() or b"{}")
            R.ok("compiler verdict invoke: %s" % json.dumps(payload)[:300])
        except Exception as e:
            R.warn("compiler not live yet (deploy-lambdas must create %s): %s" % (FN, str(e)[:160]))


if __name__ == "__main__":
    main()
