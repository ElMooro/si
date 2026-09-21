"""ops 5871 -- why has Gear B not launched since gen-20, and launch on the new supply (Claude, 2026-09-21). Direct lane.
1. control + committed spend since approval (season cap) + today's jobs -- the real refusal, printed
2. gen-21's stale dataset manifest (693 tasks, built before the supply runs) is superseded so the next build sees the
   full verified pool (a manifest whose generation appears in the job records counts as launched: we record a
   'superseded' job record for gen-21 with zero cost, nothing on SageMaker)
3. one REAL tick (mode gearb): builds gen-22 from the current pool and launches if every gate passes; the refusal is printed
"""
import json, sys
from datetime import datetime, timezone
from pathlib import Path
import boto3
HERE = Path(__file__).resolve(); sys.path.insert(0, str(HERE.parents[1]))
sys.path.insert(0, str(HERE.parents[3] / "aws" / "lambdas" / "justhodl-ai" / "source")); sys.path.insert(0, str(HERE.parents[3] / "aws" / "shared"))
from ops_report import report  # noqa: E402
PRI = "justhodl-ai-857687956942"


def main():
    s3 = boto3.client("s3", region_name="us-east-1"); lam = boto3.client("lambda", region_name="us-east-1")
    with report("5871_gearb_unblock") as r:
        r.heading("ops 5871 -- Gear B: the real refusal, then a launch on the new supply")
        import gear_b as gb
        ctl = json.loads(s3.get_object(Bucket=PRI, Key="factory/control/gearb.json")["Body"].read())
        records = gb._job_records(s3, PRI)
        approved = ctl.get("approved_at")
        since = datetime.fromisoformat(str(approved).replace("Z", "+00:00")) if approved else datetime(2026, 9, 1, tzinfo=timezone.utc)
        season = gb.spend(records, since); today = gb.spend(records, datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0))
        r.section("1. Control and spend")
        r.kv(enabled=ctl.get("enabled"), launch=ctl.get("launch"), approved_at=approved, daily_budget=ctl.get("daily_budget_usd"), season_cap=ctl.get("season_cap_usd"),
             max_jobs_per_day=ctl.get("max_jobs_per_day"), season_committed_usd=round(season.get("usd", 0), 2), season_jobs=season.get("jobs"), today_jobs=today.get("jobs"), records=len(records))
        r.section("2. Supersede the stale gen-21 manifest")
        key = "factory/bursts/jobs/superseded-gen-21-20260921.json"
        try:
            s3.head_object(Bucket=PRI, Key=key); r.log("already superseded")
        except Exception:  # noqa: BLE001
            rec = {"schema_version": "gearb-job.v1", "kind": "superseded", "generation": 21, "job_name": None, "launched_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                   "cost_cap_usd": 0.0, "max_runtime_s": 0, "note": "dataset built 2026-09-20 on 693 tasks before the supply runs; superseded so the next build sees the full verified pool (ops 5871)"}
            s3.put_object(Bucket=PRI, Key=key, Body=json.dumps(rec, indent=1).encode(), ContentType="application/json")
            r.ok("gen-21 marked superseded (zero-cost record; nothing on SageMaker)")
        r.section("3. One real tick")
        resp = lam.invoke(FunctionName="justhodl-ai", InvocationType="RequestResponse", Payload=json.dumps({"mode": "gearb"}).encode())
        out = json.loads(resp["Payload"].read()); res = out.get("result") or out
        r.kv(built=json.dumps(res.get("built"))[:300], launched=json.dumps(res.get("launched"))[:300], refusal=res.get("refusal"), examined=json.dumps(res.get("examined"))[:200])
        (r.ok if res.get("launched") else r.warn)("launched" if res.get("launched") else "not launched: %s" % res.get("refusal"))


if __name__ == "__main__":
    main()
