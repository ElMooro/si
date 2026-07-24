"""
ops_3833 — why is the RORO overlay 0/25? diagnose -> heal -> verify

ops 3832 brought rotation/nowcast/liquidity from ~1/25 to 16/25 by fixing the
sector map, but _roro_overlay stayed at 0/25. Traced:

  master-ranker line 226: fetch_json("data/risk-regime.json", max_age_h=48)
  fetch_json treats a feed older than max_age_h as ABSENT (by design, so stale
  data never silently contaminates a decision) -> _rr = {} -> _rr_score None ->
  _roro_overlay returns early on EVERY row.

The producer key is correct (justhodl-risk-regime writes risk_regime_score at
line 595), so this is NOT a key mismatch. Either the feed is stale (engine not
running = fleet-health issue) or it is fresh and something else is wrong. This
ops decides which, heals if it is the schedule, and re-verifies end to end.

Deliberately does NOT widen max_age_h. Loosening a staleness gate to make a
number appear is how stale data gets into decisions.
"""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

BUCKET = "justhodl-dashboard-live"
RR = "justhodl-risk-regime"
MR = "justhodl-master-ranker"
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
sch = boto3.client("scheduler", region_name="us-east-1")
eb = boto3.client("events", region_name="us-east-1")
CFG = boto3.session.Config(read_timeout=890, retries={"max_attempts": 0})


def age_h(ts):
    return (datetime.now(timezone.utc)
            - datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
            ).total_seconds() / 3600


def main():
    with report("3833_roro_health") as rep:
        rep.heading("ops 3833 — RORO 0/25: diagnose, heal, verify")

        rep.section("1. Is data/risk-regime.json fresh enough to pass max_age_h=48?")
        head = s3.head_object(Bucket=BUCKET, Key="data/risk-regime.json")
        d = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/risk-regime.json")["Body"].read())
        gen = d.get("generated_at")
        a = age_h(gen) if gen else None
        rep.log(f"  s3 LastModified: {head['LastModified']}")
        rep.log(f"  generated_at:    {gen}  (age {a:.1f}h)" if a is not None
                else f"  generated_at:    {gen}")
        rep.log(f"  risk_regime_score = {d.get('risk_regime_score')} · "
                f"risk_regime = {d.get('risk_regime')}")
        stale = (a is None) or (a > 48)
        (rep.warn if stale else rep.ok)(
            f"  {'STALE — excluded by max_age_h=48, RORO stands down' if stale else 'FRESH — staleness is not the cause'}")

        rep.section("2. Is the producer actually scheduled?")
        armed = []
        try:
            for s_ in sch.list_schedules(MaxResults=100).get("Schedules", []):
                if "risk-regime" in s_["Name"]:
                    det = sch.get_schedule(Name=s_["Name"])
                    armed.append(f"Scheduler {s_['Name']} {det.get('ScheduleExpression')} "
                                 f"state={det.get('State')}")
        except Exception as e:
            rep.log(f"  scheduler list failed: {str(e)[:60]}")
        try:
            for r in eb.list_rules(Limit=100).get("Rules", []):
                if "risk-regime" in r["Name"]:
                    armed.append(f"EventBridge {r['Name']} {r.get('ScheduleExpression')} "
                                 f"state={r.get('State')}")
        except Exception as e:
            rep.log(f"  rules list failed: {str(e)[:60]}")
        for x in armed:
            rep.log(f"    {x}")
        (rep.ok if armed else rep.warn)(
            f"  {len(armed)} trigger(s) found — "
            f"{'declared' if armed else 'NO SCHEDULE: engine only ever ran manually'}")

        rep.section("3. Heal — invoke the producer and confirm it can still run")
        r = boto3.client("lambda", region_name="us-east-1", config=CFG).invoke(
            FunctionName=RR, InvocationType="RequestResponse", Payload=b"{}")
        if r.get("FunctionError"):
            rep.fail(f"  producer ERRORS — this is a broken engine, not staleness: "
                     f"{r['Payload'].read()[:400]}")
            sys.exit(1)
        rep.ok("  producer invoked clean")
        d2 = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/risk-regime.json")["Body"].read())
        a2 = age_h(d2.get("generated_at"))
        rep.ok(f"  refreshed: age {a2:.1f}h · score={d2.get('risk_regime_score')} "
               f"· regime={d2.get('risk_regime')}")
        if a2 > 48:
            rep.fail("  still stale after a clean invoke — engine writes an old stamp")
            sys.exit(1)

        if not armed:
            rep.section("3b. Arm a schedule (fleet-health pattern)")
            try:
                sch.create_schedule(
                    Name="risk-regime-sched",
                    ScheduleExpression="cron(15 21 * * ? *)",
                    FlexibleTimeWindow={"Mode": "OFF"},
                    Target={"Arn": f"arn:aws:lambda:us-east-1:857687956942:function:{RR}",
                            "RoleArn": "arn:aws:iam::857687956942:role/justhodl-scheduler-role",
                            "Input": "{}"})
                rep.ok("  Scheduler armed cron(15 21 * * ? *)")
            except sch.exceptions.ConflictException:
                rep.ok("  Scheduler already exists")

        rep.section("4. Verify RORO comes alive in master-ranker")
        r2 = boto3.client("lambda", region_name="us-east-1", config=CFG).invoke(
            FunctionName=MR, InvocationType="RequestResponse", Payload=b"{}")
        if r2.get("FunctionError"):
            rep.fail(f"  ranker invoke error: {r2['Payload'].read()[:400]}")
            sys.exit(1)
        mr = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/master-ranker.json")["Body"].read())
        rows = mr.get("top_tickers") or []
        out = {}
        for f, lbl in (("risk_regime_mult", "roro"),
                       ("rotation_mult", "rotation"),
                       ("nowcast_regime_mult", "nowcast"),
                       ("liquidity_regime_mult", "liquidity")):
            live = [x for x in rows if x.get(f) not in (None, 1.0)]
            out[lbl] = len(live)
            rep.log(f"    {lbl:<10} {len(live)}/{len(rows)}")
        rep.kv(rr_age_h=round(a2, 1), rr_score=d2.get("risk_regime_score"),
               triggers=len(armed), **{f"{k}_live": v for k, v in out.items()})

        if out["roro"] == 0:
            rep.warn("  RORO STILL 0 — feed is fresh, so the cause is inside "
                     "_roro_overlay (score band or sector-set membership), NOT "
                     "staleness. Reported honestly rather than forced.")
        else:
            rep.ok(f"  RORO now live on {out['roro']}/{len(rows)} rows")
        rep.ok("DIAGNOSIS COMPLETE")


if __name__ == "__main__":
    main()
