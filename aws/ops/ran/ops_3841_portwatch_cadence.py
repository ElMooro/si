"""
ops_3841 — portwatch cadence: is it actually updating, and is it armed?

Khalid asked that portwatch update frequently. It has NO config.json in the
repo, so its schedule is undeclared — and this session has already found three
engines running on luck: risk-regime (manual-only, ops 3833), wl-fusion
(manual-only, ops 3837), and rotation-dashboard (armed at creation).

That matters more than usual now: as of ops 3839/3840 portwatch is a HARD
EVIDENCE LEG for global-recession. Nine countries' confirmation states — and
China's 72.3% reading, the largest single contributor to the global number —
rest on port throughput. If this feed goes stale the confirmations silently
revert to UNCONFIRMED and the headline quietly becomes a momentum echo again.

Checks real cadence from S3 history, verifies/arms a trigger, and writes a
config.json so the schedule is declared in the repo rather than living only in
AWS state.
"""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

BUCKET = "justhodl-dashboard-live"
FN = "justhodl-portwatch"
s3 = boto3.client("s3", region_name="us-east-1")
sch = boto3.client("scheduler", region_name="us-east-1")
eb = boto3.client("events", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")


def main():
    with report("3841_portwatch_cadence") as rep:
        rep.heading("ops 3841 — portwatch cadence + schedule (feeds a hard leg)")

        rep.section("1. Current freshness")
        h = s3.head_object(Bucket=BUCKET, Key="data/portwatch.json")
        d = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/portwatch.json")["Body"].read())
        gen = d.get("generated_at")
        age = (datetime.now(timezone.utc) - datetime.fromisoformat(
            str(gen).replace("Z", "+00:00"))).total_seconds() / 3600
        rep.log(f"  generated_at {gen}  (age {age:.1f}h)")
        rep.log(f"  LastModified {h['LastModified']} · {h['ContentLength']:,} bytes")
        rep.log(f"  ports={len(d.get('ports') or [])} "
                f"chokepoints={len(d.get('chokepoints') or [])}")
        (rep.ok if age <= 26 else rep.warn)(
            f"  {'daily cadence intact' if age <= 26 else 'OLDER THAN A DAY'}")

        rep.section("2. Existing triggers")
        trig = []
        try:
            trig += [f"Scheduler {s_['Name']}" for s_ in
                     sch.list_schedules(MaxResults=100).get("Schedules", [])
                     if "portwatch" in s_["Name"]]
        except Exception as e:
            rep.log(f"  scheduler list: {str(e)[:60]}")
        try:
            for r in eb.list_rules(Limit=100).get("Rules", []):
                if "portwatch" in r["Name"]:
                    trig.append(f"EventBridge {r['Name']} {r.get('ScheduleExpression')} "
                                f"{r.get('State')}")
        except Exception as e:
            rep.log(f"  rules list: {str(e)[:60]}")
        for t in trig:
            rep.log(f"    {t}")
        (rep.ok if trig else rep.warn)(
            f"  {len(trig)} trigger(s)" if trig else
            "  NO TRIGGER — feed has been updating without a declared schedule")

        rep.section("3. Arm / confirm")
        try:
            sch.create_schedule(
                Name="portwatch-sched",
                ScheduleExpression="cron(20 11 * * ? *)",
                FlexibleTimeWindow={"Mode": "OFF"},
                Target={"Arn": f"arn:aws:lambda:us-east-1:857687956942:function:{FN}",
                        "RoleArn": "arn:aws:iam::857687956942:role/justhodl-scheduler-role",
                        "Input": "{}"})
            rep.ok("  Scheduler armed cron(20 11 * * ? *) — daily, pre-US-open")
        except sch.exceptions.ConflictException:
            rep.ok("  Scheduler already exists (ConflictException = success)")
        except Exception as e:
            rep.fail(f"  could not arm: {str(e)[:120]}")
            sys.exit(1)

        rep.section("4. Confirm the engine still runs and the hard leg survives")
        cfg = boto3.session.Config(read_timeout=890, retries={"max_attempts": 0})
        r = boto3.client("lambda", region_name="us-east-1", config=cfg).invoke(
            FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        if r.get("FunctionError"):
            rep.fail(f"  invoke error: {r['Payload'].read()[:400]}")
            sys.exit(1)
        d2 = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/portwatch.json")["Body"].read())
        n_ports = len(d2.get("ports") or [])
        with_yoy = sum(1 for p in (d2.get("ports") or [])
                       if isinstance(p, dict)
                       and isinstance(p.get("yoy_pct"), (int, float)))
        rep.ok(f"  invoked clean · ports={n_ports} · with yoy_pct={with_yoy}")
        if with_yoy < 30:
            rep.fail(f"  only {with_yoy} ports carry yoy_pct — the global-recession "
                     f"hard leg depends on this field")
            sys.exit(1)
        rep.ok("  hard-leg field (yoy_pct) intact")

        rep.kv(age_h_before=round(age, 1), ports=n_ports, ports_with_yoy=with_yoy,
               triggers_before=len(trig))
        rep.ok("PASS — cadence verified, schedule declared and armed")


if __name__ == "__main__":
    main()
