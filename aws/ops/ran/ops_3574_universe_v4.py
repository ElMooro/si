"""ops 3574 — universe-builder v4 EXHAUSTIVE: bisection pagination (zero
truncation), ADRs included, $1M floor, exchange/country fields. Gates: settle
+ zip markers, schedule ensured (Scheduler daily — classic rule cap saturated),
sync run, feed jump (2,400 → 4,200+), consumer shape intact."""
import io, json, sys, time, urllib.request, zipfile
from datetime import datetime, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report

LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=420, retries={"max_attempts": 0}))
S3C = boto3.client("s3", "us-east-1")
SCH = boto3.client("scheduler", "us-east-1")
EVT = boto3.client("events", "us-east-1")
UA = {"User-Agent": "Mozilla/5.0 (ops-3574)"}
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-universe-builder"

with report("3574_universe_v4") as rep:
    rep.heading("ops 3574 — exhaustive all-US-listed universe (v4)")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:400]}
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:360]
        print(line); rep.log(line)
        if not ok:
            fails.append(n)

    # G1 settle + zip markers
    ok1 = False; dl = time.time() + 660
    while time.time() < dl:
        try:
            if LAM.get_function_configuration(FunctionName=FN).get("LastUpdateStatus") == "Successful":
                info = LAM.get_function(FunctionName=FN)
                with urllib.request.urlopen(urllib.request.Request(info["Code"]["Location"], headers=UA), timeout=60) as r:
                    src = zipfile.ZipFile(io.BytesIO(r.read())).read("lambda_function.py").decode("utf-8", "replace")
                if all(m in src for m in ("universe_builder_v4_exhaustive_bisection",
                                          "def fetch_screener_bucket(bucket_name, low, high, depth=0):",
                                          '"is_adr": _adr,')):
                    ok1 = True; break
        except Exception:
            pass
        time.sleep(12)
    gate("G1_settled_v4", ok1, "zip markers: v4 method + bisection + is_adr")

    # G2 schedule ensured — describe live reality FIRST, never downgrade (3573 doctrine)
    sched_detail = ""
    try:
        arn = LAM.get_function_configuration(FunctionName=FN)["FunctionArn"]
        rules = EVT.list_rule_names_by_target(TargetArn=arn).get("RuleNames") or []
        existing = []
        for rn in rules:
            try:
                existing.append(f"{rn}={EVT.describe_rule(Name=rn).get('ScheduleExpression')}")
            except Exception:
                pass
        scheds = []
        for pg in SCH.get_paginator("list_schedules").paginate():
            for s0 in pg.get("Schedules", []):
                if "universe" in s0["Name"]:
                    scheds.append(s0["Name"])
        if existing or scheds:
            sched_detail = f"live cadence kept: rules={existing} scheduler={scheds}"
        else:
            SCH.create_schedule(
                Name="justhodl-universe-builder-daily",
                ScheduleExpression="cron(10 9 * * ? *)",
                FlexibleTimeWindow={"Mode": "OFF"},
                Target={"Arn": arn,
                        "RoleArn": "arn:aws:iam::857687956942:role/justhodl-scheduler-role",
                        "Input": "{}"},
                State="ENABLED",
                Description="Exhaustive US-listed universe rebuild, daily 09:10 UTC")
            sched_detail = "created Scheduler justhodl-universe-builder-daily cron(10 9 * * ? *)"
        gate("G2_schedule", True, sched_detail)
    except Exception as e:
        gate("G2_schedule", False, str(e)[:200])

    # G3 sync run → feed
    try:
        r = LAM.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        body = json.loads(json.loads(r["Payload"].read()).get("body", "{}"))
        total = body.get("total_stocks")
        j = json.loads(S3C.get_object(Bucket=BUCKET, Key="data/universe.json")["Body"].read())
        st = j.get("stats") or {}
        s0 = (j.get("stocks") or [{}])[0]
        shape_ok = all(k in s0 for k in ("symbol", "sector", "market_cap", "cap_bucket", "exchange"))
        n_adr = st.get("n_adr")
        gate("G3_feed_exhaustive",
             j.get("schema_version") == 4 and (st.get("total_stocks") or 0) >= 4200
             and len(st.get("by_exchange") or {}) >= 3 and (n_adr or 0) >= 250 and shape_ok,
             f"total={st.get('total_stocks')} (invoke said {total}) by_exchange={st.get('by_exchange')} "
             f"n_adr={n_adr} buckets={st.get('by_cap_bucket')} shape_ok={shape_ok} "
             f"dur={j.get('duration_s')}s")
        out["universe_stats"] = st
    except Exception as e:
        gate("G3_feed_exhaustive", False, str(e)[:300])

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3574.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
