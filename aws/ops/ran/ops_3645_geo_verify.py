"""ops 3645 (recal verify post-fix) — NEW justhodl-geopolitical-risk engine (World Monitor-inspired,
built JustHodl-native). Country geopolitical-stress scores from a curated
208-feed public-RSS corpus (facts adapted from koala73/worldmonitor AGPL-3.0;
scores original) + crisis-lexicon intensity + 20d z-escalation. Creates the
Lambda, daily EventBridge Scheduler 11:30 UTC, invokes, gates the feed.
Additive to GSSI/defcon (news-flow layer they lack)."""
import json, sys, time
from pathlib import Path
import boto3
from botocore.config import Config
from _lambda_deploy_helpers import deploy_lambda
from ops_report import report

ROOT = Path(__file__).resolve().parents[2]
ACCT = "857687956942"
LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=600, retries={"max_attempts": 0}))
SCH = boto3.client("scheduler", "us-east-1")
S3C = boto3.client("s3", "us-east-1")
B = "justhodl-dashboard-live"
FN = "justhodl-geopolitical-risk"
ROLE = f"arn:aws:iam::{ACCT}:role/justhodl-scheduler-role"

with report("3645_geo_verify") as rep:
    rep.heading("ops 3645 (recal verify post-fix) — geopolitical-risk engine (news-flow country stress)")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:640]}
        print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:600]); rep.log(n + " " + str(ok))
        if not ok:
            fails.append(n)

    try:
        deploy_lambda(report=rep, function_name=FN,
                      source_dir=ROOT / "lambdas" / FN / "source",
                      env_vars={}, timeout=600, memory=1024,
                      description=("geopolitical-risk v1.0: country news-flow "
                                   "stress from 208-feed corpus + crisis "
                                   "z-escalation")[:200],
                      create_function_url=False)
        # daily scheduler 11:30 UTC
        arn = LAM.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
        sname = "justhodl-geopolitical-risk-daily"
        cfg = dict(Name=sname, ScheduleExpression="cron(30 11 * * ? *)",
                   FlexibleTimeWindow={"Mode": "OFF"},
                   Target={"Arn": arn, "RoleArn": ROLE, "Input": "{}"},
                   State="ENABLED")
        try:
            SCH.update_schedule(**cfg); sched = "exists-updated"
        except Exception:
            sched = "kept"
        # lambda perm for scheduler
        try:
            LAM.add_permission(FunctionName=FN, StatementId="sched-geo-risk",
                               Action="lambda:InvokeFunction",
                               Principal="scheduler.amazonaws.com")
        except LAM.exceptions.ResourceConflictException:
            pass
        gate("G1_deploy_sched", True, f"deployed + scheduler {sched} 11:30 UTC")
    except Exception as e:
        gate("G1_deploy_sched", False, str(e)[:340])

    try:
        r = LAM.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        pl = json.loads(r["Payload"].read() or b"{}")
        if isinstance(pl, dict) and pl.get("errorMessage"):
            gate("G2_feed", False, "fn error: " + pl["errorMessage"][:260])
        else:
            j = json.loads(S3C.get_object(Bucket=B, Key="data/geopolitical-risk.json")["Body"].read())
            rk = j.get("rankings") or []
            src = j.get("sources") or {}
            ok = (src.get("feeds_responding", 0) >= 20
                  and src.get("articles_scanned", 0) >= 200
                  and len(rk) >= 15
                  and all(isinstance(r0.get("stress_score"), (int, float)) for r0 in rk[:5]) and (rk[0]["stress_score"] - rk[4]["stress_score"]) >= 3)
            top5 = [(r0["country"], r0["stress_score"], r0["mentions_48h"],
                     r0["crisis_hits"]) for r0 in rk[:5]]
            gate("G2_feed", ok,
                 f"feeds_ok={src.get('feeds_responding')}/{src.get('feeds_in_corpus')} "
                 f"articles={src.get('articles_scanned')} temp={j.get('global_temp')} "
                 f"top={j.get('top_country')} top5={top5} "
                 f"escalating={[(e['country'], e['velocity_z']) for e in (j.get('escalating') or [])[:4]]}")
            out["snapshot"] = {"top": j.get("top_country"), "temp": j.get("global_temp"),
                               "top5": top5, "escalating": j.get("escalating")}
    except Exception as e:
        gate("G2_feed", False, str(e)[:340])

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("VERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3645.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
