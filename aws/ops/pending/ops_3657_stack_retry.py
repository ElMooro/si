"""ops 3657 — repair-backlog endgame + synthesis: [A] thesis-engine heal
(3642's only fail was invoke READ-TIMEOUT — long LLM run; ensure Scheduler,
async Event invoke, gate on feed freshness) [B] feed-registry recount (stale
should collapse post-resurrections; list residue verbatim) [C] served:
eurodollar.html BIS card + macro-leads China-Stack tile."""
import json, sys, time, urllib.request
from pathlib import Path
import boto3
from botocore.config import Config
from _lambda_deploy_helpers import deploy_lambda  # noqa: F401
from ops_report import report

ACCT = "857687956942"
LAM = boto3.client("lambda", "us-east-1", config=Config(read_timeout=120, retries={"max_attempts": 0}))
SCH = boto3.client("scheduler", "us-east-1")
S3C = boto3.client("s3", "us-east-1")
B = "justhodl-dashboard-live"
ROLE = f"arn:aws:iam::{ACCT}:role/justhodl-scheduler-role"

with report("3657_stack_retry") as rep:
    rep.heading("ops 3657 — thesis heal + stale endgame + synthesis serves")
    out = {"gates": {}}
    fails = []
    import traceback
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3657.json").write_text(json.dumps({"verdict": "STARTED"}))
    try:

        def gate(n, ok, d):
            out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:640]}
            print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:600]); rep.log(n + " " + str(ok))
            if not ok:
                fails.append(n)

        # [A] thesis-engine: schedule ensure + async invoke + freshness gate
        try:
            fn = "justhodl-thesis-engine"
            arn = LAM.get_function(FunctionName=fn)["Configuration"]["FunctionArn"]
            cfg = dict(Name="justhodl-thesis-engine-daily",
                       ScheduleExpression="cron(50 21 ? * MON-FRI *)",
                       FlexibleTimeWindow={"Mode": "OFF"},
                       Target={"Arn": arn, "RoleArn": ROLE, "Input": "{}"},
                       State="ENABLED")
            try:
                SCH.create_schedule(**cfg); sched = "created"
            except Exception:
                try:
                    SCH.update_schedule(**cfg); sched = "updated"
                except Exception as _e:
                    sched = "err:" + str(_e)[:60]
            try:
                LAM.add_permission(FunctionName=fn, StatementId="sched-thesis",
                                   Action="lambda:InvokeFunction",
                                   Principal="scheduler.amazonaws.com")
            except LAM.exceptions.ResourceConflictException:
                pass
            LAM.invoke(FunctionName=fn, InvocationType="Event", Payload=b"{}")
            ok1 = False; det = f"sched={sched} async=202 "
            dl = time.time() + 420
            while time.time() < dl:
                try:
                    h = S3C.head_object(Bucket=B, Key="data/thesis-engine.json")
                    age = (time.time() - h["LastModified"].timestamp()) / 60
                    det += f"age_min={age:.1f} "
                    if age < 12:
                        ok1 = True; break
                except Exception as e:
                    det += str(e)[:60]
                time.sleep(25)
            gate("G1_thesis", ok1, det)
        except Exception as e:
            gate("G1_thesis", False, str(e)[:340])

        # [B] registry recount
        try:
            LAM.invoke(FunctionName="justhodl-feed-registry",
                       InvocationType="RequestResponse", Payload=b"{}")
            time.sleep(2)
            reg = json.loads(S3C.get_object(Bucket=B, Key="data/feed-registry.json")["Body"].read())
            stale = [x.get("key") for x in (reg.get("stale") or [])]
            gate("G2_registry", len(stale) <= 5,
                 f"stale_n={len(stale)} residue={stale[:10]}")
            out["stale_residue"] = stale
        except Exception as e:
            gate("G2_registry", False, str(e)[:300])

        # [C] served synthesis
        ok3 = False; det3 = ""; dl = time.time() + 480
        while time.time() < dl:
            try:
                def get(u):
                    return urllib.request.urlopen(urllib.request.Request(
                        u + "?cb=" + str(int(time.time())),
                        headers={"User-Agent": "Mozilla/5.0"}), timeout=30
                    ).read().decode("utf-8", "replace")
                h1 = get("https://justhodl.ai/eurodollar.html")
                h2 = get("https://justhodl.ai/macro-leads.html")
                mk = {"bis_card": "jh-biscb" in h1,
                      "bis_link": "bis-crossborder.html" in h1,
                      "cn_tile": "China stack" in h2,
                      "bcb_fetch": "bis-crossborder.json" in h2}
                det3 = str(mk)
                if all(mk.values()):
                    ok3 = True; break
            except Exception as e:
                det3 = str(e)[:140]
            time.sleep(20)
        gate("G3_serves", ok3, det3)

    except Exception:
        out["crash"] = traceback.format_exc()[-1200:]
        print("CRASH:", out["crash"][-400:])
    out["verdict"] = ("CRASH" if out.get("crash") else
                       ("PASS_ALL" if not fails else "GAPS: " + ",".join(fails)))
    print("VERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3657.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
