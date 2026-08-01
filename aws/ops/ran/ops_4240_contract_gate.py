"""
ops_4240 — ship justhodl-contract-gate.

Sequence matters. Learn BEFORE check, and prove the learn produced real
contracts before scheduling anything: a contract registry with three
entries would pass a check trivially and report a green board over a
fleet nobody is actually watching. That is worse than no board.

Gates:
  1. zip marker present in the deployed artifact
  2. learn produces >= 50 contracts (the fleet publishes hundreds of
     artifacts; anything less means the listing is broken)
  3. check runs and returns a violation count
  4. sev-1 violations are PRINTED, not hidden — a gate that quietly
     tolerates failures is theatre
  5. daily schedule with exactly one target
  6. alarm on JustHodl/Contracts ViolationsSev1
  7. the schedule is appended to the declared manifest, or the reconciler
     would correctly flag it as UNDECLARED tomorrow
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen
import boto3
from botocore.config import Config
from ops_report import report

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
FN = "justhodl-contract-gate"
MARKER = "contract-gate v1.0.0 ops4240"
RULE = "justhodl-contract-gate-daily"
EXPR = "cron(0 13 * * ? *)"
CFG = Config(retries={"max_attempts": 6, "mode": "adaptive"}, read_timeout=300)
lam = boto3.client("lambda", region_name=REGION, config=CFG)
evb = boto3.client("events", region_name=REGION, config=CFG)
cw  = boto3.client("cloudwatch", region_name=REGION, config=CFG)
s3  = boto3.client("s3", region_name=REGION, config=CFG)
logs= boto3.client("logs", region_name=REGION, config=CFG)
ACCT = boto3.client("sts").get_caller_identity()["Account"]
ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

def zip_fn(fn):
    src = "aws/lambdas/%s/source" % fn
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(src):
            if "__pycache__" in root: continue
            for f in files:
                fp = os.path.join(root, f)
                z.write(fp, os.path.relpath(fp, src))
    return buf.getvalue()

def wait_active(fn, b=200):
    t0 = time.time()
    while time.time() - t0 < b:
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") in (None,"Successful"):
                return True
        except Exception: pass
        time.sleep(4)
    return False

with report("4240_contract_gate") as rep:
    rep.heading("ops 4240 — output contracts")
    fails = []

    rep.section("1. Deploy")
    donor = lam.get_function_configuration(FunctionName="justhodl-fleet-error-monitor")
    pkg = zip_fn(FN)
    try:
        try:
            lam.get_function_configuration(FunctionName=FN)
            wait_active(FN); lam.update_function_code(FunctionName=FN, ZipFile=pkg)
            wait_active(FN)
            lam.update_function_configuration(FunctionName=FN, Timeout=600,
                MemorySize=1024, Environment={"Variables":{"S3_BUCKET":BUCKET}})
            rep.ok("updated")
        except lam.exceptions.ResourceNotFoundException:
            lam.create_function(FunctionName=FN, Runtime=donor.get("Runtime","python3.12"),
                Role=donor["Role"], Handler="lambda_function.lambda_handler",
                Code={"ZipFile":pkg}, Timeout=600, MemorySize=1024,
                Environment={"Variables":{"S3_BUCKET":BUCKET}},
                Description="Output contract gate — asserts artifact shape, not exit code")
            rep.ok("created")
        try:
            logs.put_retention_policy(logGroupName="/aws/lambda/%s"%FN, retentionInDays=30)
        except Exception: pass
    except Exception as e:
        fails.append("deploy: %s" % str(e)[:180])

    ok = False
    for i in range(30):
        try:
            loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
            src = zipfile.ZipFile(io.BytesIO(urlopen(loc, timeout=60).read())
                                  ).read("lambda_function.py").decode("utf-8","ignore")
            if MARKER in src: ok = True; break
        except Exception: pass
        time.sleep(6)
    (rep.ok if ok else rep.fail)("GATE 1 zip marker %s" % ("verified" if ok else "MISSING"))
    if not ok: fails.append("zip marker")

    rep.section("2. GATE 2 — learn contracts from current state")
    n_c = 0
    try:
        wait_active(FN)
        r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                       Payload=json.dumps({"mode":"learn"}).encode())
        b = json.loads(r["Payload"].read() or b"{}")
        if r.get("FunctionError"):
            fails.append("learn error: %s" % str(b)[:200])
        else:
            n_c = b.get("n_contracts", 0)
            rep.log("learn -> %s" % json.dumps(b)[:200])
            if n_c < 50:
                rep.fail("only %d contracts — a green board over an "
                         "unwatched fleet is worse than no board" % n_c)
                fails.append("too few contracts (%d)" % n_c)
            else:
                rep.ok("%d contracts learned" % n_c)
    except Exception as e:
        fails.append("learn: %s" % str(e)[:180])

    rep.section("3. GATE 3/4 — check, and show the sev-1s")
    try:
        wait_active(FN)
        r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                       Payload=json.dumps({"mode":"check"}).encode())
        b = json.loads(r["Payload"].read() or b"{}")
        if r.get("FunctionError"):
            fails.append("check error: %s" % str(b)[:200])
        else:
            rep.ok("check -> %s" % json.dumps(b)[:280])
            rep.kv(section="contracts", contracts=b.get("n_contracts"),
                   violations=b.get("n_violations"), sev1=b.get("sev1"))
            d = json.loads(s3.get_object(Bucket=BUCKET,
                Key="data/contract-violations.json")["Body"].read())
            rep.log("artifacts live=%d contracted=%d uncontracted=%d"
                    % (d.get("n_artifacts",0), d.get("n_contracts",0),
                       d.get("n_uncontracted",0)))
            sev1 = [x for x in d.get("violations",[]) if x["sev"]==1]
            rep.log("SEV-1 VIOLATIONS (%d):" % len(sev1))
            for x in sev1[:25]:
                rep.fail("   %-14s %-42s %s"
                         % (x["cls"], x["artifact"][-42:], x["detail"][:90]))
                rep.kv(section="violation", cls=x["cls"],
                       artifact=x["artifact"], detail=x["detail"][:110])
            for x in [y for y in d.get("violations",[]) if y["sev"]==2][:12]:
                rep.warn("   %-14s %-42s %s"
                         % (x["cls"], x["artifact"][-42:], x["detail"][:80]))
    except Exception as e:
        fails.append("check: %s" % str(e)[:180])

    rep.section("4. GATE 5/6 — schedule + alarm")
    arn = "arn:aws:lambda:%s:%s:function:%s" % (REGION, ACCT, FN)
    try:
        evb.put_rule(Name=RULE, ScheduleExpression=EXPR, State="ENABLED",
                     Description="Daily output contract validation")
        evb.put_targets(Rule=RULE, Targets=[{"Id":"1","Arn":arn}])
        try:
            lam.add_permission(FunctionName=FN, StatementId="allow-contract-daily",
                Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                SourceArn="arn:aws:events:%s:%s:rule/%s"%(REGION,ACCT,RULE))
        except Exception: pass
        tg = evb.list_targets_by_rule(Rule=RULE)["Targets"]
        (rep.ok if len(tg)==1 else rep.fail)("schedule %s -> %d target" % (EXPR, len(tg)))
        if len(tg)!=1: fails.append("schedule targets=%d"%len(tg))
        cw.put_metric_alarm(AlarmName="justhodl-contract-sev1",
            AlarmDescription="An engine published a document that no longer "
                             "matches its contract. Open /integrity.html.",
            Namespace="JustHodl/Contracts", MetricName="ViolationsSev1",
            Statistic="Maximum", Period=86400, EvaluationPeriods=1,
            Threshold=0, ComparisonOperator="GreaterThanThreshold",
            TreatMissingData="notBreaching")
        rep.ok("alarm justhodl-contract-sev1 armed")
    except Exception as e:
        fails.append("schedule/alarm: %s" % str(e)[:170])

    rep.section("5. GATE 7 — declare it in the manifest")
    try:
        m = json.loads(s3.get_object(Bucket=BUCKET,
            Key="config/schedule-manifest.json")["Body"].read())
        if not any(r["name"]==RULE for r in m["rules"]):
            m["rules"].append({"kind":"events","name":RULE,"expr":EXPR,
                "state":"ENABLED",
                "targets":[{"id":"1","arn":arn,"input":None,"path":None}]})
            s3.put_object(Bucket=BUCKET, Key="config/schedule-manifest.json",
                Body=json.dumps(m).encode(), ContentType="application/json")
        p = ROOT/"aws"/"ops"/"audit"/"schedule-manifest.json"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(m, indent=1), encoding="utf-8")
        rep.ok("manifest now declares %s (%d rules total)" % (RULE, len(m["rules"])))
        r = lam.invoke(FunctionName="justhodl-schedule-reconciler",
                       InvocationType="RequestResponse")
        rb = json.loads(r["Payload"].read() or b"{}")
        (rep.ok if rb.get("drift_count")==0 else rep.warn)(
            "reconciler drift after declaring: %s" % rb.get("drift_count"))
    except Exception as e:
        fails.append("manifest: %s" % str(e)[:170])

    rep.section("RESULT")
    if fails:
        for f in fails: rep.fail("  %s" % f)
        raise SystemExit("FAILS: %s" % "; ".join(fails[:3]))
    rep.ok("OPS 4240 PASS — %d artifacts now have an asserted shape, "
           "checked daily." % n_c)
