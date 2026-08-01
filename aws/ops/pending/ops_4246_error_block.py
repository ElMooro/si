"""
ops_4246 — work the D8 error block: measure what the ops-4234 timeout
raises actually achieved, fix the one named silent-data bug, and
quarantine the six broken packages.

A. DID THE TIMEOUT RAISES WORK? ops 4234 raised 14 timeouts on engines
   whose AVERAGE run pinned the ceiling, and diagnosed 47 engines at
   >=20% error rate — 24 of them producing no traceback at all, meaning
   bare timeouts. This section recomputes error rates over the 24h SINCE
   that change and compares them to the recorded baseline, per engine.
   Claiming a fix worked without measuring after is the same error as
   reading 200 as success.

B. justhodl-signal-scorecard — the only failure in the block that was a
   named, understood, silent-DATA bug rather than a timeout. SSM
   Standard-tier parameters cap at 4,096 characters; the enforcement map
   outgrew it; PutParameter returned ValidationException; and the write
   sat inside a bare try/except that only printed. So the function kept
   returning success while the multiplier map that the calibrator,
   best-setups and master-ranker all read silently froze at its last good
   value. A stale map that looks fresh is worse than a crash, because a
   crash gets noticed. Now: Intelligent-Tiering (no standing cost below
   4KB), an S3 pointer above the 8KB Advanced ceiling, and the write
   outcome published INTO the artifact where the contract gate and the
   integrity board can both see it.

C. THE SIX BROKEN PACKAGES from ops 4245. Each declares
   lambda_function.lambda_handler and ships a package without that file,
   so every invocation is an ImportModuleError. Cross-referenced against
   ops 4233: all six also have no schedule and no successful
   invocations. Two still have source in the repo, four exist only as a
   broken artifact in AWS.

   They are QUARANTINED, not deleted: package bytes copied to the DR
   bucket, reserved concurrency pinned to 0 so nothing can invoke them by
   accident, a lifecycle tag applied, and a ledger written. Every step
   reverses with one call. Deleting a function whose source no longer
   exists anywhere is the one move in this whole cleanup with no undo,
   and it is not worth making for six functions that cost nothing.
"""
import io, json, os, time, zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.request import urlopen
import boto3
from botocore.config import Config
from ops_report import report

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
DR = "justhodl-dashboard-live-dr"
CFG = Config(retries={"max_attempts": 6, "mode": "adaptive"}, read_timeout=300)
lam = boto3.client("lambda", region_name=REGION, config=CFG)
cw  = boto3.client("cloudwatch", region_name=REGION, config=CFG)
s3  = boto3.client("s3", region_name=REGION, config=CFG)
NOW = datetime.now(timezone.utc)
ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))
OUT = {"ops": 4246, "ts": NOW.isoformat()}

BROKEN = ["macro-report-api", "multi-agent-orchestrator",
          "nyfed-financial-stability-fetcher", "nyfed-primary-dealer-fetcher",
          "nyfedapi-isolated", "ultimate-multi-agent"]
SC = "justhodl-signal-scorecard"
MARK = "ops 4246: SSM Standard-tier parameters cap at 4,096 characters"

def zip_fn(fn):
    src="aws/lambdas/%s/source"%fn; buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
        for root,_,files in os.walk(src):
            if "__pycache__" in root: continue
            for f in files:
                fp=os.path.join(root,f); z.write(fp, os.path.relpath(fp,src))
        if os.path.isdir("aws/shared"):
            for f in sorted(os.listdir("aws/shared")):
                if f.endswith(".py"): z.write(os.path.join("aws/shared",f), f)
    return buf.getvalue()

def wait_active(fn,b=200):
    t0=time.time()
    while time.time()-t0<b:
        try:
            c=lam.get_function_configuration(FunctionName=fn)
            if c.get("State")=="Active" and c.get("LastUpdateStatus") in (None,"Successful"): return True
        except Exception: pass
        time.sleep(4)
    return False

def rate(fn, hours):
    st = NOW - timedelta(hours=hours)
    def g(m):
        r = cw.get_metric_statistics(Namespace="AWS/Lambda", MetricName=m,
            Dimensions=[{"Name":"FunctionName","Value":fn}],
            StartTime=st, EndTime=NOW, Period=hours*3600, Statistics=["Sum"])
        return sum(p["Sum"] for p in r.get("Datapoints", []))
    inv, err = g("Invocations"), g("Errors")
    return inv, err, (100.0*err/inv if inv else None)

with report("4246_error_block") as rep:
    rep.heading("ops 4246 — the D8 error block")
    fails=[]

    rep.section("A. Did the ops-4234 timeout raises actually work?")
    try:
        base = json.loads((ROOT/"aws"/"ops"/"reports"/
                           "4234_defect_remediation.json").read_text())
    except Exception as e:
        base = {}
        rep.warn("baseline unreadable: %s"%str(e)[:100])
    raised = [c["fn"] for c in base.get("changes", [])
              if c.get("a")=="timeout"]
    diag = base.get("diagnoses", {})
    rep.log("engines whose timeout was raised in ops 4234: %d"%len(raised))
    rep.log("")
    rep.log("%-40s %8s %8s %10s %s"%("ENGINE","BEFORE","NOW24h","RUNS24h","VERDICT"))
    improved=worse=idle=same=0
    for fn in sorted(raised):
        before = (diag.get(fn) or {}).get("err_pct")
        inv, err, now_pct = rate(fn, 24)
        if inv == 0:
            v="NOT RUN YET"; idle+=1
        elif before is None:
            v="no baseline"; same+=1
        elif now_pct <= 1.0:
            v="FIXED"; improved+=1
        elif now_pct < before - 10:
            v="IMPROVED"; improved+=1
        elif now_pct > before + 10:
            v="WORSE"; worse+=1
        else:
            v="UNCHANGED"; same+=1
        rep.log("%-40s %7s%% %7s%% %10d %s"
                %(fn[:40], "%.0f"%before if before is not None else "?",
                  "%.0f"%now_pct if now_pct is not None else "-", int(inv), v))
        rep.kv(section="timeout_outcome", function=fn,
               before_pct=before, now_pct=(round(now_pct,1) if now_pct is not None else None),
               runs_24h=int(inv), verdict=v)
    rep.log("")
    rep.log("fixed/improved=%d unchanged=%d worse=%d not-yet-run=%d"
            %(improved, same, worse, idle))
    rep.warn("Engines that have not re-run since the change cannot be "
             "judged yet — most of this fleet is on daily cadence, so the "
             "honest read arrives tomorrow.")
    OUT["timeout_outcomes"]={"improved":improved,"same":same,
                             "worse":worse,"idle":idle}

    rep.section("A2. Current top of the error block")
    try:
        d = json.loads(s3.get_object(Bucket=BUCKET,
            Key="data/fleet-integrity.json")["Body"].read())
        rows=[r for r in d.get("rows",[]) if r["cls"]=="D8_errors"]
        rep.log("D8 entries on the board: %d"%len(rows))
        for r in rows[:18]:
            rep.log("   %-40s %s"%(r["id"][:40], r["detail"][:70]))
    except Exception as e:
        rep.warn("board read: %s"%str(e)[:110])

    rep.section("B. signal-scorecard — the silent-data bug")
    try:
        wait_active(SC)
        lam.update_function_code(FunctionName=SC, ZipFile=zip_fn(SC))
        ok=False
        for i in range(25):
            time.sleep(6)
            try:
                loc=lam.get_function(FunctionName=SC)["Code"]["Location"]
                src=zipfile.ZipFile(io.BytesIO(urlopen(loc,timeout=60).read())
                                    ).read("lambda_function.py").decode("utf-8","ignore")
                if MARK in src: ok=True; break
            except Exception: pass
        (rep.ok if ok else rep.fail)("marker %s"%("verified" if ok else "MISSING"))
        if not ok: fails.append("scorecard marker")
        wait_active(SC)
        r=lam.invoke(FunctionName=SC, InvocationType="RequestResponse")
        body=(r["Payload"].read() or b"")[:300].decode("utf-8","ignore")
        if r.get("FunctionError"):
            rep.fail("probe FunctionError=%s %s"%(r["FunctionError"], body[:220]))
            fails.append("scorecard probe")
        else:
            rep.ok("probe clean — %s"%body[:200])
        # the real gate: did the SSM write actually land this time?
        art=json.loads(s3.get_object(Bucket=BUCKET,
            Key="data/signal-scorecard.json")["Body"].read())
        w=art.get("ssm_writes")
        rep.log("ssm_writes -> %s"%json.dumps(w)[:300])
        rep.kv(section="ssm", ok=art.get("ssm_ok"), writes=json.dumps(w)[:150])
        if art.get("ssm_ok") is True:
            rep.ok("SSM enforcement map WROTE successfully — downstream "
                   "consumers are no longer reading a frozen map")
        elif w is None:
            rep.warn("artifact has no ssm_writes key yet (engine may not "
                     "have reached that branch on this run)")
        else:
            rep.fail("SSM still failing: %s"%json.dumps(w)[:200])
            fails.append("ssm write still failing")
    except Exception as e:
        fails.append("scorecard: %s"%str(e)[:180])

    rep.section("C. Quarantine the six broken packages")
    ledger=[]
    for fn in BROKEN:
        row={"fn":fn}
        try:
            c=lam.get_function_configuration(FunctionName=fn)
            row.update({"runtime":c.get("Runtime"),"handler":c.get("Handler"),
                        "last_modified":c.get("LastModified"),
                        "code_size":c.get("CodeSize")})
            # 1. preserve the bytes before touching anything
            loc=lam.get_function(FunctionName=fn)["Code"]["Location"]
            blob=urlopen(loc, timeout=60).read()
            key="quarantine/2026-08-01/%s.zip"%fn
            s3.put_object(Bucket=DR, Key=key, Body=blob,
                          ContentType="application/zip")
            back=s3.head_object(Bucket=DR, Key=key)["ContentLength"]
            row["backup"]="s3://%s/%s"%(DR,key)
            row["backup_bytes"]=back
            if back != len(blob):
                raise RuntimeError("backup size mismatch")
            # 2. only now make it unrunnable
            lam.put_function_concurrency(FunctionName=fn,
                                         ReservedConcurrentExecutions=0)
            row["reserved"]=0
            try:
                arn=c["FunctionArn"]
                lam.tag_resource(Resource=arn,
                    Tags={"lifecycle":"quarantined",
                          "quarantined_at":NOW.strftime("%Y-%m-%d"),
                          "reason":"ImportModuleError - handler file absent "
                                   "from package; no schedule, no successful "
                                   "invocations",
                          "ops":"4246"})
                row["tagged"]=True
            except Exception as e:
                row["tagged"]=False; row["tag_error"]=str(e)[:90]
            rep.ok("  %-36s backed up %d bytes, concurrency 0, tagged"
                   %(fn[:36], back))
            rep.kv(section="quarantine", function=fn,
                   backup=row["backup"], bytes=back,
                   runtime=row.get("runtime"), tagged=row.get("tagged"))
        except Exception as e:
            row["error"]=str(e)[:140]
            rep.fail("  %-36s %s"%(fn[:36], str(e)[:110]))
        ledger.append(row)
    try:
        s3.put_object(Bucket=BUCKET, Key="config/quarantine-ledger.json",
            Body=json.dumps({"ops":4246,"at":NOW.isoformat(),
                             "undo":"lam.delete_function_concurrency(FunctionName=..) "
                                    "restores invocability; the package zip is in "
                                    "the DR bucket under quarantine/",
                             "entries":ledger}, indent=1).encode(),
            ContentType="application/json")
        (ROOT/"config").mkdir(exist_ok=True)
        (ROOT/"config"/"quarantine-ledger.json").write_text(
            json.dumps({"ops":4246,"at":NOW.isoformat(),"entries":ledger},
                       indent=1), encoding="utf-8")
        rep.ok("quarantine ledger -> config/quarantine-ledger.json + S3")
    except Exception as e:
        fails.append("ledger: %s"%str(e)[:140])
    OUT["quarantine"]=ledger

    (ROOT/"aws"/"ops"/"reports"/"4246_error_block.json").write_text(
        json.dumps(OUT, indent=1, default=str), encoding="utf-8")

    rep.section("RESULT")
    if fails:
        for f in fails: rep.fail("  %s"%f)
        raise SystemExit("FAILS: %s"%"; ".join(fails[:3]))
    rep.ok("OPS 4246 PASS")
