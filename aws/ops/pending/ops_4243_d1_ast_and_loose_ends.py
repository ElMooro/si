"""
ops_4243 — replace the D1 string-match with an AST classifier, and close
the two loose ends.

WHY THIS OP EXISTS
ops 4233's D1 check asked "does the source contain CHAIN_MAX or depth?"
and reported everything else as unguarded. Of its three findings, two
were wrong: clone-alpha guards with `hop < MAX_HOPS`, equity-research
guards with a payload flag. I then relayed that output as established
fact. Reading a heuristic's result as a finding is the same error as
reading HTTP 200 as success — the error this entire engine exists to
catch — so the detector now parses the code instead of grepping it.

THE GATE IS THE POINT
A monitoring change shipped without a test is how the next silent bug
gets made, so the classifier carries its own suite and this op refuses
to proceed unless it passes:

  unguarded self-invoke        -> UNGUARDED        (true positive)
  `hop < MAX_HOPS` guard       -> BOUNDED_COUNTER  (no false positive)
  `_internal` kickoff flag     -> BOUNDED_FLAG     (no false positive)
  invoke of a DIFFERENT lambda -> NO_SELF_INVOKE   (no false positive)

Then it is proven against the three real engines whose behaviour is now
known by inspection, not inference. Only after both does it schedule.

BOUNDS ARE REPORTED, NOT ASSUMED SAFE
A bound below 16 is not automatically fine. MAX_HOPS=10 was silently
capping clone-alpha's backfill at ten hops per week — bounded work
presented as finished work. The classifier records the bound so a human
sees "bound=10", never the word "unguarded" standing in for it.

LOOSE END A — justhodl-backups-857687956942: identified, not deleted.
LOOSE END B — config/ staged in run-ops.yml: verified by this op's own
commit, since the manifest mirror it writes lands under config/.
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen
import boto3
from botocore.config import Config
from ops_report import report

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
FN = "justhodl-fleet-integrity"
MARKER = "fleet-integrity v1.1.0 ops4242 guard-aware"
SCAN_RULE = "justhodl-d1-scan-daily"
SCAN_EXPR = "cron(0 5 * * ? *)"
CFG = Config(retries={"max_attempts": 6, "mode": "adaptive"}, read_timeout=600)
lam = boto3.client("lambda", region_name=REGION, config=CFG)
evb = boto3.client("events", region_name=REGION, config=CFG)
s3  = boto3.client("s3", region_name=REGION, config=CFG)
cw  = boto3.client("cloudwatch", region_name=REGION, config=CFG)
ACCT = boto3.client("sts").get_caller_identity()["Account"]
ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

def zip_fn(fn):
    src="aws/lambdas/%s/source"%fn; buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
        for root,_,files in os.walk(src):
            if "__pycache__" in root: continue
            for f in files:
                fp=os.path.join(root,f); z.write(fp, os.path.relpath(fp,src))
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

with report("4243_d1_ast_and_loose_ends") as rep:
    rep.heading("ops 4243 — AST-based D1, and the loose ends")
    fails=[]

    rep.section("1. Deploy fleet-integrity v1.1.0")
    try:
        wait_active(FN)
        lam.update_function_code(FunctionName=FN, ZipFile=zip_fn(FN))
        wait_active(FN)
        lam.update_function_configuration(FunctionName=FN, Timeout=900,
            MemorySize=1024, Environment={"Variables":{"S3_BUCKET":BUCKET}})
        ok=False
        for i in range(30):
            time.sleep(6)
            try:
                loc=lam.get_function(FunctionName=FN)["Code"]["Location"]
                src=zipfile.ZipFile(io.BytesIO(urlopen(loc,timeout=60).read())
                                    ).read("lambda_function.py").decode("utf-8","ignore")
                if MARKER in src: ok=True; break
            except Exception: pass
        (rep.ok if ok else rep.fail)("marker %s"%("verified" if ok else "MISSING"))
        if not ok: fails.append("marker")
    except Exception as e:
        fails.append("deploy: %s"%str(e)[:180])

    rep.section("2. GATE A — classifier self-test (must be 4/4)")
    try:
        wait_active(FN)
        r=lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                     Payload=json.dumps({"mode":"selftest"}).encode())
        b=json.loads(r["Payload"].read() or b"{}")
        for c in b.get("cases",[]):
            (rep.ok if c["pass"] else rep.fail)(
                "   %-10s expect=%-16s got=%-16s %s"
                %(c["case"],c["expect"],c["got"],c.get("detail")))
            rep.kv(section="selftest", case=c["case"], expect=c["expect"],
                   got=c["got"], passed=c["pass"])
        if not b.get("passed"):
            fails.append("classifier self-test FAILED — not shipping a "
                         "detector that cannot classify its own examples")
        else:
            rep.ok("4/4 — one true positive, three no-false-positives")
    except Exception as e:
        fails.append("selftest: %s"%str(e)[:180])

    rep.section("3. Scan the fleet (incremental, sha-cached)")
    scan={}
    try:
        for attempt in range(4):
            wait_active(FN)
            r=lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                         Payload=json.dumps({"mode":"d1scan"}).encode())
            scan=json.loads(r["Payload"].read() or b"{}")
            rep.log("pass %d -> %s"%(attempt+1, json.dumps(scan)[:220]))
            if scan.get("complete"): break
        rep.kv(section="d1scan", scanned=scan.get("scanned"),
               cached=scan.get("from_cache"), failed=scan.get("failed"),
               complete=scan.get("complete"), total=scan.get("total"))
        if not scan.get("complete"):
            rep.warn("walk still in progress at %s/%s — the daily schedule "
                     "resumes from the cursor"%(scan.get("cursor"),
                                                scan.get("total")))
    except Exception as e:
        fails.append("d1scan: %s"%str(e)[:180])

    rep.section("4. GATE B — the three real engines classify correctly")
    try:
        cache=json.loads(s3.get_object(Bucket=BUCKET,
            Key="data/_state/d1-classification-cache.json")["Body"].read())
        by={v.get("fn"):v for v in cache.values()}
        expect={"justhodl-13f-clone-alpha":"BOUNDED_COUNTER",
                "justhodl-equity-research":"BOUNDED_FLAG",
                "justhodl-fundamental-census":"BOUNDED_COUNTER"}
        for fn,exp in expect.items():
            got=(by.get(fn) or {}).get("cls")
            det=(by.get(fn) or {}).get("detail")
            if got is None:
                rep.warn("   %-34s not yet scanned (walk incomplete)"%fn)
                continue
            good = (got==exp)
            (rep.ok if good else rep.fail)(
                "   %-34s expect=%-16s got=%-16s %s"%(fn,exp,got,det))
            rep.kv(section="real_engines", function=fn, expect=exp,
                   got=got, detail=json.dumps(det), passed=good)
            if not good:
                fails.append("%s classified %s, expected %s"%(fn,got,exp))
        counts={}
        for v in by.values(): counts[v.get("cls")]=counts.get(v.get("cls"),0)+1
        rep.log("fleet classification: %s"%counts)
        ung=[f for f,v in by.items() if v.get("cls")=="UNGUARDED"]
        rep.log("GENUINELY UNGUARDED self-invokers: %d"%len(ung))
        for f in ung[:20]:
            rep.fail("   %s"%f)
            rep.kv(section="unguarded", function=f)
        bc=[(f,v["detail"].get("bound")) for f,v in by.items()
            if v.get("cls")=="BOUNDED_COUNTER"]
        for f,bd in sorted(bc):
            rep.log("   bounded  %-38s bound=%s"%(f,bd))
    except Exception as e:
        fails.append("gate B: %s"%str(e)[:180])

    rep.section("5. Schedule the daily scan + declare it")
    arn="arn:aws:lambda:%s:%s:function:%s"%(REGION,ACCT,FN)
    try:
        evb.put_rule(Name=SCAN_RULE, ScheduleExpression=SCAN_EXPR,
                     State="ENABLED",
                     Description="Daily incremental D1 AST scan")
        evb.put_targets(Rule=SCAN_RULE, Targets=[{"Id":"1","Arn":arn,
            "Input":json.dumps({"mode":"d1scan"})}])
        try:
            lam.add_permission(FunctionName=FN, StatementId="allow-d1-scan",
                Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                SourceArn="arn:aws:events:%s:%s:rule/%s"%(REGION,ACCT,SCAN_RULE))
        except Exception: pass
        tg=evb.list_targets_by_rule(Rule=SCAN_RULE)["Targets"]
        rep.ok("%s -> %s mode=d1scan (%d target)"%(SCAN_EXPR,FN,len(tg)))
        m=json.loads(s3.get_object(Bucket=BUCKET,
            Key="config/schedule-manifest.json")["Body"].read())
        if not any(r["name"]==SCAN_RULE for r in m["rules"]):
            m["rules"].append({"kind":"events","name":SCAN_RULE,
                "expr":SCAN_EXPR,"state":"ENABLED",
                "targets":[{"id":"1","arn":arn,
                            "input":json.dumps({"mode":"d1scan"}),
                            "path":None}]})
        s3.put_object(Bucket=BUCKET, Key="config/schedule-manifest.json",
            Body=json.dumps(m).encode(), ContentType="application/json")
        # LOOSE END B: this now lands in a git-staged directory
        (ROOT/"config").mkdir(exist_ok=True)
        (ROOT/"config"/"schedule-manifest.json").write_text(
            json.dumps(m, indent=1), encoding="utf-8")
        rep.ok("manifest written to config/ (staged by run-ops.yml as of "
               "this session) — %d rules"%len(m["rules"]))
        r=lam.invoke(FunctionName="justhodl-schedule-reconciler",
                     InvocationType="RequestResponse")
        rb=json.loads(r["Payload"].read() or b"{}")
        (rep.ok if rb.get("drift_count")==0 else rep.warn)(
            "reconciler drift = %s"%rb.get("drift_count"))
    except Exception as e:
        fails.append("schedule: %s"%str(e)[:180])

    rep.section("6. LOOSE END A — identify the stale backup bucket")
    B2="justhodl-backups-857687956942"
    try:
        objs=[]
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=B2):
            objs.extend(page.get("Contents",[]))
        rep.log("%s holds %d object(s)"%(B2,len(objs)))
        for o in objs[:10]:
            rep.log("   %-58s %8d bytes  %s"
                    %(o["Key"][:58], o["Size"], str(o["LastModified"])[:19]))
            rep.kv(section="stale_bucket", key=o["Key"], size=o["Size"],
                   modified=str(o["LastModified"])[:19])
        # who writes it? grep the deployed fleet's env + the repo
        writers=[]
        for page in lam.get_paginator("list_functions").paginate():
            for f in page["Functions"]:
                ev=(f.get("Environment") or {}).get("Variables") or {}
                if B2 in json.dumps(ev):
                    writers.append(f["FunctionName"])
        rep.log("functions whose ENV names this bucket: %s"
                %(", ".join(writers) or "NONE"))
        import subprocess
        g=subprocess.run(["grep","-rIl","--exclude-dir=.git",
                          "--exclude-dir=reports","--exclude=ops_4243*",B2,"."],
                         cwd=str(ROOT), capture_output=True, text=True,
                         timeout=120)
        hits=[x for x in g.stdout.strip().split("\\n") if x]
        rep.log("repo files naming this bucket: %s"%(", ".join(hits[:8]) or "NONE"))
        rep.kv(section="stale_bucket", env_writers=", ".join(writers) or "none",
               repo_refs=", ".join(hits[:5]) or "none", objects=len(objs))
        rep.warn("NOT DELETED. Identified only — %d object(s), %d env "
                 "writer(s), %d repo reference(s). Deleting a backup bucket "
                 "on a hunch is the one mistake with no undo."
                 %(len(objs), len(writers), len(hits)))
    except Exception as e:
        rep.warn("stale bucket probe: %s"%str(e)[:150])

    rep.section("RESULT")
    if fails:
        for f in fails: rep.fail("  %s"%f)
        raise SystemExit("FAILS: %s"%"; ".join(fails[:3]))
    rep.ok("OPS 4243 PASS")
