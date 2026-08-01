"""
ops_4241 — contract-gate v1.0.1: fix the validator's own false positives.

v1.0.0 stored the principal-row path as a dotted string and split it on
"." to resolve. This fleet keys artifacts by filename, so paths like
page_reads -> "risk-regime.html" contain dots of their own. The resolver
walked into a segment that did not exist, returned None, and the gate
read None as a row collapse. Both sev-1 findings in ops 4240 were the
validator failing, not an engine.

That matters more than two bad rows. A checker that cries wolf gets
muted, and a muted checker is indistinguishable from no checker — which
is the precise condition that let the census run at 25% for months. So
the fix is at the representation (paths are segment LISTS) rather than a
patch at the parse, and the gate here is strict: re-learn, re-check, and
require that the two known-bogus violations are gone AND that the check
still finds the artifacts it is supposed to be watching.
"""
import io, json, os, time, zipfile
from pathlib import Path
from urllib.request import urlopen
import boto3
from botocore.config import Config
from ops_report import report

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
FN = "justhodl-contract-gate"
MARKER = "contract-gate v1.0.1 ops4241 path-segments"
CFG = Config(retries={"max_attempts": 6, "mode": "adaptive"}, read_timeout=300)
lam = boto3.client("lambda", region_name=REGION, config=CFG)
s3  = boto3.client("s3", region_name=REGION, config=CFG)

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
    t0=time.time()
    while time.time()-t0<b:
        try:
            c=lam.get_function_configuration(FunctionName=fn)
            if c.get("State")=="Active" and c.get("LastUpdateStatus") in (None,"Successful"):
                return True
        except Exception: pass
        time.sleep(4)
    return False

with report("4241_contract_path_fix") as rep:
    rep.heading("ops 4241 — contract-gate v1.0.1")
    fails=[]
    wait_active(FN)
    lam.update_function_code(FunctionName=FN, ZipFile=zip_fn(FN))
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

    rep.section("re-learn with segment paths")
    wait_active(FN)
    r=lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                 Payload=json.dumps({"mode":"learn"}).encode())
    b=json.loads(r["Payload"].read() or b"{}")
    rep.log("learn -> %s"%json.dumps(b)[:200])
    if b.get("n_contracts",0) < 50: fails.append("learn thin")

    rep.section("re-check")
    wait_active(FN)
    r=lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                 Payload=json.dumps({"mode":"check"}).encode())
    b=json.loads(r["Payload"].read() or b"{}")
    rep.log("check -> %s"%json.dumps(b)[:280])
    d=json.loads(s3.get_object(Bucket=BUCKET,
        Key="data/contract-violations.json")["Body"].read())
    rep.kv(section="contracts", contracts=d.get("n_contracts"),
           violations=d.get("n_violations"), sev1=d.get("sev1"))
    bogus=[x for x in d.get("violations",[])
           if x["artifact"] in ("data/dependency-graph.json","data/fleet-audit.json")
           and x["cls"]=="ROW_COLLAPSE"]
    if bogus:
        for x in bogus: rep.fail("   STILL BOGUS %s %s"%(x["artifact"],x["detail"][:90]))
        fails.append("false positives persist")
    else:
        rep.ok("the two v1.0.0 false positives are gone")
    if d.get("n_contracts",0) < 50:
        fails.append("contract count collapsed")
    else:
        rep.ok("still watching %d artifacts"%d.get("n_contracts"))
    rep.log("remaining violations by class: %s"%d.get("by_class"))
    for x in d.get("violations",[])[:20]:
        (rep.fail if x["sev"]==1 else rep.warn)(
            "   S%d %-14s %-40s %s"%(x["sev"],x["cls"],x["artifact"][-40:],x["detail"][:80]))
    if fails:
        for f in fails: rep.fail("  %s"%f)
        raise SystemExit("FAILS: %s"%"; ".join(fails[:3]))
    rep.ok("OPS 4241 PASS")
