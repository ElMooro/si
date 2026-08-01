"""
ops_4245 — fleet-integrity v1.2.0: close the scan-coverage gap.

v1.1.1 looked for a file ending in "lambda_function.py". That is the
house convention, not a rule. 23 functions use a different handler module
or a different language, and every one of them was skipped with nothing
recorded but a count. A function nobody looked at is indistinguishable
from a clean one, and that indistinguishability is the exact condition
that let the census run at 25% for months — so it is now tracked as a
defect class of its own (D14), not a footnote in a log line.

CHANGES
  * The handler file is resolved from the function's Handler
    configuration, which is the only authoritative answer, with the old
    convention kept as a fallback.
  * Non-Python packages get a regex pass explicitly LABELLED
    low-confidence. A weak answer that admits it is weak is useful; a
    weak answer dressed as a strong one is how D1 misled me twice.
  * Anything still unreadable is stored as UNSCANNED with the reason,
    the runtime, the handler, and a file listing — so the next person
    starts from evidence instead of repeating the discovery.

GATES
  1. self-test still 5/5 (the classifier itself is unchanged; prove it)
  2. scanned + unscanned + failed == total — full accounting, no
     function silently absent from the ledger
  3. coverage strictly improves on v1.1.1's 745/768
  4. every remaining UNSCANNED entry carries a machine-readable reason
"""
import io, json, os, time, zipfile
from pathlib import Path
from urllib.request import urlopen
import boto3
from botocore.config import Config
from ops_report import report

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
FN = "justhodl-fleet-integrity"
MARKER = "fleet-integrity v1.2.0 ops4245 handler-aware"
PRIOR_SCANNED = 745
CFG = Config(retries={"max_attempts": 6, "mode": "adaptive"}, read_timeout=600)
lam = boto3.client("lambda", region_name=REGION, config=CFG)
s3  = boto3.client("s3", region_name=REGION, config=CFG)

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

with report("4245_scan_coverage") as rep:
    rep.heading("ops 4245 — D1 scan coverage")
    fails=[]

    rep.section("1. Deploy v1.2.0")
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

    rep.section("2. GATE 1 — self-test unchanged at 5/5")
    wait_active(FN)
    r=lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                 Payload=json.dumps({"mode":"selftest"}).encode())
    b=json.loads(r["Payload"].read() or b"{}")
    for c in b.get("cases",[]):
        (rep.ok if c["pass"] else rep.fail)(
            "   %-11s expect=%-15s got=%-15s"%(c["case"],c["expect"],c["got"]))
    if not b.get("passed"): fails.append("self-test regressed")
    else: rep.ok("5/5 — the classifier is unchanged and still correct")

    rep.section("3. Rescan with handler resolution")
    for k in ("data/_state/d1-classification-cache.json",
              "data/_state/d1-scan-cursor.json"):
        try: s3.delete_object(Bucket=BUCKET, Key=k)
        except Exception: pass
    rep.log("cache cleared — resolution logic changed")
    scan={}
    for attempt in range(5):
        wait_active(FN)
        r=lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                     Payload=json.dumps({"mode":"d1scan"}).encode())
        scan=json.loads(r["Payload"].read() or b"{}")
        rep.log("pass %d -> %s"%(attempt+1, json.dumps(scan)[:230]))
        if scan.get("complete"): break

    rep.section("4. GATE 2/3 — full accounting, coverage improved")
    tot=scan.get("total",0)
    sc=scan.get("scanned",0); un=scan.get("unscanned",0); fl=scan.get("failed",0)
    rep.log("scanned=%d unscanned=%d failed=%d total=%d  (sum=%d)"
            %(sc,un,fl,tot,sc+un+fl))
    rep.kv(section="coverage", scanned=sc, unscanned=un, failed=fl,
           total=tot, prior=PRIOR_SCANNED)
    if sc+un+fl != tot:
        rep.fail("accounting does not close: %d != %d"%(sc+un+fl,tot))
        fails.append("accounting")
    else:
        rep.ok("accounting closes — every function is in the ledger")
    if sc <= PRIOR_SCANNED:
        rep.fail("coverage did not improve: %d <= %d"%(sc,PRIOR_SCANNED))
        fails.append("coverage regression")
    else:
        rep.ok("coverage %d -> %d (+%d functions now analysed)"
               %(PRIOR_SCANNED, sc, sc-PRIOR_SCANNED))

    rep.section("5. GATE 4 — what remains, and why")
    cache=json.loads(s3.get_object(Bucket=BUCKET,
        Key="data/_state/d1-classification-cache.json")["Body"].read())
    by={v.get("fn"):v for v in cache.values()}
    counts={}
    for v in by.values(): counts[v.get("cls")]=counts.get(v.get("cls"),0)+1
    rep.log("classification: %s"%counts)
    rem=[(f,v.get("detail") or {}) for f,v in by.items()
         if v.get("cls") in ("UNSCANNED","REVIEW")]
    rep.log("still unscanned or needing review: %d"%len(rem))
    noreason=0
    for f,d in sorted(rem):
        reason=d.get("reason") or d.get("note")
        if not reason: noreason+=1
        rep.warn("   %-40s rt=%-12s %s"
                 %(f[:40], (d.get("runtime") or "?")[:12],
                   str(reason)[:70]))
        rep.kv(section="unscanned", function=f, runtime=d.get("runtime"),
               handler=d.get("handler"), reason=str(reason)[:90])
    if noreason:
        fails.append("%d unscanned entries carry no reason"%noreason)
    else:
        rep.ok("every remaining entry carries a machine-readable reason")
    ung=[f for f,v in by.items() if v.get("cls")=="UNGUARDED"]
    rep.log("GENUINELY UNGUARDED after full-coverage scan: %d"%len(ung))
    for f in ung:
        rep.fail("   %s"%f)
        rep.kv(section="unguarded", function=f)

    rep.section("6. Audit with coverage now tracked as D14")
    wait_active(FN)
    r=lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                 Payload=json.dumps({"mode":"audit"}).encode())
    b=json.loads(r["Payload"].read() or b"{}")
    rep.ok("audit -> %s"%json.dumps(b)[:250])
    d=json.loads(s3.get_object(Bucket=BUCKET,
        Key="data/fleet-integrity.json")["Body"].read())
    for c,n in sorted(d.get("totals",{}).items(), key=lambda x:-x[1]):
        rep.log("   %-26s %d"%(c,n))
        rep.kv(section="totals", defect_class=c, count=n)

    rep.section("RESULT")
    if fails:
        for f in fails: rep.fail("  %s"%f)
        raise SystemExit("FAILS: %s"%"; ".join(fails[:3]))
    rep.ok("OPS 4245 PASS")
