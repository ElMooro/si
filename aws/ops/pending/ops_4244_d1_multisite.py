"""
ops_4244 — fleet-integrity v1.1.1: stop collapsing multi-site
self-invokers, and re-run every gate.

ops 4243's gate did its job: it failed. justhodl-fundamental-census came
back BOUNDED_FLAG when the bound of 12 was the fact that mattered. The
cause was not a bad guess — it was a modelling error. A function can
self-invoke from SEVERAL places with DIFFERENT guards, and census does
exactly that: one site continues the walk under `depth + 1 < CHAIN_MAX`,
another fires the terminal aggregate phase under a payload flag. v1.1.0
sorted the sites, returned the first, and threw the rest away.

v1.1.1 makes the headline binary — UNGUARDED if ANY site is unguarded,
otherwise BOUNDED — and enumerates every site beneath it. The headline
answers "is this a defect"; the sites answer "what is the bound", which
is the question that actually matters once you know MAX_HOPS=10 was
silently capping a backfill at ten hops a week.

The self-test gains a fifth case built from the census shape, so this
specific collapse can never regress unnoticed.
"""
import io, json, os, time, zipfile
from pathlib import Path
from urllib.request import urlopen
import boto3
from botocore.config import Config
from ops_report import report

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
FN = "justhodl-fleet-integrity"
MARKER = "fleet-integrity v1.1.1 ops4244 multi-site"
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

with report("4244_d1_multisite") as rep:
    rep.heading("ops 4244 — D1 v1.1.1 multi-site classification")
    fails=[]

    rep.section("1. Deploy")
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

    rep.section("2. GATE A — self-test, now 5 cases")
    wait_active(FN)
    r=lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                 Payload=json.dumps({"mode":"selftest"}).encode())
    b=json.loads(r["Payload"].read() or b"{}")
    for c in b.get("cases",[]):
        (rep.ok if c["pass"] else rep.fail)(
            "   %-11s expect=%-15s got=%-15s %s"
            %(c["case"],c["expect"],c["got"],json.dumps(c.get("detail"))[:90]))
        rep.kv(section="selftest", case=c["case"], expect=c["expect"],
               got=c["got"], passed=c["pass"])
    if not b.get("passed"): fails.append("self-test failed")
    else: rep.ok("%d/%d"%(len(b.get("cases",[])),len(b.get("cases",[]))))

    rep.section("3. Rescan (cache is keyed by sha; code changed, so rescan)")
    try:
        s3.delete_object(Bucket=BUCKET,
                         Key="data/_state/d1-classification-cache.json")
        s3.delete_object(Bucket=BUCKET, Key="data/_state/d1-scan-cursor.json")
        rep.log("cache cleared — classifier semantics changed, stale "
                "classifications would be worse than none")
    except Exception as e:
        rep.warn("cache clear: %s"%str(e)[:100])
    scan={}
    for attempt in range(4):
        wait_active(FN)
        r=lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                     Payload=json.dumps({"mode":"d1scan"}).encode())
        scan=json.loads(r["Payload"].read() or b"{}")
        rep.log("pass %d -> %s"%(attempt+1, json.dumps(scan)[:200]))
        if scan.get("complete"): break
    rep.kv(section="scan", scanned=scan.get("scanned"),
           failed=scan.get("failed"), total=scan.get("total"),
           complete=scan.get("complete"))
    if scan.get("failed"):
        rep.warn("%s function(s) had no lambda_function.py to parse "
                 "(non-Python or nested handler) — reported, not assumed "
                 "clean"%scan.get("failed"))

    rep.section("4. GATE B — the three real engines")
    cache=json.loads(s3.get_object(Bucket=BUCKET,
        Key="data/_state/d1-classification-cache.json")["Body"].read())
    by={v.get("fn"):v for v in cache.values()}
    checks=[("justhodl-13f-clone-alpha","BOUNDED",10),
            ("justhodl-equity-research","BOUNDED",None),
            ("justhodl-fundamental-census","BOUNDED",12)]
    for fn,exp_cls,exp_bound in checks:
        v=by.get(fn) or {}
        got=v.get("cls"); det=v.get("detail") or {}
        good = (got==exp_cls)
        if exp_bound is not None:
            good = good and exp_bound in (det.get("bounds") or [])
        (rep.ok if good else rep.fail)(
            "   %-34s cls=%-14s sites=%s bounds=%s flags=%s"
            %(fn,got,det.get("n_sites"),det.get("bounds"),det.get("flags")))
        rep.kv(section="real_engines", function=fn, cls=got,
               sites=det.get("n_sites"), bounds=str(det.get("bounds")),
               flags=str(det.get("flags")), passed=good)
        if not good:
            fails.append("%s cls=%s bounds=%s"%(fn,got,det.get("bounds")))
    counts={}
    for v in by.values(): counts[v.get("cls")]=counts.get(v.get("cls"),0)+1
    rep.log("fleet: %s"%counts)
    ung=[(f,v.get("detail")) for f,v in by.items() if v.get("cls")=="UNGUARDED"]
    rep.log("GENUINELY UNGUARDED: %d"%len(ung))
    for f,d in ung[:20]:
        rep.fail("   %-40s %s"%(f,json.dumps(d)[:110]))
        rep.kv(section="unguarded", function=f, detail=json.dumps(d)[:120])
    for f,v in sorted(by.items()):
        if v.get("cls")=="BOUNDED":
            d=v.get("detail") or {}
            rep.log("   bounded %-38s sites=%s bounds=%s flags=%s"
                    %(f,d.get("n_sites"),d.get("bounds"),d.get("flags")))

    rep.section("5. Full audit with the corrected D1")
    wait_active(FN)
    r=lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                 Payload=json.dumps({"mode":"audit"}).encode())
    b=json.loads(r["Payload"].read() or b"{}")
    rep.ok("audit -> %s"%json.dumps(b)[:250])
    rep.kv(section="audit", defects=b.get("n_defects"), new=b.get("n_new"),
           fixed=b.get("n_fixed"), sev1=b.get("sev1"))

    rep.section("RESULT")
    if fails:
        for f in fails: rep.fail("  %s"%f)
        raise SystemExit("FAILS: %s"%"; ".join(fails[:3]))
    rep.ok("OPS 4244 PASS")
