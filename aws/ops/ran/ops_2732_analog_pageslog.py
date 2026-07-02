"""ops 2732 — PAGES-DEPLOY AUTOPSY (runner-side) + CRISIS-ANALOG ENGINE.

Step 0 downloads the failing "Deploy to GitHub Pages" job log from the runner
(sandbox egress cannot reach the Azure log blob) and prints/records the exact
error — the deploy step has failed 3x including one API rerun, so the cause
is content- or config-level and must be read, not guessed.
Steps 1-2 ship justhodl-positioning-analog: 7-dim weekly condition vector
(1997->), euclidean crisis analogs, SPX forward map, AI historian outlook with
deterministic fallback. Feed: data/positioning-analog.json (page v2.1 panel).
Report: aws/ops/reports/2732_analog_pageslog.json.
"""
import os, io, json, time, zipfile, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
GH = "https://api.github.com/repos/ElMooro/si"
TOK = os.environ.get("GH_API_TOKEN") or os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN") or ""  # run-ops.yml exports GH_API_TOKEN
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=290, retries={"max_attempts": 1}))
ev = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2732, "ts": datetime.now(timezone.utc).isoformat()}
def gh(path, raw=False):
    hdr = {"User-Agent": "jh-ops", "Accept": "application/vnd.github+json"}
    if TOK: hdr["Authorization"] = "token " + TOK
    req = urllib.request.Request(GH + path, headers=hdr)
    with urllib.request.urlopen(req, timeout=40) as r:
        b = r.read()
        return b if raw else json.loads(b)
def zip_fn(fn):
    src = "aws/lambdas/%s/source" % fn
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(src):
            if "__pycache__" in root: continue
            for f in files:
                z.write(os.path.join(root, f), os.path.relpath(os.path.join(root, f), src))
        for f in sorted(os.listdir("aws/shared")):
            if f.endswith(".py"): z.write(os.path.join("aws/shared", f), f)
    return buf.getvalue()
def wait_ok(fn, budget=240):
    t0 = time.time()
    while time.time() - t0 < budget:
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") in (None, "Successful"): return
        time.sleep(5)
def retry(call, what, tries=6):
    for i in range(tries):
        try: return call()
        except ClientError as e:
            if e.response["Error"]["Code"] in ("ResourceConflictException", "TooManyRequestsException"): time.sleep(18)
            else: raise
    raise RuntimeError(what)

print("settling 30s…"); time.sleep(30)
print("== 0/3 PAGES DEPLOY LOG (runner egress) ==")
try:
    jobs = gh("/actions/runs/28606752048/jobs")["jobs"]
    jid = next(j["id"] for j in jobs if j["name"] == "deploy")
    log = gh("/actions/jobs/%d/logs" % jid, raw=True).decode("utf-8", "ignore")
    errs = [ln.strip() for ln in log.splitlines()
            if any(k in ln.lower() for k in ("error", "fail", "invalid", "exceed", "denied", "unable"))
            and "0 error" not in ln.lower()][:12]
    print("  deploy job %d log %dB; error lines:" % (jid, len(log)))
    for ln in errs: print("   ", ln[:150])
    R["pages_deploy_errors"] = errs
except Exception as e:
    R["pages_deploy_errors"] = ["LOG_FETCH_FAILED: " + str(e)[:120]]
    print("  ", R["pages_deploy_errors"][0])

print("== 1/3 CREATE + RUN positioning-analog ==")
name = "justhodl-positioning-analog"
cfg = json.load(open("aws/lambdas/%s/config.json" % name)); zb = zip_fn(name)
try:
    lam.get_function(FunctionName=name); wait_ok(name)
    retry(lambda: lam.update_function_code(FunctionName=name, ZipFile=zb), name); wait_ok(name)
except lam.exceptions.ResourceNotFoundException:
    retry(lambda: lam.create_function(FunctionName=name, Runtime=cfg["runtime"], Role=cfg["role"],
          Handler=cfg["handler"], Code={"ZipFile": zb}, Timeout=cfg["timeout"], MemorySize=cfg["memory"],
          Architectures=cfg["architectures"], Description=cfg["description"][:250]), "create")
    wait_ok(name); print("  CREATED", name)
sch = cfg["schedule"]
ra = ev.put_rule(Name=sch["name"], ScheduleExpression=sch["expression"], State="ENABLED",
                 Description=sch["description"])["RuleArn"]
try:
    lam.add_permission(FunctionName=name, StatementId="evt-" + sch["name"], Action="lambda:InvokeFunction",
                       Principal="events.amazonaws.com", SourceArn=ra)
except lam.exceptions.ResourceConflictException: pass
ev.put_targets(Rule=sch["name"], Targets=[{"Id": "1", "Arn": "arn:aws:lambda:%s:857687956942:function:%s" % (REGION, name)}])
r = lam.invoke(FunctionName=name, InvocationType="RequestResponse")
pay = json.loads(r["Payload"].read() or b"{}")
print("  invoke ->", json.dumps(pay, default=str)[:300])
assert not r.get("FunctionError") and pay.get("ok"), pay
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/positioning-analog.json")["Body"].read())
R["analog"] = {"weeks": d["panel_weeks"], "dims": d["dims"], "verdict": d["verdict"],
               "as_of": d["as_of_week"], "vector": d["vector_today_z"],
               "top3": d["analogs"][:3], "stats": d["forward_stats"],
               "outlook_src": d["ai_outlook_src"], "outlook": d["ai_outlook"][:260]}
print("  verdict:", d["verdict"], "| top:", json.dumps(d["analogs"][0]))
print("  outlook[%s]: %s" % (d["ai_outlook_src"], d["ai_outlook"][:200]))
assert d["panel_weeks"] >= 900 and d["dims"] >= 5
print("  dim_depth:", json.dumps(d.get("dim_depth")), "dropped:", d.get("dims_dropped"))
assert len(d["analogs"]) >= 4 and all(a["similarity"] > 0 for a in d["analogs"])
labels = [a["label"] for a in d["analogs"] if a["label"] != "unlabeled regime"]
assert d["forward_stats"]["median_fwd_3m_pct"] is not None
br = d["ai_outlook"]
assert 90 <= len(br) <= 760 and br.rstrip().endswith((".", "!", "?")) and "**" not in br

print("== 2/3 public feed strict (worker /data route) ==")
okf = False
for a in range(3):
    time.sleep(20)
    try:
        req = urllib.request.Request("https://justhodl.ai/data/positioning-analog.json?v=%d" % a,
                                     headers={"User-Agent": "Mozilla/5.0", "Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=25) as rr:
            body = rr.read()
        json.loads(body.decode(), parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
        okf = True; print("  strict VALID (%dB)" % len(body)); break
    except Exception as e:
        print("  attempt %d: %s" % (a + 1, str(e)[:80]))
R["public_feed"] = okf
assert okf, "analog feed not strict-valid at domain"
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2732_analog_pageslog.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2732 COMPLETE — history now testifies on the desk")

# rev2 spx-chunks + GH_API_TOKEN

# rev3 tuple-weeks
