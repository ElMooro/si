"""ops 2714 — DARK POOL v2.0: full-resource fusion + the missing schedule.

Audit findings fixed: (1) justhodl-dark-pool had schedule:None while SEVEN
engines consume its feed — daily rule created; (2) daily FINRA regsho tape
(finra-short: short + TOTAL TRF volume, with history) now FUSED per-name
(daily_short_pct/z, conviction upgrades, DISTRIBUTION_INTO_STRENGTH flag);
(3) OWN market-level DIX proxy ($vol-weighted 1-short% across the daily
tape) — independent of the SqueezeMetrics CSV, published with history+z;
(4) FINRA monthly ATS probed in-engine (block fusion staged on OK);
(5) board row "Dark Pool"; (6) page v2 strip. Probes also record the
SqueezeMetrics feed freshness and Quiver offexchange entitlement.
Report: aws/ops/reports/2714_dark_pool_v2.json.
"""
import os, io, json, time, zipfile, re, urllib.request, urllib.error
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=290, retries={"max_attempts": 1}))
ev = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2714, "ts": datetime.now(timezone.utc).isoformat()}
def sect(t): print("\n" + "=" * 8 + " " + t + " " + "=" * 8)
def get(url, timeout=22, data=None, hdr=None):
    req = urllib.request.Request(url, data=data, headers={"User-Agent": "jh/1", **(hdr or {})})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "ignore")
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

sect("1/4 PROBES — dix freshness, FINRA monthly, quiver offexchange")
try:
    h = s3.head_object(Bucket=BUCKET, Key="data/dix.json")
    age = (datetime.now(timezone.utc) - h["LastModified"]).total_seconds() / 3600
    R["dix_feed_age_h"] = round(age, 1); print("  data/dix.json age %.1fh" % age)
except Exception:
    R["dix_feed_age_h"] = None; print("  data/dix.json MISSING")
try:
    body = json.dumps({"limit": 3, "compareFilters": [{"compareType": "EQUAL", "fieldName": "summaryTypeCode", "fieldValue": "ATS_M_SMBL"}]}).encode()
    j = json.loads(get("https://api.finra.org/data/group/otcMarket/name/monthlySummary", data=body,
                       hdr={"Content-Type": "application/json", "Accept": "application/json"}))
    R["finra_monthly_probe"] = ("OK n=%d keys=%s" % (len(j), sorted(j[0].keys())[:6])) if isinstance(j, list) and j else str(j)[:120]
except Exception as e:
    R["finra_monthly_probe"] = "ERR " + str(e)[:80]
print("  FINRA monthly:", R["finra_monthly_probe"])
try:
    tok = re.findall(r"Token ([A-Za-z0-9\.\-_]+)", open("aws/lambdas/justhodl-political-stocks/source/lambda_function.py").read())
    if tok:
        try:
            get("https://api.quiverquant.com/beta/live/offexchange", hdr={"Authorization": "Token " + tok[0]}, timeout=15)
            R["quiver_offexchange_probe"] = 200
        except urllib.error.HTTPError as he:
            R["quiver_offexchange_probe"] = he.code
    else:
        R["quiver_offexchange_probe"] = "no-token"
except Exception as e:
    R["quiver_offexchange_probe"] = str(e)[:60]
print("  quiver offexchange:", R["quiver_offexchange_probe"])

sect("2/4 DEPLOY dark-pool v2 + CREATE MISSING SCHEDULE + RUN")
print("  settling 30s…"); time.sleep(30)
for fn in ("justhodl-dark-pool", "justhodl-signal-board"):
    retry(lambda f=fn: (wait_ok(f), lam.update_function_code(FunctionName=f, ZipFile=zip_fn(f)))[-1], fn)
    wait_ok(fn); print("  synced", fn)
cfg = json.load(open("aws/lambdas/justhodl-dark-pool/config.json"))
sch = cfg["schedule"]
arn = "arn:aws:lambda:%s:857687956942:function:justhodl-dark-pool" % REGION
ra = ev.put_rule(Name=sch["name"], ScheduleExpression=sch["expression"], State="ENABLED", Description=sch["description"])["RuleArn"]
try:
    lam.add_permission(FunctionName="justhodl-dark-pool", StatementId="evt-" + sch["name"],
                       Action="lambda:InvokeFunction", Principal="events.amazonaws.com", SourceArn=ra)
except lam.exceptions.ResourceConflictException:
    pass
ev.put_targets(Rule=sch["name"], Targets=[{"Id": "1", "Arn": arn}])
print("  SCHEDULE CREATED:", sch["expression"], "(was None — 7 consumers unblocked)")
r = lam.invoke(FunctionName="justhodl-dark-pool", InvocationType="RequestResponse")
pay = json.loads(r["Payload"].read() or b"{}")
print("  invoke ->", json.dumps(pay)[:200])
assert not r.get("FunctionError"), pay
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/dark-pool.json")["Body"].read())
DF, DX, MO = d.get("daily_fusion") or {}, d.get("dix") or {}, d.get("monthly_ats") or {}
R["v2"] = {"version": d.get("version"), "latest_week": d.get("latest_week"), "n_scored": d.get("n_scored"),
           "daily_joined": DF.get("joined"), "of": DF.get("of"),
           "own_dix": DX.get("own_dix_pct"), "own_dix_z": DX.get("own_dix_z"),
           "sq_dix": DX.get("squeezemetrics_dix"), "dix_read": DX.get("read"),
           "monthly": MO.get("status"), "monthly_note": MO.get("note") or MO.get("err"),
           "hi_conviction": sum(1 for x in d.get("board") or [] if x.get("conviction") == "HIGH"),
           "dist_into_strength": [x["ticker"] for x in d.get("board") or [] if x.get("flag") == "DISTRIBUTION_INTO_STRENGTH"][:8],
           "sample": [{k: x.get(k) for k in ("ticker", "state", "dark_pool_pct", "daily_short_pct", "daily_short_z", "conviction", "flag")} for x in (d.get("board") or [])[:5]]}
print(json.dumps(R["v2"], indent=1, default=str)[:1200])
assert d.get("version") == "2.0.0"
assert isinstance(DF.get("joined"), int) and DF["joined"] >= 30, "daily fusion thin: %s" % DF
assert isinstance(DX.get("own_dix_pct"), (int, float)) and 35 <= DX["own_dix_pct"] <= 75, "own DIX insane: %s" % DX.get("own_dix_pct")
assert MO.get("status") in ("OK", "UNAVAILABLE")
assert any("daily_short_pct" in x for x in (d.get("board") or [])[:20])
r = lam.invoke(FunctionName="justhodl-signal-board", InvocationType="RequestResponse")
assert not r.get("FunctionError")
sb = s3.get_object(Bucket=BUCKET, Key="data/signal-board.json")["Body"].read().decode()
m = re.search(r"\{[^{}]*Dark Pool[^{}]*\}", sb)
assert m, "board row missing"
R["board"] = m.group(0)[:200]; print("  board:", R["board"])

sect("3/4 PAGE")
time.sleep(70)
try:
    R["page"] = "LIVE" if "OWN-DIX" in get("https://justhodl.ai/dark-pool.html") else "200_no_marker"
except Exception as e:
    R["page"] = "propagating: " + str(e)[:60]
print("  page:", R["page"])

sect("4/4 REPORT")
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2714_dark_pool_v2.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2714 COMPLETE — dark pool now runs the full FINRA stack, daily")
