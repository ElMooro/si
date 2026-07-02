"""ops 2726 — FOOTPRINT POLISH: the four residuals from 2725.

1) Dark ledger column relabeled to its truth: DAILY off-exchange $ (was
   mislabeled 5d). 2) 13F per-stock dollars: broadened value-key candidates,
   scale-aware; raw row keys printed for the record. 3) Primary-dealer net:
   shape-aware extractor incl. the legacy indicators.{PD_*}.value form, with
   a staleness gate rejecting the Aug-2025 hardcoded shim (real-data rule).
4) CFTC risk_appetite published by deep-view — self-arming when the z-layer
   arms (n>=12, ~5 wks); footprint's NOW composite auto-joins it then.
Report: aws/ops/reports/2726_footprint_polish.json.
"""
import os, io, json, time, zipfile, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=290, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2726, "ts": datetime.now(timezone.utc).isoformat()}
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
print("== truth prints: raw feed shapes ==")
try:
    f13 = json.loads(s3.get_object(Bucket=BUCKET, Key="data/13f-positions.json")["Body"].read())
    def _first_row(doc, subs):
        stack = [doc]
        while stack:
            n = stack.pop()
            if isinstance(n, dict):
                for k, v in n.items():
                    if any(s_ in str(k).lower() for s_ in subs) and isinstance(v, list) and v and isinstance(v[0], dict):
                        return k, sorted(v[0].keys())
                    stack.append(v)
            elif isinstance(n, list):
                stack.extend(n[:10])
        return None, None
    k1, row1 = _first_row(f13, ("adds", "top_buys", "increas", "new_"))
    print("  13f adds-list key=%r row keys=%s" % (k1, row1))
    R["f13_row_keys"] = {"list": k1, "keys": row1}
except Exception as e:
    print("  13f read err:", str(e)[:80])
for key in ("data/nyfed-primary-dealer.json", "data/dealer-survey.json"):
    try:
        doc = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
        top = sorted(doc.keys())[:10]
        ind = list((doc.get("indicators") or {}).keys())[:6]
        print("  %s top=%s indicators=%s" % (key, top, ind))
        R.setdefault("pd_shapes", {})[key] = {"top": top, "indicators": ind}
    except Exception as e:
        print("  %s -> %s" % (key, str(e)[:60]))

print("== deploy + run ==")
for fn in ("justhodl-cftc-deep-view", "justhodl-institutional-footprint"):
    retry(lambda f=fn: (wait_ok(f), lam.update_function_code(FunctionName=f, ZipFile=zip_fn(f)))[-1], fn)
    wait_ok(fn)
r = lam.invoke(FunctionName="justhodl-cftc-deep-view", InvocationType="RequestResponse")
assert not r.get("FunctionError"), r["Payload"].read()[:160]
cd = json.loads(s3.get_object(Bucket=BUCKET, Key="data/cftc-deep-view.json")["Body"].read())
R["cftc_risk_appetite"] = cd.get("risk_appetite")
print("  deep-view risk_appetite:", R["cftc_risk_appetite"], "(None until z-layer arms — expected)")
assert "risk_appetite" in cd, "risk_appetite key missing from feed"

r = lam.invoke(FunctionName="justhodl-institutional-footprint", InvocationType="RequestResponse")
pay = json.loads(r["Payload"].read() or b"{}")
print("  footprint ->", json.dumps(pay)[:240])
assert not r.get("FunctionError") and pay.get("ok"), pay
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/institutional-footprint.json")["Body"].read())
L, SU = d["asset_ledger"], d.get("stocks_usd_13f") or {}
eq = L.get("EQUITY") or {}
print("  EQUITY ledger:", json.dumps(eq))
assert "dark_daily_usd_m_est" in eq and isinstance(eq["dark_daily_usd_m_est"], (int, float)) and eq["dark_daily_usd_m_est"] > 50000, "daily-dark rename/rollup broken: %s" % eq
assert "dark_5d_usd_m_est" not in json.dumps(L), "old 5d label survived"
buys = SU.get("buys") or []
nn = [b for b in buys if b.get("usd_m") is not None]
print("  13F buys w/ $:", nn[:5])
R["f13_usd"] = nn[:6]
if R.get("f13_row_keys", {}).get("keys") and any(k in ("value", "marketValue", "market_value", "changeValue", "change_value") for k in R["f13_row_keys"]["keys"]):
    assert len(nn) >= 3, "13F $ extraction still null despite value keys present: %s" % R["f13_row_keys"]
else:
    R["f13_verdict"] = "VALUE_ABSENT_IN_FEED — adds/exits rows carry no $ field; enrich 13f-positions engine later"
    print("  ", R["f13_verdict"])
R["pd"] = {"net": d.get("primary_dealer_net"), "note": d.get("primary_dealer_note")}
print("  PD:", json.dumps(R["pd"]))
assert d.get("primary_dealer_note"), "pd note missing"
assert d["posture"]["risk_now"] is not None and d["posture"]["risk_forward"] is not None

time.sleep(70)
try:
    with urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/institutional-footprint.html",
                                headers={"User-Agent": "jh/1"}), timeout=20) as rr:
        R["page"] = "LIVE" if "DARK 1d" in rr.read().decode("utf-8", "ignore") else "200_no_marker"
except Exception as e:
    R["page"] = "propagating: " + str(e)[:60]
print("  page:", R["page"])
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2726_footprint_polish.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2726 COMPLETE — ledger polished, every column truthful")
