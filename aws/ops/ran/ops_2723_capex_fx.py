"""ops 2723 — CAPEX FX (Khalid's correction): publish foreign issuers, don't exclude.

FMP reportedCurrency detected per name; local-currency capex converted to USD
at spot (FRED DEX cache primary, FMP /stable/quote forex fallback, memoized).
TTM and prior share the spot, so yoy%% equals the local-currency truth. TSM,
TM, SONY, the Japanese banks, NVO, BABA rejoin the market totals with fx
provenance; the 35%%-mcap gate now catches only true contamination.
Report: aws/ops/reports/2723_capex_fx.json.
"""
import os, io, json, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=290, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2723, "ts": datetime.now(timezone.utc).isoformat()}
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
retry(lambda: (wait_ok("justhodl-capex-pulse"), lam.update_function_code(FunctionName="justhodl-capex-pulse", ZipFile=zip_fn("justhodl-capex-pulse")))[-1], "capex")
wait_ok("justhodl-capex-pulse")
r = lam.invoke(FunctionName="justhodl-capex-pulse", InvocationType="RequestResponse")
pay = json.loads(r["Payload"].read() or b"{}")
print("invoke ->", json.dumps(pay)[:240])
assert not r.get("FunctionError") and pay.get("ok"), pay
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/capex-pulse.json")["Body"].read())
HY, MK = d["hyperscalers"], d["market"]
FXC = d.get("fx_converted") or []
R["fx_converted"] = [(x["ticker"], x["ccy"], x["capex_ttm_b"], x["src"]) for x in FXC]
R["excluded"] = [(e["ticker"], e.get("capex_ttm_b"), (e.get("why") or "")[:36]) for e in (d.get("excluded_outliers") or [])]
R["market"] = MK
R["hyp"] = {k: HY.get(k) for k in ("total_ttm_b", "yoy_pct")}
print("fx_converted:", R["fx_converted"])
print("excluded:", R["excluded"])
print("market:", json.dumps(MK), "| hyperscalers:", json.dumps(R["hyp"]))
assert len(FXC) >= 5, "fx conversion missing: %d" % len(FXC)
assert d.get("version") == "1.1.0"
tsm = next((r0 for r0 in d["rows"] if r0["ticker"] == "TSM"), None)
assert tsm and 20 <= tsm["capex_ttm_b"] <= 65 and tsm.get("fx"), "TSM conversion off: %s" % tsm
tm = next((r0 for r0 in d["rows"] if r0["ticker"] == "TM"), None)
assert tm is None or (15 <= tm["capex_ttm_b"] <= 50 and tm.get("fx")), "TM conversion off: %s" % tm
assert len(d.get("excluded_outliers") or []) <= 4, "gate still over-firing"
assert isinstance(MK.get("capex_ttm_b"), (int, float)) and 900 <= MK["capex_ttm_b"] <= 3500
assert isinstance(HY.get("total_ttm_b"), (int, float)) and HY["total_ttm_b"] >= 150
R["TSM"], R["TM"] = tsm, tm
r = lam.invoke(FunctionName="justhodl-global-flow-desk", InvocationType="RequestResponse")
assert not r.get("FunctionError")
g = json.loads(s3.get_object(Bucket=BUCKET, Key="data/global-flow-desk.json")["Body"].read())
R["gfd_capex"] = g.get("capex")
print("gfd capex:", json.dumps(R["gfd_capex"], default=str)[:220])
assert (R["gfd_capex"] or {}).get("source") == "capex-pulse"
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2723_capex_fx.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2723 COMPLETE — foreign capex published in USD")
