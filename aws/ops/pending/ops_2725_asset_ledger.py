"""ops 2725 — FOOTPRINT v1.1: ASSET LEDGER (Khalid: per-class buy/dump $, lit + dark).

CFTC deep-view rot fixed (cache stored int where series belonged) -> the
treasuries/gold/crypto/FX FUTURES institutional leg rejoins. Footprint v1.1
adds the ASSET LEDGER (per class: LIT ETF 5d $ | DARK 5d $ est from the
off-exchange tape | CFTC net-spec | verdict), DARK-$-BY-SECTOR rollup with
top names, 13F per-stock dollars, primary-dealer net. Page table + strip.
Report: aws/ops/reports/2725_asset_ledger.json.
"""
import os, io, json, time, zipfile, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=290, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2725, "ts": datetime.now(timezone.utc).isoformat()}
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
print("== 1/3 CFTC deep-view fix ==")
retry(lambda: (wait_ok("justhodl-cftc-deep-view"), lam.update_function_code(FunctionName="justhodl-cftc-deep-view", ZipFile=zip_fn("justhodl-cftc-deep-view")))[-1], "cftc")
wait_ok("justhodl-cftc-deep-view")
try:
    _cc = json.loads(s3.get_object(Bucket=BUCKET, Key="data/cftc-all-cache.json")["Body"].read())
    print("  CACHE top keys:", sorted(map(str, _cc.keys()))[:16])
    for _k, _v in list(_cc.items())[:4]:
        if isinstance(_v, dict):
            print("   sample %r -> dict keys %s" % (_k, sorted(map(str, _v.keys()))[:8]))
        elif isinstance(_v, list):
            print("   sample %r -> list[%d] first=%s" % (_k, len(_v),
                  sorted((_v[0] or {}).keys())[:8] if _v and isinstance(_v[0], dict) else type(_v[0]).__name__ if _v else "-"))
        else:
            print("   sample %r -> %s %s" % (_k, type(_v).__name__, str(_v)[:40]))
except Exception as e:
    print("  CACHE read err:", str(e)[:90])
cfg = lam.get_function_configuration(FunctionName="justhodl-cftc-deep-view")
print("  deployed sha:", cfg.get("CodeSha256", "")[:20], "| handler:", cfg.get("Handler"),
      "| modified:", str(cfg.get("LastModified"))[:19])
r = lam.invoke(FunctionName="justhodl-cftc-deep-view", InvocationType="RequestResponse")
full = (r["Payload"].read() or b"").decode("utf-8", "ignore")
if r.get("FunctionError"):
    try:
        err = json.loads(full)
        print("  cftc ERRTYPE:", err.get("errorType"), "|", err.get("errorMessage"))
        for fr in (err.get("stackTrace") or []):
            print("    FRAME:", str(fr).replace("\n", " ").strip()[:160])
    except Exception:
        print("  cftc ERR raw:", full[:1200])
else:
    print("  cftc ->", full[:220])
assert not r.get("FunctionError"), full[:500]
c = json.loads(s3.get_object(Bucket=BUCKET, Key="data/cftc-deep-view.json")["Body"].read())
R["cftc"] = {"top_keys": sorted(c.keys())[:10], "n_analyzed": c.get("n_contracts_analyzed")}
print("  cftc feed keys:", R["cftc"]["top_keys"], "| n_analyzed:", c.get("n_contracts_analyzed"))
aa = c.get("all_contract_analyses") or []
print("  contract statuses:", [(a.get("symbol"), a.get("status"), a.get("n_records")) for a in aa[:8]])

print("== 2/3 FOOTPRINT v1.1 ==")
retry(lambda: (wait_ok("justhodl-institutional-footprint"), lam.update_function_code(FunctionName="justhodl-institutional-footprint", ZipFile=zip_fn("justhodl-institutional-footprint")))[-1], "fp")
wait_ok("justhodl-institutional-footprint")
r = lam.invoke(FunctionName="justhodl-institutional-footprint", InvocationType="RequestResponse")
pay = json.loads(r["Payload"].read() or b"{}")
print("  invoke ->", json.dumps(pay)[:260])
assert not r.get("FunctionError") and pay.get("ok"), pay
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/institutional-footprint.json")["Body"].read())
L, DS, SU = d.get("asset_ledger") or {}, d.get("dark_by_sector") or {}, d.get("stocks_usd_13f") or {}
R["ledger"] = L
R["dark_sectors"] = {k: v["dark_usd_5d_m_est"] for k, v in list(DS.items())[:11]}
R["stocks_usd"] = {"buys": SU.get("buys", [])[:5], "sells": SU.get("sells", [])[:5]}
R["pd_net"] = d.get("primary_dealer_net")
R["posture"] = d.get("posture")
print("  LEDGER:", json.dumps(L, default=str)[:700])
print("  DARK sectors:", json.dumps(R["dark_sectors"]))
print("  13F $:", json.dumps(R["stocks_usd"], default=str)[:300])
lit_n = sum(1 for v in L.values() if isinstance(v.get("lit_etf_5d_usd_m"), (int, float)))
cftc_n = sum(1 for v in L.values() if isinstance(v.get("cftc_net_spec"), (int, float)))
assert len(L) >= 9 and lit_n >= 8, "ledger lit thin: %d/%d" % (lit_n, len(L))
assert isinstance((L.get("EQUITY") or {}).get("dark_5d_usd_m_est"), (int, float)) and L["EQUITY"]["dark_5d_usd_m_est"] > 500, "dark equity $ missing"
assert cftc_n >= 3, "cftc classes thin: %d (keys %s)" % (cftc_n, R["cftc"]["top_keys"])
assert len(DS) >= 8, "dark sectors thin"
assert d["posture"].get("risk_now") is not None
p_now = d["posture"].get("now_components") or {}
R["cftc_in_composite"] = "cftc_risk_appetite" in p_now
print("  cftc in NOW composite:", R["cftc_in_composite"])

print("== 3/3 PAGE + REPORT ==")
time.sleep(70)
try:
    with urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/institutional-footprint.html",
                                headers={"User-Agent": "jh/1"}), timeout=20) as rr:
        R["page"] = "LIVE" if "ASSET LEDGER" in rr.read().decode("utf-8", "ignore") else "200_no_marker"
except Exception as e:
    R["page"] = "propagating: " + str(e)[:60]
print("  page:", R["page"])
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2725_asset_ledger.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2725 COMPLETE — every asset class, lit and dark, on one ledger")

# rev2 fullstack

# rev3 frame-dump

# rev4 shape-aware

# rev5 cache-truth

# rev7 statuses
