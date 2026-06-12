# ops 1634 — sentinel v1.4 deploy+verify; PROBE banked carries: FMP insider/institutional
# ownership endpoints, key-metrics SBC key, finra-short feed shape (factor 11/12/13 truth)
import json, zipfile, io, os, time, urllib.request
import boto3
from botocore.config import Config
cfg = Config(read_timeout=880, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
K = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
out = {"ops": 1634, "probes": {}}
def jget(u):
    req = urllib.request.Request(u, headers={"User-Agent": "JustHodl Research admin@justhodl.ai"})
    return json.loads(urllib.request.urlopen(req, timeout=20).read())
for name, url in [
  ("insider_ownership", f"https://financialmodelingprep.com/stable/insider-ownership?symbol=AAPL&apikey={K}"),
  ("institutional_ownership", f"https://financialmodelingprep.com/stable/institutional-ownership?symbol=AAPL&apikey={K}"),
  ("institutional_holders", f"https://financialmodelingprep.com/stable/institutional-holders?symbol=AAPL&apikey={K}"),
  ("share_float", f"https://financialmodelingprep.com/stable/shares-float?symbol=AAPL&apikey={K}"),
]:
    try:
        j = jget(url)
        row = j[0] if isinstance(j, list) and j else j
        out["probes"][name] = {"ok": True, "type": type(j).__name__,
                                "n": len(j) if isinstance(j, list) else 1,
                                "keys": sorted(list(row.keys()))[:16] if isinstance(row, dict) else str(row)[:120]}
    except Exception as e:
        out["probes"][name] = {"ok": False, "err": str(e)[:90]}
    time.sleep(0.3)
try:
    j = jget(f"https://financialmodelingprep.com/stable/key-metrics-ttm?symbol=AAPL&apikey={K}")
    ks = sorted(j[0].keys()) if isinstance(j, list) and j else []
    out["probes"]["km_sbc_keys"] = [k for k in ks if "tock" in k or "ompensation" in k] or "NONE"
except Exception as e:
    out["probes"]["km_sbc_keys"] = str(e)[:80]
try:
    fs = json.loads(s3.get_object(Bucket=B, Key="data/finra-short.json")["Body"].read())
    out["probes"]["finra_short"] = {"top_keys": sorted(fs.keys())[:12],
        "squeeze_n": len(fs.get("squeeze_candidates") or []),
        "row_keys": sorted((fs.get("squeeze_candidates") or [{}])[0].keys())[:14],
        "has_full_map": any(k in fs for k in ("all", "rows", "by_ticker", "tickers", "universe"))}
except Exception as e:
    out["probes"]["finra_short"] = str(e)[:90]
# sentinel deploy + verify
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    for root, _, fls in os.walk("aws/lambdas/justhodl-alert-sentinel/source"):
        for f2 in fls:
            fp = os.path.join(root, f2)
            z.write(fp, os.path.relpath(fp, "aws/lambdas/justhodl-alert-sentinel/source"))
for _ in range(6):
    try:
        lam.update_function_code(FunctionName="justhodl-alert-sentinel", ZipFile=buf.getvalue()); break
    except Exception as e:
        if "ResourceConflict" in str(e): time.sleep(8)
        else: raise
for _ in range(40):
    c = lam.get_function_configuration(FunctionName="justhodl-alert-sentinel")
    if c.get("LastUpdateStatus") != "InProgress":
        break
    time.sleep(3)
lam.invoke(FunctionName="justhodl-alert-sentinel", InvocationType="RequestResponse", Payload=b"{}")
ds = json.loads(s3.get_object(Bucket=B, Key="data/alert-sentinel.json")["Body"].read())
out["sentinel"] = {"version": ds.get("version"), "n_changes": ds.get("n_changes"),
                    "state_saved": ds.get("state_saved"),
                    "papers_watch": (ds.get("snapshot") or {}).get("papers"),
                    "ul_leader": (ds.get("snapshot") or {}).get("ul_leader"),
                    "new_msgs": [m for m in (ds.get("changes") or []) if "research" in m or "Underlooked" in m][:5]}
os.makedirs("aws/ops/reports", exist_ok=True)
open("aws/ops/reports/1634_probe_carries.json", "w").write(json.dumps(out, indent=1, default=str))
print(json.dumps({"sent_v": out["sentinel"]["version"], "probes": list(out["probes"].keys())}))
