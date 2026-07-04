"""ops 2801 — DEFINITIVE: download equity-research's deployed code, verify the cap
enforcement (within_daily_cap) is actually present in the bundled llm_router.py."""
import os, io, json, time, zipfile, urllib.request
from datetime import datetime, timezone
import boto3
lam = boto3.client("lambda", region_name="us-east-1")
R = {"ops": 2801, "ts": datetime.now(timezone.utc).isoformat(), "now_utc": datetime.now(timezone.utc).isoformat()}
checks = {}
for fn in ["justhodl-equity-research", "justhodl-ticker-deep-research", "justhodl-research-critique", "justhodl-news-wire"]:
    d = {}
    try:
        cfg = lam.get_function_configuration(FunctionName=fn)
        d["last_modified"] = cfg["LastModified"]
        # download code
        loc = lam.get_function(FunctionName=fn)["Code"]["Location"]
        raw = urllib.request.urlopen(loc, timeout=60).read()
        z = zipfile.ZipFile(io.BytesIO(raw))
        names = z.namelist()
        d["has_llm_router"] = "llm_router.py" in names
        d["has_llm_cost"] = "llm_cost.py" in names
        d["has_anthropic_shim"] = "anthropic_shim.py" in names
        router = z.read("llm_router.py").decode() if "llm_router.py" in names else ""
        shim = z.read("anthropic_shim.py").decode() if "anthropic_shim.py" in names else ""
        cost = z.read("llm_cost.py").decode() if "llm_cost.py" in names else ""
        d["router_has_cap"] = "within_daily_cap" in router
        d["shim_has_cap"] = "within_daily_cap" in shim
        d["cost_has_cap_fn"] = "def within_daily_cap" in cost
        d["router_haiku_fallback"] = "falling back to Haiku" in router
    except Exception as e:
        d["err"] = str(e)[:100]
    checks[fn] = d
R["checks"] = checks
# verdict: is the cap code deployed on the engines?
def ok(fn):
    c = checks.get(fn, {})
    return c.get("cost_has_cap_fn") and (c.get("router_has_cap") or c.get("shim_has_cap"))
R["cap_deployed_all"] = all(ok(fn) for fn in checks)
R["status"] = "CAP CODE DEPLOYED" if R["cap_deployed_all"] else "STALE — NEEDS REDEPLOY"
for fn, c in checks.items():
    print("%s | mod=%s | router_cap=%s shim_cap=%s cost_fn=%s haiku_fb=%s" % (
        fn, c.get("last_modified", "?")[:19], c.get("router_has_cap"), c.get("shim_has_cap"),
        c.get("cost_has_cap_fn"), c.get("router_haiku_fallback")))
print("VERDICT:", R["status"])
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/2801_deployed_code_check.json", "w"), indent=1, default=str)
print("OPS 2801 COMPLETE")
