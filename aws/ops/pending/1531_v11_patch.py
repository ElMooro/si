# ops 1531 — deploy v1.1 patches (tide TGA units, apex falsy-zero + 1src damp + log errors) + verify
import json, os, time, zipfile, io, boto3
from botocore.config import Config
from botocore.exceptions import ClientError
cfg = Config(read_timeout=300, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
out = {"ops": 1531}


def retry_conflict(fn, tries=10, wait=8):
    for i in range(tries):
        try:
            return fn()
        except ClientError as e:
            if e.response["Error"]["Code"] in ("ResourceConflictException", "TooManyRequestsException"):
                time.sleep(wait); continue
            raise
    raise RuntimeError("retries exhausted")


def settle(fn_name):
    for _ in range(40):
        c = lam.get_function_configuration(FunctionName=fn_name)
        if c.get("LastUpdateStatus") in ("Successful", None) and c.get("State") in ("Active", None):
            return c
        time.sleep(3)
    return c


def zip_src(src):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for r, _, fs in os.walk(src):
            for f in fs:
                if "__pycache__" in r or f.endswith(".pyc"):
                    continue
                zf.write(os.path.join(r, f), arcname=os.path.relpath(os.path.join(r, f), src))
    return buf.getvalue()


def rd(key):
    try:
        return json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key=key)["Body"].read())
    except Exception as e:
        return {"_err": str(e)[:80]}


for fn, src in (("justhodl-global-tide", "aws/lambdas/justhodl-global-tide/source"),
                ("justhodl-apex-fusion", "aws/lambdas/justhodl-apex-fusion/source")):
    z = zip_src(src)
    retry_conflict(lambda: lam.update_function_code(FunctionName=fn, ZipFile=z))
    settle(fn)
    r = retry_conflict(lambda: lam.invoke(FunctionName=fn, InvocationType="RequestResponse",
                                          Payload=json.dumps({"no_tg": True}).encode()))
    out[fn] = {"function_error": r.get("FunctionError", "NONE"),
               "resp": r["Payload"].read().decode()[:180]}
    time.sleep(2)

gt = rd("data/global-tide.json")
out["global_tide"] = {"version": gt.get("version"), "headline": gt.get("headline"),
                      "fed": gt.get("fed"), "gli": gt.get("gli"), "risk": gt.get("risk"),
                      "spx_60d_pct": gt.get("spx_60d_pct"),
                      "indicators": {k: {kk: v.get(kk) for kk in ("signal", "spx_60d_pct", "gli_impulse", "impulse", "score")}
                                     for k, v in (gt.get("indicators") or {}).items()}}
ax = rd("data/apex-fusion.json")
out["apex"] = {"version": ax.get("version"), "by_tier": ax.get("by_tier"),
               "tier_inversion": ax.get("tier_inversion"),
               "n_logged": ax.get("n_logged_to_ddb"), "log_errors": ax.get("log_errors"),
               "weights": ax.get("weights_used"),
               "top8": [{k: t.get(k) for k in ("ticker", "apex_score", "tier", "n_sources", "sources", "fade_flag", "price")}
                        for t in (ax.get("top") or [])[:8]]}

open("aws/ops/reports/1531_v11.json", "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps({"tide_fed_nl": (out["global_tide"]["fed"] or {}).get("net_liquidity_usd_bn"),
                  "tide_g4": (out["global_tide"]["gli"] or {}).get("g4_stock_usd_tn"),
                  "apex_inv": (out["apex"]["tier_inversion"] or {}).get("active"),
                  "apex_logged": out["apex"]["n_logged"], "apex_logerr": out["apex"]["log_errors"]}, default=str))
