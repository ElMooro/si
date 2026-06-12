# ops 1620 — P/S rollout: map v1.2.0 (timeout 600, first revenue build), insider v1.1.0, sentinel v1.2.0
import json, zipfile, io, os, time, base64
import boto3
from botocore.config import Config
cfg = Config(read_timeout=880, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1620}
def zipdir(d):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, fs in os.walk(d):
            for f in fs:
                fp = os.path.join(root, f)
                z.write(fp, os.path.relpath(fp, d))
    return buf.getvalue()
def upd(fn, zb):
    for _ in range(6):
        try:
            lam.update_function_code(FunctionName=fn, ZipFile=zb); break
        except Exception as e:
            if "ResourceConflict" in str(e): time.sleep(8)
            else: raise
    for _ in range(40):
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("LastUpdateStatus") != "InProgress" and c.get("State") != "Pending":
            return c
        time.sleep(3)
c = upd("justhodl-market-map", zipdir("aws/lambdas/justhodl-market-map/source"))
if (c.get("Timeout") or 0) < 600:
    lam.update_function_configuration(FunctionName="justhodl-market-map", Timeout=600)
    for _ in range(40):
        cc = lam.get_function_configuration(FunctionName="justhodl-market-map")
        if cc.get("LastUpdateStatus") != "InProgress": break
        time.sleep(3)
    out["map_cfg"] = "timeout->600"
r = lam.invoke(FunctionName="justhodl-market-map", InvocationType="RequestResponse",
                LogType="Tail", Payload=b"{}")
out["map_err"] = r.get("FunctionError", "NONE")
out["map_log"] = base64.b64decode(r.get("LogResult", "")).decode(errors="replace")[-1000:]
d = json.loads(s3.get_object(Bucket=B, Key="data/market-map.json")["Body"].read())
tiles = d.get("tiles") or []
top = sorted(tiles, key=lambda x: -(x.get("mc") or 0))[:6]
sa = d.get("sector_agg") or {}
out["map"] = {"version": d.get("version"), "diagnostics": d.get("diagnostics"),
               "ps_coverage": d.get("ps_coverage"), "n_tiles": d.get("n_tiles"),
               "tiles_top_ps": [{k: t.get(k) for k in ("t", "s", "ps", "m3")} for t in top],
               "sector_ps_medians": {k: v.get("ps_median") for k, v in sa.items()},
               "cheap_head": (d.get("cheap_candidates") or [])[:8],
               "ps_logged": d.get("ps_logged")}
g2 = json.loads(s3.get_object(Bucket=B, Key="data/sector-groups.json")["Body"].read())
out["groups_ps"] = [{"etf": g.get("etf"), "ps_median": g.get("ps_median")}
                     for g in (g2.get("groups") or [])[:6]]
g3 = json.loads(s3.get_object(Bucket=B, Key="data/themes.json")["Body"].read())
out["themes_ps"] = [{"theme": t.get("theme"), "ps_median": t.get("ps_median")}
                     for t in (g3.get("themes") or [])[:8]]
upd("justhodl-insider-radar", zipdir("aws/lambdas/justhodl-insider-radar/source"))
r2 = lam.invoke(FunctionName="justhodl-insider-radar", InvocationType="RequestResponse", Payload=b"{}")
out["ins_err"] = r2.get("FunctionError", "NONE")
di = json.loads(s3.get_object(Bucket=B, Key="data/insider-radar.json")["Body"].read())
out["insider"] = {"version": di.get("version"),
                   "clusters_ps": [{"t": c.get("ticker"), "ps": c.get("ps_ttm"),
                                     "ret60": c.get("ret_60d_pct")}
                                    for c in (di.get("clusters") or [])[:6]],
                   "decline_ps": [{"t": b.get("ticker"), "ps": b.get("ps_ttm"),
                                    "ret60": b.get("ret_60d_pct")}
                                   for b in (di.get("decline_buys") or [])[:5]],
                   "diag_tail": (di.get("diagnostics") or [])[-3:]}
upd("justhodl-alert-sentinel", zipdir("aws/lambdas/justhodl-alert-sentinel/source"))
r3 = lam.invoke(FunctionName="justhodl-alert-sentinel", InvocationType="RequestResponse", Payload=b"{}")
out["sen_err"] = r3.get("FunctionError", "NONE")
ds = json.loads(s3.get_object(Bucket=B, Key="data/alert-sentinel.json")["Body"].read())
out["sentinel"] = {"version": ds.get("version"), "n_changes": ds.get("n_changes"),
                    "state_saved": ds.get("state_saved"),
                    "value_pump": (ds.get("snapshot") or {}).get("value_pump"),
                    "changes": (ds.get("changes") or [])[:6]}
os.makedirs("aws/ops/reports", exist_ok=True)
open("aws/ops/reports/1620_ps_everywhere.json", "w").write(json.dumps(out, indent=1, default=str))
print(json.dumps({"map_err": out["map_err"], "ps_cov": out["map"]["ps_coverage"],
                   "cheap": len(out["map"]["cheap_head"]), "ins_err": out["ins_err"],
                   "sen_err": out["sen_err"]}, default=str))
