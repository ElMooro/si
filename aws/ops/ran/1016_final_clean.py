#!/usr/bin/env python3
"""Step 1016 — FINAL cleanup pass: deploy fixed near-miss-monitor + 
expanded engine-signal-map; verify unknowns → 0 and near-miss counts > 0
for the opportunities + crisis-composite signals.
"""
import io, json, os, time, zipfile, pathlib
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1016_final_clean.json"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def deploy_and_invoke(name):
    src_dir = pathlib.Path(f"aws/lambdas/{name}/source")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in src_dir.glob("*.py"):
            zf.writestr(f.name, f.read_bytes())
    zb = buf.getvalue()
    
    for attempt in range(4):
        try:
            lam.update_function_code(FunctionName=name, ZipFile=zb, Publish=False)
            lam.get_waiter("function_updated").wait(FunctionName=name)
            break
        except Exception as e:
            if "ResourceConflict" in str(e) and attempt < 3:
                time.sleep(5 * (attempt + 1))
                continue
            return {"deploy_err": str(e)[:300]}
    
    time.sleep(2)
    r = lam.invoke(FunctionName=name, InvocationType="RequestResponse", Payload=b"{}")
    body = r["Payload"].read().decode("utf-8", errors="replace")
    out = {"zip_size": len(zb), "fn_err": r.get("FunctionError")}
    try:
        p = json.loads(body)
        out["result"] = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
    except Exception:
        out["raw"] = body[:500]
    return out


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    out["engine_signal_map"] = deploy_and_invoke("justhodl-engine-signal-map")
    time.sleep(3)
    out["near_miss_monitor"] = deploy_and_invoke("justhodl-near-miss-monitor")
    time.sleep(3)
    out["miss_detector"] = deploy_and_invoke("justhodl-miss-detector")
    time.sleep(3)
    out["miss_calibrator"] = deploy_and_invoke("justhodl-miss-calibrator")
    time.sleep(3)
    out["alpha_compass"] = deploy_and_invoke("justhodl-alpha-compass")
    
    # Verify final state
    for k in ("data/engine-signal-map.json",
              "data/near-misses-by-signal.json",
              "data/miss-summary.json",
              "data/miss-calibrator-proposals.json",
              "data/alpha-compass.json"):
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=k)
            d = json.loads(obj["Body"].read().decode())
            if "engine-signal-map" in k:
                out["engine_map_final"] = {
                    "totals": d.get("totals"),
                    "unknown_remaining": d.get("unknown_signal_types", [])[:10],
                }
            elif "near-misses-by-signal" in k:
                out["near_miss_final"] = {
                    "totals": d.get("totals"),
                    "near_misses_by_signal": d.get("near_misses_by_signal"),
                    "diagnostics": d.get("diagnostics", [])[:8],
                }
            elif "miss-summary" in k:
                out["miss_summary_final"] = {
                    "totals": d.get("totals"),
                    "near_misses_by_signal": d.get("near_misses_by_signal"),
                    "near_miss_monitor_meta": d.get("near_miss_monitor"),
                }
            elif "miss-calibrator-proposals" in k:
                out["miss_calibrator_final"] = {
                    "totals": d.get("totals"),
                    "proposals": d.get("proposals", [])[:6],
                }
            elif "alpha-compass" in k:
                out["alpha_compass_final"] = {
                    "top_calls": len(d.get("top_calls", [])),
                    "watchlist": len(d.get("watchlist", [])),
                    "cards_with_distribution": sum(
                        1 for c in d.get("top_calls", []) + d.get("watchlist", [])
                        if c.get("distribution")
                    ),
                    "feeds": {kk: v.get("present") for kk, v in (d.get("source_feeds") or {}).items()},
                }
        except Exception as e:
            out[f"{k}_err"] = str(e)[:150]
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
