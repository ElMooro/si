#!/usr/bin/env python3
"""1071 — deploy liquidity-agent cache-first patch + verify $0.0T fix.

PATCH
═════
Previous: liquidity-agent hits FRED directly for 30+ series in burst, no
backoff, no cache. Hits 429 rate limit (shared FRED key with 30 other
Lambdas), gets nulls, writes $0 net liquidity.

Now: cache-first read from data/fred-cache.json (maintained by
justhodl-financial-secretary v2.2, 88% hit rate, 207 series cached
including WALCL/WTREGEN/RRPONTSYD with fresh data through 2026-05-27).
Falls back to live FRED only on cache miss, with exponential backoff
2s/4s/8s on 429.

VERIFIES
════════
  1. Lambda code re-deployed (force update)
  2. Sync-invoke returns non-zero net liquidity
  3. liquidity-data.json shows actual WALCL/WTREGEN/RRPONTSYD values
"""
import io, json, os, pathlib, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1071_liquidity_fix.json"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=180))
s3 = boto3.client("s3", region_name=REGION)


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # Phase 1: force redeploy
    print("[1071] phase 1: force redeploy liquidity-agent…")
    try:
        src = pathlib.Path("aws/lambdas/justhodl-liquidity-agent/source")
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for f in src.glob("*.py"):
                zf.writestr(f.name, f.read_bytes())
        zb = buf.getvalue()
        
        for attempt in range(3):
            try:
                lam.update_function_code(
                    FunctionName="justhodl-liquidity-agent",
                    ZipFile=zb, Publish=False,
                )
                lam.get_waiter("function_updated").wait(FunctionName="justhodl-liquidity-agent")
                break
            except Exception as e:
                if "ResourceConflict" in str(e) and attempt < 2:
                    time.sleep(5); continue
                raise
        out["redeploy"] = {"zip_size": len(zb), "status": "ok"}
    except Exception as e:
        out["redeploy_err"] = str(e)[:300]
        out["finished"] = datetime.now(timezone.utc).isoformat()
        pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
        pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
        return
    
    time.sleep(3)
    
    # Phase 2: sync-invoke
    print("[1071] phase 2: sync-invoke…")
    t0 = time.time()
    try:
        r = lam.invoke(FunctionName="justhodl-liquidity-agent",
                        InvocationType="RequestResponse", Payload=b"{}")
        body = r["Payload"].read().decode("utf-8", errors="replace")
        try:
            p = json.loads(body)
            result = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
            out["invoke"] = {
                "elapsed_s":           round(time.time() - t0, 1),
                "status":              r.get("StatusCode"),
                "net_liquidity_bn":    result.get("net_liquidity_bn"),
                "score":               result.get("score"),
                "label":               result.get("label"),
                "regime":              result.get("regime"),
                "spy_signal":          result.get("spy_signal"),
                "elapsed_sec":         result.get("elapsed_sec"),
            }
        except Exception:
            out["invoke"] = {
                "elapsed_s": round(time.time() - t0, 1),
                "raw":       body[:600],
            }
    except Exception as e:
        out["invoke_err"] = str(e)[:200]
    
    time.sleep(2)
    
    # Phase 3: read live S3 output
    print("[1071] phase 3: read liquidity-data.json…")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="liquidity-data.json")
        body = obj["Body"].read()
        d = json.loads(body)
        out["s3_snapshot"] = {
            "size_bytes":    len(body),
            "last_modified": obj["LastModified"].isoformat(),
            "meta":          d.get("meta", {}),
        }
        core = d.get("core", {})
        nl = core.get("net_liquidity", {})
        out["s3_snapshot"]["net_liquidity"] = {
            "value_bn":   nl.get("value_bn"),
            "label":      nl.get("label"),
            "components": nl.get("components", {}),
        }
        # 3 main metrics
        for sid in ["walcl", "wtregen", "rrpontsyd"]:
            entry = core.get(sid) or {}
            if isinstance(entry, dict):
                out["s3_snapshot"][f"core_{sid}"] = {
                    "latest":       entry.get("latest"),
                    "latest_value": entry.get("latest_value"),
                    "value_bn":     entry.get("value_bn"),
                    "trend":        entry.get("trend"),
                }
        for label_key in ["fed_balance_sheet", "tga", "rrp"]:
            entry = core.get(label_key) or {}
            if isinstance(entry, dict):
                out["s3_snapshot"][f"display_{label_key}"] = {
                    k: entry.get(k)
                    for k in ["latest_value", "value_bn", "as_of", "n_obs", "trend"]
                    if entry.get(k) is not None
                }
    except Exception as e:
        out["s3_snapshot_err"] = str(e)[:200]
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1071] DONE → {REPORT}")


if __name__ == "__main__":
    main()
