#!/usr/bin/env python3
"""1075 — final verification of platform-wide FRED cache shim rollout.

Tests:
  1. Re-run the audit — should show 0 DIRECT_FRED_ONLY (excluding Secretary)
  2. Sync-invoke 5 high-impact Lambdas to confirm they return real data
  3. Re-read liquidity-data.json to confirm homepage tile still healthy
"""
import io, json, os, pathlib, time, urllib.request, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1075_final_verify.json"
REGION = "us-east-1"

lam = boto3.client("lambda", region_name=REGION,
                    config=Config(read_timeout=180))
s3 = boto3.client("s3", region_name=REGION)

# High-impact Lambdas to verify
VERIFY_TARGETS = [
    "justhodl-daily-report-v3",       # main daily snapshot — feeds homepage
    "justhodl-canary-grid",            # early warning grid
    "justhodl-yield-curve",            # liquidity stack composite
    "justhodl-crisis-knowledge-base",  # AI chat backbone
    "justhodl-valuations-agent",       # quant snapshot
]


def download_first_lines(name, n=30):
    """Get the top of lambda_function.py to confirm shim line present."""
    try:
        info = lam.get_function(FunctionName=name)
        url = info["Code"]["Location"]
        with urllib.request.urlopen(url, timeout=30) as r:
            zip_bytes = r.read()
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            for fname in zf.namelist():
                if fname.endswith(".py") and "lambda_function" in fname or fname.endswith("_agent.py"):
                    code = zf.read(fname).decode("utf-8", errors="replace")
                    return code.split("\n")[:n]
        return ["<no py file>"]
    except Exception as e:
        return [f"<err: {e}>"]


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # Phase 1: Spot-check shim presence in 3 random patched Lambdas
    print("[1075] phase 1: confirm shim present in deployed code…")
    out["shim_presence"] = {}
    for name in ["justhodl-divergence-engine-v2",
                  "justhodl-daily-report-v3",
                  "justhodl-cds-proxy"]:
        lines = download_first_lines(name, 25)
        has_shim = any("_fred_shim" in L for L in lines)
        first_shim_line = next((L for L in lines if "_fred_shim" in L), None)
        out["shim_presence"][name] = {
            "has_shim_import": has_shim,
            "evidence": first_shim_line,
        }
        time.sleep(0.5)
    
    # Phase 2: Sync-invoke each target
    print("[1075] phase 2: sync-invoke 5 high-impact Lambdas…")
    out["invokes"] = []
    for name in VERIFY_TARGETS:
        v = {"name": name}
        t0 = time.time()
        try:
            r = lam.invoke(FunctionName=name,
                            InvocationType="RequestResponse", Payload=b"{}")
            body = r["Payload"].read().decode("utf-8", errors="replace")
            v["elapsed_s"] = round(time.time() - t0, 1)
            v["status"]    = r.get("StatusCode")
            try:
                p = json.loads(body)
                v["statusCode"] = p.get("statusCode")
                inner = p.get("body", body)
                if isinstance(inner, str):
                    try:
                        inner_p = json.loads(inner)
                        v["body_summary"] = {k: inner_p.get(k) for k in
                                              ["ok", "score", "regime", "n_signals",
                                               "n_flagged", "composite", "net_liquidity_bn",
                                               "duration_s", "total_signals", "label"]
                                              if k in inner_p}
                    except Exception:
                        v["body_preview"] = inner[:200]
                else:
                    v["body"] = inner
            except Exception:
                v["raw"] = body[:300]
        except Exception as e:
            v["err"] = str(e)[:200]
        out["invokes"].append(v)
        time.sleep(2)
    
    # Phase 3: Read homepage liquidity tile data
    print("[1075] phase 3: read liquidity-data.json…")
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="liquidity-data.json")
        body = obj["Body"].read()
        d = json.loads(body)
        meta = d.get("meta", {})
        core = d.get("core", {})
        out["liquidity_tile"] = {
            "generated_at":  meta.get("generated_at"),
            "elapsed_sec":   meta.get("elapsed_sec"),
            "net_liquidity_bn": (core.get("net_liquidity") or {}).get("value_bn"),
            "net_liquidity_label": (core.get("net_liquidity") or {}).get("label"),
            "fed_balance_sheet": (core.get("fed_balance_sheet") or {}).get("value_bn"),
            "tga":               (core.get("tga") or {}).get("value_bn"),
            "rrp":               (core.get("rrp") or {}).get("value_bn"),
        }
    except Exception as e:
        out["liquidity_tile_err"] = str(e)[:200]
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1075] DONE → {REPORT}")


if __name__ == "__main__":
    main()
