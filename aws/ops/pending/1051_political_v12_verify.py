#!/usr/bin/env python3
"""1051 — verify the v1.2 political-stocks fix:
1. Chamber count bug fixed (House + Senate should sum to ~1000)
2. Full party map auto-loaded (~535 mappings vs 39 hardcoded)
3. Bipartisan detection finally fires"""
import io, json, os, pathlib, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1051_political_v12_verify.json"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION,
                    config=Config(read_timeout=300))
long_lam = boto3.client("lambda", region_name=REGION,
                          config=Config(read_timeout=300))
s3 = boto3.client("s3", region_name=REGION)


def build_zip(name):
    src = pathlib.Path(f"aws/lambdas/{name}/source")
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in src.glob("*.py"):
            zf.writestr(f.name, f.read_bytes())
    return buf.getvalue()


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # Deploy
    zb = build_zip("justhodl-political-stocks")
    out["zip_size"] = len(zb)
    for attempt in range(4):
        try:
            lam.update_function_code(FunctionName="justhodl-political-stocks",
                                        ZipFile=zb, Publish=False)
            lam.get_waiter("function_updated").wait(FunctionName="justhodl-political-stocks")
            out["deploy"] = "ok"
            break
        except Exception as e:
            if "ResourceConflict" in str(e) and attempt < 3:
                time.sleep(5 * (attempt + 1)); continue
            out["deploy_err"] = str(e)[:300]; break
    
    time.sleep(3)
    
    # Sync-invoke
    print("[1051] invoking…")
    try:
        r = long_lam.invoke(FunctionName="justhodl-political-stocks",
                              InvocationType="RequestResponse", Payload=b"{}")
        body = r["Payload"].read().decode("utf-8", errors="replace")
        try:
            p = json.loads(body)
            out["invoke"] = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
        except Exception:
            out["invoke_raw"] = body[:400]
    except Exception as e:
        out["invoke_err"] = str(e)[:200]
    
    time.sleep(2)
    
    # Verify output
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/political-stocks.json")
        ps = json.loads(obj["Body"].read().decode("utf-8"))
        c = ps.get("congress") or {}
        
        out["snapshot"] = {
            "schema_version":    ps.get("schema_version"),
            "method":             ps.get("method"),
            "data_source":        ps.get("data_source"),
            "party_source":       ps.get("party_source"),
            
            "n_trades_total":     c.get("n_trades_total"),
            "n_trades_house":     c.get("n_trades_house"),
            "n_trades_senate":    c.get("n_trades_senate"),
            "n_tickers":          c.get("n_tickers"),
            "n_party_map":        c.get("n_party_map"),
            
            "chambers_sum_check": (c.get("n_trades_house") or 0) + (c.get("n_trades_senate") or 0),
            
            "n_clusters":         len(c.get("clusters") or []),
            "n_bipartisan":       len(c.get("bipartisan_buys") or []),
            "n_top_buys":         len(c.get("top_buys") or []),
        }
        
        # Top 5 buys with party split (should now show actual parties)
        out["snapshot"]["top_5_buys_with_parties"] = [
            {"ticker": r["ticker"], "score": r["score"],
             "n_buys": r["n_buys"], "n_sells": r["n_sells"],
             "n_politicians": r["n_politicians"],
             "parties": r["parties"],
             "cluster": r["cluster_signal"],
             "bipartisan": r["bipartisan"]}
            for r in (c.get("top_buys") or [])[:5]
        ]
        # Bipartisan buys (NEW - should populate this time)
        out["snapshot"]["bipartisan_buys_top_5"] = [
            {"ticker": r["ticker"], "score": r["score"],
             "n_buys": r["n_buys"], "parties": r["parties"],
             "n_politicians": r["n_politicians"]}
            for r in (c.get("bipartisan_buys") or [])[:5]
        ]
    except Exception as e:
        out["snapshot_err"] = str(e)[:200]
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
