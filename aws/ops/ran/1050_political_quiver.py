#!/usr/bin/env python3
"""1050 — deploy Quiver-based political-stocks Lambda + verify Congress
data finally populates correctly."""
import io, json, os, pathlib, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1050_political_quiver.json"
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
    
    # ── Deploy
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
            out["deploy_err"] = str(e)[:300]
            break
    
    time.sleep(3)
    
    # ── Sync-invoke
    print("[1050] sync-invoke political-stocks…")
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
    
    # ── Read output
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/political-stocks.json")
        ps = json.loads(obj["Body"].read().decode("utf-8"))
        out["snapshot"] = {
            "generated_at":     ps.get("generated_at"),
            "schema_version":   ps.get("schema_version"),
            "data_source":      ps.get("data_source"),
            "lookback_days":    ps.get("lookback_days"),
            "duration_s":       ps.get("duration_s"),
            
            "trump_n_positions": len((ps.get("trump_holdings") or {}).get("positions") or []),
            "trump_filing_date": (ps.get("trump_holdings") or {}).get("filing_date"),
            
            "congress_n_trades_total":  (ps.get("congress") or {}).get("n_trades_total"),
            "congress_n_trades_house":  (ps.get("congress") or {}).get("n_trades_house"),
            "congress_n_trades_senate": (ps.get("congress") or {}).get("n_trades_senate"),
            "congress_n_tickers":       (ps.get("congress") or {}).get("n_tickers"),
            "congress_n_known_parties": (ps.get("congress") or {}).get("n_known_parties"),
            "congress_n_top_buys":      len((ps.get("congress") or {}).get("top_buys") or []),
            "congress_n_clusters":      len((ps.get("congress") or {}).get("clusters") or []),
            "congress_n_bipartisan":    len((ps.get("congress") or {}).get("bipartisan_buys") or []),
        }
        # Top 5 buys
        out["snapshot"]["top_5_buys"] = [
            {"ticker": r["ticker"], "score": r["score"],
             "n_buys": r["n_buys"], "n_sells": r["n_sells"],
             "n_politicians": r["n_politicians"],
             "cluster": r["cluster_signal"],
             "bipartisan": r["bipartisan"],
             "parties": r["parties"]}
            for r in ((ps.get("congress") or {}).get("top_buys") or [])[:5]
        ]
        # Top 5 clusters
        out["snapshot"]["top_5_clusters"] = [
            {"ticker": r["ticker"], "score": r["score"],
             "n_buys": r["n_buys"], "n_sells": r["n_sells"],
             "type": r["cluster_signal"],
             "bipartisan": r["bipartisan"],
             "n_politicians": r["n_politicians"]}
            for r in ((ps.get("congress") or {}).get("clusters") or [])[:5]
        ]
        # Top 3 most-traded with recent trade samples
        out["snapshot"]["sample_trades"] = []
        for r in ((ps.get("congress") or {}).get("all_tickers") or [])[:3]:
            out["snapshot"]["sample_trades"].append({
                "ticker": r["ticker"],
                "recent_trades": [{
                    "politician": t["politician"],
                    "party": t["party"],
                    "chamber": t["chamber"],
                    "date": t["date"],
                    "type": t["type"][:30],
                    "amount": t["amount"][:30],
                } for t in (r.get("recent_trades") or [])[:3]],
            })
    except Exception as e:
        out["snapshot_err"] = str(e)[:200]
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
