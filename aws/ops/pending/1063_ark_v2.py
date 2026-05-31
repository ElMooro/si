#!/usr/bin/env python3
"""1063 — verify ARK after migration to arkfunds.io community API."""
import json, os, pathlib, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1063_ark_v2.json"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION,
                    config=Config(read_timeout=180))
s3 = boto3.client("s3", region_name=REGION)


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    print("[1063] sync-invoke ARK v2 (arkfunds.io)…")
    t0 = time.time()
    try:
        r = lam.invoke(FunctionName="justhodl-ark-holdings",
                        InvocationType="RequestResponse", Payload=b"{}")
        body = r["Payload"].read().decode("utf-8", errors="replace")
        try:
            p = json.loads(body)
            result = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
            out["ark_invoke"] = {
                "elapsed_s":        round(time.time() - t0, 1),
                "ok":               result.get("ok"),
                "n_funds":          result.get("n_funds"),
                "n_positions":      result.get("n_positions"),
                "n_unique_tickers": result.get("n_unique_tickers"),
                "n_new":            result.get("n_new_positions"),
                "n_adds":           result.get("n_adds"),
                "n_trims":          result.get("n_trims"),
                "n_closed":         result.get("n_closed"),
                "duration_s":       result.get("duration_s"),
                "raw":              result.get("_raw"),
            }
        except Exception as e:
            out["ark_invoke"] = {"elapsed_s": round(time.time() - t0, 1),
                                   "parse_err": str(e), "raw": body[:300]}
    except Exception as e:
        out["ark_invoke_err"] = str(e)[:300]
    
    time.sleep(2)
    
    print("[1063] read S3 output…")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/ark-holdings.json")
        body = obj["Body"].read()
        d = json.loads(body)
        snap = {
            "size_kb":      round(len(body) / 1024, 1),
            "schema":       d.get("schema_version"),
            "method":       d.get("method"),
            "generated_at": d.get("generated_at"),
            "duration_s":   d.get("duration_s"),
            "n_funds":      d.get("n_funds_fetched"),
            "n_positions": d.get("n_positions_total"),
            "n_unique":     d.get("n_unique_tickers"),
            "data_source":  d.get("data_source"),
        }
        diff = d.get("diff_vs_prev", {})
        snap["diff"] = {
            "new":    diff.get("n_new_positions"),
            "adds":   diff.get("n_position_adds"),
            "trims":  diff.get("n_position_trims"),
            "closed": diff.get("n_closed_positions"),
        }
        snap["fund_breakdown"] = {
            fund: len(positions)
            for fund, positions in (d.get("holdings_by_fund") or {}).items()
        }
        snap["top_5_cross_fund"] = [
            {"ticker": r["ticker"], "n_funds": r["n_funds"],
             "total_value": r["total_value"], "company": (r.get("company") or "")[:30]}
            for r in (d.get("cross_fund_top") or [])[:5]
        ]
        out["snapshot"] = snap
    except Exception as e:
        out["snapshot_err"] = str(e)[:300]
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1063] DONE → {REPORT}")


if __name__ == "__main__":
    main()
