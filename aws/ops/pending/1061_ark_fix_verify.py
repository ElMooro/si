#!/usr/bin/env python3
"""1061 — re-verify ARK after URL fix + patent stub state."""
import json, os, pathlib, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1061_ark_fix_verify.json"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION,
                    config=Config(read_timeout=180))
s3 = boto3.client("s3", region_name=REGION)


def invoke_sync(name):
    r = lam.invoke(FunctionName=name, InvocationType="RequestResponse", Payload=b"{}")
    body = r["Payload"].read().decode("utf-8", errors="replace")
    try:
        p = json.loads(body)
        return json.loads(p["body"]) if isinstance(p.get("body"), str) else p
    except Exception:
        return {"_raw": body[:400]}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    print("[1061] sync-invoke ARK (after URL fix)…")
    t0 = time.time()
    try:
        r = invoke_sync("justhodl-ark-holdings")
        out["ark_invoke"] = {
            "elapsed_s":   round(time.time() - t0, 1),
            "ok":          r.get("ok"),
            "n_funds":     r.get("n_funds"),
            "n_positions": r.get("n_positions"),
            "n_unique":    r.get("n_unique_tickers"),
            "n_new":       r.get("n_new_positions"),
            "n_adds":      r.get("n_adds"),
            "n_trims":     r.get("n_trims"),
            "n_closed":    r.get("n_closed"),
            "duration_s":  r.get("duration_s"),
            "raw":         r.get("_raw"),
        }
    except Exception as e:
        out["ark_invoke_err"] = str(e)[:300]
    
    time.sleep(2)
    
    print("[1061] sync-invoke patent (should write stub)…")
    t0 = time.time()
    try:
        r = invoke_sync("justhodl-patent-velocity")
        out["patent_invoke"] = {
            "elapsed_s":  round(time.time() - t0, 1),
            "ok":         r.get("ok"),
            "reason":     r.get("reason"),
            "message":    (r.get("message") or "")[:200],
        }
    except Exception as e:
        out["patent_invoke_err"] = str(e)[:300]
    
    time.sleep(2)
    
    print("[1061] read S3 outputs…")
    for key, label in [
        ("data/ark-holdings.json",    "ark_snapshot"),
        ("data/patent-velocity.json", "patent_snapshot"),
    ]:
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            body = obj["Body"].read()
            d = json.loads(body)
            snap = {
                "size_kb":      round(len(body) / 1024, 1),
                "schema":       d.get("schema_version"),
                "generated_at": d.get("generated_at"),
                "status":       d.get("status"),
            }
            if label == "ark_snapshot":
                snap["n_funds"]     = d.get("n_funds_fetched")
                snap["n_positions"] = d.get("n_positions_total")
                snap["n_unique"]    = d.get("n_unique_tickers")
                diff = d.get("diff_vs_prev", {})
                snap["diff"] = {"new": diff.get("n_new_positions"),
                                  "adds": diff.get("n_position_adds"),
                                  "trims": diff.get("n_position_trims"),
                                  "closed": diff.get("n_closed_positions")}
                snap["top_5_cross_fund"] = [
                    {"ticker": r["ticker"], "n_funds": r["n_funds"],
                     "total_value": r["total_value"], "company": (r.get("company") or "")[:30]}
                    for r in (d.get("cross_fund_top") or [])[:5]
                ]
                snap["fund_breakdown"] = {
                    fund: len(positions)
                    for fund, positions in (d.get("holdings_by_fund") or {}).items()
                }
            elif label == "patent_snapshot":
                snap["needs_setup"] = d.get("needs_setup", "")[:200]
                snap["register_url"] = d.get("register_url")
                snap["universe_size"] = d.get("universe_size")
            out[label] = snap
        except Exception as e:
            out[label + "_err"] = str(e)[:200]
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1061] DONE → {REPORT}")


if __name__ == "__main__":
    main()
