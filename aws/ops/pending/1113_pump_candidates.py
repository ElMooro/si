"""1113 — invoke convergence-radar after directional classifier added.
Verify pump_candidates list and AVGO's specific bullish/bearish breakdown.
"""
import json, pathlib, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1113_pump_candidates.json"
lam = boto3.client("lambda", region_name="us-east-1",
                    config=Config(read_timeout=300))
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # Invoke
    print("[1113] invoking…")
    t0 = time.time()
    r = lam.invoke(FunctionName="justhodl-convergence-radar",
                    InvocationType="RequestResponse", Payload=b"{}")
    out["elapsed_s"] = round(time.time() - t0, 1)
    body = r["Payload"].read().decode("utf-8", errors="replace")
    try:
        p = json.loads(body)
        out["status_code"] = p.get("statusCode")
        if isinstance(p.get("body"), str):
            try:
                out["summary"] = json.loads(p["body"])
            except Exception:
                pass
    except Exception:
        out["raw"] = body[:400]

    # Read output
    time.sleep(2)
    obj = s3.get_object(Bucket="justhodl-dashboard-live",
                          Key="data/convergence-radar.json")
    d = json.loads(obj["Body"].read())
    out["meta"] = {
        "size_kb":       round(obj["ContentLength"]/1024, 1),
        "last_modified": obj["LastModified"].isoformat(),
        "schema":        d.get("schema_version"),
    }
    out["summary_full"] = d.get("summary")

    # Top 15 pump candidates with full detail
    cands = d.get("pump_candidates") or []
    out["pump_candidates_top_15"] = []
    for c in cands[:15]:
        out["pump_candidates_top_15"].append({
            "ticker":           c["ticker"],
            "category":         c["pump_category"],
            "pump_likelihood":  c["pump_likelihood"],
            "directional":      c["directional_score"],
            "n_engines":        c["n_engines"],
            "n_bullish_eng":    c["n_bullish_eng"],
            "n_bearish_eng":    c["n_bearish_eng"],
            "domains":          c["domain_coverage"],
            "bullish_drivers":  [{"e": b["engine"], "note": b["note"]} for b in c["bullish_engines"][:5]],
            "bearish_drag":     [{"e": b["engine"], "note": b["note"]} for b in c["bearish_engines"][:3]],
            "components":       c.get("pump_components"),
        })

    # Specifically: how did AVGO fare under directional filtering?
    avgo = next((r for r in (d.get("tickers") or []) if r["ticker"] == "AVGO"), None)
    if avgo:
        out["AVGO_detail"] = {
            "n_engines":         avgo["n_engines"],
            "convergence_score": avgo["convergence_score"],
            "directional_score": avgo["directional_score"],
            "pump_likelihood":   avgo["pump_likelihood"],
            "pump_category":     avgo["pump_category"],
            "exclude_from_longs": avgo["exclude_from_longs"],
            "bullish_drivers":   [{"e": b["engine"], "note": b["note"], "weighted": b["weighted"]}
                                    for b in avgo["bullish_engines"][:6]],
            "bearish_drag":      [{"e": b["engine"], "note": b["note"], "weighted": b["weighted"]}
                                    for b in avgo["bearish_engines"][:4]],
            "pump_components":   avgo.get("pump_components"),
        }
    
    # ARM, PLTR, AMD too for comparison
    for tk in ["ARM", "PLTR", "AMD", "TSLA", "NVDA"]:
        rec = next((r for r in (d.get("tickers") or []) if r["ticker"] == tk), None)
        if rec:
            out[f"{tk}_detail"] = {
                "directional_score": rec["directional_score"],
                "pump_likelihood":   rec["pump_likelihood"],
                "pump_category":     rec["pump_category"],
                "n_bullish_eng":     rec["n_bullish_eng"],
                "n_bearish_eng":     rec["n_bearish_eng"],
                "top_bullish":       [b["engine"] for b in rec["bullish_engines"][:4]],
                "top_bearish":       [b["engine"] for b in rec["bearish_engines"][:2]],
            }

    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1113] DONE")


if __name__ == "__main__":
    main()
