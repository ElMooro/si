"""ops/601 — verify sector-heatmap + ESI fixes landed and produce real data."""
import json, os, time, base64
import boto3
from datetime import datetime, timezone

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def fetch_sidecar(key):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        return {"_error": f"{type(e).__name__}: {str(e)[:200]}"}


def force_invoke(fname):
    try:
        r = lam.invoke(FunctionName=fname, InvocationType="RequestResponse",
                        Payload=b"{}", LogType="Tail")
        log = base64.b64decode(r.get("LogResult", b"")).decode("utf-8", errors="replace") if r.get("LogResult") else ""
        body = r["Payload"].read().decode("utf-8", errors="replace") if r.get("Payload") else ""
        return {"status": r.get("StatusCode"), "fn_error": r.get("FunctionError"),
                "response_preview": body[:600], "log_tail": log[-2200:]}
    except Exception as e:
        return {"status": "error", "err": str(e)[:300]}


def main():
    report = {"started": datetime.now(timezone.utc).isoformat()}

    # Wait briefly for deploys to settle
    time.sleep(5)

    # Sector-heatmap
    print("=== sector-heatmap ===")
    sh = {}
    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-sector-heatmap")
        sh["last_modified"] = cfg.get("LastModified")
        sh["memory"] = cfg.get("MemorySize")
        sh["timeout"] = cfg.get("Timeout")
    except Exception as e:
        sh["preflight_err"] = str(e)
    sh["invoke"] = force_invoke("justhodl-sector-heatmap")
    sh["sidecar"] = fetch_sidecar("data/sector-heatmap.json")
    sc = sh["sidecar"]
    if isinstance(sc, dict) and "_error" not in sc:
        sh["size_kb"] = round(len(json.dumps(sc, default=str))/1024, 1)
        mr = sc.get("market_regime", {})
        sh["summary"] = {
            "n_tickers_total": sc.get("n_tickers_total"),
            "n_tickers_with_1d": sc.get("n_tickers_with_1d"),
            "regime": mr.get("regime"),
            "breadth_pct": mr.get("breadth_pct"),
            "weighted_1d_pct": mr.get("weighted_return_1d_pct"),
            "n_sectors": len(sc.get("sectors") or {}),
            "sector_rank_top3": [(r.get("sector"), r.get("weighted_return_1d_pct"))
                                  for r in (sc.get("sector_rank_1d") or [])[:3]],
            "sector_rank_bot3": [(r.get("sector"), r.get("weighted_return_1d_pct"))
                                  for r in (sc.get("sector_rank_1d") or [])[-3:]],
            "leaders_top3": [(l.get("symbol"), l.get("change_pct"))
                              for l in mr.get("leaders", [])[:3]],
            "laggers_top3": [(l.get("symbol"), l.get("change_pct"))
                              for l in mr.get("laggers", [])[:3]],
        }
    report["A_sector_heatmap"] = sh

    # ESI
    print("=== ESI ===")
    e = {"invoke": force_invoke("justhodl-esi"),
         "sidecar": fetch_sidecar("data/esi.json")}
    sc = e["sidecar"]
    if isinstance(sc, dict) and "_error" not in sc:
        e["summary"] = {
            "composite_60d": sc.get("composite_60d"),
            "composite_30d": sc.get("composite_30d"),
            "composite_7d": sc.get("composite_7d"),
            "regime": sc.get("regime"),
            "n_events_60d": sc.get("n_events_60d"),
            "n_events_30d": sc.get("n_events_30d"),
            "n_events_7d": sc.get("n_events_7d"),
            "by_category": sc.get("by_category"),
            "top_recent_beats": (sc.get("top_recent_beats") or [])[:3],
            "top_recent_misses": (sc.get("top_recent_misses") or [])[:3],
            "err": sc.get("err"),
        }
    report["B_esi"] = e

    report["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/601_post_fix_verify.json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    print("DONE -> 601_post_fix_verify.json")


if __name__ == "__main__":
    main()
