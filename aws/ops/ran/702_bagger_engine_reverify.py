"""ops/702 — re-invoke bagger-engine after the 20/25yr horizon fix."""
import json, os, time, base64
import boto3
from botocore.config import Config
from datetime import datetime, timezone

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=900, connect_timeout=20, retries={"max_attempts": 0}))
s3 = boto3.client("s3", region_name=REGION)


def main():
    report = {"started": datetime.now(timezone.utc).isoformat()}
    fname = "justhodl-bagger-engine"

    # wait for redeploy
    for _ in range(16):
        try:
            cfg = lam.get_function_configuration(FunctionName=fname)
            if cfg.get("LastUpdateStatus") == "Successful":
                report["deploy_modified"] = cfg.get("LastModified")
                break
        except Exception:
            pass
        time.sleep(15)

    t0 = time.time()
    try:
        r = lam.invoke(FunctionName=fname, InvocationType="RequestResponse",
                        Payload=b"{}", LogType="Tail")
        log = base64.b64decode(r.get("LogResult", b"")).decode("utf-8", errors="replace") if r.get("LogResult") else ""
        body = r["Payload"].read().decode("utf-8", errors="replace") if r.get("Payload") else ""
        report["invoke"] = {"status": r.get("StatusCode"), "fn_error": r.get("FunctionError"),
                            "elapsed_s": round(time.time() - t0, 1),
                            "response": body[:600], "log_tail": log[-1500:]}
    except Exception as e:
        report["invoke"] = {"error": str(e)[:300]}

    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/bagger-engine.json")
        sc = json.loads(obj["Body"].read())
        tiers = sc.get("tiers", {})
        report["sidecar"] = {
            "size_kb": round(len(json.dumps(sc)) / 1024, 1),
            "n_scored": sc.get("n_scored"),
            "tier_counts": sc.get("tier_counts"),
            "potential_100x": [
                {"rank": r.get("rank"), "symbol": r.get("symbol"),
                 "name": (r.get("name") or "")[:30], "score": r.get("bagger_score"),
                 "cap_bucket": r.get("cap_bucket"),
                 "rev_cagr": r.get("key_stats", {}).get("revenue_cagr_pct"),
                 "roic": r.get("key_stats", {}).get("roic_pct"),
                 "years_to_100x": r.get("twin_engine", {}).get("years_to_100x"),
                 "yr20_rerated": r.get("twin_engine", {}).get("yr20", {}).get("with_rerating_x"),
                 "thesis": r.get("thesis", "")[:160]}
                for r in (tiers.get("potential_100x") or [])
            ],
            "top_20": [
                {"rank": r.get("rank"), "symbol": r.get("symbol"),
                 "score": r.get("bagger_score"), "cls": r.get("twin_engine", {}).get("classification"),
                 "rev_cagr": r.get("key_stats", {}).get("revenue_cagr_pct")}
                for r in (sc.get("top_100") or [])[:20]
            ],
        }
    except Exception as e:
        report["sidecar"] = {"error": str(e)[:200]}

    report["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/702_bagger_engine_reverify.json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    print("DONE -> 702_bagger_engine_reverify.json")


if __name__ == "__main__":
    main()
