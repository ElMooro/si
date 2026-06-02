"""1149 — test prewarm Lambda with 3 tickers (limit override)."""
import json, pathlib, time
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1149_prewarm_test.json"

lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # Invoke prewarm with just 3 tickers (sync = wait for response)
    t0 = time.time()
    try:
        resp = lam.invoke(
            FunctionName="justhodl-equity-prewarm",
            InvocationType="RequestResponse",
            Payload=json.dumps({"tickers": ["UBER", "CRWD", "ZM"]}).encode(),
        )
        elapsed = round(time.time() - t0, 1)
        payload = json.loads(resp["Payload"].read())
        out["lambda_invocation"] = {
            "status_code": resp["StatusCode"],
            "elapsed_s":   elapsed,
            "log_result":  resp.get("LogResult"),
            "payload":     payload,
        }
        # Parse the body
        if isinstance(payload, dict) and "body" in payload:
            try:
                out["lambda_body_parsed"] = json.loads(payload["body"])
            except Exception:
                pass
    except Exception as e:
        out["lambda_invocation"] = {"error": str(e)[:500], "elapsed_s": round(time.time()-t0, 1)}

    # Check that the latest.json log was written
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="equity-prewarm/runs/latest.json")
        log = json.loads(obj["Body"].read())
        out["s3_log_latest"] = {
            "wall_seconds":  log.get("wall_seconds"),
            "n_succeeded":   log.get("n_succeeded"),
            "n_failed":      log.get("n_failed"),
            "tickers":       log.get("tickers"),
            "result_summaries": [
                {"ticker": r.get("ticker"), "status": r.get("status"),
                 "elapsed_s": r.get("elapsed_s"), "size_kb": r.get("size_kb"),
                 "rating": r.get("rating")}
                for r in (log.get("results") or [])
            ],
        }
    except Exception as e:
        out["s3_log_latest"] = {"error": str(e)[:300]}

    # Verify the cached tickers are now retrievable from CF
    out["cf_proxy_check"] = {}
    import urllib.request, urllib.error
    for t in ["UBER", "CRWD", "ZM"]:
        url = f"https://justhodl-data-proxy.raafouis.workers.dev/equity-research/{t}.json"
        try:
            with urllib.request.urlopen(url, timeout=5) as r:
                body = r.read()
                out["cf_proxy_check"][t] = {"http": r.status, "size_kb": round(len(body)/1024, 1)}
        except urllib.error.HTTPError as e:
            out["cf_proxy_check"][t] = {"http_error": e.code}
        except Exception as e:
            out["cf_proxy_check"][t] = {"error": str(e)[:200]}

    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1149] DONE")


if __name__ == "__main__":
    main()
