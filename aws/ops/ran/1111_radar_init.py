"""1111 — clear convergence-radar state + invoke patched Lambda.

Triggers the new SYSTEM_INITIALIZED Telegram with current top 8 ULTRA tickers.
"""
import json, pathlib, time, traceback
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1111_radar_init.json"
lam = boto3.client("lambda", region_name="us-east-1",
                    config=Config(read_timeout=300))
s3 = boto3.client("s3", region_name="us-east-1")


def phase(out, name, fn):
    try:
        r = fn()
        out["phases"].append({"name": name, "status": "ok", "result": r})
        return r
    except Exception as e:
        out["phases"].append({"name": name, "status": "ERROR",
                                "error": str(e)[:300],
                                "traceback": traceback.format_exc()[:1000]})
        return None


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "phases": []}
    fn = "justhodl-convergence-radar"

    # Clear prior state so this becomes a "first run"
    def clear_state():
        results = {}
        for k in ["data/_alerts/convergence-radar-state.json",
                    "data/_alerts/convergence-radar-alerted.json"]:
            try:
                s3.delete_object(Bucket="justhodl-dashboard-live", Key=k)
                results[k] = "deleted"
            except Exception as e:
                results[k] = f"err: {str(e)[:80]}"
        return results
    phase(out, "clear_state", clear_state)

    # Invoke
    def invoke():
        t0 = time.time()
        r = lam.invoke(FunctionName=fn,
                        InvocationType="RequestResponse", Payload=b"{}")
        elapsed = round(time.time() - t0, 1)
        body = r["Payload"].read().decode("utf-8", errors="replace")
        try:
            p = json.loads(body)
            if isinstance(p.get("body"), str):
                try:
                    return {"elapsed_s": elapsed, "summary": json.loads(p["body"])}
                except Exception:
                    return {"elapsed_s": elapsed, "body": p["body"][:400]}
            return {"elapsed_s": elapsed, "p": str(p)[:400]}
        except Exception:
            return {"elapsed_s": elapsed, "raw": body[:500]}
    phase(out, "invoke", invoke)

    # Read output - confirm alert info now shows is_first_run=True
    def read_output():
        time.sleep(2)
        obj = s3.get_object(Bucket="justhodl-dashboard-live",
                              Key="data/convergence-radar.json")
        d = json.loads(obj["Body"].read())
        return {
            "size_kb":        round(obj["ContentLength"]/1024, 1),
            "last_modified":  obj["LastModified"].isoformat(),
            "summary":        d.get("summary"),
            "alert_info":     d.get("alert_info"),
            "top_8":          [{
                "ticker":    r["ticker"],
                "tier":      r["tier"],
                "n_engines": r["n_engines"],
                "score":     r["convergence_score"],
                "domains":   r["domain_coverage"],
            } for r in (d.get("tickers") or [])[:8]],
        }
    phase(out, "read_output", read_output)

    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1111] DONE")


if __name__ == "__main__":
    main()
