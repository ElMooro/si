"""1120 — re-invoke pump-radar-brief after the fix + show full brief."""
import json, pathlib, time, traceback
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1120_brief_retry.json"
lam = boto3.client("lambda", region_name="us-east-1", config=Config(read_timeout=350))
s3  = boto3.client("s3", region_name="us-east-1")


def phase(out, name, fn):
    try:
        r = fn()
        out["phases"].append({"name": name, "status": "ok", "result": r})
        return r
    except Exception as e:
        out["phases"].append({"name": name, "status": "ERROR", "error": str(e)[:400],
                                "traceback": traceback.format_exc()[:1500]})
        return None


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "phases": []}

    # Re-invoke brief Lambda
    def invoke_brief():
        t0 = time.time()
        r = lam.invoke(FunctionName="justhodl-pump-radar-brief", InvocationType="RequestResponse", Payload=b"{}")
        elapsed = round(time.time() - t0, 1)
        body = r["Payload"].read().decode("utf-8", errors="replace")
        try:
            p = json.loads(body)
            if isinstance(p.get("body"), str):
                return {"elapsed_s": elapsed, "summary": json.loads(p["body"])}
            return {"elapsed_s": elapsed, "p_str": str(p)[:400]}
        except Exception:
            return {"elapsed_s": elapsed, "raw": body[:600]}
    phase(out, "invoke_brief_retry", invoke_brief)

    # Read full brief
    def read_brief():
        time.sleep(2)
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/pump-radar-brief.json")
        d = json.loads(obj["Body"].read())
        pathlib.Path("aws/ops/reports/1120_brief_full.json").write_text(
            json.dumps(d, indent=2, default=str))
        if d.get("status") == "error":
            return {"status": "error", "error": d.get("error"), "preview": (d.get("raw_preview") or "")[:300]}
        return {
            "size_kb":             round(obj["ContentLength"]/1024, 1),
            "elapsed_sec":         d.get("elapsed_sec"),
            "claude_elapsed":      d.get("claude_elapsed"),
            "conviction_grade":    d.get("conviction_grade"),
            "executive_summary":   d.get("executive_summary"),
            "macro_frame":         d.get("macro_frame"),
            "market_temperature":  d.get("market_temperature"),
            "top_3_long_ideas":    d.get("top_3_long_ideas", []),
            "top_2_pair_trades":   d.get("top_2_pair_trades", []),
            "what_to_watch_today": d.get("what_to_watch_today", []),
            "risk_warnings":       d.get("risk_warnings", []),
            "whats_changed":       d.get("whats_changed_narrative"),
            "whats_changed_data":  d.get("whats_changed_data"),
            "source_versions":     d.get("source_versions"),
        }
    phase(out, "read_brief_full", read_brief)

    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1120] DONE")


if __name__ == "__main__":
    main()
