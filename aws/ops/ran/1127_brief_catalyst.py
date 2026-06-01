"""1127 — re-invoke brief with new catalyst prompt, verify rendering."""
import json, pathlib, time, traceback
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1127_brief_catalyst.json"
lam = boto3.client("lambda", region_name="us-east-1", config=Config(read_timeout=350))
s3  = boto3.client("s3", region_name="us-east-1")


def phase(out, name, fn):
    try:
        r = fn()
        out["phases"].append({"name": name, "status": "ok", "result": r})
    except Exception as e:
        out["phases"].append({"name": name, "status": "ERROR", "error": str(e)[:300],
                                "traceback": traceback.format_exc()[:1200]})


def invoke(fn_name: str) -> dict:
    t0 = time.time()
    r = lam.invoke(FunctionName=fn_name, InvocationType="RequestResponse", Payload=b"{}")
    elapsed = round(time.time() - t0, 1)
    body = r["Payload"].read().decode("utf-8", errors="replace")
    try:
        p = json.loads(body)
        if isinstance(p.get("body"), str):
            try: return {"elapsed_s": elapsed, "summary": json.loads(p["body"])}
            except Exception: return {"elapsed_s": elapsed, "body": p["body"][:600]}
        return {"elapsed_s": elapsed, "p": str(p)[:600]}
    except Exception: return {"elapsed_s": elapsed, "raw": body[:800]}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "phases": []}

    phase(out, "invoke_brief", lambda: invoke("justhodl-pump-radar-brief"))

    def read_brief():
        time.sleep(2)
        obj = s3.get_object(Bucket="justhodl-dashboard-live",
                              Key="data/pump-radar-brief.json")
        d = json.loads(obj["Body"].read())
        pathlib.Path("aws/ops/reports/1127_brief_full.json").write_text(
            json.dumps(d, indent=2, default=str))
        return {
            "size_kb":              round(obj["ContentLength"]/1024, 1),
            "conviction":           d.get("conviction_grade"),
            "market_temp_label":    d.get("market_temperature_label"),
            "executive_summary":    d.get("executive_summary"),
            "macro_frame":          d.get("macro_frame"),
            "top_3_long_ideas":     d.get("top_3_long_ideas"),
            "catalyst_clusters":    d.get("catalyst_clusters"),
            "suggested_additions":  d.get("suggested_additions"),
            "early_detection":      d.get("early_detection"),
            "risk_warnings":        d.get("risk_warnings"),
            "whats_changed":        d.get("whats_changed"),
        }
    phase(out, "read_brief", read_brief)

    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print("[1127] DONE")


if __name__ == "__main__":
    main()
