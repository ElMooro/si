#!/usr/bin/env python3
"""530 — Diagnose google-trends-agent (no sidecar) + read cb-stance full sidecar."""
import json, os, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/530_trends_cb_diag.json"
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
eb = boto3.client("events", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # ── Invoke google-trends-agent ──
    name = "google-trends-agent"
    try:
        r = lam.invoke(FunctionName=name, InvocationType="RequestResponse",
                        LogType="Tail", Payload=b"{}")
        out["trends_invoke"] = {"status": r.get("StatusCode"), "fn_error": r.get("FunctionError")}
        body = r["Payload"].read().decode("utf-8")
        try:
            p = json.loads(body)
            out["trends_invoke"]["response"] = json.loads(p["body"]) if p.get("body") else p
        except: out["trends_invoke"]["raw"] = body[:2000]
        if r.get("LogResult"):
            out["trends_invoke"]["log_tail"] = base64.b64decode(r["LogResult"]).decode("utf-8","replace")[-3500:]
    except Exception as e:
        out["trends_invoke_err"] = str(e)[:400]

    # Code inspection — get first 80 lines of trends agent
    try:
        resp = lam.get_function(FunctionName=name)
        out["trends_code_url"] = resp["Code"]["Location"][:200]
        cfg = resp["Configuration"]
        out["trends_handler"] = cfg.get("Handler")
        out["trends_runtime"] = cfg.get("Runtime")
        out["trends_env_keys"] = sorted((cfg.get("Environment", {}) or {}).get("Variables", {}).keys())
        out["trends_description"] = cfg.get("Description", "")[:300]
    except Exception as e:
        out["trends_cfg_err"] = str(e)[:200]

    # ── Read cb-stance full sidecar ──
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/cb-stance.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["cb_stance_full"] = {
            "size_kb": round(len(body)/1024, 1),
            "modified": obj["LastModified"].isoformat()[:19],
            "version": p.get("version"),
            "generated_at": p.get("generated_at"),
            "top_keys": list(p.keys()),
            "fed": p.get("fed"),
            "ecb": p.get("ecb"),
            "boe": p.get("boe"),
            "boj": p.get("boj"),
        }
    except Exception as e:
        out["cb_stance_err"] = str(e)[:300]

    # ── Re-invoke cb-stance to make sure it's still working ──
    try:
        r = lam.invoke(FunctionName="justhodl-cb-stance", InvocationType="RequestResponse",
                        LogType="Tail", Payload=b"{}")
        out["cb_invoke"] = {"status": r.get("StatusCode"), "fn_error": r.get("FunctionError")}
        body = r["Payload"].read().decode("utf-8")
        try:
            p = json.loads(body)
            out["cb_invoke"]["response"] = json.loads(p["body"]) if p.get("body") else p
        except: out["cb_invoke"]["raw"] = body[:2000]
        if r.get("LogResult"):
            out["cb_invoke"]["log_tail"] = base64.b64decode(r["LogResult"]).decode("utf-8","replace")[-2500:]
    except Exception as e:
        out["cb_invoke_err"] = str(e)[:400]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
