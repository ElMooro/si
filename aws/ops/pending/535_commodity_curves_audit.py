#!/usr/bin/env python3
"""535 — Audit commodity-curves Lambda + sidecar."""
import json, os, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/535_commodity_curves_audit.json"
lam = boto3.client("lambda", region_name="us-east-1")
eb = boto3.client("events", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    NAME = "justhodl-commodity-curves"
    try:
        cfg = lam.get_function_configuration(FunctionName=NAME)
        out["lambda"] = {"exists": True,
                          "last_modified": cfg.get("LastModified"),
                          "memory": cfg.get("MemorySize"), "timeout": cfg.get("Timeout"),
                          "env_keys": sorted((cfg.get("Environment", {}) or {}).get("Variables", {}).keys())}
        rules = []
        for r in eb.list_rules()["Rules"]:
            try:
                ts = eb.list_targets_by_rule(Rule=r["Name"])["Targets"]
                if any(NAME in t.get("Arn", "") for t in ts):
                    rules.append({"name": r["Name"], "schedule": r.get("ScheduleExpression"), "state": r.get("State")})
            except: pass
        out["lambda"]["rules"] = rules

        # Invoke
        r = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse",
                        LogType="Tail", Payload=b"{}")
        out["invoke_status"] = r.get("StatusCode")
        out["fn_error"] = r.get("FunctionError")
        body = r["Payload"].read().decode("utf-8")
        try:
            p = json.loads(body)
            out["response"] = json.loads(p["body"]) if p.get("body") else p
        except: out["raw"] = body[:1500]
        if r.get("LogResult"):
            out["log_tail"] = base64.b64decode(r["LogResult"]).decode("utf-8", "replace")[-3000:]
    except lam.exceptions.ResourceNotFoundException:
        out["lambda"] = {"exists": False}

    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/commodity-curves.json")
        body = obj["Body"].read()
        p = json.loads(body)
        comp = p.get("composite") or {}
        out["sidecar"] = {
            "size_kb": round(len(body)/1024, 1),
            "modified": obj["LastModified"].isoformat()[:19],
            "version": p.get("version"),
            "n_with_data": p.get("n_with_data"),
            "n_with_err": p.get("n_with_err"),
            "composite_regime": p.get("composite_regime"),
            "composite_signal": p.get("composite_signal"),
            "top_keys": list(p.keys())[:25],
            "composite_keys": list(comp.keys())[:20] if isinstance(comp, dict) else None,
            "energy_avg_20d": comp.get("energy_avg_20d") if isinstance(comp, dict) else None,
            "metals_avg_20d": comp.get("metals_avg_20d") if isinstance(comp, dict) else None,
            "agri_avg_20d": comp.get("agri_avg_20d") if isinstance(comp, dict) else None,
            "top_3_by_20d": comp.get("top_3_by_20d") if isinstance(comp, dict) else None,
            "bottom_3_by_20d": comp.get("bottom_3_by_20d") if isinstance(comp, dict) else None,
            "ranked_full_first_10": (comp.get("ranked_20d") or [])[:10] if isinstance(comp, dict) else None,
            "gold_silver_ratio": comp.get("gold_silver_ratio") if isinstance(comp, dict) else None,
            "wti_curve_proxy": comp.get("wti_curve_slope_pp") if isinstance(comp, dict) else None,
        }
    except s3.exceptions.NoSuchKey:
        out["sidecar"] = {"exists": False}
    except Exception as e:
        out["sidecar_err"] = str(e)[:300]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
