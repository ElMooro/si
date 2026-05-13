#!/usr/bin/env python3
"""519 — Check BUILD 6 (crypto-funding) deployed state."""
import json, os
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/519_crypto_funding_audit.json"
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
eb = boto3.client("events", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    # Lambda
    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-crypto-funding")
        out["lambda"] = {"exists": True,
                          "last_modified": cfg.get("LastModified"),
                          "mem": cfg.get("MemorySize"), "to": cfg.get("Timeout"),
                          "env_keys": sorted((cfg.get("Environment", {}) or {}).get("Variables", {}).keys()),
                          "state": cfg.get("State")}
        # rules
        rules = []
        for r in eb.list_rules()["Rules"]:
            try:
                ts = eb.list_targets_by_rule(Rule=r["Name"])["Targets"]
                if any("justhodl-crypto-funding" in t.get("Arn", "") for t in ts):
                    rules.append({"name": r["Name"], "schedule": r.get("ScheduleExpression"), "state": r.get("State")})
            except: pass
        out["lambda"]["rules"] = rules
        # Try invoke
        try:
            r = lam.invoke(FunctionName="justhodl-crypto-funding", InvocationType="RequestResponse", Payload=b"{}")
            body = r["Payload"].read().decode("utf-8")
            try:
                p = json.loads(body)
                out["invoke"] = {"status": r.get("StatusCode"), "fn_error": r.get("FunctionError"),
                                  "response": json.loads(p["body"]) if p.get("body") else p}
            except: out["invoke"] = {"status": r.get("StatusCode"), "raw": body[:1500]}
        except Exception as e: out["invoke_err"] = str(e)[:200]
    except lam.exceptions.ResourceNotFoundException:
        out["lambda"] = {"exists": False}

    # Sidecar
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/crypto-funding.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["sidecar"] = {
            "size_kb": round(len(body)/1024, 1),
            "modified": obj["LastModified"].isoformat()[:19],
            "top_keys": list(p.keys())[:20],
            "version": p.get("version"),
            "generated_at": p.get("generated_at"),
            "composite_regime": p.get("composite_regime"),
            "composite_signal": p.get("composite_signal"),
            "market_composite": p.get("market_composite"),
            "n_coins": len(p.get("coins") or p.get("by_coin") or []),
            "sample_coin_keys": list((p.get("coins") or p.get("by_coin") or {}).keys())[:10] if isinstance((p.get("coins") or p.get("by_coin")), dict) else None,
        }
    except s3.exceptions.NoSuchKey:
        out["sidecar"] = {"exists": False}
    except Exception as e:
        out["sidecar_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
