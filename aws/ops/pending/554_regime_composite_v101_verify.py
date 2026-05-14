#!/usr/bin/env python3
"""554 — Verify v1.0.1 patches landed: check Lambda version, force-invoke,
read sidecar, confirm DIX_NEUTRAL/INSIDER_BUYING_PRESENT appear."""
import io, json, os, time as _time, base64
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/554_regime_composite_v101_verify.json"
NAME = "justhodl-regime-composite"

lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    # Wait for any in-flight updates
    for i in range(15):
        try:
            cfg = lam.get_function(FunctionName=NAME)["Configuration"]
            state = cfg.get("State")
            lus = cfg.get("LastUpdateStatus")
            last_mod = cfg.get("LastModified")
            if state == "Active" and lus == "Successful":
                out["lambda_state"] = {"state": state, "last_update": lus, "last_modified": last_mod}
                break
            out[f"wait_{i}"] = {"state": state, "lus": lus}
        except Exception as e:
            out[f"wait_err_{i}"] = str(e)[:120]
        _time.sleep(4)

    # Invoke fresh
    try:
        resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse",
                           LogType="Tail", Payload=b"{}")
        out["invoke_status"] = resp.get("StatusCode")
        out["fn_error"] = resp.get("FunctionError")
        body = resp["Payload"].read().decode("utf-8")
        try:
            p = json.loads(body)
            out["response_body"] = json.loads(p["body"]) if p.get("body") else p
        except Exception:
            out["raw"] = body[:1000]
        if resp.get("LogResult") and resp.get("FunctionError"):
            out["log_tail"] = base64.b64decode(resp["LogResult"]).decode("utf-8", "replace")[-2500:]
    except Exception as e:
        out["invoke_err"] = str(e)[:200]

    _time.sleep(2)

    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/regime-composite.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["sidecar"] = {
            "size_kb": round(len(body) / 1024, 1),
            "modified": obj["LastModified"].isoformat()[:19],
            "version": p.get("version"),
            "meta_regime": p.get("meta_regime"),
            "composite_score": p.get("composite_score"),
            "n_modules_with_data": p.get("n_modules_with_data"),
            "n_modules_missing": p.get("n_modules_missing"),
            "duration_s": p.get("duration_s"),
            "dimensions": p.get("dimensions"),
        }
        for m in (p.get("modules") or []):
            if "DIX" in m.get("label", ""):
                out["dix_module"] = m
            if m.get("label") == "Insider Clusters":
                out["insider_module"] = m
    except Exception as e:
        out["sidecar_err"] = str(e)[:200]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
