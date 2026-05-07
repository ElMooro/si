#!/usr/bin/env python3
"""Step 345 — Force-invoke plumbing-aggregator + verify 28/28 indicators."""
import json
import os
import time
from datetime import datetime, timezone

import boto3

REPORT = "aws/ops/reports/345_plumbing_final.json"
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}

    print("[345] Force-invoking plumbing-aggregator…")
    started = time.time()
    resp = lam.invoke(
        FunctionName="justhodl-plumbing-aggregator",
        InvocationType="RequestResponse", Payload=b"{}",
    )
    body = resp["Payload"].read().decode("utf-8")
    out["invoke"] = {
        "status": resp.get("StatusCode"),
        "fn_err": resp.get("FunctionError"),
        "duration_s": round(time.time() - started, 1),
    }
    try:
        out["invoke"]["body"] = json.loads(body)
    except Exception:
        out["invoke"]["body_raw"] = body[:500]

    time.sleep(2)

    obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/plumbing-stress.json")
    d = json.loads(obj["Body"].read())

    raw = d.get("raw_indicators", {})
    by_layer = {"L1": [], "L2": [], "L3": [], "L4": []}
    errors = []
    for sid, v in raw.items():
        layer = v.get("layer")
        if layer in by_layer:
            by_layer[layer].append({
                "id": sid, "label": v.get("label"),
                "value": v.get("value"), "z": v.get("z_score"),
                "stress": v.get("stress_score"), "err": v.get("err"),
            })
        if v.get("err"):
            errors.append({"id": sid, "label": v.get("label"), "err": v.get("err")})

    out["summary"] = {
        "as_of": d.get("as_of"),
        "composite_score": d.get("composite_score"),
        "composite_label": d.get("composite_label"),
        "n_total": len(raw),
        "n_with_data": sum(1 for v in raw.values() if v.get("stress_score") is not None),
        "n_errors": len(errors),
        "duration_s": d.get("duration_s"),
        "alerts_n": len(d.get("alerts", [])),
        "alerts_top5": d.get("alerts", [])[:5],
        "by_layer": {
            l: {"n": len(items), "with_data": sum(1 for x in items if x.get("stress") is not None),
                "errors": [x for x in items if x.get("err")]}
            for l, items in by_layer.items()
        },
        "all_errors": errors,
    }

    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:6000])


if __name__ == "__main__":
    main()
