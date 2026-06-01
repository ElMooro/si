#!/usr/bin/env python3
"""1088 — sanity check that [skip-deploy] worked and no Lambdas redeployed.

Lambdas that lost their shared-file copies in the previous cleanup commit
should still have those files in their deployed zip (from the LAST deploy).
Their last_modified timestamp should pre-date the cleanup commit time.

Cleanup commit pushed around 00:42 UTC. Any Lambda with last_modified >
00:42 was redeployed — bad. Any with last_modified < 00:42 was correctly
skipped — good.
"""
import json, os, pathlib
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1088_skip_deploy_verify.json"
lam = boto3.client("lambda", region_name="us-east-1", config=Config(read_timeout=180))

# Sample of Lambdas that had files removed in the cleanup
SPOT_CHECK = [
    "justhodl-divergence-engine-v2",  # had api_auth.py removed
    "justhodl-ai-chat",                # had _sentry_lite.py removed
    "justhodl-ark-holdings",           # had _sentry_lite + system_events
    "justhodl-calibrator",             # had _sentry_lite + ka_aliases + system_events
    "justhodl-bloomberg-v8",           # had ka_aliases.py
    "justhodl-edge-engine",            # had _sentry_lite + api_auth (calibration KEPT)
    "justhodl-options-flow",           # had api_auth.py
    "justhodl-yield-curve",            # had api_auth.py
    "justhodl-cds-proxy",              # had api_auth.py
    "justhodl-bond-trace",             # had api_auth.py
]


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "lambdas": []}
    
    for name in SPOT_CHECK:
        try:
            info = lam.get_function(FunctionName=name)
            cfg = info["Configuration"]
            out["lambdas"].append({
                "name":          name,
                "last_modified": cfg.get("LastModified"),
                "code_size":     cfg.get("CodeSize"),
            })
        except Exception as e:
            out["lambdas"].append({"name": name, "err": str(e)[:100]})
    
    # Also do a sync-invoke on one of them to confirm still functional
    print("[1088] sync-invoke divergence-engine-v2 to confirm functional…")
    import time
    t0 = time.time()
    try:
        r = lam.invoke(FunctionName="justhodl-divergence-engine-v2",
                        InvocationType="RequestResponse", Payload=b"{}")
        body = r["Payload"].read().decode("utf-8", errors="replace")
        out["sample_invoke"] = {
            "elapsed_s":  round(time.time() - t0, 1),
            "status":     r.get("StatusCode"),
        }
        try:
            p = json.loads(body)
            if isinstance(p.get("body"), str):
                inner = json.loads(p["body"])
                out["sample_invoke"]["body_status"] = p.get("statusCode")
                out["sample_invoke"]["ok"] = inner.get("ok")
                out["sample_invoke"]["n_relationships"] = inner.get("n_relationships")
                out["sample_invoke"]["composite_index"] = inner.get("composite_index")
        except Exception:
            out["sample_invoke"]["raw"] = body[:300]
    except Exception as e:
        out["sample_invoke_err"] = str(e)[:200]
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1088] DONE")


if __name__ == "__main__":
    main()
