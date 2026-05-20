"""
ops 960 -- act on the ground truth from ops 959:

  Edge 1: dump data/capitulation.json schema (this is the REAL deployed Edge 1)
  Edge 2: dump data/insider-clusters.json schema (REAL deployed Edge 2)
  Edge 3: invoke justhodl-breadth-thrust if exists, else report missing
  Edge 4: invoke justhodl-vol-target-unwind to seed data/vol-target-unwind.json

Output: full key listings + canonical-schema compliance check for each.
"""

import json
import os
import time
import boto3
import datetime as dt

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
REPORT_PATH = "aws/ops/reports/960_real_edges_1_4_inspect.json"

session_cfg = boto3.session.Config(
    region_name=REGION, read_timeout=600,
    connect_timeout=20, retries={"max_attempts": 0},
)
lam_invoke = boto3.client("lambda", config=session_cfg)
lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)

CANONICAL = ["engine", "version", "as_of", "state", "signal_strength",
             "trigger_conditions", "forward_expectations",
             "recommended_trade", "why_now_explainer",
             "methodology", "sources", "schedule"]

report = {"ops": 960, "title": "real edges 1-4 inspection + seed missing S3",
          "run_at": dt.datetime.utcnow().isoformat() + "Z",
          "edges": {}}


def inspect_s3(edge_label, key):
    out = {"key": key}
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        body = obj["Body"].read()
        out["size_b"] = len(body)
        out["last_modified"] = obj["LastModified"].isoformat()[:19]
        try:
            d = json.loads(body)
            out["parsed_ok"] = True
            out["top_keys"] = sorted(list(d.keys()))[:30]
            out["engine"] = d.get("engine")
            out["state"] = d.get("state") or d.get("calendar_phase")
            out["as_of"] = d.get("as_of")
            out["signal_strength"] = d.get("signal_strength")
            missing = [k for k in CANONICAL if k not in d]
            out["canonical_compliance"] = "FULL" if not missing else f"MISSING {missing}"
            # Sample of why_now if present
            wn = d.get("why_now_explainer", "")
            out["why_now_len"] = len(wn) if isinstance(wn, str) else None
            out["why_now_preview"] = wn[:120] if isinstance(wn, str) else None
            # Schedule
            out["schedule"] = d.get("schedule")
            # Forward expectations shape
            fwd = d.get("forward_expectations", {})
            if isinstance(fwd, dict):
                out["forward_horizons"] = sorted(list(fwd.keys()))
        except Exception as e:
            out["parsed_ok"] = False
            out["parse_err"] = str(e)[:200]
    except Exception as e:
        out["err"] = str(e)[:200]
    return out


# ---- Edge 1 ----
print("=== Edge 1 (real: justhodl-capitulation) ===")
report["edges"]["1"] = {
    "real_lambda": "justhodl-capitulation",
    "primary_s3": inspect_s3("e1", "data/capitulation.json"),
    "history_s3": inspect_s3("e1h", "data/capitulation-history.json"),
}

# ---- Edge 2 ----
print("=== Edge 2 (real: justhodl-insider-cluster-scanner) ===")
report["edges"]["2"] = {
    "real_lambda_candidates": ["justhodl-insider-cluster-scanner",
                                "justhodl-insider-aggregate"],
    "insider_clusters_s3": inspect_s3("e2c", "data/insider-clusters.json"),
    "insider_aggregate_s3": inspect_s3("e2a", "data/insider-aggregate.json"),
    "smart_money_clusters_s3": inspect_s3("e2sm", "data/smart-money-clusters.json"),
    "insider_buys_enriched_s3": inspect_s3("e2e", "data/insider-buys-enriched.json"),
}

# ---- Edge 3 ----
print("=== Edge 3 (breadth-thrust): check if Lambda exists ===")
e3 = {"repo_dir": "aws/lambdas/justhodl-breadth-thrust"}
try:
    info = lam.get_function(FunctionName="justhodl-breadth-thrust")
    e3["lambda_exists"] = True
    e3["mem"] = info["Configuration"].get("MemorySize")
    e3["timeout"] = info["Configuration"].get("Timeout")
    e3["runtime"] = info["Configuration"].get("Runtime")
    # Try invoke
    print("  Lambda exists; attempting invoke")
    t0 = time.time()
    try:
        resp = lam_invoke.invoke(FunctionName="justhodl-breadth-thrust",
                                  InvocationType="RequestResponse",
                                  Payload=b"{}")
        body = resp["Payload"].read().decode("utf-8", errors="replace")
        e3["invoke"] = {
            "dur_s": round(time.time() - t0, 2),
            "status": resp.get("StatusCode"),
            "err": resp.get("FunctionError"),
            "body": body[:400],
        }
    except Exception as e:
        e3["invoke_err"] = str(e)[:200]
    time.sleep(2)
    e3["s3_breadth_thrust"] = inspect_s3("e3", "data/breadth-thrust.json")
except Exception as e:
    e3["lambda_exists"] = False
    e3["lookup_err"] = str(e)[:200]
report["edges"]["3"] = e3

# ---- Edge 4 ----
print("=== Edge 4: invoke justhodl-vol-target-unwind to seed S3 ===")
e4 = {"real_lambda": "justhodl-vol-target-unwind"}
t0 = time.time()
try:
    resp = lam_invoke.invoke(FunctionName="justhodl-vol-target-unwind",
                              InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8", errors="replace")
    e4["invoke"] = {
        "dur_s": round(time.time() - t0, 2),
        "status": resp.get("StatusCode"),
        "err": resp.get("FunctionError"),
        "body": body[:400],
    }
except Exception as e:
    e4["invoke_err"] = str(e)[:200]
time.sleep(2)
e4["s3_vol_target_unwind"] = inspect_s3("e4a", "data/vol-target-unwind.json")
e4["s3_vol_target"] = inspect_s3("e4b", "data/vol-target.json")
report["edges"]["4"] = e4

# Save
os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
with open(REPORT_PATH, "w") as f:
    json.dump(report, f, indent=2, default=str)
print(f"\nreport written to {REPORT_PATH}")
