#!/usr/bin/env python3
"""1080 — comprehensive investigation of edge/flow data pipeline.

Steps:
  1. Read existing edge-data.json + flow-data.json content (25-day-old data) —
     understand the schema the homepage expects
  2. Look for ANY Lambda that writes 'edge-data' or 'flow-data' or 'data.edge'
     or 'data.flow' to S3
  3. Look for Lambdas that CALL justhodl-edge-engine or justhodl-options-flow
     Function URLs (might be a cache-writer in disguise)
  4. Get the Function URLs for edge-engine + options-flow
  5. Test calling each Function URL directly to see what they return
     (will tell us if the live response schema matches what the homepage expects)
  6. Check Function URL configs (auth type, who can call them)
"""
import io, json, os, pathlib, urllib.request, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1080_edge_flow_investigation.json"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # Phase 1: read existing stale files
    print("[1080] phase 1: read existing edge-data + flow-data…")
    for key in ["edge-data.json", "flow-data.json"]:
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            body = obj["Body"].read()
            d = json.loads(body)
            
            entry = {
                "size_kb":       round(len(body) / 1024, 1),
                "last_modified": obj["LastModified"].isoformat(),
                "top_keys":      list(d.keys()) if isinstance(d, dict) else None,
            }
            
            # Schema overview
            if isinstance(d, dict):
                meta = d.get("meta") or d.get("metadata") or {}
                entry["meta"] = meta
                entry["timestamp_field"] = d.get("timestamp") or d.get("generated_at")
                
                # Show 2-level deep keys for understanding schema
                deep_keys = {}
                for k, v in d.items():
                    if isinstance(v, dict):
                        deep_keys[k] = list(v.keys())[:10]
                    elif isinstance(v, list):
                        deep_keys[k] = f"<list of {len(v)} items>"
                    else:
                        deep_keys[k] = f"<{type(v).__name__}: {str(v)[:50]}>"
                entry["deep_keys"] = deep_keys
            
            out[key] = entry
        except Exception as e:
            out[key] = {"err": str(e)[:200]}
    
    # Phase 2: search all Lambdas for 'edge-data' or 'flow-data' write patterns
    print("[1080] phase 2: scanning all Lambdas for edge/flow write patterns…")
    edge_writers = []
    flow_writers = []
    callers_of_engine = []
    
    paginator = lam.get_paginator("list_functions")
    n_scanned = 0
    for page in paginator.paginate():
        for f in page["Functions"]:
            name = f["FunctionName"]
            n_scanned += 1
            try:
                info = lam.get_function(FunctionName=name)
                url = info["Code"]["Location"]
                with urllib.request.urlopen(url, timeout=30) as r:
                    zip_bytes = r.read()
                with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
                    for fname in zf.namelist():
                        if not fname.endswith(".py"):
                            continue
                        try:
                            content = zf.read(fname).decode("utf-8", errors="replace")
                        except Exception:
                            continue
                        # Check for edge-data writes
                        if "edge-data" in content:
                            for line in content.split("\n"):
                                if "edge-data" in line and any(t in line for t in ["put_object", "Key=", '"edge-data', "'edge-data"]):
                                    edge_writers.append({
                                        "lambda":   name,
                                        "file":     fname,
                                        "line":     line.strip()[:200],
                                    })
                                    break
                        # Check for flow-data writes
                        if "flow-data" in content:
                            for line in content.split("\n"):
                                if "flow-data" in line and any(t in line for t in ["put_object", "Key=", '"flow-data', "'flow-data"]):
                                    flow_writers.append({
                                        "lambda":   name,
                                        "file":     fname,
                                        "line":     line.strip()[:200],
                                    })
                                    break
                        # Check for callers of the engine URLs
                        if "edge-engine" in content or "options-flow" in content:
                            if name not in ("justhodl-edge-engine", "justhodl-options-flow"):
                                for line in content.split("\n"):
                                    if ("edge-engine" in line or "options-flow" in line):
                                        if name not in [c["lambda"] for c in callers_of_engine]:
                                            callers_of_engine.append({
                                                "lambda":  name,
                                                "file":    fname,
                                                "line":    line.strip()[:200],
                                            })
                                        break
            except Exception:
                continue
    
    out["n_scanned"]         = n_scanned
    out["edge_data_writers"] = edge_writers
    out["flow_data_writers"] = flow_writers
    out["callers_of_engine"] = callers_of_engine
    
    # Phase 3: get Function URL configs + test calls
    print("[1080] phase 3: probe Function URLs of edge-engine + options-flow…")
    out["function_urls"] = {}
    for name in ["justhodl-edge-engine", "justhodl-options-flow"]:
        try:
            url_config = lam.get_function_url_config(FunctionName=name)
            entry = {
                "url":        url_config.get("FunctionUrl"),
                "auth_type":  url_config.get("AuthType"),
                "cors":       url_config.get("Cors", {}),
            }
            
            # Try to call it (anonymously — might fail with 403)
            fu = url_config.get("FunctionUrl", "").rstrip("/")
            try:
                req = urllib.request.Request(fu, headers={
                    "User-Agent": "JustHodl-Diag/1.0",
                    "Origin": "https://justhodl.ai",
                })
                with urllib.request.urlopen(req, timeout=30) as r:
                    body = r.read()
                    entry["call_status"] = r.status
                    entry["call_size"]   = len(body)
                    try:
                        d = json.loads(body)
                        if isinstance(d, dict):
                            entry["response_keys"] = list(d.keys())[:15]
                            # Sample nested structure
                            sample = {}
                            for k, v in d.items():
                                if isinstance(v, dict):
                                    sample[k] = list(v.keys())[:8]
                                elif isinstance(v, (int, float, str, bool)):
                                    sample[k] = v
                                elif isinstance(v, list):
                                    sample[k] = f"<list of {len(v)}>"
                            entry["response_structure"] = sample
                    except Exception:
                        entry["response_preview"] = body[:300].decode("utf-8", errors="replace")
            except urllib.error.HTTPError as e:
                entry["call_err"] = f"HTTP {e.code}"
                try:
                    entry["call_err_body"] = e.read().decode("utf-8")[:200]
                except Exception:
                    pass
            except Exception as e:
                entry["call_err"] = f"{type(e).__name__}: {str(e)[:120]}"
            out["function_urls"][name] = entry
        except Exception as e:
            out["function_urls"][name] = {"err": str(e)[:200]}
    
    # Phase 4: check EventBridge rules for these Lambdas
    print("[1080] phase 4: EventBridge rules…")
    events = boto3.client("events", region_name=REGION)
    out["schedules"] = {}
    for name in ["justhodl-edge-engine", "justhodl-options-flow"]:
        try:
            target_arn = f"arn:aws:lambda:{REGION}:857687956942:function:{name}"
            result = events.list_rule_names_by_target(TargetArn=target_arn)
            rule_names = result.get("RuleNames", [])
            out["schedules"][name] = []
            for rn in rule_names:
                r = events.describe_rule(Name=rn)
                out["schedules"][name].append({
                    "name":     rn,
                    "schedule": r.get("ScheduleExpression"),
                    "state":    r.get("State"),
                })
        except Exception as e:
            out["schedules"][name] = {"err": str(e)[:200]}
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1080] DONE — scanned {n_scanned} Lambdas")


if __name__ == "__main__":
    main()
