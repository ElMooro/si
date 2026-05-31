#!/usr/bin/env python3
"""1081 — inspect S3 write paths + check if Function URL calls updated S3.

Specific questions:
  1. Did my Function URL probes at 23:50 UTC actually update S3?
     (compare current S3 LastModified to before)
  2. What's the actual code path around s3.put_object for edge-data.json?
     Is it gated on event type / scheduled / etc?
  3. Same for flow-data.json in options-flow.
  4. Does the scheduled edge-engine cron actually call the same code path
     as the Function URL?
"""
import io, json, os, pathlib, time, urllib.request, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1081_write_path_inspect.json"
lam = boto3.client("lambda", region_name="us-east-1", config=Config(read_timeout=180))
s3 = boto3.client("s3", region_name="us-east-1")


def get_function_code(name):
    info = lam.get_function(FunctionName=name)
    url = info["Code"]["Location"]
    with urllib.request.urlopen(url, timeout=30) as r:
        zip_bytes = r.read()
    files = {}
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for fname in zf.namelist():
            if fname.endswith(".py"):
                files[fname] = zf.read(fname).decode("utf-8", errors="replace")
    return files


def find_s3_write_path(code, s3_key):
    """Find the lines around `s3.put_object` calls for the given key.
    Returns list of {start_line, code_block} context.
    """
    lines = code.split("\n")
    matches = []
    for i, line in enumerate(lines):
        if s3_key in line and ("put_object" in line or any(
            "put_object" in lines[max(0, i-5):i+1][j] for j in range(min(6, i+1))
        )):
            # Capture 15 lines before to 5 lines after
            start = max(0, i - 15)
            end = min(len(lines), i + 6)
            matches.append({
                "around_line": i + 1,
                "context": "\n".join(lines[start:end]),
            })
            break  # one per file is enough
    return matches


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # Phase 1: check current S3 state
    print("[1081] phase 1: check current S3 state of both files…")
    for key in ["edge-data.json", "flow-data.json"]:
        try:
            obj = s3.head_object(Bucket="justhodl-dashboard-live", Key=key)
            out[f"current_{key}"] = {
                "last_modified": obj["LastModified"].isoformat(),
                "size":          obj["ContentLength"],
            }
        except Exception as e:
            out[f"current_{key}_err"] = str(e)[:120]
    
    # Phase 2: inspect S3 write code paths
    print("[1081] phase 2: inspect S3 write code paths…")
    for lambda_name, target_key in [
        ("justhodl-edge-engine",  "edge-data.json"),
        ("justhodl-options-flow", "flow-data.json"),
    ]:
        print(f"[1081]   inspecting {lambda_name}…")
        try:
            files = get_function_code(lambda_name)
            entry = {"files": list(files.keys())}
            for fname, code in files.items():
                if not fname.endswith(".py") or fname.startswith("_"):
                    continue
                matches = find_s3_write_path(code, target_key)
                if matches:
                    entry[f"writes_in_{fname}"] = matches
            
            # Also: find the lambda_handler function and look for branching
            # logic that gates whether S3 write happens
            for fname, code in files.items():
                if fname == "lambda_function.py":
                    lines = code.split("\n")
                    # Find handler
                    for i, line in enumerate(lines):
                        if "def lambda_handler" in line or "def handler" in line:
                            # Grab first 60 lines of the handler
                            entry["handler_first_60_lines"] = "\n".join(
                                lines[i:i+60]
                            )
                            break
                    # Count occurrences of s3.put_object overall + grep all
                    put_count = code.count("s3.put_object") + code.count("put_object(")
                    entry["put_object_count"] = put_count
                    # Find the LAST 200 lines (often where the writer/main is)
                    entry["last_60_lines"] = "\n".join(lines[-60:])
            
            out[lambda_name] = entry
        except Exception as e:
            out[lambda_name] = {"err": str(e)[:200]}
    
    # Phase 3: get full EventBridge target details for edge-engine
    print("[1081] phase 3: check EventBridge target for edge-engine…")
    events = boto3.client("events", region_name="us-east-1")
    try:
        target_arn = "arn:aws:lambda:us-east-1:857687956942:function:justhodl-edge-engine"
        rule_names_resp = events.list_rule_names_by_target(TargetArn=target_arn)
        rule_names = rule_names_resp.get("RuleNames", [])
        out["edge_engine_schedules"] = []
        for rn in rule_names:
            rule_info = events.describe_rule(Name=rn)
            targets = events.list_targets_by_rule(Rule=rn)
            for t in targets.get("Targets", []):
                if t.get("Arn") == target_arn:
                    out["edge_engine_schedules"].append({
                        "rule_name":      rn,
                        "schedule":       rule_info.get("ScheduleExpression"),
                        "state":          rule_info.get("State"),
                        "target_id":      t.get("Id"),
                        "input":          t.get("Input"),  # ← key: what payload does cron send?
                        "input_path":     t.get("InputPath"),
                    })
    except Exception as e:
        out["edge_engine_schedules_err"] = str(e)[:200]
    
    # Phase 4: try direct sync-invoke with EMPTY event (mimics cron)
    print("[1081] phase 4: sync-invoke each with empty event (mimics cron)…")
    for name in ["justhodl-edge-engine", "justhodl-options-flow"]:
        t0 = time.time()
        try:
            r = lam.invoke(FunctionName=name, InvocationType="RequestResponse", Payload=b"{}")
            body = r["Payload"].read().decode("utf-8", errors="replace")
            entry = {
                "elapsed_s": round(time.time() - t0, 1),
                "status":    r.get("StatusCode"),
                "raw":       body[:600],
            }
            try:
                p = json.loads(body)
                entry["body_status"] = p.get("statusCode")
                if isinstance(p.get("body"), str):
                    try:
                        inner = json.loads(p["body"])
                        entry["inner_top_keys"] = list(inner.keys())[:15] if isinstance(inner, dict) else None
                    except Exception:
                        entry["body_preview"] = p["body"][:200]
            except Exception:
                pass
            out[f"empty_invoke_{name}"] = entry
        except Exception as e:
            out[f"empty_invoke_{name}_err"] = str(e)[:200]
        time.sleep(2)
    
    # Phase 5: re-check S3 LastModified
    print("[1081] phase 5: re-check S3 after invokes…")
    for key in ["edge-data.json", "flow-data.json"]:
        try:
            obj = s3.head_object(Bucket="justhodl-dashboard-live", Key=key)
            out[f"after_invoke_{key}"] = {
                "last_modified": obj["LastModified"].isoformat(),
                "size":          obj["ContentLength"],
            }
        except Exception as e:
            out[f"after_invoke_{key}_err"] = str(e)[:120]
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1081] DONE → {REPORT}")


if __name__ == "__main__":
    main()
