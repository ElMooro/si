#!/usr/bin/env python3
"""1082 — fix the 25-day staleness of edge-data + flow-data.

ROOT CAUSE (per ops/1081)
═════════════════════════
Both Lambdas have an auth gate (`authorize(event, allowed_origins=...)`)
as the first step in `lambda_handler`. Auth was added 2026-05-06 (memory
item #7, "Public API Auth Tiers Phase 1+2A-D LIVE") which is exactly
when the S3 files froze.

When EventBridge cron fires the Lambda with an empty event, there's no
Authorization header, no x-api-key, and no Origin header → auth returns
401 → handler exits BEFORE the S3 write code at the bottom.

Function URL calls from the browser succeed because they DO have an
Origin header (justhodl.ai) which matches allowed_origins. So the
Lambdas appear to work but the cron path is silently broken.

FIX
═══
1. Add internal-invocation bypass at the top of lambda_handler:
     if not event.get("requestContext", {}).get("http"):
         # No HTTP context = EventBridge cron / boto3 direct invoke.
         # These can only reach the Lambda through trusted internal
         # paths (Function URL public access requires requestContext.http).
         # Skip auth gate.
         key_meta = {"auth_mode": "internal", "tier": "ENTERPRISE",
                     "tier_label": "Internal cron/direct"}
     else:
         key_meta, err = authorize(event, allowed_origins=ALLOWED_ORIGINS)
         if err:
             return err

2. Add EventBridge schedule for justhodl-options-flow (currently has
   none — explains why flow-data.json hasn't been updated since 5/6
   regardless of auth issue).

WHY OPS-SCRIPT INSTEAD OF REPO DEPLOY
═════════════════════════════════════
deploy-lambdas.yml only zips $dir/source/ — it doesn't include
aws/shared/_fred_shim.py. Pushing source changes would un-shim these
Lambdas. So we patch the deployed zip directly (preserves shim) and
mark the repo update as [skip-deploy].
"""
import io, json, os, pathlib, re, time, urllib.request, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1082_auth_gate_fix.json"
REGION = "us-east-1"

lam = boto3.client("lambda", region_name=REGION,
                    config=Config(read_timeout=180))
s3 = boto3.client("s3", region_name=REGION)
events = boto3.client("events", region_name=REGION)


# The auth bypass block to inject. Detects EventBridge cron / boto3
# direct invokes (which lack requestContext.http) and skips authorize().
BYPASS_BLOCK = '''
    # Internal invocation bypass (ops/1082) — EventBridge cron and boto3
    # direct invokes lack requestContext.http (only Function URL calls
    # set it). Public attack surface requires Function URL, so absence
    # of requestContext.http means trusted internal caller.
    if not event.get("requestContext", {}).get("http"):
        # Skip auth gate — internal cron/direct invocation
        key_meta = {"auth_mode": "internal", "tier": "ENTERPRISE",
                    "tier_label": "Internal cron/direct", "owner_email": "",
                    "label": "internal-bypass", "created_at": ""}
    else:
'''


def download_code(name):
    info = lam.get_function(FunctionName=name)
    url = info["Code"]["Location"]
    with urllib.request.urlopen(url, timeout=30) as r:
        zip_bytes = r.read()
    files = {}
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for fname in zf.namelist():
            files[fname] = zf.read(fname)
    return files


def patch_auth_gate(code_bytes):
    """Inject the internal-invocation bypass before `authorize()` call.
    Idempotent — returns same input if already patched.
    """
    src = code_bytes.decode("utf-8", errors="replace")
    
    if "Internal invocation bypass (ops/1082)" in src:
        return code_bytes, False, "already_patched"
    
    # Find the pattern: 'key_meta, err = authorize(event, allowed_origins=ALLOWED_ORIGINS)'
    # and wrap it in an if/else.
    lines = src.split("\n")
    
    for i, line in enumerate(lines):
        # Match the authorize() call line
        if "authorize(event" in line and "allowed_origins" in line:
            # Find the indentation level
            indent = len(line) - len(line.lstrip())
            indent_str = " " * indent
            
            # The next line should be the `err` check, like:
            #   if err:
            #       return err
            # We need to wrap THESE lines as the `else` block.
            
            # Look ahead 3 lines for the `if err:` pattern
            err_check_end = i
            for j in range(i+1, min(i+4, len(lines))):
                if "return err" in lines[j]:
                    err_check_end = j
                    break
            
            if err_check_end == i:
                return code_bytes, False, "could_not_find_err_check"
            
            # Build the replacement:
            #   <bypass block (`else:` is the last line)>
            #       <original authorize call indented further>
            #       <original err check indented further>
            
            bypass_lines = [
                f"{indent_str}# Internal invocation bypass (ops/1082) — EventBridge cron and",
                f"{indent_str}# boto3 direct invokes lack requestContext.http. Skip auth.",
                f"{indent_str}if not event.get(\"requestContext\", {{}}).get(\"http\"):",
                f"{indent_str}    key_meta = {{\"auth_mode\": \"internal\", \"tier\": \"ENTERPRISE\",",
                f"{indent_str}                \"tier_label\": \"Internal cron/direct\",",
                f"{indent_str}                \"owner_email\": \"\", \"label\": \"internal-bypass\",",
                f"{indent_str}                \"created_at\": \"\"}}",
                f"{indent_str}    err = None",
                f"{indent_str}else:",
            ]
            
            # Indent original authorize call + err check by 4 more spaces
            new_authorize_line = "    " + line
            new_err_lines = ["    " + L for L in lines[i+1:err_check_end+1]]
            
            # Build new file
            new_lines = (
                lines[:i] +
                bypass_lines +
                [new_authorize_line] +
                new_err_lines +
                lines[err_check_end+1:]
            )
            return "\n".join(new_lines).encode("utf-8"), True, "patched"
    
    return code_bytes, False, "no_authorize_call_found"


def patch_and_deploy(name):
    """Download, patch, redeploy. Preserves _fred_shim.py."""
    out = {"name": name}
    try:
        files = download_code(name)
        if "lambda_function.py" not in files:
            out["err"] = "no lambda_function.py"
            return out
        
        patched, did_patch, status = patch_auth_gate(files["lambda_function.py"])
        out["patch_status"] = status
        out["had_shim"] = "_fred_shim.py" in files
        
        if not did_patch:
            out["action"] = "skip"
            return out
        
        # Rebuild zip preserving all original files (including shim)
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for fname, content in files.items():
                if fname == "lambda_function.py":
                    zf.writestr(fname, patched)
                else:
                    zf.writestr(fname, content)
        zb = buf.getvalue()
        
        for attempt in range(3):
            try:
                lam.update_function_code(FunctionName=name, ZipFile=zb, Publish=False)
                lam.get_waiter("function_updated").wait(FunctionName=name)
                out["deploy"] = {"ok": True, "zip_size": len(zb)}
                break
            except Exception as e:
                if "ResourceConflict" in str(e) and attempt < 2:
                    time.sleep(5); continue
                out["deploy"] = {"ok": False, "err": str(e)[:200]}
                break
    except Exception as e:
        out["err"] = str(e)[:200]
    
    return out


def ensure_schedule(name, schedule_expression, rule_name):
    """Create or update an EventBridge schedule for a Lambda."""
    out = {"name": name, "rule_name": rule_name}
    try:
        # Create/update the rule
        events.put_rule(
            Name=rule_name,
            ScheduleExpression=schedule_expression,
            State="ENABLED",
            Description=f"Scheduled invocation of {name} for cache refresh",
        )
        
        # Get Lambda ARN
        info = lam.get_function(FunctionName=name)
        lambda_arn = info["Configuration"]["FunctionArn"]
        
        # Add the Lambda as a target
        events.put_targets(
            Rule=rule_name,
            Targets=[{
                "Id":  "1",
                "Arn": lambda_arn,
            }],
        )
        
        # Grant EventBridge permission to invoke the Lambda
        try:
            lam.add_permission(
                FunctionName=name,
                StatementId=f"{rule_name}-invoke",
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=events.describe_rule(Name=rule_name)["Arn"],
            )
            out["permission"] = "added"
        except lam.exceptions.ResourceConflictException:
            out["permission"] = "already_exists"
        
        out["status"] = "ok"
    except Exception as e:
        out["err"] = str(e)[:200]
    
    return out


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # Phase 1: patch both Lambdas
    print("[1082] phase 1: patch auth gate in both Lambdas…")
    out["patches"] = []
    for name in ["justhodl-edge-engine", "justhodl-options-flow"]:
        result = patch_and_deploy(name)
        out["patches"].append(result)
        print(f"[1082]   {name}: status={result.get('patch_status','?')} "
                f"deploy_ok={result.get('deploy',{}).get('ok','?')}")
        time.sleep(2)
    
    # Phase 2: add EventBridge schedule for options-flow
    print("[1082] phase 2: add EventBridge schedule for options-flow…")
    # Every 30 min — flow data is intraday-sensitive
    out["schedule"] = ensure_schedule(
        "justhodl-options-flow",
        "rate(30 minutes)",
        "justhodl-options-flow-30m",
    )
    
    # Phase 3: sync-invoke both with EMPTY event (mimics cron) → should now succeed
    print("[1082] phase 3: sync-invoke with empty event to verify fix…")
    out["verifications"] = []
    for name in ["justhodl-edge-engine", "justhodl-options-flow"]:
        v = {"name": name}
        t0 = time.time()
        try:
            r = lam.invoke(FunctionName=name, InvocationType="RequestResponse", Payload=b"{}")
            body = r["Payload"].read().decode("utf-8", errors="replace")
            v["elapsed_s"] = round(time.time() - t0, 1)
            v["status"]    = r.get("StatusCode")
            try:
                p = json.loads(body)
                v["body_status"] = p.get("statusCode")
                # Extract some indicators from the inner body
                if isinstance(p.get("body"), str):
                    try:
                        inner = json.loads(p["body"])
                        if isinstance(inner, dict):
                            keys = list(inner.keys())[:8]
                            v["inner_keys"] = keys
                            v["composite_score"] = inner.get("composite_score") or inner.get("data", {}).get("sentiment", {}).get("composite")
                    except Exception:
                        pass
            except Exception:
                v["raw"] = body[:300]
        except Exception as e:
            v["err"] = str(e)[:200]
        out["verifications"].append(v)
        time.sleep(3)
    
    # Phase 4: re-check S3 LastModified to confirm writes happened
    print("[1082] phase 4: re-check S3 timestamps…")
    out["s3_after"] = {}
    for key in ["edge-data.json", "flow-data.json"]:
        try:
            obj = s3.head_object(Bucket="justhodl-dashboard-live", Key=key)
            out["s3_after"][key] = {
                "last_modified": obj["LastModified"].isoformat(),
                "size":          obj["ContentLength"],
            }
        except Exception as e:
            out["s3_after"][key] = {"err": str(e)[:120]}
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1082] DONE → {REPORT}")


if __name__ == "__main__":
    main()
