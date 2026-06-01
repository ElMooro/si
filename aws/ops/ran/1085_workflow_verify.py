#!/usr/bin/env python3
"""1085 — verify workflow change preserves _fred_shim.py + auth_authority etc.

Tests:
  1. justhodl-yield-curve was redeployed via workflow_dispatch — confirm
     its zip now contains _fred_shim.py (proves bundling works)
  2. The Lambda still functions correctly (invoke + verify)
  3. Spot-check 2 other shimmed Lambdas to confirm their zips have shim
     (without being redeployed)
"""
import io, json, os, pathlib, time, urllib.request, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1085_workflow_verify.json"
REGION = "us-east-1"

lam = boto3.client("lambda", region_name=REGION,
                    config=Config(read_timeout=180))


def inspect_zip(name):
    info = lam.get_function(FunctionName=name)
    url = info["Code"]["Location"]
    with urllib.request.urlopen(url, timeout=30) as r:
        zip_bytes = r.read()
    
    file_list = []
    fred_shim_first_line = None
    has_shim_import = False
    
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for fname in zf.namelist():
            file_list.append(fname)
            if fname == "_fred_shim.py":
                content = zf.read(fname).decode("utf-8", errors="replace")
                fred_shim_first_line = content.split("\n")[0]
            elif fname == "lambda_function.py":
                content = zf.read(fname).decode("utf-8", errors="replace")
                has_shim_import = "_fred_shim" in content
    
    return {
        "last_modified":       info["Configuration"].get("LastModified"),
        "zip_size":            len(zip_bytes),
        "files":               file_list,
        "has_fred_shim_file":  "_fred_shim.py" in file_list,
        "shim_first_line":     fred_shim_first_line,
        "lambda_imports_shim": has_shim_import,
    }


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # Phase 1: inspect the canary (just redeployed)
    print("[1085] phase 1: inspect canary (justhodl-yield-curve)…")
    try:
        info = inspect_zip("justhodl-yield-curve")
        out["canary"] = info
    except Exception as e:
        out["canary_err"] = str(e)[:200]
    
    # Phase 2: invoke canary to confirm still works
    print("[1085] phase 2: invoke canary…")
    t0 = time.time()
    try:
        r = lam.invoke(FunctionName="justhodl-yield-curve",
                        InvocationType="RequestResponse", Payload=b"{}")
        body = r["Payload"].read().decode("utf-8", errors="replace")
        out["canary_invoke"] = {
            "elapsed_s": round(time.time() - t0, 1),
            "status":    r.get("StatusCode"),
            "raw":       body[:400],
        }
        try:
            p = json.loads(body)
            if isinstance(p.get("body"), str):
                inner = json.loads(p["body"])
                out["canary_invoke"]["regime"]   = inner.get("regime")
                out["canary_invoke"]["n_signals"] = inner.get("n_signals")
                out["canary_invoke"]["twos_tens_bps"] = inner.get("twos_tens_bps")
        except Exception:
            pass
    except Exception as e:
        out["canary_invoke_err"] = str(e)[:200]
    
    # Phase 3: spot-check 2 other Lambdas (NOT redeployed) still have shim
    print("[1085] phase 3: spot-check shimmed Lambdas (no redeploy)…")
    out["other_lambdas"] = {}
    for name in ["justhodl-divergence-engine-v2",
                  "justhodl-cds-proxy",
                  "justhodl-daily-report-v3"]:
        try:
            info = inspect_zip(name)
            out["other_lambdas"][name] = {
                "has_fred_shim_file":  info["has_fred_shim_file"],
                "lambda_imports_shim": info["lambda_imports_shim"],
                "last_modified":       info["last_modified"],
                "zip_size":            info["zip_size"],
            }
        except Exception as e:
            out["other_lambdas"][name] = {"err": str(e)[:200]}
    
    # Phase 4: check that an UN-shimmed Lambda's zip now ALSO has the shim
    # (because the new workflow bundles it automatically). Pick a Lambda
    # that wasn't in our 71-Lambda batch — should NOT have shim today,
    # but a workflow-dispatch on it would add it. Don't deploy though.
    print("[1085] phase 4: confirm canary's lambda_function.py still imports shim…")
    if out.get("canary", {}).get("lambda_imports_shim"):
        out["canary_lambda_function_still_imports_shim"] = True
    else:
        out["canary_lambda_function_still_imports_shim"] = False
        # This would mean the workflow OVERWROTE the lambda_function.py
        # with the unshimmed source/ version. CRITICAL.
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1085] DONE → {REPORT}")


if __name__ == "__main__":
    main()
