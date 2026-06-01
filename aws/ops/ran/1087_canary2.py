#!/usr/bin/env python3
"""1087 — verify cds-proxy still works after deploy via new workflow.

This is the second canary: cds-proxy was shimmed in batch 1, then synced
to repo via ops/1086. The workflow_dispatch just redeployed it from the
synced repo source. Should preserve everything.
"""
import io, json, os, pathlib, time, urllib.request, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1087_canary2.json"
lam = boto3.client("lambda", region_name="us-east-1", config=Config(read_timeout=180))


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    name = "justhodl-cds-proxy"
    
    # Inspect deployed code
    info = lam.get_function(FunctionName=name)
    url = info["Code"]["Location"]
    with urllib.request.urlopen(url, timeout=30) as r:
        zip_bytes = r.read()
    
    files = []
    has_shim_file = False
    has_shim_import = False
    has_shared_files = []
    
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for fname in zf.namelist():
            files.append(fname)
            if fname == "_fred_shim.py":
                has_shim_file = True
            elif fname == "lambda_function.py":
                code = zf.read(fname).decode("utf-8", errors="replace")
                has_shim_import = "_fred_shim" in code
            # Check for other shared files that should now bundle automatically
            if fname in ["api_auth.py", "_sentry_lite.py", "system_events.py",
                          "ka_aliases.py", "calibration.py", "finra_si.py"]:
                has_shared_files.append(fname)
    
    out["zip_inspection"] = {
        "last_modified": info["Configuration"].get("LastModified"),
        "zip_size":      len(zip_bytes),
        "files":         files,
        "has_shim_file":   has_shim_file,
        "has_shim_import": has_shim_import,
        "auto_bundled_shared_files": has_shared_files,
    }
    
    # Sync-invoke to confirm functional
    t0 = time.time()
    r = lam.invoke(FunctionName=name, InvocationType="RequestResponse", Payload=b"{}")
    body = r["Payload"].read().decode("utf-8", errors="replace")
    
    out["invoke"] = {
        "elapsed_s":  round(time.time() - t0, 1),
        "status":     r.get("StatusCode"),
        "raw":        body[:400],
    }
    try:
        p = json.loads(body)
        out["invoke"]["body_status"] = p.get("statusCode")
        if isinstance(p.get("body"), str):
            inner = json.loads(p["body"])
            if isinstance(inner, dict):
                out["invoke"]["composite"] = inner.get("composite")
                out["invoke"]["regime"]    = inner.get("regime")
                out["invoke"]["inner_keys"] = list(inner.keys())[:10]
    except Exception:
        pass
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1087] DONE")


if __name__ == "__main__":
    main()
