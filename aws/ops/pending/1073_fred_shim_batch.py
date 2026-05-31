#!/usr/bin/env python3
"""1073 — batch-install _fred_shim into all scheduled FRED-direct Lambdas.

STRATEGY
════════
69 Lambdas hit FRED directly with no cache. Of those, ~19 are on
EventBridge schedules (currently running and likely affected by 429s).
This script:

  1. Picks the top 19 priority Lambdas (scheduled + DIRECT_FRED_ONLY)
  2. For each:
     a. Downloads deployed source
     b. Adds `import _fred_shim` line at the top of lambda_function.py
        (after the docstring + existing imports, before any code)
     c. Bundles _fred_shim.py + the patched code into a new zip
     d. update_function_code
  3. Reports per-Lambda result + sync-invoke verification on 3 sample Lambdas

SAFE FAIL-OVER
══════════════
If _fred_shim.py fails to load S3 cache, it silently falls through to
live FRED with 429 backoff. Lambdas continue to function exactly as
before — no new failure modes introduced.
"""
import io, json, os, pathlib, time, urllib.request, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1073_fred_shim_batch.json"
REGION = "us-east-1"

lam = boto3.client("lambda", region_name=REGION,
                    config=Config(read_timeout=180))

# Priority targets — scheduled Lambdas that are DIRECT_FRED_ONLY per ops/1072
# Sorted by priority: refs DESC, then schedule density
PRIORITY_LAMBDAS = [
    "justhodl-divergence-engine-v2",     # rate(2 hours) — most frequent
    "justhodl-cross-asset-confirm",      # rate(3 hours)
    "justhodl-liquidity-pulse",          # rate(6 hours)
    "justhodl-crisis-plumbing",          # rate(6 hours)
    "justhodl-yield-curve",              # rate(6 hours)
    "justhodl-liquidity-credit-engine",  # rate(6 hours)
    "justhodl-macro-surprise",           # rate(6 hours)
    "justhodl-macro-nowcast",            # rate(6 hours)
    "justhodl-cds-proxy",                # cron 3x/day MON-FRI
    "justhodl-bond-trace",               # cron daily MON-FRI
    "justhodl-bank-stress",              # cron daily MON-FRI
    "justhodl-repo-lending",             # cron daily MON-FRI
    "justhodl-margin-lending",           # cron daily MON-FRI
    "justhodl-china-liquidity",          # cron daily MON-FRI
    "justhodl-global-liquidity",         # cron daily MON-FRI
    "justhodl-cross-asset-rv",           # cron MON-FRI 22:45
    "justhodl-tic-flows",                # cron weekly THU
    "justhodl-implied-prob",             # 11 refs, no-schedule but heavy
    "justhodl-liquidity-flow",           # 0 refs but FRED imported
]


SHIM_IMPORT_LINE = "import _fred_shim  # noqa: F401  — cache-first FRED + 429 backoff (ops/1073)"


def download_code(name):
    """Download deployed Lambda zip, return list of (filename, bytes)."""
    info = lam.get_function(FunctionName=name)
    url = info["Code"]["Location"]
    with urllib.request.urlopen(url, timeout=30) as r:
        zip_bytes = r.read()
    files = {}
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for fname in zf.namelist():
            files[fname] = zf.read(fname)
    return files, info["Configuration"]


def patch_lambda_code(code_bytes):
    """Insert `import _fred_shim` after the module docstring + existing imports.
    Returns (patched_code_bytes, did_patch)."""
    src = code_bytes.decode("utf-8", errors="replace")
    
    # Idempotency: if already patched, skip
    if "_fred_shim" in src:
        return code_bytes, False
    
    lines = src.split("\n")
    
    # Find insertion point: after docstring + existing imports, but before
    # any code statements. We use a simple heuristic:
    #   1. Skip shebang/encoding header
    #   2. Skip module docstring (triple-quoted)
    #   3. Skip leading import/from lines
    #   4. Insert before first non-import line
    
    i = 0
    n = len(lines)
    
    # Skip shebang + encoding
    while i < n and (lines[i].startswith("#!") or "coding" in lines[i][:20]):
        i += 1
    
    # Skip module docstring
    if i < n and (lines[i].startswith('"""') or lines[i].startswith("'''")):
        quote = lines[i][:3]
        # Check if single-line docstring
        if lines[i].count(quote) >= 2:
            i += 1
        else:
            i += 1
            while i < n and quote not in lines[i]:
                i += 1
            if i < n:
                i += 1  # include closing line
    
    # Skip blank lines
    while i < n and not lines[i].strip():
        i += 1
    
    # Skip leading imports
    last_import_idx = i - 1
    while i < n:
        stripped = lines[i].strip()
        if stripped.startswith("import ") or stripped.startswith("from "):
            last_import_idx = i
            i += 1
        elif stripped == "" or stripped.startswith("#"):
            i += 1  # blank/comment between imports OK
        else:
            break  # hit first non-import statement
    
    # Insert after the last import found (or at top if none)
    insert_idx = last_import_idx + 1
    lines.insert(insert_idx, SHIM_IMPORT_LINE)
    
    return "\n".join(lines).encode("utf-8"), True


def deploy_patched(name, original_files, patched_lambda_code, shim_bytes):
    """Build a new zip with patched lambda_function.py + _fred_shim.py.
    Update function code."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        # Copy all original files EXCEPT lambda_function.py
        for fname, content in original_files.items():
            if fname == "lambda_function.py":
                zf.writestr(fname, patched_lambda_code)
            elif fname == "_fred_shim.py":
                # Will be added below from local source (newer version wins)
                continue
            else:
                zf.writestr(fname, content)
        # Add the shim
        zf.writestr("_fred_shim.py", shim_bytes)
    
    zb = buf.getvalue()
    for attempt in range(3):
        try:
            lam.update_function_code(FunctionName=name, ZipFile=zb, Publish=False)
            lam.get_waiter("function_updated").wait(FunctionName=name)
            return {"ok": True, "zip_size": len(zb)}
        except Exception as e:
            if "ResourceConflict" in str(e) and attempt < 2:
                time.sleep(4 * (attempt + 1)); continue
            return {"ok": False, "err": str(e)[:200]}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "patches": []}
    
    # Read the shim from local repo
    shim_path = "aws/shared/_fred_shim.py"
    with open(shim_path, "rb") as f:
        shim_bytes = f.read()
    out["shim_size"] = len(shim_bytes)
    
    print(f"[1073] patching {len(PRIORITY_LAMBDAS)} priority Lambdas…")
    for i, name in enumerate(PRIORITY_LAMBDAS):
        print(f"\n[1073] {i+1}/{len(PRIORITY_LAMBDAS)} {name}")
        entry = {"name": name}
        
        try:
            # Download current code
            files, cfg = download_code(name)
            if "lambda_function.py" not in files:
                entry["err"] = "no lambda_function.py in deployed code"
                out["patches"].append(entry)
                continue
            
            # Patch
            patched, did_patch = patch_lambda_code(files["lambda_function.py"])
            entry["already_patched"] = not did_patch
            
            if not did_patch:
                print(f"[1073]   already patched (skipping deploy)")
                # But still ensure shim file is present
                if "_fred_shim.py" not in files:
                    # Add shim to existing deployment
                    deploy_result = deploy_patched(name, files, files["lambda_function.py"], shim_bytes)
                    entry["deploy"] = deploy_result
                    entry["action"] = "shim_only"
                else:
                    entry["action"] = "noop"
                out["patches"].append(entry)
                continue
            
            # Deploy
            deploy_result = deploy_patched(name, files, patched, shim_bytes)
            entry["deploy"] = deploy_result
            entry["action"] = "patched_and_deployed"
            print(f"[1073]   {entry['action']}: {deploy_result}")
        except Exception as e:
            entry["err"] = f"{type(e).__name__}: {str(e)[:200]}"
            print(f"[1073]   ERR: {entry['err']}")
        
        out["patches"].append(entry)
        time.sleep(1)  # pace deploys
    
    # Summary
    out["summary"] = {
        "total":           len(out["patches"]),
        "patched":         sum(1 for p in out["patches"]
                              if p.get("action") == "patched_and_deployed"
                              and p.get("deploy", {}).get("ok")),
        "already":         sum(1 for p in out["patches"] if p.get("already_patched")),
        "shim_only":       sum(1 for p in out["patches"] if p.get("action") == "shim_only"),
        "errors":          sum(1 for p in out["patches"] if p.get("err")
                              or not p.get("deploy", {}).get("ok", True)),
    }
    
    # Sync-invoke 3 priority Lambdas to verify
    print(f"\n[1073] verifying 3 sample Lambdas via sync-invoke…")
    out["verifications"] = []
    for name in ["justhodl-divergence-engine-v2",
                  "justhodl-yield-curve",
                  "justhodl-cds-proxy"]:
        v = {"name": name}
        t0 = time.time()
        try:
            r = lam.invoke(FunctionName=name, InvocationType="RequestResponse", Payload=b"{}")
            body = r["Payload"].read().decode("utf-8", errors="replace")
            v["elapsed_s"] = round(time.time() - t0, 1)
            v["status"]    = r.get("StatusCode")
            try:
                p = json.loads(body)
                v["body_status_code"] = p.get("statusCode")
                v["body_preview"]     = (str(p.get("body", "")) or body)[:300]
            except Exception:
                v["raw"] = body[:300]
        except Exception as e:
            v["err"] = str(e)[:200]
        out["verifications"].append(v)
        time.sleep(2)
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1073] DONE → {REPORT}")
    print(f"[1073] patched={out['summary']['patched']} "
            f"already={out['summary']['already']} "
            f"shim_only={out['summary']['shim_only']} "
            f"errors={out['summary']['errors']}")


if __name__ == "__main__":
    main()
