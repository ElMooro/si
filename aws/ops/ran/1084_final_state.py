#!/usr/bin/env python3
"""1084 — add FRED shim to justhodl-edge-engine + capture final state.

edge-engine has an `engine_liquidity` sub-engine that pulls Fed B/S, M2,
RRP, SOFR, FF rate from FRED. The original shim batches (ops/1073,1074)
missed it because the audit classified by direct api.stlouisfed.org
references in the top-level lambda_function.py, but FRED calls may be
wrapped through helpers.

Also captures final state of edge-data.json + flow-data.json for the
session record.
"""
import io, json, os, pathlib, time, urllib.request, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1084_final_state.json"
REGION = "us-east-1"

lam = boto3.client("lambda", region_name=REGION,
                    config=Config(read_timeout=180))
s3 = boto3.client("s3", region_name=REGION)


SHIM_IMPORT_LINE = "import _fred_shim  # noqa: F401  — cache-first FRED + 429 backoff (ops/1084)"


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


def patch_lambda_code(code_bytes):
    """Insert `import _fred_shim` after the module docstring + imports.
    Returns (patched_code_bytes, did_patch)."""
    src = code_bytes.decode("utf-8", errors="replace")
    if "_fred_shim" in src:
        return code_bytes, False
    lines = src.split("\n")
    i, n = 0, len(lines)
    while i < n and (lines[i].startswith("#!") or "coding" in lines[i][:20]):
        i += 1
    if i < n and (lines[i].startswith('"""') or lines[i].startswith("'''")):
        quote = lines[i][:3]
        if lines[i].count(quote) >= 2:
            i += 1
        else:
            i += 1
            while i < n and quote not in lines[i]:
                i += 1
            if i < n:
                i += 1
    while i < n and not lines[i].strip():
        i += 1
    last_import_idx = i - 1
    while i < n:
        stripped = lines[i].strip()
        if stripped.startswith("import ") or stripped.startswith("from "):
            last_import_idx = i
            i += 1
        elif stripped == "" or stripped.startswith("#"):
            i += 1
        else:
            break
    lines.insert(last_import_idx + 1, SHIM_IMPORT_LINE)
    return "\n".join(lines).encode("utf-8"), True


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # Phase 1: add shim to edge-engine
    print("[1084] phase 1: add FRED shim to edge-engine…")
    name = "justhodl-edge-engine"
    try:
        with open("aws/shared/_fred_shim.py", "rb") as f:
            shim_bytes = f.read()
        
        files = download_code(name)
        patched, did_patch = patch_lambda_code(files.get("lambda_function.py", b""))
        out["already_shimmed"] = not did_patch
        out["had_shim_file"]  = "_fred_shim.py" in files
        
        if did_patch:
            # Build new zip
            buf = io.BytesIO()
            with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
                for fname, content in files.items():
                    if fname == "lambda_function.py":
                        zf.writestr(fname, patched)
                    elif fname == "_fred_shim.py":
                        continue
                    else:
                        zf.writestr(fname, content)
                zf.writestr("_fred_shim.py", shim_bytes)
            zb = buf.getvalue()
            
            for attempt in range(3):
                try:
                    lam.update_function_code(FunctionName=name, ZipFile=zb, Publish=False)
                    lam.get_waiter("function_updated").wait(FunctionName=name)
                    out["shim_deploy"] = {"ok": True, "zip_size": len(zb)}
                    break
                except Exception as e:
                    if "ResourceConflict" in str(e) and attempt < 2:
                        time.sleep(5); continue
                    out["shim_deploy"] = {"ok": False, "err": str(e)[:200]}
                    break
    except Exception as e:
        out["shim_err"] = str(e)[:200]
    
    time.sleep(3)
    
    # Phase 2: Sync-invoke both to confirm everything works post-all-fixes
    print("[1084] phase 2: final sync-invoke verifications…")
    out["invokes"] = []
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
                if isinstance(p.get("body"), str):
                    try:
                        inner = json.loads(p["body"])
                        if isinstance(inner, dict):
                            # Capture key business signals
                            if name == "justhodl-edge-engine":
                                v["composite_score"] = inner.get("composite_score")
                                v["regime"]          = inner.get("regime")
                                v["engine_scores"]   = inner.get("engine_scores")
                                v["alerts"]          = inner.get("alerts")
                            elif name == "justhodl-options-flow":
                                data = inner.get("data", {})
                                v["vix"]   = (data.get("vix_complex") or {}).get("vix", {}).get("value")
                                v["pc"]    = (data.get("put_call") or {}).get("total_put_call_ratio")
                                v["pc_signal"] = (data.get("put_call") or {}).get("pc_signal")
                                v["gex"]   = (data.get("gamma_exposure") or {}).get("total_gex")
                                v["gex_regime"] = (data.get("gamma_exposure") or {}).get("regime")
                                v["sentiment"] = (data.get("sentiment") or {}).get("composite")
                    except Exception:
                        v["body_preview"] = p["body"][:200]
            except Exception:
                v["raw"] = body[:300]
        except Exception as e:
            v["err"] = str(e)[:200]
        out["invokes"].append(v)
        time.sleep(3)
    
    # Phase 3: final S3 timestamps
    print("[1084] phase 3: final S3 state…")
    out["s3"] = {}
    for key in ["edge-data.json", "flow-data.json"]:
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=key)
            body = obj["Body"].read()
            d_data = json.loads(body)
            entry = {
                "last_modified": obj["LastModified"].isoformat(),
                "size":          obj["ContentLength"],
            }
            # Look at the actual generated_at inside the file
            if isinstance(d_data, dict):
                entry["meta_generated_at"] = (
                    d_data.get("generated_at") or
                    d_data.get("timestamp") or
                    (d_data.get("meta") or {}).get("generated_at")
                )
                entry["top_keys"] = list(d_data.keys())[:8]
            out["s3"][key] = entry
        except Exception as e:
            out["s3"][key] = {"err": str(e)[:200]}
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1084] DONE → {REPORT}")


if __name__ == "__main__":
    main()
