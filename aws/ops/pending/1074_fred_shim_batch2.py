#!/usr/bin/env python3
"""1074 — batch 2: patch remaining DIRECT_FRED_ONLY Lambdas with _fred_shim.

EXCLUSIONS (CRITICAL)
═════════════════════
  - justhodl-financial-secretary: this is the cache BUILDER. Patching it
    would create circular logic — it would serve from the cache it's
    trying to refresh, freezing the cache forever.
  - justhodl-liquidity-agent: already patched inline (ops/1071) with the
    same cache-first logic. Shim install would be redundant but harmless.
    Skipping for cleanliness.

ROLLBACK
════════
If any Lambda breaks, the shim's fail-safe (silent fallthrough on cache
load failure) means worst case = Lambda continues to function exactly
as before. To remove the shim: delete `import _fred_shim` line and
re-deploy.
"""
import io, json, os, pathlib, time, urllib.request, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1074_fred_shim_batch2.json"
REGION = "us-east-1"

lam = boto3.client("lambda", region_name=REGION,
                    config=Config(read_timeout=180))

# Per ops/1072 + exclusions for cache-builder + already-patched
REMAINING_LAMBDAS = [
    "volatility-monitor-agent",
    "justhodl-global-macro",
    "justhodl-event-study",
    "justhodl-ecb-detail",
    "bond-indices-agent",
    "justhodl-construction-housing",
    "justhodl-forward-returns",
    "justhodl-email-reports-v2",
    "justhodl-consumer-pulse",
    "justhodl-global-stress",
    "justhodl-commodity-curves",
    "dollar-strength-agent",
    "justhodl-bond-vol",
    "justhodl-global-business-cycle",
    "justhodl-plumbing-aggregator",
    "justhodl-cb-injection",
    "justhodl-repo-monitor",
    "justhodl-anomaly-detector",
    "justhodl-daily-report-v3",
    "justhodl-chart-data",
    "xccy-basis-agent",
    "justhodl-dollar-radar",
    "justhodl-correlation-breaks",
    "justhodl-skew-tail-hedging",
    "justhodl-options-flow",
    "justhodl-supply-inflection-scanner",
    "justhodl-historical-analogs",
    "justhodl-khalid-metrics",
    "justhodl-episode-reference",
    "justhodl-boj-detail",
    "securities-banking-agent",
    "justhodl-crisis-knowledge-base",
    "justhodl-canary-grid",
    "justhodl-bloomberg-v8",
    "manufacturing-global-agent",
    "justhodl-put-call-extreme",
    "justhodl-ka-metrics",
    "justhodl-carry-surface",
    "justhodl-yen-carry",
    "justhodl-tenor-signal-interpreter",
    "justhodl-euro-fragmentation",
    "justhodl-eurodollar-stress",
    "justhodl-cds-monitor",
    "justhodl-auction-crisis-detector",
    "justhodl-snb-detail",
    "justhodl-credit-stress",
    "macro-financial-intelligence",
    # EXCLUDED: "justhodl-financial-secretary" — cache builder, circular
    # EXCLUDED: "justhodl-liquidity-agent" — patched inline (ops/1071)
    "justhodl-eva-spread",
    "justhodl-vol-surface",
    "openbb-system2-api",
    "justhodl-valuations-agent",
]

SHIM_IMPORT_LINE = "import _fred_shim  # noqa: F401  — cache-first FRED + 429 backoff (ops/1074)"


def download_code(name):
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


def deploy_patched(name, original_files, patched_lambda_code, shim_bytes):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for fname, content in original_files.items():
            if fname == "lambda_function.py":
                zf.writestr(fname, patched_lambda_code)
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
            return {"ok": True, "zip_size": len(zb)}
        except Exception as e:
            if "ResourceConflict" in str(e) and attempt < 2:
                time.sleep(4 * (attempt + 1)); continue
            return {"ok": False, "err": str(e)[:200]}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "patches": []}
    
    with open("aws/shared/_fred_shim.py", "rb") as f:
        shim_bytes = f.read()
    out["shim_size"] = len(shim_bytes)
    
    print(f"[1074] patching {len(REMAINING_LAMBDAS)} remaining Lambdas…")
    for i, name in enumerate(REMAINING_LAMBDAS):
        print(f"[1074] {i+1}/{len(REMAINING_LAMBDAS)} {name}")
        entry = {"name": name}
        try:
            files, _ = download_code(name)
            if "lambda_function.py" not in files:
                # Try alternative entry filenames (some have differently-named handlers)
                py_files = [f for f in files if f.endswith(".py") and not f.startswith("_")]
                if py_files:
                    main_file = py_files[0]
                    entry["main_file"] = main_file
                    patched, did_patch = patch_lambda_code(files[main_file])
                    if did_patch:
                        new_files = dict(files)
                        new_files[main_file] = patched
                        # Build zip preserving original structure
                        buf = io.BytesIO()
                        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
                            for fname, content in new_files.items():
                                if fname != "_fred_shim.py":
                                    zf.writestr(fname, content)
                            zf.writestr("_fred_shim.py", shim_bytes)
                        zb = buf.getvalue()
                        for attempt in range(3):
                            try:
                                lam.update_function_code(FunctionName=name, ZipFile=zb, Publish=False)
                                lam.get_waiter("function_updated").wait(FunctionName=name)
                                entry["deploy"] = {"ok": True, "zip_size": len(zb)}
                                entry["action"] = "patched_alt_entry"
                                break
                            except Exception as e:
                                if "ResourceConflict" in str(e) and attempt < 2:
                                    time.sleep(4 * (attempt + 1)); continue
                                entry["deploy"] = {"ok": False, "err": str(e)[:200]}
                                break
                    else:
                        entry["action"] = "already_in_alt"
                else:
                    entry["err"] = "no python files in deployed code"
                out["patches"].append(entry); time.sleep(1); continue
            
            patched, did_patch = patch_lambda_code(files["lambda_function.py"])
            entry["already_patched"] = not did_patch
            if not did_patch:
                if "_fred_shim.py" not in files:
                    deploy_result = deploy_patched(name, files, files["lambda_function.py"], shim_bytes)
                    entry["deploy"] = deploy_result
                    entry["action"] = "shim_only"
                else:
                    entry["action"] = "noop"
            else:
                deploy_result = deploy_patched(name, files, patched, shim_bytes)
                entry["deploy"] = deploy_result
                entry["action"] = "patched_and_deployed"
        except Exception as e:
            entry["err"] = f"{type(e).__name__}: {str(e)[:200]}"
        out["patches"].append(entry)
        time.sleep(0.8)
    
    out["summary"] = {
        "total":     len(out["patches"]),
        "patched":   sum(1 for p in out["patches"]
                        if p.get("action", "").startswith("patched")
                        and p.get("deploy", {}).get("ok")),
        "already":   sum(1 for p in out["patches"] if p.get("already_patched") or p.get("action") == "already_in_alt"),
        "shim_only": sum(1 for p in out["patches"] if p.get("action") == "shim_only"),
        "errors":    sum(1 for p in out["patches"] if p.get("err")
                        or (p.get("deploy", {}) and not p.get("deploy", {}).get("ok"))),
    }
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1074] DONE — patched={out['summary']['patched']} "
            f"already={out['summary']['already']} "
            f"shim_only={out['summary']['shim_only']} "
            f"errors={out['summary']['errors']}")


if __name__ == "__main__":
    main()
