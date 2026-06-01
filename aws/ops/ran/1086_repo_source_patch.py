#!/usr/bin/env python3
"""1086 — TWO-PART FIX:
  1. Immediate: re-patch justhodl-yield-curve deployed code to restore shim
  2. Permanent: patch repo source files for all 71 shimmed Lambdas so
     future workflow deploys carry the import line forward
"""
import io, json, os, pathlib, time, urllib.request, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1086_repo_source_patch.json"
REGION = "us-east-1"

lam = boto3.client("lambda", region_name=REGION,
                    config=Config(read_timeout=180))

SHIM_IMPORT_LINE = "import _fred_shim  # noqa: F401  — cache-first FRED + 429 backoff (ops/1086 repo)"

# All 71 Lambdas that were shimmed via ops/1071 (inline) + 1073 (batch 1) + 1074 (batch 2) + 1084 (edge-engine)
SHIMMED_LAMBDAS = [
    # Batch 1 (1073) — 19 scheduled
    "justhodl-divergence-engine-v2", "justhodl-cross-asset-confirm",
    "justhodl-liquidity-pulse", "justhodl-crisis-plumbing",
    "justhodl-yield-curve", "justhodl-liquidity-credit-engine",
    "justhodl-macro-surprise", "justhodl-macro-nowcast",
    "justhodl-cds-proxy", "justhodl-bond-trace",
    "justhodl-bank-stress", "justhodl-repo-lending",
    "justhodl-margin-lending", "justhodl-china-liquidity",
    "justhodl-global-liquidity", "justhodl-cross-asset-rv",
    "justhodl-tic-flows", "justhodl-implied-prob",
    "justhodl-liquidity-flow",
    # Batch 2 (1074) — 51 ad-hoc
    "volatility-monitor-agent", "justhodl-global-macro",
    "justhodl-event-study", "justhodl-ecb-detail",
    "bond-indices-agent", "justhodl-construction-housing",
    "justhodl-forward-returns", "justhodl-email-reports-v2",
    "justhodl-consumer-pulse", "justhodl-global-stress",
    "justhodl-commodity-curves", "dollar-strength-agent",
    "justhodl-bond-vol", "justhodl-global-business-cycle",
    "justhodl-plumbing-aggregator", "justhodl-cb-injection",
    "justhodl-repo-monitor", "justhodl-anomaly-detector",
    "justhodl-daily-report-v3", "justhodl-chart-data",
    "xccy-basis-agent", "justhodl-dollar-radar",
    "justhodl-correlation-breaks", "justhodl-skew-tail-hedging",
    "justhodl-options-flow", "justhodl-supply-inflection-scanner",
    "justhodl-historical-analogs", "justhodl-khalid-metrics",
    "justhodl-episode-reference", "justhodl-boj-detail",
    "securities-banking-agent", "justhodl-crisis-knowledge-base",
    "justhodl-canary-grid", "justhodl-bloomberg-v8",
    "manufacturing-global-agent", "justhodl-put-call-extreme",
    "justhodl-ka-metrics", "justhodl-carry-surface",
    "justhodl-yen-carry", "justhodl-tenor-signal-interpreter",
    "justhodl-euro-fragmentation", "justhodl-eurodollar-stress",
    "justhodl-cds-monitor", "justhodl-auction-crisis-detector",
    "justhodl-snb-detail", "justhodl-credit-stress",
    "macro-financial-intelligence", "justhodl-eva-spread",
    "justhodl-vol-surface", "openbb-system2-api",
    "justhodl-valuations-agent",
    # Edge-engine (ops/1084) and liquidity-agent (ops/1071 inline)
    "justhodl-edge-engine",
    "justhodl-liquidity-agent",  # patched inline differently; still needs source consistency
]


def download_lambda_function_py(name):
    """Get the current deployed lambda_function.py source."""
    info = lam.get_function(FunctionName=name)
    url = info["Code"]["Location"]
    with urllib.request.urlopen(url, timeout=30) as r:
        zip_bytes = r.read()
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        if "lambda_function.py" in zf.namelist():
            return zf.read("lambda_function.py").decode("utf-8", errors="replace")
        # Try alternative entry names
        for fname in zf.namelist():
            if fname.endswith(".py") and not fname.startswith("_"):
                return zf.read(fname).decode("utf-8", errors="replace")
    return None


def patch_repo_source(name, code):
    """Patch the repo's source/lambda_function.py to include the shim line.
    
    Strategy: write the entire deployed lambda_function.py (which already has
    the shim line) to the repo source, since the deployed version is the
    source of truth (it has the most recent patches: shim, auth bypass, etc.).
    
    Returns 'wrote' | 'unchanged' | 'no_source_dir' | 'err'.
    """
    src_path = pathlib.Path(f"aws/lambdas/{name}/source/lambda_function.py")
    
    if not src_path.parent.exists():
        return "no_source_dir"
    
    if not src_path.exists():
        # Some Lambdas have alt-named entry files (e.g. lambda_securities_agent.py)
        candidates = list(src_path.parent.glob("*.py"))
        candidates = [c for c in candidates if not c.name.startswith("_")
                       and c.name not in ("api_auth.py", "calibration.py",
                                            "system_events.py", "finra_si.py",
                                            "ka_aliases.py")]
        if not candidates:
            return "no_source_file"
        # Pick the largest .py (likely the main handler)
        src_path = max(candidates, key=lambda p: p.stat().st_size)
    
    existing = src_path.read_text(encoding="utf-8")
    if "_fred_shim" in existing and "import _fred_shim" in existing:
        # Already has the shim
        return "unchanged"
    
    # Write the deployed code (which has the shim) over the repo source.
    # Normalize line endings to LF for clean git diffs.
    normalized = code.replace("\r\n", "\n").replace("\r", "\n")
    src_path.write_text(normalized, encoding="utf-8", newline="\n")
    return f"wrote → {src_path.name}"


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "results": []}
    
    print(f"[1086] patching {len(SHIMMED_LAMBDAS)} repo source files…")
    for i, name in enumerate(SHIMMED_LAMBDAS):
        entry = {"name": name}
        try:
            code = download_lambda_function_py(name)
            if not code:
                entry["status"] = "no_python_file"
                out["results"].append(entry)
                continue
            entry["deployed_has_shim"] = "_fred_shim" in code
            entry["status"] = patch_repo_source(name, code)
            print(f"[1086] {i+1}/{len(SHIMMED_LAMBDAS):>2} {name:40s} {entry['status']}")
        except Exception as e:
            entry["err"] = str(e)[:200]
            print(f"[1086] {i+1}/{len(SHIMMED_LAMBDAS):>2} {name:40s} ERR: {entry['err'][:80]}")
        out["results"].append(entry)
        time.sleep(0.3)  # gentle on Lambda API
    
    # Also restore yield-curve's deployed code immediately since we broke it
    print(f"\n[1086] re-patching yield-curve deployed code (it was broken)…")
    try:
        # Download current zip
        info = lam.get_function(FunctionName="justhodl-yield-curve")
        url = info["Code"]["Location"]
        with urllib.request.urlopen(url, timeout=30) as r:
            zip_bytes = r.read()
        
        files = {}
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            for fname in zf.namelist():
                files[fname] = zf.read(fname)
        
        # Patch lambda_function.py to re-add the shim line
        lambda_code = files["lambda_function.py"].decode("utf-8", errors="replace")
        if "_fred_shim" not in lambda_code:
            lines = lambda_code.split("\n")
            # Insert after first non-import statement
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
            files["lambda_function.py"] = "\n".join(lines).encode("utf-8")
            
            # Rebuild + deploy
            buf = io.BytesIO()
            with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
                for fname, content in files.items():
                    zf.writestr(fname, content)
            zb = buf.getvalue()
            
            for attempt in range(3):
                try:
                    lam.update_function_code(FunctionName="justhodl-yield-curve",
                                                ZipFile=zb, Publish=False)
                    lam.get_waiter("function_updated").wait(FunctionName="justhodl-yield-curve")
                    out["canary_restored"] = {"ok": True, "zip_size": len(zb)}
                    break
                except Exception as e:
                    if "ResourceConflict" in str(e) and attempt < 2:
                        time.sleep(5); continue
                    out["canary_restored"] = {"ok": False, "err": str(e)[:200]}
                    break
        else:
            out["canary_restored"] = {"ok": True, "note": "already had shim"}
    except Exception as e:
        out["canary_restored_err"] = str(e)[:200]
    
    # Verify canary works again
    time.sleep(3)
    print(f"[1086] re-invoking yield-curve to confirm restoration…")
    t0 = time.time()
    try:
        r = lam.invoke(FunctionName="justhodl-yield-curve",
                        InvocationType="RequestResponse", Payload=b"{}")
        body = r["Payload"].read().decode("utf-8", errors="replace")
        p = json.loads(body)
        inner = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
        out["canary_verify"] = {
            "elapsed_s":      round(time.time() - t0, 1),
            "regime":         inner.get("regime"),
            "twos_tens_bps":  inner.get("twos_tens_bps"),
            "butterfly_bps":  inner.get("butterfly_bps"),
            "n_signals":      inner.get("n_signals"),
        }
    except Exception as e:
        out["canary_verify_err"] = str(e)[:200]
    
    # Summary
    out["summary"] = {
        "total": len(out["results"]),
        "wrote": sum(1 for r in out["results"] if r.get("status", "").startswith("wrote")),
        "unchanged": sum(1 for r in out["results"] if r.get("status") == "unchanged"),
        "no_source_dir": sum(1 for r in out["results"] if r.get("status") == "no_source_dir"),
        "no_source_file": sum(1 for r in out["results"] if r.get("status") == "no_source_file"),
        "no_python_file": sum(1 for r in out["results"] if r.get("status") == "no_python_file"),
        "errors": sum(1 for r in out["results"] if r.get("err")),
    }
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1086] DONE → {REPORT}")


if __name__ == "__main__":
    main()
