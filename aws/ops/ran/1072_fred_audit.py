#!/usr/bin/env python3
"""1072 — comprehensive audit: which Lambdas hit FRED, which use cache, which don't.

For each Lambda with FRED_API_KEY in env:
  1. Download deployed code
  2. Detect FRED access pattern (direct api.stlouisfed.org? imports cache helper?)
  3. Classify: CACHE-AWARE | DIRECT-FRED | UNUSED
  4. Cross-reference with EventBridge schedule (how often does it run?)
  5. Identify what S3 output it writes (which dashboards affected)

Output: prioritized patch list.
"""
import io, json, os, pathlib, re, time, urllib.request, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1072_fred_audit.json"
REGION = "us-east-1"

lam = boto3.client("lambda", region_name=REGION,
                    config=Config(read_timeout=180))
events = boto3.client("events", region_name=REGION)


def download_code(name):
    """Download deployed Lambda zip, return list of (filename, content)."""
    info = lam.get_function(FunctionName=name)
    url = info["Code"]["Location"]
    with urllib.request.urlopen(url, timeout=30) as r:
        zip_bytes = r.read()
    files = {}
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for fname in zf.namelist():
            if fname.endswith(".py"):
                try:
                    files[fname] = zf.read(fname).decode("utf-8", errors="replace")
                except Exception:
                    pass
    return files, info["Configuration"]


def classify_lambda(files):
    """Classify FRED usage. Returns (pattern, evidence)."""
    has_direct_fred = False
    direct_fred_lines = []
    has_cache_read = False
    cache_evidence = []
    n_fred_series_refs = 0
    
    for fname, content in files.items():
        for i, line in enumerate(content.split("\n")):
            # Direct FRED API call
            if "api.stlouisfed.org" in line or "FRED_BASE" in line or "stlouisfed" in line.lower():
                if "import" not in line and "#" not in line.lstrip()[:1]:
                    has_direct_fred = True
                    if len(direct_fred_lines) < 3:
                        direct_fred_lines.append(f"{fname}:{i+1} {line.strip()[:120]}")
            # Cache-aware patterns
            if any(p in line for p in [
                "fred-cache.json", "fred_cache.json",
                "_load_fred_cache", "load_fred_cache",
                "from fred_cache", "import fred_cache",
                "from _fred_cache", "data/fred-cache",
            ]):
                has_cache_read = True
                if len(cache_evidence) < 2:
                    cache_evidence.append(f"{fname}:{i+1} {line.strip()[:120]}")
            # FRED series refs (rough counter)
            if re.search(r'\b(fetch_fred|fetch_series|series_id\s*=)\b', line):
                n_fred_series_refs += 1
    
    if has_cache_read and has_direct_fred:
        return ("HYBRID_CACHE_AND_DIRECT", direct_fred_lines + cache_evidence,
                n_fred_series_refs)
    elif has_cache_read:
        return ("CACHE_AWARE", cache_evidence, n_fred_series_refs)
    elif has_direct_fred:
        return ("DIRECT_FRED_ONLY", direct_fred_lines, n_fred_series_refs)
    else:
        return ("NO_FRED_REFERENCES", [], 0)


def get_schedule(lambda_name):
    """Find EventBridge rule that targets this Lambda."""
    try:
        target_arn = f"arn:aws:lambda:{REGION}:857687956942:function:{lambda_name}"
        result = events.list_rule_names_by_target(TargetArn=target_arn)
        rule_names = result.get("RuleNames", [])
        if rule_names:
            r = events.describe_rule(Name=rule_names[0])
            return r.get("ScheduleExpression", "no-cron")
    except Exception:
        return "err"
    return "no-schedule"


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "lambdas": []}
    
    # Find all Lambdas with FRED_API_KEY in env
    print("[1072] enumerating Lambdas with FRED_API_KEY…")
    fred_lambdas = []
    paginator = lam.get_paginator("list_functions")
    for page in paginator.paginate():
        for f in page["Functions"]:
            cfg = f
            env_vars = (cfg.get("Environment") or {}).get("Variables") or {}
            if "FRED_API_KEY" in env_vars or "FRED_KEY" in env_vars:
                fred_lambdas.append(cfg["FunctionName"])
    print(f"[1072] {len(fred_lambdas)} Lambdas with FRED env")
    
    # Inspect each
    for i, name in enumerate(fred_lambdas):
        print(f"[1072] {i+1}/{len(fred_lambdas)} inspecting {name}…")
        try:
            files, cfg = download_code(name)
            pattern, evidence, n_refs = classify_lambda(files)
            schedule = get_schedule(name)
            
            entry = {
                "name":         name,
                "last_modified": cfg.get("LastModified"),
                "memory":       cfg.get("MemorySize"),
                "timeout":      cfg.get("Timeout"),
                "pattern":      pattern,
                "n_fred_refs":  n_refs,
                "schedule":     schedule,
                "evidence":     evidence,
            }
            out["lambdas"].append(entry)
        except Exception as e:
            out["lambdas"].append({"name": name, "err": str(e)[:200]})
        time.sleep(0.3)  # be gentle on Lambda API
    
    # Summary
    out["summary"] = {
        "total":              len(out["lambdas"]),
        "cache_aware":        sum(1 for L in out["lambdas"] if L.get("pattern") == "CACHE_AWARE"),
        "hybrid":             sum(1 for L in out["lambdas"] if L.get("pattern") == "HYBRID_CACHE_AND_DIRECT"),
        "direct_fred_only":   sum(1 for L in out["lambdas"] if L.get("pattern") == "DIRECT_FRED_ONLY"),
        "no_fred_refs":       sum(1 for L in out["lambdas"] if L.get("pattern") == "NO_FRED_REFERENCES"),
        "errors":             sum(1 for L in out["lambdas"] if L.get("err")),
    }
    
    # Sort: DIRECT_FRED_ONLY first (highest priority), with most n_fred_refs first
    out["needs_patching"] = sorted(
        [L for L in out["lambdas"] if L.get("pattern") == "DIRECT_FRED_ONLY"],
        key=lambda L: -L.get("n_fred_refs", 0),
    )
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1072] DONE → {REPORT}")
    print(f"[1072] cache_aware={out['summary']['cache_aware']} "
            f"hybrid={out['summary']['hybrid']} "
            f"direct_only={out['summary']['direct_fred_only']} "
            f"no_refs={out['summary']['no_fred_refs']} "
            f"errors={out['summary']['errors']}")


if __name__ == "__main__":
    main()
