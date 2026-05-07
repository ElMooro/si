#!/usr/bin/env python3
"""Step 318 — Audit existing Lambdas against the 6 proposed improvements.

For each "suspect" Lambda (whose name suggests it might cover one of the
proposals), pull docstring + S3 outputs to determine if it's:
  COVERS    — already does what we proposed; skip
  PARTIAL   — does part of it; enhance instead of build new
  UNRELATED — different concept; we should still build the new one

Also checks:
  - 3 broken HTML refs (asymmetric-setups, red-flags, risk-sized-positions)
    → which page(s) reference them, and what's the fix
  - portfolio/state.json (12d stale) → who depends on it
"""
import json
import os
import re
import time
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
REPORT = "aws/ops/reports/318_proposal_audit.json"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)

# Lambdas to inspect — these *might* cover some proposed feature
SUSPECTS = {
    "compound-aggregator":       "MASTER_RANKER",   # signal aggregator?
    "cross-asset-regime":        "META_REGIME",     # cross-asset regime detector
    "event-study":               "MASTER_RANKER",   # event ranking?
    "earnings-tracker":          "EARNINGS_WHISPER",
    "earnings-pead":             "EARNINGS_WHISPER",
    "pead-detector":             "EARNINGS_WHISPER",
    "revenue-acceleration":      "EARNINGS_WHISPER",
    "narrative-density-tracker": "FED_SPEAK",       # speech parsing?
    "news-sentiment":            "FED_SPEAK",       # news → could include Fed
    "pre-pump-detector":         "EARNINGS_WHISPER",
    "options-flow":              "TAPE_READER",
    "options-flow-scanner":      "TAPE_READER",
    "options-gamma":             "TAPE_READER",
    "auction-crisis-detector":   "META_REGIME",
    "regime-anomaly":            "META_REGIME",
    "labor-leading":             "META_REGIME",
    "alert-router":              "MASTER_RANKER",   # could route master alerts?
    "redflag-alerter":           "MASTER_RANKER",
    "watchlist-debate":          "MASTER_RANKER",
    "ka-metrics":                "MASTER_RANKER",   # composite metrics?
    "khalid-metrics":            "MASTER_RANKER",
    "ai-brief":                  "MASTER_RANKER",   # the AI brief might already be a ranker
    "intelligence":              "MASTER_RANKER",
}


def get_lambda_top(name):
    """Get description + first 60 lines of source from a Lambda's deployed code."""
    try:
        cfg = lam.get_function_configuration(FunctionName=f"justhodl-{name}")
        env = cfg.get("Environment", {}).get("Variables", {})
    except ClientError:
        return {"err": "lambda not found"}

    # Get the deployment URL to download zip + extract docstring
    try:
        info = lam.get_function(FunctionName=f"justhodl-{name}")
        zip_url = info.get("Code", {}).get("Location")
    except Exception as e:
        return {"err": str(e)[:200]}

    return {
        "exists": True,
        "description": (cfg.get("Description") or "")[:500],
        "env_keys": list(env.keys()),
        "s3_outputs": [v for k, v in env.items()
                        if "KEY" in k.upper() and "/" in str(v)],
        "memory": cfg.get("MemorySize"),
        "timeout": cfg.get("Timeout"),
        "last_modified": cfg.get("LastModified"),
    }


def find_html_refs(s3_key, html_dir="."):
    """Find HTML files in cwd that reference a specific S3 key."""
    out = []
    if not os.path.isdir(html_dir):
        return out
    for fn in os.listdir(html_dir):
        if not fn.endswith(".html"):
            continue
        try:
            with open(os.path.join(html_dir, fn), "r", encoding="utf-8", errors="replace") as f:
                content = f.read()
            if s3_key in content:
                # Show the line
                for i, line in enumerate(content.split("\n"), 1):
                    if s3_key in line:
                        out.append({"file": fn, "line": i, "snippet": line.strip()[:140]})
                        break  # first match only
        except Exception:
            continue
    return out


def s3_check_potential_replacements(broken_key):
    """For broken HTML refs, suggest the most likely renamed S3 key."""
    base = broken_key.replace("data/", "").replace(".json", "")
    candidates = []
    # Common rename patterns
    if "asymmetric-setups" in base:
        candidates = ["data/asymmetric-scorer.json", "data/asymmetric-hunter.json"]
    elif "red-flags" in base:
        candidates = ["data/redflag-alerts.json", "data/red-flag-alerts.json"]
    elif "risk-sized-positions" in base:
        candidates = ["data/risk-sizer.json", "data/sizing.json", "data/positions.json"]
    out = []
    for c in candidates:
        try:
            obj = s3.head_object(Bucket=BUCKET, Key=c)
            out.append({
                "candidate": c, "exists": True,
                "size_kb": round(obj["ContentLength"] / 1024, 1),
            })
        except ClientError:
            out.append({"candidate": c, "exists": False})
    return out


def main():
    started = time.time()
    out = {"started": datetime.now(timezone.utc).isoformat(), "suspects": {}}

    # Inspect each suspect Lambda
    print(f"[318] Inspecting {len(SUSPECTS)} suspect Lambdas…")
    for short_name, proposal in SUSPECTS.items():
        info = get_lambda_top(short_name)
        info["which_proposal"] = proposal
        out["suspects"][short_name] = info

    # Audit broken HTML refs
    print("[318] Tracing broken HTML refs…")
    broken_refs = ["data/asymmetric-setups.json", "data/red-flags.json",
                   "data/risk-sized-positions.json"]
    out["broken_refs"] = {}
    for ref in broken_refs:
        out["broken_refs"][ref] = {
            "html_consumers": find_html_refs(ref),
            "candidate_replacements": s3_check_potential_replacements(ref),
        }

    # Audit portfolio/state.json — find dependents
    print("[318] Tracing portfolio/state.json dependents…")
    out["portfolio_state"] = {
        "html_consumers": find_html_refs("portfolio/state.json"),
    }

    out["duration_s"] = round(time.time() - started, 1)

    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)

    # Pretty summary
    print()
    print("═" * 80)
    print("  PROPOSAL AUDIT — does each proposed feature already exist?")
    print("═" * 80)

    by_proposal = {}
    for name, info in out["suspects"].items():
        prop = info.get("which_proposal", "?")
        by_proposal.setdefault(prop, []).append((name, info))

    for prop, lams in sorted(by_proposal.items()):
        print(f"\n  📍 PROPOSAL: {prop}")
        for name, info in lams:
            if info.get("err"):
                print(f"    ❌ {name:<32s} (not found)")
                continue
            desc = info.get("description", "")[:120].replace("\n", " ")
            outputs = info.get("s3_outputs", [])
            print(f"    ✓ justhodl-{name}")
            print(f"       desc: {desc[:100]}")
            if outputs:
                print(f"       outputs: {outputs[:3]}")

    print()
    print("═" * 80)
    print("  BROKEN REFS — where to point them")
    print("═" * 80)
    for ref, info in out["broken_refs"].items():
        consumers = info.get("html_consumers", [])
        cands = info.get("candidate_replacements", [])
        print(f"\n  ❌ {ref}")
        print(f"     consumed by:")
        for c in consumers:
            print(f"       {c['file']} (line {c['line']}): {c['snippet']}")
        for cand in cands:
            icon = "✅" if cand.get("exists") else "❌"
            print(f"     candidate: {icon} {cand['candidate']} ({cand.get('size_kb','-')}KB)")

    print()
    print("  portfolio/state.json:")
    for c in out["portfolio_state"].get("html_consumers", []):
        print(f"    {c['file']} (line {c['line']}): {c['snippet']}")


if __name__ == "__main__":
    main()
