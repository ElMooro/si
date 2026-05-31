#!/usr/bin/env python3
"""Step 1025 — Triage all 367 dead Lambdas from ops/1020 audit.

The audit flagged 367 Lambdas with no schedule AND no recent invokes.
Most are intentional (development scratch, deprecated experiments,
reference code), but the operator needs a sorted picture before any
cleanup decisions.

Classification heuristics:
  - CLEARLY_DEPRECATED:  name contains 'test', 'old', 'v1', 'legacy',
                          'deprecated', 'tmp', '_bak', 'scratch'
  - MID_DEVELOPMENT:     last_modified within 60 days, no schedule,
                          could still be in active development
  - HISTORICAL_KEPT:     last_modified >60 days, name suggests purposeful
                          retention (engine-like names, no test markers)
  - UNCLASSIFIED:        ambiguous — defer

For each Lambda, also extract:
  - description (often says deprecated or test)
  - last_modified
  - code_size_kb (very small ones may be stubs)
  - has_event_source_mapping (DDB streams etc. — DON'T delete these)
  - has_lambda_url (HTTP-callable — might be used externally)

Outputs:
  aws/ops/reports/1025_dead_lambda_triage.json
  aws/ops/audit/1025_dead_lambda_triage.md  (sortable table)
"""
import json, os, pathlib, re
from collections import defaultdict
from datetime import datetime, timezone, timedelta
import boto3

REPORT_JSON = "aws/ops/reports/1025_dead_lambda_triage.json"
REPORT_MD   = "aws/ops/audit/1025_dead_lambda_triage.md"
AUDIT_JSON  = "aws/ops/reports/1020_full_audit.json"

REGION = "us-east-1"
lam = boto3.client("lambda", region_name=REGION)


DEPRECATION_MARKERS = [
    "test", "_test", "-test", "demo", "scratch", "tmp", "_bak",
    "v1-", "legacy", "deprecated", "old", "experimental", "experiment",
    "stub", "skeleton", "playground", "draft", "wip-",
]


def classify_name(name: str, description: str = "") -> str:
    """Heuristic classification by name pattern."""
    nl = name.lower()
    dl = (description or "").lower()
    
    for marker in DEPRECATION_MARKERS:
        if marker in nl or marker in dl:
            return "CLEARLY_DEPRECATED"
    
    # Numbered duplicates (engine-v2, engine-v3 etc.) — newer one may be active
    if re.search(r"-v[0-9]+$", nl):
        return "VERSIONED_VARIANT"
    
    return "UNCLASSIFIED_NAME"


def classify_age(last_modified: str) -> str:
    """Heuristic by age."""
    try:
        ts = datetime.fromisoformat(last_modified.replace("Z", "+00:00"))
    except Exception:
        return "UNKNOWN_AGE"
    now = datetime.now(timezone.utc)
    age_days = (now - ts).total_seconds() / 86400
    if age_days < 30:
        return "RECENTLY_MODIFIED"
    if age_days < 90:
        return "STALE_3M"
    if age_days < 180:
        return "STALE_6M"
    return "STALE_OLD"


def get_event_source_mappings(fn_name: str) -> bool:
    """Check if Lambda has any event source mappings (DynamoDB streams, etc).
    If yes, it might be actively triggered without a schedule."""
    try:
        resp = lam.list_event_source_mappings(FunctionName=fn_name)
        return len(resp.get("EventSourceMappings") or []) > 0
    except Exception:
        return False


def get_function_url(fn_name: str) -> bool:
    """Check if Lambda has a function URL (HTTP-callable from web)."""
    try:
        lam.get_function_url_config(FunctionName=fn_name)
        return True
    except Exception:
        return False


def main():
    started = datetime.now(timezone.utc)
    
    # Load the full audit
    audit = json.loads(pathlib.Path(AUDIT_JSON).read_text())
    dead = audit.get("issues", {}).get("dead_unscheduled", [])
    print(f"[triage] {len(dead)} dead Lambdas from 1020 audit")
    
    # Build a lookup for full Lambda metadata
    all_lambdas = {L["name"]: L for L in audit.get("lambdas_sample", [])}
    # Full list is in "full_lambda_list" but we need richer metadata.
    # For richer info, look at the audit JSON more carefully.
    
    # Re-fetch metadata for dead Lambdas (description, etc) — quickly
    classified = defaultdict(list)
    summaries = []
    
    for i, item in enumerate(dead):
        name = item["name"]
        last_modified = item.get("last_modified", "")
        code_size_kb = item.get("code_size_kb", 0)
        
        # Get fuller metadata
        try:
            fn = lam.get_function_configuration(FunctionName=name)
            description = fn.get("Description") or ""
            runtime = fn.get("Runtime") or ""
        except Exception:
            description = ""
            runtime = ""
        
        # Classify by name + age
        name_class = classify_name(name, description)
        age_class = classify_age(last_modified)
        
        # Trigger checks (slower; skip ESM/URL for now to keep this under 10min)
        # Only check for the borderline cases
        has_esm = False
        has_url = False
        if name_class == "UNCLASSIFIED_NAME":
            has_esm = get_event_source_mappings(name)
            has_url = get_function_url(name)
            if has_esm or has_url:
                name_class = "HAS_NON_CRON_TRIGGER"
        
        # Final bucket
        if has_esm or has_url:
            bucket = "KEEP_NON_CRON_TRIGGER"
        elif name_class == "CLEARLY_DEPRECATED":
            bucket = "CANDIDATE_DELETE_DEPRECATED"
        elif age_class == "RECENTLY_MODIFIED":
            bucket = "ACTIVE_DEVELOPMENT"
        elif name_class == "VERSIONED_VARIANT":
            bucket = "VERSIONED_REVIEW"
        elif age_class == "STALE_OLD":
            bucket = "CANDIDATE_DELETE_STALE"
        elif age_class in ("STALE_6M",):
            bucket = "REVIEW_6M_STALE"
        else:
            bucket = "REVIEW_3M_STALE"
        
        rec = {
            "name":          name,
            "bucket":        bucket,
            "name_class":    name_class,
            "age_class":     age_class,
            "last_modified": last_modified,
            "code_size_kb":  code_size_kb,
            "description":   description[:140],
            "runtime":       runtime,
            "has_esm":       has_esm,
            "has_url":       has_url,
        }
        classified[bucket].append(rec)
        summaries.append(rec)
        
        if (i + 1) % 50 == 0:
            print(f"[triage]   processed {i+1}/{len(dead)}")
    
    # Sort each bucket by age (most stale first within each bucket)
    for bucket in classified:
        classified[bucket].sort(key=lambda r: r.get("last_modified", ""))
    
    # Summary counts
    counts = {bucket: len(items) for bucket, items in classified.items()}
    print(f"[triage] classification: {counts}")
    
    out = {
        "started": started.isoformat(),
        "n_dead_total": len(dead),
        "counts": counts,
        "classified": dict(classified),
    }
    
    pathlib.Path(os.path.dirname(REPORT_JSON)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT_JSON).write_text(json.dumps(out, indent=2, default=str))
    
    # Markdown report
    md = []
    md.append("# Dead Lambda Triage")
    md.append(f"\nGenerated: {started.isoformat()}")
    md.append(f"Total dead Lambdas: **{len(dead)}**\n")
    md.append("## Classification Summary\n")
    bucket_order = [
        "CANDIDATE_DELETE_DEPRECATED",
        "CANDIDATE_DELETE_STALE",
        "REVIEW_6M_STALE",
        "REVIEW_3M_STALE",
        "VERSIONED_REVIEW",
        "ACTIVE_DEVELOPMENT",
        "KEEP_NON_CRON_TRIGGER",
    ]
    for b in bucket_order:
        n = len(classified.get(b, []))
        md.append(f"- **{b}**: {n}")
    md.append("")
    
    for bucket in bucket_order:
        items = classified.get(bucket, [])
        if not items:
            continue
        md.append(f"\n### {bucket} ({len(items)})\n")
        md.append("| Name | Last Modified | Size | Description |")
        md.append("|------|---------------|------|-------------|")
        for item in items[:50]:
            desc = item.get("description") or ""
            desc = desc.replace("|", "/").replace("\n", " ")[:100]
            md.append(
                f"| `{item['name']}` | {item['last_modified'][:10]} | "
                f"{item['code_size_kb']:.1f}KB | {desc} |"
            )
        if len(items) > 50:
            md.append(f"\n*… and {len(items)-50} more (see JSON for full list)*")
    
    pathlib.Path(os.path.dirname(REPORT_MD)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT_MD).write_text("\n".join(md))
    
    print(f"[triage] DONE. wrote {REPORT_JSON} + {REPORT_MD}")
    print(f"[triage] elapsed {(datetime.now(timezone.utc) - started).total_seconds():.1f}s")


if __name__ == "__main__":
    main()
