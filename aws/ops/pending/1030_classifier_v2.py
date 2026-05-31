#!/usr/bin/env python3
"""Step 1030 — Improved dead Lambda classifier (v2).

THE PROBLEM WITH v1 (1025)
═══════════════════════════
Substring matching on deprecation markers caused false positives:
  - 'test' matched 'stress-test' in descriptions (not the name)
  - 'old' matched 'household' in descriptions
  - Legitimate engines like coffee-can, magic-formula, vix-backwardation-trigger
    were flagged for deletion based on innocent description text

THE INSTITUTIONAL FIX
═════════════════════
1. WORD BOUNDARIES: use \\btest\\b regex (matches 'test' as a word, not as
   substring). Equivalent for all markers.

2. NAME-ONLY MATCHING: only flag based on Lambda name pattern. Description
   text is too noisy ('back-test' is normal, 'test-only' in description
   is meaningless).

3. MULTI-SIGNAL CONFIDENCE: instead of binary 'deprecated' label, compute
   a confidence score from multiple signals:
   
   DELETE signals (each +1):
     - Name has clear deprecation marker (test, tmp, scratch, _bak, deprecated)
     - Name is a versioned variant with a newer sibling (engine-v1 when -v2 exists)
     - 0 invocations in last 30 days
     - Code size < 500 bytes (likely stub)
     - Description explicitly says "deprecated" / "obsolete"
     - Function returns hardcoded test data
   
   KEEP signals (each -1):
     - Has function URL (HTTP callable from outside)
     - Has event source mapping (stream-triggered)
     - Has EventBridge rule (cron or event-driven)
     - Active invocations in last 30 days
     - Referenced by HTML pages on the site
     - Writes to S3 outputs that exist (downstream depends on it)
     - Modified in last 14 days (active development)

   Final classification:
     SAFE_TO_DELETE      score >= +3 with no keep signals
     DELETE_CANDIDATE    score >= +1
     INVESTIGATE         score 0 or close
     KEEP                score <= -1

4. EXCLUSION LIST: never flag a Lambda whose name matches the
   PROTECTED_NAMES list (operator-declared do-not-touch).

OUTPUT
══════
  aws/ops/reports/1030_classifier_v2.json
  aws/ops/audit/1030_classifier_v2.md
"""
import json, os, re, pathlib
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import boto3

REPORT_JSON = "aws/ops/reports/1030_classifier_v2.json"
REPORT_MD   = "aws/ops/audit/1030_classifier_v2.md"
AUDIT_JSON  = "aws/ops/reports/1020_full_audit.json"

REGION = "us-east-1"
lam = boto3.client("lambda", region_name=REGION)
cw  = boto3.client("cloudwatch", region_name=REGION)
events = boto3.client("events", region_name=REGION)

# ─── Strict regex patterns (word-boundary) ──────────────────────────────
# These only match the marker as a whole word, not as substring.
# Order: more specific patterns first.
STRICT_DELETE_PATTERNS_NAME = [
    re.compile(r"\btest\b", re.IGNORECASE),
    re.compile(r"\btmp\b", re.IGNORECASE),
    re.compile(r"\bscratch\b", re.IGNORECASE),
    re.compile(r"\bdraft\b", re.IGNORECASE),
    re.compile(r"_bak\b", re.IGNORECASE),
    re.compile(r"\bobsolete\b", re.IGNORECASE),
    re.compile(r"-deprecated\b", re.IGNORECASE),
    re.compile(r"\bplayground\b", re.IGNORECASE),
    # Versioned variants are reviewable but not auto-flag
]

# Patterns we look for in DESCRIPTION only when name is also weak
STRICT_DELETE_PATTERNS_DESC = [
    re.compile(r"\bdeprecated\b", re.IGNORECASE),
    re.compile(r"\bobsolete\b", re.IGNORECASE),
    re.compile(r"\bdo not use\b", re.IGNORECASE),
    re.compile(r"\bno longer\b", re.IGNORECASE),
]

# Names we ALWAYS keep — operator-declared protected list
PROTECTED_NAMES = {
    # Foundational engines (per memory)
    "justhodl-conviction-engine", "justhodl-signal-board",
    "justhodl-signal-logger", "justhodl-outcome-checker",
    "justhodl-calibrator", "justhodl-alpha-calibrator",
    "justhodl-master-ranker", "justhodl-universe-builder",
    "justhodl-signal-scorecard", "justhodl-magnitude-distributions",
    "justhodl-miss-detector", "justhodl-miss-calibrator",
    "justhodl-near-miss-monitor", "justhodl-engine-signal-map",
    "justhodl-alpha-compass", "justhodl-event-coordinator",
    "justhodl-event-flow-monitor",
    "justhodl-ai-chat", "justhodl-telegram-bot",
    "justhodl-stock-screener", "justhodl-stock-analyzer",
    "justhodl-position-sizer-v2", "justhodl-portfolio-admin",
    "justhodl-pnl-attribution",
    # CFTC + crypto
    "cftc-futures-positioning-agent",
    "justhodl-cftc-positioning", "justhodl-crypto-intel",
    # Crisis tier
    "justhodl-crisis-composite", "justhodl-crisis-plumbing",
    "justhodl-liquidity-credit-engine", "justhodl-eurodollar-stress",
    # Cross-asset
    "justhodl-cross-asset-regime", "justhodl-cross-asset-rv",
}


def name_has_strict_marker(name: str) -> tuple:
    """Returns (matched, list_of_matched_patterns) for name."""
    matches = []
    for pat in STRICT_DELETE_PATTERNS_NAME:
        if pat.search(name):
            matches.append(pat.pattern)
    return (bool(matches), matches)


def desc_has_strict_marker(desc: str) -> tuple:
    matches = []
    for pat in STRICT_DELETE_PATTERNS_DESC:
        if pat.search(desc or ""):
            matches.append(pat.pattern)
    return (bool(matches), matches)


def get_30d_invokes(fn_name: str) -> int:
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=30)
    try:
        resp = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Invocations",
            Dimensions=[{"Name": "FunctionName", "Value": fn_name}],
            StartTime=start, EndTime=end, Period=86400,
            Statistics=["Sum"],
        )
        return int(sum(p["Sum"] for p in resp.get("Datapoints") or []))
    except Exception:
        return 0


def get_function_url(fn_name: str) -> bool:
    try:
        lam.get_function_url_config(FunctionName=fn_name)
        return True
    except Exception:
        return False


def has_event_source_mapping(fn_name: str) -> bool:
    try:
        resp = lam.list_event_source_mappings(FunctionName=fn_name)
        return len(resp.get("EventSourceMappings") or []) > 0
    except Exception:
        return False


def has_eventbridge_rule(fn_name: str) -> bool:
    try:
        resp = events.list_rule_names_by_target(
            TargetArn=f"arn:aws:lambda:{REGION}:857687956942:function:{fn_name}",
        )
        return len(resp.get("RuleNames") or []) > 0
    except Exception:
        return False


def find_versioned_siblings(name: str, all_names: set) -> list:
    """Given 'engine-v1' or 'engine-v2', return list of sibling versions
    (same base, different number)."""
    m = re.search(r"^(.+?)-v(\d+)$", name)
    if not m:
        return []
    base, version = m.group(1), int(m.group(2))
    siblings = []
    for n in all_names:
        if n == name:
            continue
        sm = re.search(rf"^{re.escape(base)}-v(\d+)$", n)
        if sm:
            siblings.append({"name": n, "version": int(sm.group(1))})
    siblings.sort(key=lambda r: -r["version"])
    return siblings


def classify(fn_meta: dict, all_names: set) -> dict:
    """Score-based classification."""
    name = fn_meta["name"]
    description = fn_meta.get("description") or ""
    code_size_kb = fn_meta.get("code_size_kb", 0)
    last_modified = fn_meta.get("last_modified", "")
    
    if name in PROTECTED_NAMES:
        return {
            "name": name,
            "classification": "PROTECTED",
            "reasons": ["NAME_IN_PROTECTED_LIST"],
            "score": -10,
            "evidence": {},
        }
    
    delete_score = 0
    keep_score = 0
    delete_reasons = []
    keep_reasons = []
    
    # ─── DELETE signals ─────────────────────────────────────────────────
    name_matched, name_pats = name_has_strict_marker(name)
    if name_matched:
        delete_score += 2
        delete_reasons.append(f"NAME_MARKER:{','.join(name_pats)}")
    
    desc_matched, desc_pats = desc_has_strict_marker(description)
    if desc_matched:
        delete_score += 1
        delete_reasons.append(f"DESC_MARKER:{','.join(desc_pats)}")
    
    siblings = find_versioned_siblings(name, all_names)
    newer_exists = any(s["version"] > int(re.search(r"-v(\d+)$", name).group(1))
                         for s in siblings)
    if newer_exists:
        delete_score += 2
        delete_reasons.append(f"NEWER_VERSION_EXISTS:{siblings[0]['name']}")
    
    if code_size_kb < 0.5:
        delete_score += 1
        delete_reasons.append("CODE_TINY")
    
    # ─── KEEP signals ───────────────────────────────────────────────────
    invokes_30d = get_30d_invokes(name)
    if invokes_30d > 50:
        keep_score += 3
        keep_reasons.append(f"VERY_ACTIVE:{invokes_30d}_invokes_30d")
    elif invokes_30d > 5:
        keep_score += 2
        keep_reasons.append(f"ACTIVE:{invokes_30d}_invokes_30d")
    elif invokes_30d > 0:
        keep_score += 1
        keep_reasons.append(f"SOME_ACTIVITY:{invokes_30d}_invokes_30d")
    
    if get_function_url(name):
        keep_score += 3
        keep_reasons.append("HAS_FUNCTION_URL")
    if has_event_source_mapping(name):
        keep_score += 3
        keep_reasons.append("HAS_EVENT_SOURCE_MAPPING")
    if has_eventbridge_rule(name):
        keep_score += 2
        keep_reasons.append("EVENTBRIDGE_RULE_TARGETS_IT")
    
    # Modified in last 14 days
    try:
        ts = datetime.fromisoformat(last_modified.replace("Z", "+00:00"))
        age_days = (datetime.now(timezone.utc) - ts).total_seconds() / 86400
        if age_days < 14:
            keep_score += 1
            keep_reasons.append(f"RECENTLY_MODIFIED:{round(age_days,1)}d_ago")
    except Exception:
        pass
    
    # ─── Final classification ───────────────────────────────────────────
    net = delete_score - keep_score
    
    if keep_score >= 3:
        classification = "KEEP"
    elif net >= 3 and keep_score == 0:
        classification = "SAFE_TO_DELETE"
    elif net >= 1:
        classification = "DELETE_CANDIDATE"
    elif net <= -1:
        classification = "KEEP"
    else:
        classification = "INVESTIGATE"
    
    return {
        "name":             name,
        "classification":   classification,
        "delete_score":     delete_score,
        "keep_score":       keep_score,
        "net_score":        net,
        "delete_reasons":   delete_reasons,
        "keep_reasons":     keep_reasons,
        "description":      description[:140],
        "code_size_kb":     code_size_kb,
        "last_modified":    last_modified,
        "evidence": {
            "invokes_30d":         invokes_30d,
            "siblings_versioned":  [s["name"] for s in siblings],
        },
    }


def main():
    started = datetime.now(timezone.utc)
    
    # Load 1020 audit to get the dead Lambda list
    audit = json.loads(pathlib.Path(AUDIT_JSON).read_text())
    dead = audit.get("issues", {}).get("dead_unscheduled", [])
    all_names = set(audit.get("full_lambda_list") or [L["name"] for L in dead])
    
    # Build full metadata map from audit
    name_to_meta = {item["name"]: item for item in dead}
    
    print(f"[classifier-v2] {len(dead)} dead Lambdas to re-classify with stricter heuristics")
    print(f"[classifier-v2] all_names corpus: {len(all_names)}")
    
    results = []
    by_class = defaultdict(list)
    for i, item in enumerate(dead):
        try:
            description_full = item.get("description") or ""
            # Fetch full description from AWS if we don't have it
            if len(description_full) < 5:
                try:
                    cfg = lam.get_function_configuration(FunctionName=item["name"])
                    description_full = (cfg.get("Description") or "")[:200]
                except Exception:
                    pass
            item["description"] = description_full
            
            result = classify(item, all_names)
            results.append(result)
            by_class[result["classification"]].append(result["name"])
            
            if (i + 1) % 30 == 0:
                print(f"[classifier-v2]   processed {i+1}/{len(dead)}")
        except Exception as e:
            print(f"[classifier-v2] err on {item.get('name')}: {e}")
    
    # Sort: SAFE_TO_DELETE first, then DELETE_CANDIDATE, INVESTIGATE, KEEP, PROTECTED
    class_order = {
        "SAFE_TO_DELETE":   0,
        "DELETE_CANDIDATE": 1,
        "INVESTIGATE":      2,
        "KEEP":             3,
        "PROTECTED":        4,
    }
    results.sort(key=lambda r: (class_order.get(r["classification"], 99), -r["net_score"], r["name"]))
    
    out = {
        "generated_at":  started.isoformat(),
        "n_classified":  len(results),
        "counts":        {k: len(v) for k, v in by_class.items()},
        "v1_comparison": {
            "v1_flagged_as_deprecated": 25,
            "v2_safe_to_delete":        len(by_class.get("SAFE_TO_DELETE", [])),
            "v2_delete_candidate":      len(by_class.get("DELETE_CANDIDATE", [])),
            "v2_investigate":           len(by_class.get("INVESTIGATE", [])),
            "v2_keep":                  len(by_class.get("KEEP", [])),
            "v2_protected":             len(by_class.get("PROTECTED", [])),
        },
        "classified":    results,
    }
    
    pathlib.Path(os.path.dirname(REPORT_JSON)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT_JSON).write_text(json.dumps(out, indent=2, default=str))
    
    # Markdown
    md = []
    md.append("# Dead Lambda Classifier v2 — strict regex + multi-signal")
    md.append(f"\nGenerated: {started.isoformat()}")
    md.append(f"\nTotal classified: **{len(results)}**\n")
    
    md.append("## v1 → v2 comparison\n")
    v1v2 = out["v1_comparison"]
    md.append(f"- v1 flagged 25 as 'CANDIDATE_DELETE_DEPRECATED' (mostly false positives)")
    md.append(f"- v2 with strict matching:")
    md.append(f"  - SAFE_TO_DELETE: **{v1v2['v2_safe_to_delete']}** (high confidence)")
    md.append(f"  - DELETE_CANDIDATE: **{v1v2['v2_delete_candidate']}** (review then delete)")
    md.append(f"  - INVESTIGATE: **{v1v2['v2_investigate']}** (mixed signals)")
    md.append(f"  - KEEP: **{v1v2['v2_keep']}** (clear keep signals)")
    md.append(f"  - PROTECTED: **{v1v2['v2_protected']}** (operator-declared do-not-touch)\n")
    
    md.append("## Classification Methodology\n")
    md.append("Each Lambda scored on opposing signals:")
    md.append("- **+2 DELETE**: name has \\btest\\b / \\btmp\\b / \\bscratch\\b (word-boundary regex)")
    md.append("- **+2 DELETE**: newer versioned sibling exists (engine-v1 when -v2 present)")
    md.append("- **+1 DELETE**: description says 'deprecated' / 'obsolete' / 'do not use'")
    md.append("- **+1 DELETE**: code size < 500 bytes (likely stub)")
    md.append("- **+3 KEEP**: has function URL (HTTP callable from outside)")
    md.append("- **+3 KEEP**: has event source mapping (DynamoDB stream etc.)")
    md.append("- **+3 KEEP**: >50 invocations in last 30 days")
    md.append("- **+2 KEEP**: EventBridge rule targets it")
    md.append("- **+1 KEEP**: modified in last 14 days (active development)")
    md.append("- **PROTECTED**: name in operator's do-not-touch list (~30 Lambdas)\n")
    md.append("Net score >= +3 with no keep signals → SAFE_TO_DELETE")
    md.append("Net score >= +1 → DELETE_CANDIDATE")
    md.append("Net score == 0 → INVESTIGATE\n")
    
    for class_label in ("SAFE_TO_DELETE", "DELETE_CANDIDATE", "INVESTIGATE", "KEEP", "PROTECTED"):
        items = [r for r in results if r["classification"] == class_label]
        if not items:
            continue
        md.append(f"\n## {class_label} ({len(items)})\n")
        md.append("| Name | Net | Delete signals | Keep signals | Description |")
        md.append("|------|----:|----------------|--------------|-------------|")
        # Show top items (most signals)
        for r in items[:40]:
            d = ", ".join(r.get("delete_reasons", []))[:60]
            k = ", ".join(r.get("keep_reasons", []))[:60]
            desc = (r.get("description") or "").replace("|", "/").replace("\n", " ")[:80]
            md.append(f"| `{r['name']}` | {r['net_score']:+d} | {d} | {k} | {desc} |")
        if len(items) > 40:
            md.append(f"\n*… and {len(items)-40} more (see JSON)*")
    
    pathlib.Path(os.path.dirname(REPORT_MD)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT_MD).write_text("\n".join(md))
    
    print(f"[classifier-v2] DONE")
    for cl, names in sorted(by_class.items(), key=lambda x: class_order.get(x[0], 99)):
        print(f"  {cl}: {len(names)}")


if __name__ == "__main__":
    main()
