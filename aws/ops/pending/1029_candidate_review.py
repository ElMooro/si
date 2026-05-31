#!/usr/bin/env python3
"""Step 1029 — Institutional-grade review report for candidate-deprecated Lambdas.

For each candidate, gather the FULL EVIDENCE the operator needs to make
a KEEP / DELETE / INVESTIGATE decision:

  EVIDENCE GATHERED PER LAMBDA:
    1. Last invocation timestamp (CloudWatch Invocations metric, 30d window)
    2. Last log event timestamp (CloudWatch Logs)
    3. Total invocations in last 30d
    4. S3 outputs they write (grep for s3.put_object in source code)
    5. Pages that reference them (grep through *.html for the Lambda name
       or its Lambda URL)
    6. Function URL (if exists)
    7. Event source mappings (DDB streams, etc — DO NOT delete these)
    8. EventBridge rules targeting them
    9. Description from config + Lambda metadata
   10. Code structure: line count, key API calls (boto3, fred, polygon...)

  KEEP/DELETE/INVESTIGATE RECOMMENDATION:
    Auto-suggested based on evidence:
      KEEP if invoked in last 30d OR has URL OR has ESM OR referenced in pages
      DELETE if 0 invokes 30d AND no URL/ESM/page-ref AND clear deprecation marker
      INVESTIGATE otherwise

OUTPUT
══════
  aws/ops/reports/1029_candidate_review.json  (full structured)
  aws/ops/audit/1029_candidate_review.md       (operator-friendly table)
"""
import json, os, re, pathlib, subprocess
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import boto3

REPORT_JSON = "aws/ops/reports/1029_candidate_review.json"
REPORT_MD   = "aws/ops/audit/1029_candidate_review.md"
TRIAGE_JSON = "aws/ops/reports/1025_dead_lambda_triage.json"

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION)
cw  = boto3.client("cloudwatch", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)
events = boto3.client("events", region_name=REGION)
s3  = boto3.client("s3", region_name=REGION)


def get_last_invoke_ts(fn_name: str, days: int = 30) -> dict:
    """Walk back day-by-day looking for the most recent invocation."""
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=days)
    try:
        resp = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Invocations",
            Dimensions=[{"Name": "FunctionName", "Value": fn_name}],
            StartTime=start, EndTime=end, Period=86400,
            Statistics=["Sum"],
        )
        datapoints = resp.get("Datapoints") or []
        if not datapoints:
            return {"n_invocations_30d": 0, "last_invoke_date": None}
        # Find most recent day with Sum > 0
        datapoints.sort(key=lambda p: p["Timestamp"], reverse=True)
        total = int(sum(p["Sum"] for p in datapoints))
        last_with_invokes = next((p for p in datapoints if p["Sum"] > 0), None)
        return {
            "n_invocations_30d": total,
            "last_invoke_date": (last_with_invokes["Timestamp"].isoformat()
                                  if last_with_invokes else None),
        }
    except Exception as e:
        return {"err": str(e)[:120]}


def get_last_log_event(fn_name: str) -> str:
    try:
        lg_name = f"/aws/lambda/{fn_name}"
        resp = logs.describe_log_streams(
            logGroupName=lg_name, orderBy="LastEventTime",
            descending=True, limit=1,
        )
        streams = resp.get("logStreams") or []
        if not streams:
            return None
        ts = streams[0].get("lastEventTimestamp")
        if ts:
            return datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat()
        return None
    except Exception:
        return None


def get_function_url(fn_name: str) -> str:
    try:
        resp = lam.get_function_url_config(FunctionName=fn_name)
        return resp.get("FunctionUrl")
    except Exception:
        return None


def get_event_source_mappings(fn_name: str) -> list:
    try:
        resp = lam.list_event_source_mappings(FunctionName=fn_name)
        return [
            {"source_arn": m.get("EventSourceArn"), "state": m.get("State")}
            for m in resp.get("EventSourceMappings", [])
        ]
    except Exception:
        return []


def get_eventbridge_rules(fn_name: str) -> list:
    """Find EventBridge rules whose target is this Lambda."""
    try:
        resp = events.list_rule_names_by_target(
            TargetArn=f"arn:aws:lambda:{REGION}:857687956942:function:{fn_name}",
        )
        return resp.get("RuleNames", [])
    except Exception:
        return []


def find_s3_outputs_in_source(fn_name: str) -> list:
    """Grep the Lambda's source for S3 put_object calls. Captures the Key
    argument as a literal or variable name."""
    src_path = pathlib.Path(f"aws/lambdas/{fn_name}/source/lambda_function.py")
    if not src_path.exists():
        return ["(no local source)"]
    try:
        content = src_path.read_text()
        keys = set()
        # Look for: put_object(... Key="..." ...)
        for m in re.finditer(r'put_object\([^)]*Key\s*=\s*([f]?["\']([^"\']+)["\'])',
                              content):
            keys.add(m.group(2))
        # Also: S3_KEY = "..." patterns
        for m in re.finditer(r'(?:S3_KEY|OUT_KEY|OUTPUT_KEY)(?:_OUT)?\s*=\s*["\']([^"\']+)["\']',
                              content):
            keys.add(m.group(1))
        # Also: f-string keys like Key=f"data/...{name}.json"
        for m in re.finditer(r'put_object\([^)]*Key\s*=\s*f["\']([^"\']+)["\']',
                              content):
            keys.add(m.group(1))
        return sorted(keys)[:8]
    except Exception:
        return []


def find_page_references(fn_name: str) -> list:
    """Grep top-level *.html for references to this Lambda's name or URL."""
    refs = []
    # 1. Search for the literal Lambda name
    try:
        result = subprocess.run(
            ["grep", "-l", "-r", fn_name, "--include=*.html",
              "--include=*.js", ".",
              "--exclude-dir=.git", "--exclude-dir=node_modules",
              "--exclude-dir=aws"],
            capture_output=True, text=True, timeout=20,
        )
        if result.returncode == 0:
            for line in result.stdout.strip().split("\n"):
                if line and not line.startswith("./aws/"):
                    refs.append(line.replace("./", ""))
    except Exception:
        pass
    
    return refs[:5]


def get_description(fn_name: str) -> str:
    try:
        cfg = lam.get_function_configuration(FunctionName=fn_name)
        return (cfg.get("Description") or "")[:200]
    except Exception:
        return ""


def get_code_stats(fn_name: str) -> dict:
    src_path = pathlib.Path(f"aws/lambdas/{fn_name}/source/lambda_function.py")
    if not src_path.exists():
        return {"local_source": False}
    try:
        content = src_path.read_text()
        return {
            "local_source":  True,
            "loc":           len(content.split("\n")),
            "imports_boto3": "import boto3" in content,
            "imports_fred":  "fred" in content.lower(),
            "imports_polygon": "polygon" in content.lower(),
            "imports_dynamodb": "dynamodb" in content.lower(),
            "has_handler":   "def lambda_handler" in content or "def handler" in content,
        }
    except Exception:
        return {"err": "read failed"}


def auto_recommend(evidence: dict) -> tuple:
    """Given the evidence, propose a recommendation + reasoning."""
    reasons = []
    
    has_url = bool(evidence.get("function_url"))
    has_esm = bool(evidence.get("event_source_mappings"))
    has_eb_rules = bool(evidence.get("eventbridge_rules"))
    invokes_30d = evidence.get("invoke_stats", {}).get("n_invocations_30d", 0)
    has_page_refs = bool(evidence.get("page_references"))
    has_s3_outputs = bool(evidence.get("s3_outputs"))
    
    keep_signals = 0
    delete_signals = 0
    
    if has_url:
        keep_signals += 3; reasons.append("HAS_FUNCTION_URL")
    if has_esm:
        keep_signals += 3; reasons.append("HAS_EVENT_SOURCE_MAPPING")
    if has_eb_rules:
        keep_signals += 3; reasons.append("EVENTBRIDGE_TARGET")
    if invokes_30d > 5:
        keep_signals += 2; reasons.append(f"ACTIVE_{invokes_30d}_INVOKES_30D")
    elif invokes_30d > 0:
        keep_signals += 1; reasons.append(f"SOME_INVOKES_{invokes_30d}_30D")
    if has_page_refs:
        keep_signals += 2; reasons.append(f"REFERENCED_BY_{len(evidence['page_references'])}_PAGES")
    if has_s3_outputs:
        keep_signals += 1; reasons.append(f"WRITES_S3")
    
    # Delete signals — would need stronger evidence
    desc = (evidence.get("description") or "").lower()
    name = evidence.get("name", "").lower()
    
    # Strict name patterns (not substrings)
    if re.search(r"\btest\b", name):
        delete_signals += 2; reasons.append("NAME_HAS_TEST")
    if re.search(r"\btmp\b|\bscratch\b|\bdraft\b", name):
        delete_signals += 2; reasons.append("NAME_HAS_TMP_SCRATCH")
    if re.search(r"\b(deprecated|obsolete)\b", desc):
        delete_signals += 2; reasons.append("DESC_SAYS_DEPRECATED")
    if invokes_30d == 0 and not has_url and not has_esm:
        delete_signals += 1; reasons.append("NO_INVOKES_30D_NO_TRIGGERS")
    
    # Decide
    if keep_signals >= 3:
        return "KEEP", reasons
    elif delete_signals >= 3 and keep_signals == 0:
        return "DELETE_CANDIDATE", reasons
    elif keep_signals > delete_signals:
        return "KEEP", reasons
    else:
        return "INVESTIGATE", reasons


def main():
    started = datetime.now(timezone.utc)
    
    # Load the 1025 triage and pull out CANDIDATE_DELETE_DEPRECATED
    try:
        triage = json.loads(pathlib.Path(TRIAGE_JSON).read_text())
    except Exception as e:
        print(f"[review] cannot load 1025 triage: {e}")
        return
    
    candidates = triage.get("classified", {}).get("CANDIDATE_DELETE_DEPRECATED", [])
    print(f"[review] reviewing {len(candidates)} candidates flagged by 1025")
    
    results = []
    for i, cand in enumerate(candidates):
        name = cand["name"]
        print(f"[review] [{i+1}/{len(candidates)}] {name}")
        
        evidence = {
            "name":              name,
            "description":       get_description(name),
            "config_last_modified": cand.get("last_modified"),
            "code_size_kb":      cand.get("code_size_kb"),
            "invoke_stats":      get_last_invoke_ts(name, days=30),
            "last_log_event":    get_last_log_event(name),
            "function_url":      get_function_url(name),
            "event_source_mappings": get_event_source_mappings(name),
            "eventbridge_rules": get_eventbridge_rules(name),
            "s3_outputs":        find_s3_outputs_in_source(name),
            "page_references":   find_page_references(name),
            "code_stats":        get_code_stats(name),
        }
        
        recommendation, reasons = auto_recommend(evidence)
        evidence["recommendation"] = recommendation
        evidence["reasons"] = reasons
        results.append(evidence)
    
    # Sort: DELETE_CANDIDATE first (operator's attention), then INVESTIGATE, then KEEP
    rec_order = {"DELETE_CANDIDATE": 0, "INVESTIGATE": 1, "KEEP": 2}
    results.sort(key=lambda r: (rec_order.get(r["recommendation"], 99), r["name"]))
    
    # Aggregate
    by_rec = defaultdict(list)
    for r in results:
        by_rec[r["recommendation"]].append(r["name"])
    
    out = {
        "generated_at":  started.isoformat(),
        "n_reviewed":    len(results),
        "counts":        {k: len(v) for k, v in by_rec.items()},
        "candidates":    results,
    }
    
    pathlib.Path(os.path.dirname(REPORT_JSON)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT_JSON).write_text(json.dumps(out, indent=2, default=str))
    
    # Markdown
    md = []
    md.append("# Candidate Lambda Review — full evidence")
    md.append(f"\nGenerated: {started.isoformat()}\n")
    md.append(f"Total reviewed: **{len(results)}**\n")
    md.append("## Recommendation summary\n")
    for rec, names in sorted(by_rec.items(), key=lambda x: rec_order.get(x[0], 99)):
        md.append(f"- **{rec}**: {len(names)}")
    md.append("")
    md.append("> **How to use this report**: For each candidate, the evidence")
    md.append("> below should be enough to make a KEEP / DELETE / INVESTIGATE decision.")
    md.append("> The auto-recommendation is conservative — when in doubt, INVESTIGATE.")
    md.append("> If you see ⚠️ flags (function URL, ESM, page refs), do NOT delete without")
    md.append("> verifying the downstream consumers can handle the loss.\n")
    md.append("")
    
    for rec_label in ("DELETE_CANDIDATE", "INVESTIGATE", "KEEP"):
        items = [r for r in results if r["recommendation"] == rec_label]
        if not items:
            continue
        md.append(f"\n## {rec_label} ({len(items)})\n")
        for r in items:
            warning = ""
            if r.get("function_url"):       warning += " ⚠️ URL"
            if r.get("event_source_mappings"): warning += " ⚠️ ESM"
            if r.get("page_references"):    warning += " ⚠️ PAGES"
            
            md.append(f"### `{r['name']}`{warning}\n")
            md.append(f"- **Recommendation**: `{r['recommendation']}`")
            md.append(f"- **Reasons**: {', '.join(r['reasons']) if r['reasons'] else 'none'}")
            md.append(f"- **Description**: {r.get('description','(none)') or '(none)'}")
            md.append(f"- **Last invoke (30d)**: {r['invoke_stats'].get('last_invoke_date','never') or 'never'}")
            md.append(f"- **Invoke count (30d)**: {r['invoke_stats'].get('n_invocations_30d', 0)}")
            md.append(f"- **Last log event**: {r.get('last_log_event','never') or 'never'}")
            if r.get("function_url"):
                md.append(f"- **Function URL**: `{r['function_url']}` ← HTTP callable, do not delete")
            if r.get("event_source_mappings"):
                md.append(f"- **Event source mappings**: {len(r['event_source_mappings'])}  ← stream-triggered, do not delete")
            if r.get("eventbridge_rules"):
                md.append(f"- **EventBridge rules**: {', '.join(r['eventbridge_rules'])}")
            if r.get("s3_outputs"):
                md.append(f"- **S3 outputs written**: `{', '.join(r['s3_outputs'][:5])}`")
            if r.get("page_references"):
                md.append(f"- **Pages referencing**: `{', '.join(r['page_references'])}`")
            cs = r.get("code_stats") or {}
            if cs.get("local_source"):
                md.append(f"- **Code stats**: {cs.get('loc')} LOC, "
                           f"boto3={cs.get('imports_boto3', False)} "
                           f"fred={cs.get('imports_fred', False)} "
                           f"polygon={cs.get('imports_polygon', False)} "
                           f"dynamodb={cs.get('imports_dynamodb', False)}")
            md.append("")
    
    pathlib.Path(os.path.dirname(REPORT_MD)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT_MD).write_text("\n".join(md))
    
    print(f"[review] DONE")
    print(f"[review]   DELETE_CANDIDATE: {len(by_rec.get('DELETE_CANDIDATE', []))}")
    print(f"[review]   INVESTIGATE:      {len(by_rec.get('INVESTIGATE', []))}")
    print(f"[review]   KEEP:             {len(by_rec.get('KEEP', []))}")
    print(f"[review]   Report: {REPORT_MD}")


if __name__ == "__main__":
    main()
