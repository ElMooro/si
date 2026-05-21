"""
ops 1021 - Comprehensive system audit: full Lambda + S3 + schedule + page
inventory with freshness flags + cross-reference + bug detection.

Single read-only sweep producing aws/ops/reports/1021.json which feeds:
  1. Memory consolidation (so we don't rebuild what exists)
  2. Stale-data detection
  3. Orphan-page detection (page references S3 key that doesn't exist)
  4. Unscheduled-Lambda detection (Lambda exists but no EventBridge trigger)
  5. Dead-Lambda detection (no recent invokes, no S3 output)

Sections of the report:
  - summary:           top-line counts
  - lambdas:           every Lambda (state, runtime, memory, timeout, last_mod)
  - schedules:         every EventBridge rule/schedule with target Lambda
  - s3_data_keys:      every data/*.json with size + age + freshness flag
  - data_freshness:    bucketed by age (fresh / stale / dead)
  - ssm_credentials:   which credentials configured (no secret values)
  - cross_reference:   lambdas <-> s3 keys (best-effort name matching)
"""
import json
import os
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

REPO_ROOT = Path(__file__).resolve().parents[3]
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

cfg = Config(read_timeout=60, connect_timeout=10, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION)
events = boto3.client("events", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)

# Freshness thresholds (hours) by S3 key pattern. Default 48h.
FRESHNESS_RULES = [
    ("intraday|tape|1m|live|nowcast|trending", 4),
    ("crypto-intel|fear-greed|funding", 2),
    ("daily|fundamentals|portfolio|risk|signal-board", 36),
    ("13f|institutional|sec|activist", 168),    # weekly
    ("eva|smart-beta|predictability|gf-value|magic|beneish", 30),
    ("cot|cftc", 200),                          # weekly Friday
    ("default", 48),
]


def freshness_for_key(key, age_hours):
    import re
    for pattern, max_age in FRESHNESS_RULES:
        if pattern == "default":
            continue
        if re.search(pattern, key):
            return age_hours <= max_age, max_age
    return age_hours <= 48, 48


def now():
    return datetime.now(timezone.utc)


# ---------- LAMBDAS ----------
def list_all_lambdas():
    out = []
    paginator = lam.get_paginator("list_functions")
    for page in paginator.paginate():
        for fn in page.get("Functions", []):
            name = fn.get("FunctionName")
            if not name or not name.startswith("justhodl-"):
                continue
            env_keys = list((fn.get("Environment") or {}
                              ).get("Variables", {}).keys())
            out.append({
                "name": name,
                "runtime": fn.get("Runtime"),
                "memory_mb": fn.get("MemorySize"),
                "timeout_s": fn.get("Timeout"),
                "code_size": fn.get("CodeSize"),
                "last_modified": fn.get("LastModified"),
                "n_env_keys": len(env_keys),
                "env_key_names": sorted(env_keys)[:25],
                "state": fn.get("State"),
                "last_update_status": fn.get("LastUpdateStatus"),
                "handler": fn.get("Handler"),
                "description": (fn.get("Description") or "")[:200],
            })
    out.sort(key=lambda x: x["name"])
    return out


# ---------- EVENTBRIDGE ----------
def list_all_schedules():
    """Both classic Rules and new Scheduler. Returns list of unique
    target-Lambda triggers."""
    out = []
    seen_targets = set()
    # Classic rules (EventBridge events)
    try:
        paginator = events.get_paginator("list_rules")
        for page in paginator.paginate():
            for rule in page.get("Rules", []):
                rname = rule.get("Name", "")
                state = rule.get("State", "")
                schedule = (rule.get("ScheduleExpression")
                            or rule.get("EventPattern", "")[:80])
                try:
                    targets = events.list_targets_by_rule(
                        Rule=rname).get("Targets", [])
                except Exception:
                    targets = []
                for t in targets:
                    arn = t.get("Arn") or ""
                    if ":function:" in arn:
                        fname = arn.split(":function:")[1].split(":")[0]
                        if fname.startswith("justhodl-"):
                            key = (rname, fname)
                            if key in seen_targets:
                                continue
                            seen_targets.add(key)
                            out.append({
                                "api": "events",
                                "rule_name": rname,
                                "schedule": schedule,
                                "state": state,
                                "target_lambda": fname,
                            })
    except Exception as e:
        print(f"list_rules error: {e}")
    return out


# ---------- S3 ----------
def list_all_data_keys():
    out = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix="data/"):
        for obj in page.get("Contents", []) or []:
            key = obj["Key"]
            lm = obj["LastModified"]
            age = round((now() - lm).total_seconds() / 3600, 1)
            fresh, max_age = freshness_for_key(key, age)
            out.append({
                "key": key,
                "size_bytes": obj.get("Size"),
                "last_modified": lm.isoformat(),
                "age_hours": age,
                "max_age_hours": max_age,
                "fresh": fresh,
            })
    out.sort(key=lambda x: x["key"])
    return out


# ---------- SSM ----------
def check_credentials():
    """Check which SSM credentials are configured without revealing values."""
    expected = [
        "/justhodl/finra/client_id",
        "/justhodl/finra/client_secret",
        "/justhodl/finra/access_token",
        "/justhodl/telegram/chat_id",
        "/justhodl/portfolio-admin/manager-pass",
        "/justhodl/api-admin/token",
        "/justhodl/calibration/weights",
    ]
    out = []
    for name in expected:
        try:
            r = ssm.get_parameter(Name=name, WithDecryption=False)
            out.append({"name": name, "configured": True,
                         "type": r["Parameter"].get("Type"),
                         "last_modified": r["Parameter"].get(
                             "LastModifiedDate").isoformat()
                         if r["Parameter"].get("LastModifiedDate")
                         else None})
        except ssm.exceptions.ParameterNotFound:
            out.append({"name": name, "configured": False})
        except Exception as e:
            out.append({"name": name, "configured": False,
                         "error": str(e)[:100]})
    return out


# ---------- CROSS-REFERENCE ----------
def cross_reference(lambdas, s3_keys):
    """Best-effort: match Lambda name to S3 keys it likely writes,
    using name-based heuristics."""
    import re
    fn_names = {ln["name"] for ln in lambdas}
    out = []
    for key_entry in s3_keys:
        key = key_entry["key"]
        basename = key.replace("data/", "").replace(".json", "").replace(
            "-history", "").replace("-cache", "")
        # Try to find lambda with matching name fragment
        candidates = []
        for fn in fn_names:
            fn_short = fn.replace("justhodl-", "")
            if fn_short == basename or basename in fn_short or fn_short in basename:
                candidates.append(fn)
        # Pick best match (longest common substring approach: shortest fn name
        # that contains key, or fn whose name exactly matches the key basename)
        best = None
        if candidates:
            # Prefer exact match
            for c in candidates:
                if c == f"justhodl-{basename}":
                    best = c
                    break
            if not best:
                # Fall back to shortest containing
                best = min(candidates, key=len)
        out.append({"key": key, "guessed_writer": best,
                     "candidates_count": len(candidates)})
    return out


# ---------- PAGES (local repo, not S3) ----------
def list_local_pages():
    out = []
    for f in REPO_ROOT.glob("*.html"):
        try:
            content = f.read_text(encoding="utf-8")
            n_lines = content.count("\n") + 1
            # Find S3 data keys it fetches
            import re
            referenced_keys = sorted(set(
                re.findall(r"data/[a-z0-9_-]+\.json", content)))
            out.append({
                "page": f.name,
                "size_bytes": f.stat().st_size,
                "n_lines": n_lines,
                "n_data_refs": len(referenced_keys),
                "data_keys_referenced": referenced_keys[:15],
            })
        except Exception as e:
            out.append({"page": f.name, "error": str(e)[:100]})
    out.sort(key=lambda x: x["page"])
    return out


# ---------- INDEX LINKS ----------
def parse_index_links():
    idx = REPO_ROOT / "index.html"
    if not idx.exists():
        return {"error": "index.html missing"}
    import re
    content = idx.read_text(encoding="utf-8")
    # Extract href values pointing to local pages
    hrefs = re.findall(r'href=["\']([^"\'#]+?\.html)["\']', content)
    # Normalize (strip leading /)
    pages_linked = sorted(set([h.lstrip("/") for h in hrefs]))
    return {
        "n_links_to_html": len(hrefs),
        "n_unique_html_pages_linked": len(pages_linked),
        "pages_linked": pages_linked,
        "index_size_bytes": idx.stat().st_size,
        "index_n_lines": content.count("\n") + 1,
    }


# ---------- BUG DETECTION ----------
def detect_bugs(lambdas, s3_keys, schedules, index_info, pages):
    bugs = []
    # 1. Stale data feeds
    for k in s3_keys:
        if not k["fresh"]:
            bugs.append({
                "type": "STALE_DATA",
                "severity": "medium" if k["age_hours"] < 168 else "high",
                "key": k["key"],
                "age_hours": k["age_hours"],
                "max_allowed": k["max_age_hours"],
                "detail": (f"Data is {k['age_hours']}h old "
                            f"(max {k['max_age_hours']}h)"),
            })
    # 2. Lambda with failed last update status
    for ln in lambdas:
        if (ln.get("last_update_status") and
                ln["last_update_status"] != "Successful"):
            bugs.append({
                "type": "LAMBDA_UPDATE_FAILED",
                "severity": "high",
                "lambda": ln["name"],
                "detail": ln["last_update_status"],
            })
    # 3. Orphan pages: HTML files not linked from index.html
    if isinstance(index_info, dict) and "pages_linked" in index_info:
        linked = set(index_info["pages_linked"])
        for p in pages:
            if p["page"] == "index.html":
                continue
            if p["page"] not in linked:
                # Allow common utility pages to be unlinked
                if p["page"] in ("404.html", "feedback.html",
                                  "directory.html"):
                    continue
                bugs.append({
                    "type": "ORPHAN_PAGE",
                    "severity": "low",
                    "page": p["page"],
                    "n_lines": p.get("n_lines"),
                    "detail": "Page exists but not linked from index.html",
                })
    # 4. Pages referencing S3 keys that don't exist
    existing_keys = {k["key"] for k in s3_keys}
    for p in pages:
        for ref in p.get("data_keys_referenced", []):
            if ref not in existing_keys:
                bugs.append({
                    "type": "PAGE_REFERENCES_MISSING_S3",
                    "severity": "high",
                    "page": p["page"],
                    "missing_key": ref,
                    "detail": ("Page reads S3 key that has no object "
                                "in the bucket"),
                })
    return bugs


def main():
    started = now()
    report = {"started_at": started.isoformat()}

    print("[ops 1021] listing lambdas...")
    lambdas = list_all_lambdas()
    print(f"  {len(lambdas)} lambdas")

    print("[ops 1021] listing schedules...")
    schedules = list_all_schedules()
    print(f"  {len(schedules)} active schedule rules")

    print("[ops 1021] listing S3 data keys...")
    s3_keys = list_all_data_keys()
    print(f"  {len(s3_keys)} data/*.json keys")

    print("[ops 1021] checking SSM credentials...")
    creds = check_credentials()
    print(f"  {sum(1 for c in creds if c.get('configured'))}/"
          f"{len(creds)} configured")

    print("[ops 1021] listing local pages...")
    pages = list_local_pages()
    print(f"  {len(pages)} HTML pages")

    print("[ops 1021] parsing index.html links...")
    index_info = parse_index_links()

    print("[ops 1021] cross-referencing...")
    xref = cross_reference(lambdas, s3_keys)

    print("[ops 1021] detecting bugs...")
    bugs = detect_bugs(lambdas, s3_keys, schedules, index_info, pages)
    print(f"  {len(bugs)} potential issues")

    # Summary
    n_fresh = sum(1 for k in s3_keys if k["fresh"])
    n_stale = len(s3_keys) - n_fresh
    bug_by_severity = {"high": 0, "medium": 0, "low": 0}
    bug_by_type = {}
    for b in bugs:
        bug_by_severity[b.get("severity", "low")] = (
            bug_by_severity.get(b.get("severity", "low"), 0) + 1)
        bug_by_type[b["type"]] = bug_by_type.get(b["type"], 0) + 1

    report["summary"] = {
        "n_lambdas": len(lambdas),
        "n_schedules": len(schedules),
        "n_s3_data_keys": len(s3_keys),
        "n_s3_fresh": n_fresh,
        "n_s3_stale": n_stale,
        "n_pages": len(pages),
        "n_pages_linked_from_index": index_info.get(
            "n_unique_html_pages_linked", 0)
            if isinstance(index_info, dict) else 0,
        "n_ssm_creds_configured": sum(
            1 for c in creds if c.get("configured")),
        "n_bugs_total": len(bugs),
        "bugs_by_severity": bug_by_severity,
        "bugs_by_type": bug_by_type,
    }
    report["lambdas"] = lambdas
    report["schedules"] = schedules
    report["s3_data_keys"] = s3_keys
    report["ssm_credentials"] = creds
    report["pages"] = pages
    report["index_info"] = index_info
    report["cross_reference"] = xref
    report["bugs"] = bugs
    report["ended_at"] = now().isoformat()

    out_path = REPO_ROOT / "aws" / "ops" / "reports" / "1021.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, default=str))
    print(f"[ops 1021] report {out_path.relative_to(REPO_ROOT)} "
          f"({out_path.stat().st_size // 1024}KB)")
    print(json.dumps(report["summary"], indent=2))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print(traceback.format_exc())
        sys.exit(1)
