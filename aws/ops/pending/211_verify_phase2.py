#!/usr/bin/env python3
"""
Step 211 — Verify Phase 2 dual-write succeeded.

For each producer Lambda:
  A. Force-invoke (or read most recent S3 output)
  B. Check S3 output JSON contains ka_* aliases beside khalid_*
  C. Verdict: DUAL-WRITE-OK / OLD-ONLY (still khalid_* only) /
              ERR / NO-DATA-YET

This confirms the deploy-lambdas.yml workflow actually deployed
each Lambda and the helper is being called at runtime.
"""
import io, json, time
from ops_report import report
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


# (Lambda name, S3 key it writes, list of khalid_* keys to find)
TARGETS = [
    ("justhodl-intelligence",        "intelligence-report.json",     ["khalid_index", "khalid_strategy"]),
    ("justhodl-daily-report-v3",     "data/report.json",             ["khalid_index"]),
    ("justhodl-pnl-tracker",         "portfolio/pnl-daily.json",     ["khalid_strategy", "khalid_strategy_value_usd", "khalid_return_pct"]),
    ("justhodl-reports-builder",     "scorecard.json",               ["khalid_score", "khalid_timeline"]),
    ("justhodl-bloomberg-v8",        "data/report.json",             ["khalid_index"]),  # may overlap with daily-report-v3
    ("justhodl-crypto-intel",        "crypto-intel.json",            ["khalid_index"]),
    ("justhodl-calibrator",          "calibration/latest.json",      ["khalid_component_weights", "khalid_index"]),
]


def find_keys_in_obj(obj, prefix, _seen=None, _depth=0):
    """Recursively find all keys starting with prefix in obj."""
    if _seen is None: _seen = set()
    if _depth > 12: return _seen
    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(k, str) and k.startswith(prefix):
                _seen.add(k)
            find_keys_in_obj(v, prefix, _seen, _depth + 1)
    elif isinstance(obj, list):
        for x in obj:
            find_keys_in_obj(x, prefix, _seen, _depth + 1)
    return _seen


def has_ka_alias_for(obj, khalid_key):
    """Check if `khalid_<x>` has a corresponding `ka_<x>` somewhere in obj."""
    target_ka = "ka_" + khalid_key[len("khalid_"):]
    found = find_keys_in_obj(obj, "ka_")
    return target_ka in found


with report("verify_phase2_dual_write") as r:
    r.heading("Verify Phase 2 dual-write — ka_* aliases in S3 outputs")

    # Force-invoke a few Lambdas to bake fresh data, since their schedules
    # may not have triggered yet
    r.section("A. Force-invoke producer Lambdas to bake fresh outputs")
    invoke_targets = ["justhodl-intelligence", "justhodl-daily-report-v3",
                      "justhodl-pnl-tracker", "justhodl-crypto-intel"]
    for fn in invoke_targets:
        try:
            t0 = time.time()
            resp = lam.invoke(FunctionName=fn, InvocationType="RequestResponse",
                              Payload=json.dumps({}))
            elapsed = time.time() - t0
            err = resp.get("FunctionError")
            mark = "🟢" if not err else "🔴"
            payload = resp["Payload"].read().decode("utf-8", errors="replace")[:200]
            r.log(f"  {mark} {fn:30} {elapsed:5.1f}s  err={err or 'none'}")
            if err: r.log(f"      payload: {payload}")
        except Exception as e:
            r.warn(f"  ✗ {fn}: {e}")

    time.sleep(5)  # give S3 a moment to settle

    # Per-target verification
    r.section("B. Verify ka_* aliases in S3 outputs")
    results = {}
    for fn, key, khalid_keys in TARGETS:
        r.section(f"📦 {fn} → s3://{BUCKET}/{key}")
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            content = obj["Body"].read()
            from datetime import datetime, timezone
            age_h = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 3600
            r.log(f"  size: {len(content)}B  age: {age_h:.2f}h")

            try:
                data = json.loads(content)
            except Exception as e:
                r.warn(f"  parse err: {e}")
                results[fn] = "PARSE-ERR"
                continue

            # Find all khalid_* and ka_* keys recursively
            khalid_found = find_keys_in_obj(data, "khalid_")
            ka_found = find_keys_in_obj(data, "ka_")
            r.log(f"  khalid_* keys found: {sorted(khalid_found)[:8]}{' …' if len(khalid_found) > 8 else ''}")
            r.log(f"  ka_* keys found:     {sorted(ka_found)[:8]}{' …' if len(ka_found) > 8 else ''}")

            # Specific aliases for documented khalid keys
            missing_aliases = []
            for kk in khalid_keys:
                expected_ka = "ka_" + kk[len("khalid_"):]
                if kk in khalid_found and expected_ka not in ka_found:
                    missing_aliases.append((kk, expected_ka))

            if missing_aliases:
                for kk, expected_ka in missing_aliases:
                    r.warn(f"  ⚠ {kk} present but {expected_ka} missing")
                results[fn] = "PARTIAL-DUAL"
            elif ka_found and len(ka_found) >= len(khalid_keys):
                r.log(f"  ▸ DUAL-WRITE-OK: {len(ka_found)} ka_* aliases for {len(khalid_found)} khalid_* keys")
                results[fn] = "DUAL-WRITE-OK"
            elif khalid_found and not ka_found:
                r.warn(f"  ✗ OLD-ONLY: khalid_* present, no ka_* aliases. Lambda may not be redeployed yet.")
                results[fn] = "OLD-ONLY"
            else:
                r.log(f"  no khalid_ keys at top scan")
                results[fn] = "NO-KHALID-KEYS"

        except s3.exceptions.NoSuchKey:
            r.warn(f"  S3 key not found")
            results[fn] = "NO-DATA"
        except Exception as e:
            r.warn(f"  err: {e}")
            results[fn] = "ERR"

    # ── Summary ─────────────────────────────────────────────────────────
    r.section("FINAL")
    counts = {}
    for fn, v in results.items():
        counts[v] = counts.get(v, 0) + 1
    for v, c in sorted(counts.items()):
        r.log(f"  {v}: {c}")
    n_ok = counts.get("DUAL-WRITE-OK", 0)
    r.log(f"\n  {n_ok}/{len(TARGETS)} producers fully dual-writing")
    r.log("Done")
