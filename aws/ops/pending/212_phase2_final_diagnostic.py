#!/usr/bin/env python3
"""
Step 212 — Phase 2 final diagnostic + force-invoke pass.

A. For each of the 4 still-pending producers: download deployed
   Lambda zip and check 'add_ka_aliases' is actually present in
   the deployed code (vs only in repo source).

B. Find justhodl-reports-builder's actual output S3 key (verifier
   guessed 'scorecard.json' but path may differ).

C. Force-invoke ALL 10 producers and capture errors. Add 30s
   spacing to avoid TooManyRequests we hit in step 211.

D. Re-read ALL 7 target S3 outputs and verify ka_* aliases.

E. FIXED verifier logic — the previous "no khalid_keys at top scan"
   message was a labeling bug. Use clear DUAL-WRITE-OK / OLD-ONLY
   verdict based on actual data.
"""
import io, json, time, urllib.request, zipfile
from datetime import datetime, timezone
from ops_report import report
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


# All 10 producers we patched in Phase 2
PRODUCERS = [
    "justhodl-intelligence",
    "justhodl-daily-report-v3",
    "justhodl-pnl-tracker",
    "justhodl-investor-agents",
    "justhodl-morning-intelligence",
    "justhodl-reports-builder",
    "justhodl-signal-logger",
    "justhodl-bloomberg-v8",
    "justhodl-crypto-intel",
    "justhodl-calibrator",
]

# Map producer → S3 output key (or None if we don't know)
S3_TARGETS = {
    "justhodl-intelligence":         "intelligence-report.json",
    "justhodl-daily-report-v3":      "data/report.json",
    "justhodl-pnl-tracker":          "portfolio/pnl-daily.json",
    "justhodl-bloomberg-v8":         "data/report.json",  # collides with daily-report
    "justhodl-crypto-intel":         "crypto-intel.json",
    "justhodl-calibrator":           "calibration/latest.json",
}


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


def has_helper_in_deployed_code(fn_name):
    """Download deployed Lambda zip and check ka_aliases.py is there +
    the source code calls add_ka_aliases."""
    try:
        info = lam.get_function(FunctionName=fn_name)
        zip_url = info["Code"]["Location"]
        with urllib.request.urlopen(zip_url, timeout=30) as r:
            zip_bytes = r.read()
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            files = zf.namelist()
            has_helper = "ka_aliases.py" in files
            try:
                handler_src = zf.read("lambda_function.py").decode("utf-8", errors="replace")
                calls_helper = "add_ka_aliases(" in handler_src
            except KeyError:
                handler_src = ""
                calls_helper = False
            return has_helper, calls_helper, len(zip_bytes)
    except Exception as e:
        return None, None, str(e)


with report("phase2_final_diagnostic") as r:
    r.heading("Phase 2 final diagnostic + force-invoke pass")

    # ─── A. Confirm deployed code matches repo ─────────────────────────
    r.section("A. Deployed-code audit — does each Lambda have the patch live?")
    deploy_status = {}
    for fn in PRODUCERS:
        has_helper, calls_helper, info = has_helper_in_deployed_code(fn)
        if has_helper is None:
            r.warn(f"  ✗ {fn}: {info}")
            deploy_status[fn] = "ERR"
        elif has_helper and calls_helper:
            r.log(f"  ✅ {fn} ({info}B zip) — helper module + add_ka_aliases() call present")
            deploy_status[fn] = "DEPLOYED"
        else:
            r.warn(f"  ⚠ {fn} — has_helper={has_helper} calls_helper={calls_helper} ({info}B)")
            deploy_status[fn] = "PARTIAL"

    # ─── B. Find reports-builder's actual output S3 key ────────────────
    r.section("B. Locate reports-builder S3 output")
    try:
        info = lam.get_function(FunctionName="justhodl-reports-builder")
        with urllib.request.urlopen(info["Code"]["Location"], timeout=30) as resp:
            zb = resp.read()
        with zipfile.ZipFile(io.BytesIO(zb)) as zf:
            src = zf.read("lambda_function.py").decode("utf-8", errors="replace")
        # Look for SCORECARD_KEY definition or s3.put_object Key=
        import re
        defs = re.findall(r'SCORECARD_KEY\s*=\s*["\']([^"\']+)["\']', src)
        keys = re.findall(r'Key\s*=\s*[\'"]([^\'"]+)[\'"]', src)
        keys += re.findall(r'Key\s*=\s*([A-Z_]+)\b', src)
        r.log(f"  SCORECARD_KEY defs found: {defs}")
        r.log(f"  All S3 keys in source: {sorted(set(keys))[:8]}")
    except Exception as e:
        r.warn(f"  err: {e}")

    # ─── C. Force-invoke all 10 (with spacing to avoid rate-limit) ─────
    r.section("C. Force-invoke all 10 producers")
    invoke_results = {}
    for fn in PRODUCERS:
        try:
            t0 = time.time()
            resp = lam.invoke(FunctionName=fn, InvocationType="RequestResponse",
                              Payload=json.dumps({}))
            elapsed = time.time() - t0
            err = resp.get("FunctionError")
            mark = "✅" if not err else "❌"
            payload = resp["Payload"].read().decode("utf-8", errors="replace")
            r.log(f"  {mark} {fn:30} {elapsed:5.1f}s  err={err or 'none'}")
            if err:
                r.log(f"      payload preview: {payload[:300]}")
            invoke_results[fn] = "OK" if not err else "ERR"
        except Exception as e:
            r.warn(f"  ✗ {fn}: {e}")
            invoke_results[fn] = "INVOKE-FAIL"
        time.sleep(2)  # rate-limit pacing

    time.sleep(8)  # let S3 settle

    # ─── D. Verify S3 outputs ──────────────────────────────────────────
    r.section("D. Verify ka_* aliases in S3 outputs (FIXED verdict logic)")
    final_results = {}
    for fn, key in S3_TARGETS.items():
        r.section(f"📦 {fn} → s3://{BUCKET}/{key}")
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            content = obj["Body"].read()
            age_s = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds()
            r.log(f"  size: {len(content)}B  age: {age_s:.1f}s")
            data = json.loads(content)

            khalid_found = find_keys_in_obj(data, "khalid_")
            ka_found = find_keys_in_obj(data, "ka_")
            r.log(f"  khalid_* keys ({len(khalid_found)}): {sorted(khalid_found)[:5]}")
            r.log(f"  ka_* keys     ({len(ka_found)}): {sorted(ka_found)[:5]}")

            # Verdict: every khalid_<x> should have a matching ka_<x>
            missing = [k for k in khalid_found if "ka_" + k[len("khalid_"):] not in ka_found]
            if not khalid_found:
                # No khalid_* at all — could be that keys aren't deeply nested
                # (fine, no aliases needed for this output)
                r.log(f"  ▸ N/A: no khalid_* keys in this output")
                final_results[fn] = "N/A"
            elif not missing:
                r.log(f"  ▸ ✅ DUAL-WRITE-OK — all {len(khalid_found)} khalid_* keys have ka_* aliases")
                final_results[fn] = "DUAL-WRITE-OK"
            elif len(missing) < len(khalid_found):
                r.warn(f"  ⚠ PARTIAL: {len(missing)}/{len(khalid_found)} keys missing aliases: {missing[:3]}")
                final_results[fn] = "PARTIAL"
            else:
                r.warn(f"  ✗ OLD-ONLY: 0 ka_* aliases for {len(khalid_found)} khalid_* keys")
                final_results[fn] = "OLD-ONLY"
        except s3.exceptions.NoSuchKey:
            r.warn(f"  S3 key not found")
            final_results[fn] = "NO-DATA"
        except Exception as e:
            r.warn(f"  err: {e}")
            final_results[fn] = "ERR"

    # ─── E. Final summary ──────────────────────────────────────────────
    r.section("FINAL SUMMARY")
    r.log("\nDeployment status:")
    for fn, v in deploy_status.items():
        r.log(f"  {v:10} {fn}")
    r.log("\nInvoke status:")
    for fn, v in invoke_results.items():
        r.log(f"  {v:12} {fn}")
    r.log("\nDual-write verdict:")
    for fn, v in final_results.items():
        r.log(f"  {v:14} {fn}")
    n_ok = sum(1 for v in final_results.values() if v in ("DUAL-WRITE-OK", "N/A"))
    r.log(f"\n  {n_ok}/{len(final_results)} producers fully aliased (or N/A)")

    r.log("Done")
