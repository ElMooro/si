#!/usr/bin/env python3
"""
Follow-up health check — fill in gaps from 44_system_health_check.py:
  A. Examine actual Secretary JSON shape (tier-2 cards really present?)
  B. List duplicate EB rules that should be consolidated
  C. Verify recent FRED skip rate (re-run without the import bug)
  D. Read justhodl.ai CNAME record still resolves to GitHub Pages
"""
import json
import re
import urllib.request
from datetime import datetime, timezone, timedelta
from ops_report import report
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

s3 = boto3.client("s3", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)
eb = boto3.client("events", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


with report("health_check_followup") as r:
    r.heading("Follow-up — Secretary JSON, EB duplicates, FRED skip rate")

    # ═════════ A — Secretary JSON inspection ═════════
    r.section("A. Secretary v2.2 — actual JSON shape")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/secretary-latest.json")
        data = json.loads(obj["Body"].read())
        r.log(f"  All top-level keys: {sorted(data.keys())}")
        r.log("")
        # Check for expected v2.2 keys (be more flexible about name variants)
        expected = {
            "options_flow": ["options_flow", "options", "flow", "tier2_flow", "options_card"],
            "crypto_intel": ["crypto_intel", "crypto", "crypto_card", "tier2_crypto"],
            "sector_rotation": ["sector_rotation", "sectors", "rotation", "sector_card"],
            "hit_rate": ["hit_rate_pct", "hit_rate", "yesterday_hit_rate", "yesterday_deltas", "deltas"],
            "picks": ["top_picks", "picks", "signals", "top_stocks", "top_signals"],
            "regime": ["regime", "market_regime", "regime_info"],
        }
        for feature, candidates in expected.items():
            found = None
            for c in candidates:
                if c in data:
                    val = data[c]
                    preview = str(val)[:120]
                    found = c
                    r.log(f"  {feature}: key='{c}', value_preview={preview}")
                    break
            if not found:
                r.log(f"  {feature}: ✗ NOT FOUND under any of {candidates}")
        r.log("")
        # Specifically look for sector rotation format
        sr = data.get("sector_rotation") or data.get("sectors") or {}
        if sr and isinstance(sr, dict):
            r.log(f"  sector_rotation payload: {json.dumps(sr, indent=2)[:500]}")
    except Exception as e:
        r.fail(f"  {e}")

    # ═════════ B — EB duplicate rules detection ═════════
    r.section("B. EventBridge rules — detect duplicates firing same Lambda")
    try:
        rules = eb.list_rules()["Rules"]
        # Build map: target_lambda → [rules]
        target_to_rules = {}
        for rule in rules:
            try:
                targets = eb.list_targets_by_rule(Rule=rule["Name"]).get("Targets", [])
                for t in targets:
                    arn = t.get("Arn", "")
                    if ":lambda:" in arn:
                        fn = arn.split(":")[-1]
                        target_to_rules.setdefault(fn, []).append({
                            "rule_name": rule["Name"],
                            "schedule": rule.get("ScheduleExpression", "(none)"),
                            "state": rule.get("State", "?"),
                        })
            except Exception:
                pass

        r.log("  Lambdas with multiple EB rules pointing at them:")
        for fn, rs in sorted(target_to_rules.items()):
            if len(rs) > 1:
                r.log(f"\n  🔴 {fn} — {len(rs)} rules:")
                for r_info in rs:
                    r.log(f"    [{r_info['state']}] {r_info['rule_name']}  {r_info['schedule']}")
        r.log("")
        # Singleton rules — normal
        singles = [f for f, rs in target_to_rules.items() if len(rs) == 1]
        r.log(f"  Lambdas with exactly 1 rule (normal): {len(singles)}")
        r.kv(check="eb-duplicates", duplicate_targets=sum(1 for rs in target_to_rules.values() if len(rs) > 1))
    except Exception as e:
        r.warn(f"  {e}")

    # ═════════ C — FRED skip rate (re-check with re imported correctly) ═════════
    r.section("C. FRED v3.2 skip rate — last 3 runs")
    try:
        streams = logs.describe_log_streams(
            logGroupName="/aws/lambda/justhodl-daily-report-v3",
            orderBy="LastEventTime", descending=True, limit=3,
        ).get("logStreams", [])
        skip_counts = []
        fetch_times = []
        done_times = []
        for s in streams[:3]:
            start = int((datetime.now(timezone.utc) - timedelta(minutes=30)).timestamp() * 1000)
            ev = logs.get_log_events(
                logGroupName="/aws/lambda/justhodl-daily-report-v3",
                logStreamName=s["logStreamName"], startTime=start, limit=200, startFromHead=False,
            )
            for e in ev.get("events", []):
                m = e.get("message", "").strip()
                if "FRED v3.2: skipped" in m:
                    mt = re.search(r"skipped (\d+) via smart TTL.*fetching (\d+)", m)
                    if mt:
                        skip_counts.append((int(mt.group(1)), int(mt.group(2))))
                if "FRED: " in m and "/233 in " in m:
                    mt = re.search(r"FRED: (\d+)/233 in ([\d.]+)s", m)
                    if mt:
                        fetch_times.append(float(mt.group(2)))
                if "V10] DONE" in m:
                    mt = re.search(r"DONE ([\d.]+)s", m)
                    if mt:
                        done_times.append(float(mt.group(1)))

        if skip_counts:
            r.log(f"  Skip counts (skipped, fetched): {skip_counts[:5]}")
            for skip, fetch in skip_counts[:5]:
                total = skip + fetch
                pct = 100 * skip / total if total > 0 else 0
                r.log(f"    → {skip}/{total} = {pct:.0f}% skip")
            latest_pct = 100 * skip_counts[0][0] / (skip_counts[0][0] + skip_counts[0][1]) if sum(skip_counts[0]) > 0 else 0
            if latest_pct >= 80:
                r.ok(f"  ✓ Healthy: {latest_pct:.0f}% skip on latest run")
            else:
                r.warn(f"  ⚠ Low: {latest_pct:.0f}% skip on latest run")
        else:
            r.log("  No v3.2 log lines found")
        if fetch_times:
            r.log(f"\n  FRED fetch times: {[f'{t:.1f}s' for t in fetch_times[:5]]}")
            avg = sum(fetch_times) / len(fetch_times)
            r.log(f"  Average: {avg:.1f}s")
        if done_times:
            r.log(f"  End-to-end DONE: {[f'{t:.1f}s' for t in done_times[:5]]}")
        r.kv(skip_rate_pct=round(latest_pct, 0) if skip_counts else None,
             avg_fetch_s=round(sum(fetch_times)/len(fetch_times), 1) if fetch_times else None,
             avg_done_s=round(sum(done_times)/len(done_times), 1) if done_times else None)
    except Exception as e:
        r.warn(f"  {e}")

    # ═════════ D — Frontend reachability ═════════
    r.section("D. Frontend — justhodl.ai pages (indirect via S3 ref probe)")
    # We can't hit justhodl.ai from sandbox due to egress filter, but we can
    # verify the CNAME file + check that GH Pages resolution is correct
    try:
        # Verify the repo's CNAME file (tells GH Pages the domain)
        import subprocess
        result = subprocess.run(
            ["git", "-C", "/home/claude/si", "show", "HEAD:CNAME"],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode == 0:
            r.log(f"  CNAME file: {result.stdout.strip()}")
        # Check GH Pages status from the IP range
        import socket
        ip = socket.gethostbyname("justhodl.ai")
        first_octet = ip.split(".")[0]
        if first_octet == "185":
            r.log(f"  DNS: justhodl.ai → {ip} (185.x = GitHub Pages ✓)")
        else:
            r.warn(f"  DNS: justhodl.ai → {ip} (not GitHub Pages range)")
        api_ip = socket.gethostbyname("api.justhodl.ai")
        first = api_ip.split(".")[0]
        if first in ("104", "172", "188"):
            r.log(f"  DNS: api.justhodl.ai → {api_ip} (Cloudflare range ✓)")
        else:
            r.warn(f"  DNS: api.justhodl.ai → {api_ip}")
    except Exception as e:
        r.warn(f"  {e}")

    r.log("Done")
