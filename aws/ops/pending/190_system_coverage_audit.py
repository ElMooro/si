#!/usr/bin/env python3
"""
Step 190 — System coverage audit.

Question: is every feature of the system displayed and accessible
on the website?

Method:
  A. Inventory every Lambda in account 857687956942
  B. Inventory every S3 data file
  C. Inventory every HTML page
  D. Inventory every link in the main nav (top of new index.html)
  E. Cross-reference: which Lambdas → which data → which page?
     Identify gaps:
       1. Lambdas producing data but no page surfaces it
       2. Pages with broken/dead data sources
       3. S3 data files unused by any page
       4. Pages not linked from anywhere
"""
import json
import os
import re
import time
from collections import defaultdict
from ops_report import report
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
events = boto3.client("events", region_name=REGION)


with report("system_coverage_audit") as r:
    r.heading("System Coverage Audit — every feature on website?")

    # ─── A. Lambda inventory ────────────────────────────────────────────
    r.section("A. Lambda inventory (full account)")
    all_lambdas = []
    paginator = lam.get_paginator("list_functions")
    for page in paginator.paginate():
        all_lambdas.extend(page.get("Functions", []))
    r.log(f"  Total: {len(all_lambdas)} Lambdas")

    # Classify by name pattern
    justhodl_lambdas = [f for f in all_lambdas if "justhodl" in f["FunctionName"].lower()]
    other_lambdas = [f for f in all_lambdas if "justhodl" not in f["FunctionName"].lower()]
    r.log(f"  justhodl-* : {len(justhodl_lambdas)}")
    r.log(f"  other      : {len(other_lambdas)}")

    # Find URL-exposed Lambdas (the ones browsers can hit)
    r.log(f"\n  Looking for Lambdas with Function URLs (browser-exposed):")
    url_exposed = []
    for f in all_lambdas:
        try:
            url_cfg = lam.get_function_url_config(FunctionName=f["FunctionName"])
            url_exposed.append({
                "name": f["FunctionName"],
                "url": url_cfg.get("FunctionUrl"),
                "auth": url_cfg.get("AuthType"),
                "last_mod": f["LastModified"][:10],
            })
        except ClientError:
            pass
    r.log(f"  Found {len(url_exposed)} Lambdas with public Function URLs:")
    for ue in sorted(url_exposed, key=lambda x: x["name"]):
        r.log(f"    {ue['name']:40} {ue['auth']:8} mod={ue['last_mod']}")

    # ─── B. EventBridge schedules — find scheduled Lambdas ──────────────
    r.section("B. EventBridge schedules (auto-running Lambdas)")
    rules_resp = events.list_rules(Limit=200)
    rules = [rule for rule in rules_resp.get("Rules", []) if rule.get("ScheduleExpression")]
    r.log(f"  Found {len(rules)} scheduled rules")

    rule_to_lambda = {}
    for rule in rules:
        try:
            tgts = events.list_targets_by_rule(Rule=rule["Name"], Limit=10)
            for t in tgts.get("Targets", []):
                arn = t.get("Arn", "")
                if ":lambda:" in arn:
                    name = arn.split(":")[-1]
                    rule_to_lambda[rule["Name"]] = {
                        "lambda": name,
                        "schedule": rule["ScheduleExpression"],
                        "state": rule.get("State"),
                    }
        except Exception:
            pass

    r.log(f"\n  Active scheduled Lambdas:")
    for rule_name, info in sorted(rule_to_lambda.items()):
        if info["state"] == "ENABLED":
            r.log(f"    {info['lambda']:40} {info['schedule']:30} ({rule_name[:25]})")

    # ─── C. S3 inventory — all data files ──────────────────────────────
    r.section("C. S3 data inventory")
    all_s3_keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET):
        all_s3_keys.extend(page.get("Contents", []))
    r.log(f"  Total: {len(all_s3_keys)} objects")

    # Group by prefix
    by_prefix = defaultdict(list)
    for o in all_s3_keys:
        k = o["Key"]
        if "/" in k:
            prefix = k.split("/")[0] + "/"
        else:
            prefix = "/"
        by_prefix[prefix].append({"key": k, "size": o["Size"], "mod": o["LastModified"]})

    r.log(f"\n  By prefix:")
    for prefix, items in sorted(by_prefix.items()):
        r.log(f"    {prefix:35} {len(items):>4} files")

    # Fresh data files (root JSONs and key subfolders)
    r.log(f"\n  All JSON data sources (recent only — last 7 days):")
    from datetime import datetime, timezone, timedelta
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    fresh_jsons = [o for o in all_s3_keys
                   if o["Key"].endswith(".json") and o["LastModified"] > cutoff]
    fresh_jsons.sort(key=lambda x: x["LastModified"], reverse=True)
    for o in fresh_jsons[:50]:
        age_h = (datetime.now(timezone.utc) - o["LastModified"]).total_seconds() / 3600
        r.log(f"    {o['Key']:50} {o['Size']:>9}B  {age_h:5.1f}h ago")

    # ─── D. HTML page inventory + extract links + data sources ──────────
    r.section("D. HTML page inventory")
    repo_root = os.environ.get("GITHUB_WORKSPACE", "/home/claude/si")
    html_files = []
    for f in os.listdir(repo_root):
        if f.endswith(".html") and f != "index-old.html":
            html_files.append(f)
    # Subdirs
    for d in ["stock", "screener", "crypto", "positioning", "agent", "bot", "khalid", "euro", "stocks"]:
        if os.path.exists(os.path.join(repo_root, d, "index.html")):
            html_files.append(f"{d}/index.html")
    html_files.sort()

    r.log(f"  Total HTML pages: {len(html_files)}")

    # For each page, extract:
    #   - data sources (URLs fetched)
    #   - whether linked from index.html
    page_data_sources = {}
    page_size = {}
    page_status = {}
    for f in html_files:
        path = os.path.join(repo_root, f)
        try:
            with open(path) as fp:
                content = fp.read()
            page_size[f] = len(content)
            urls = set()
            urls.update(re.findall(r'https?://[^\s"\'<>]+\.(?:json|html)', content))
            urls.update(re.findall(r'https?://[a-zA-Z0-9-]+\.execute-api\.[^\s"\'<>]+', content))
            urls.update(re.findall(r'https?://[a-zA-Z0-9]+\.lambda-url\.[^\s"\'<>]+', content))
            urls.update(re.findall(r'https?://api\.justhodl\.ai[^\s"\'<>]*', content))
            page_data_sources[f] = list(urls)[:8]
            # Quick status heuristic
            if len(content) < 500:
                page_status[f] = "STUB"
            elif "execute-api" in content and "i70jxru6md" in content:
                page_status[f] = "BROKEN-OPENBB"
            elif "zzmoq2mq4vtphjyhm4i7hqpzvm0hkwsj" in content:
                page_status[f] = "BROKEN-ECB"
            elif "s3-website-us-east-1.amazonaws.com" in content and "http://" in content:
                page_status[f] = "BROKEN-MIXED-CONTENT"
            else:
                page_status[f] = "ok"
        except Exception as e:
            page_status[f] = f"err: {e}"

    r.log(f"\n  Page status summary:")
    status_counts = defaultdict(int)
    for s in page_status.values():
        if s.startswith("err"):
            status_counts["err"] += 1
        else:
            status_counts[s] += 1
    for k in sorted(status_counts.keys()):
        r.log(f"    {k:30} {status_counts[k]} pages")

    r.log(f"\n  Pages by status:")
    for status_label in ["ok", "STUB", "BROKEN-OPENBB", "BROKEN-ECB", "BROKEN-MIXED-CONTENT"]:
        same = [f for f,s in page_status.items() if s == status_label]
        if not same: continue
        r.log(f"\n  ── {status_label} ({len(same)}):")
        for f in sorted(same):
            sz = page_size.get(f, 0)
            r.log(f"    {f:40} {sz:>7}B")

    # ─── E. Index.html nav coverage ────────────────────────────────────
    r.section("E. New index.html nav + tile launcher coverage")
    with open(os.path.join(repo_root, "index.html")) as f:
        idx_content = f.read()

    # Extract all internal page links from new index.html
    nav_links = re.findall(r'href="(/[a-zA-Z0-9_/.-]+\.html?|/[a-zA-Z0-9_/-]+/?)"', idx_content)
    nav_pages = set()
    for link in nav_links:
        link = link.strip("/")
        if not link or link == "":
            continue
        # normalize
        if link.endswith("/"):
            link += "index.html"
        elif "." not in link:
            link += "/index.html"
        nav_pages.add(link)

    r.log(f"  Pages linked from new index.html: {len(nav_pages)}")
    for p in sorted(nav_pages):
        r.log(f"    /{p}")

    # ─── F. GAPS ──────────────────────────────────────────────────────
    r.section("F. GAPS — features not surfaced on the website")

    # Gap 1: pages that exist but aren't in the new index.html nav
    page_basenames = {f for f in html_files}
    not_linked = sorted(page_basenames - nav_pages)
    r.log(f"\n  Pages NOT linked from new index.html ({len(not_linked)}):")
    for f in not_linked:
        sz = page_size.get(f, 0)
        st = page_status.get(f, "?")
        r.log(f"    {f:40} {sz:>7}B  {st}")

    # Gap 2: Lambdas that produce data but no page reads it
    # Check S3 prefixes vs page data sources
    # Build set of all S3 keys referenced by pages
    all_referenced_s3 = set()
    for srcs in page_data_sources.values():
        for u in srcs:
            if "justhodl-dashboard-live" in u:
                # Extract key
                m = re.search(r'justhodl-dashboard-live(?:\.s3[^/]*\.amazonaws\.com|\.s3-website-us-east-1\.amazonaws\.com)/([^\s"\'<>?]+)', u)
                if m:
                    all_referenced_s3.add(m.group(1))

    r.log(f"\n  S3 keys REFERENCED by HTML pages: {len(all_referenced_s3)}")
    r.log(f"\n  S3 JSONs UNUSED by any page (potential gaps):")
    s3_jsons = {o["Key"] for o in all_s3_keys if o["Key"].endswith(".json")}
    unused = sorted(s3_jsons - all_referenced_s3)
    for k in unused[:30]:
        # Find size + age
        for o in all_s3_keys:
            if o["Key"] == k:
                age_h = (datetime.now(timezone.utc) - o["LastModified"]).total_seconds() / 3600
                if age_h < 168:  # only show fresh ones
                    r.log(f"    {k:50} {o['Size']:>9}B  {age_h:5.1f}h")
                break

    # Gap 3: Lambdas that don't have an obvious page mapping
    r.log(f"\n  All justhodl-* Lambda names (so we can spot orphans):")
    for f in sorted(justhodl_lambdas, key=lambda x: x["FunctionName"]):
        r.log(f"    {f['FunctionName']:50} mod={f['LastModified'][:10]}")

    r.log("Done")
