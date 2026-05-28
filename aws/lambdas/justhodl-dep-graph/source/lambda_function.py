"""
justhodl-dep-graph — Arch #8: Platform Dependency Graph
========================================================
Auto-generated daily map of: Lambdas → S3 outputs → consuming pages/Lambdas.

Surfaces:
  - Orphan Lambdas (write to S3 keys that nothing reads)
  - Orphan feeds (S3 keys with no consumers)
  - Blast radius (delete this Lambda → these N pages break)
  - Cross-Lambda dependencies (Lambda A reads what Lambda B writes)

Method:
  1. Lambda → outputs: scan Lambda descriptions + last-modified time of S3 keys
     in the feed catalog to associate writers with feeds.
  2. Page → reads:    scan each .html in the github repo (via raw.githubusercontent)
     for s3://.../<key> and proxy-URL patterns.
  3. Lambda → Lambda: feeds where one Lambda reads what another wrote.
  4. Assemble graph as nodes (Lambdas, feeds, pages) + edges (writes, reads).

Output:   data/dependency-graph.json
Schedule: daily-eve via the scheduler manifest.

USES jhcore.
"""
import json
import os
import re
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

from jhcore import s3io, notify

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
REPO_RAW = "https://raw.githubusercontent.com/ElMooro/si/main"

_lam = boto3.client("lambda", region_name=REGION)
_s3 = boto3.client("s3", region_name=REGION)


def list_justhodl_lambdas():
    fns = []
    paginator = _lam.get_paginator("list_functions")
    for page in paginator.paginate():
        for f in page.get("Functions", []):
            n = f["FunctionName"]
            if n.startswith("justhodl") or n.startswith("jhk"):
                fns.append({"name": n, "desc": (f.get("Description","") or "")[:500]})
    return fns


def get_root_html_files():
    """List .html files at the repo root via GitHub API."""
    api_url = "https://api.github.com/repos/ElMooro/si/contents/?ref=main"
    headers = {"User-Agent": "JustHodl-DepGraph/1.0"}
    tok = os.environ.get("GH_API_TOKEN")
    if tok:
        headers["Authorization"] = f"Bearer {tok}"
    try:
        req = urllib.request.Request(api_url, headers=headers)
        with urllib.request.urlopen(req, timeout=20) as r:
            data = json.loads(r.read())
        return [item["name"] for item in data
                if item.get("type") == "file" and item["name"].endswith(".html")]
    except Exception as e:
        print(f"[dep-graph] could not list root HTML: {e}")
        return []


def scan_page_reads(filename):
    """Fetch one HTML page, return the set of data/*.json keys it references."""
    url = f"{REPO_RAW}/{filename}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-DepGraph/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            body = r.read(2_000_000).decode("utf-8", errors="replace")
    except Exception:
        return {filename: []}
    # Patterns to match: "data/foo.json", "/data/foo.json", ${S3}foo.json
    found = set()
    for m in re.finditer(r"['\"]?data/([a-zA-Z0-9_\-./]+\.json)['\"]?", body):
        found.add(f"data/{m.group(1)}")
    for m in re.finditer(r"\$\{S3\}([a-zA-Z0-9_\-./]+\.json)", body):
        found.add(f"data/{m.group(1)}")
    # Proxy URL references
    for m in re.finditer(r"justhodl-data-proxy\.[^/]+/([a-zA-Z0-9_\-./]+\.json)", body):
        found.add(f"data/{m.group(1)}")
    return {filename: sorted(found)}


def scan_lambda_source_reads(name):
    """Fetch the Lambda's source from the repo, find S3 keys it reads.
    Repo layout: aws/lambdas/<name>/source/lambda_function.py
    """
    url = f"{REPO_RAW}/aws/lambdas/{name}/source/lambda_function.py"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-DepGraph/1.0"})
        with urllib.request.urlopen(req, timeout=12) as r:
            body = r.read(500_000).decode("utf-8", errors="replace")
    except Exception:
        return {name: {"reads": [], "writes": []}}
    reads = set(); writes = set()
    # Look for get_object/get_json/head_object patterns with data/* keys
    for m in re.finditer(r"['\"](data/[a-zA-Z0-9_\-./]+\.json)['\"]", body):
        # Heuristic: if preceded by 'put' or 's3io.put' or 'Body=', it's a write
        ctx = body[max(0, m.start()-60):m.start()]
        key = m.group(1)
        if re.search(r"put_object|put_json|put_text|s3\.put|put_item|save|write|upload", ctx, re.I):
            writes.add(key)
        else:
            reads.add(key)
    return {name: {"reads": sorted(reads), "writes": sorted(writes)}}


def lambda_handler(event=None, context=None):
    started = time.time()
    print("[dep-graph] starting")

    lambdas = list_justhodl_lambdas()
    print(f"[dep-graph] {len(lambdas)} Lambdas to analyze")

    # Cap parallel scanning to be polite to GitHub raw
    lambda_io = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = [ex.submit(scan_lambda_source_reads, fn["name"]) for fn in lambdas]
        for f in as_completed(futures):
            try: lambda_io.update(f.result())
            except Exception: pass

    pages = get_root_html_files()
    print(f"[dep-graph] {len(pages)} pages to scan")

    page_reads = {}
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = [ex.submit(scan_page_reads, p) for p in pages]
        for f in as_completed(futures):
            try: page_reads.update(f.result())
            except Exception: pass

    # Aggregate by feed
    feed_writers = {}    # key -> [lambdas]
    feed_readers_lambda = {}  # key -> [lambdas]
    feed_readers_page = {}    # key -> [pages]
    for fn, io_dict in lambda_io.items():
        for k in io_dict.get("writes", []):
            feed_writers.setdefault(k, []).append(fn)
        for k in io_dict.get("reads", []):
            feed_readers_lambda.setdefault(k, []).append(fn)
    for page, reads in page_reads.items():
        for k in reads:
            feed_readers_page.setdefault(k, []).append(page)

    all_feeds = set(feed_writers) | set(feed_readers_lambda) | set(feed_readers_page)

    # Compute insights
    orphan_lambdas = []     # write to nothing that's read
    blast_radius = {}        # lambda -> [pages affected if removed]
    for fn, io_dict in lambda_io.items():
        writes = io_dict.get("writes", [])
        if not writes:
            continue
        consumers = set()
        for k in writes:
            consumers |= set(feed_readers_page.get(k, []))
            consumers |= set(feed_readers_lambda.get(k, []))
        consumers.discard(fn)
        blast_radius[fn] = sorted(consumers)
        if not consumers:
            orphan_lambdas.append(fn)

    orphan_feeds = sorted([k for k in all_feeds
                           if k in feed_writers
                           and not feed_readers_page.get(k)
                           and not feed_readers_lambda.get(k)])

    feeds_no_writer = sorted([k for k in all_feeds if k not in feed_writers])

    # Top-N most-consumed feeds
    consumption = []
    for k in all_feeds:
        n = len(feed_readers_page.get(k, [])) + len(feed_readers_lambda.get(k, []))
        if n > 0:
            consumption.append({"feed": k, "n_consumers": n,
                                "pages": feed_readers_page.get(k, [])[:10],
                                "lambdas": feed_readers_lambda.get(k, [])[:10]})
    consumption.sort(key=lambda x: -x["n_consumers"])

    graph = {
        "version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - started, 2),
        "stats": {
            "lambdas_analyzed": len(lambdas),
            "lambdas_with_io": sum(1 for v in lambda_io.values() if v.get("reads") or v.get("writes")),
            "pages_analyzed": len(page_reads),
            "feeds_total": len(all_feeds),
            "orphan_lambdas": len(orphan_lambdas),
            "orphan_feeds": len(orphan_feeds),
        },
        "orphan_lambdas": sorted(orphan_lambdas),
        "orphan_feeds": orphan_feeds,
        "feeds_no_writer": feeds_no_writer[:50],
        "top_consumed_feeds": consumption[:30],
        "blast_radius": blast_radius,     # lambda -> [pages/lambdas affected]
        "lambda_io": lambda_io,
        "page_reads": page_reads,
    }

    s3io.put_json("data/dependency-graph.json", graph, cache_control="public, max-age=3600")

    duration = round(time.time() - started, 2)
    print(f"[dep-graph] OK — {len(all_feeds)} feeds, {len(orphan_lambdas)} orphans, {duration}s")

    if len(orphan_lambdas) > 30:
        notify.alert("INFO", "Dep Graph",
                     f"{len(orphan_lambdas)} orphan Lambdas (write feeds nothing reads). Check data/dependency-graph.json.")

    return {"statusCode": 200, "body": json.dumps({
        "feeds_total": len(all_feeds),
        "orphan_lambdas": len(orphan_lambdas),
        "orphan_feeds": len(orphan_feeds),
        "duration_s": duration,
    })}
