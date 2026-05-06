#!/usr/bin/env python3
"""
Step 189 — Pull ath.html content from S3 + commit it to repo.

The page exists on S3 (15998B, Feb 26) but bucket policy doesn't
allow public read on .html files — only on root JSONs and Phase
prefixes. The current GitHub Pages ath.html is just a 6-line
redirect to that S3 file (broken: mixed-content + 403).

Fix: pull the actual content via authenticated boto3 GetObject,
then have GitHub Actions auto-commit it as the new ath.html. From
that point on, GitHub Pages serves it directly — no S3 dependency,
no redirect, no mixed content.

If the content references HTTP S3 URLs internally, we patch those
to HTTPS REST endpoints in the same commit.
"""
import os
import re
from ops_report import report
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
KEY = "ath.html"
DEST = "ath.html"  # repo root

s3 = boto3.client("s3", region_name=REGION)


with report("pull_ath_from_s3") as r:
    r.heading("Pull ath.html from S3 → commit directly")

    # ─── A. Fetch ───────────────────────────────────────────────────────
    r.section("A. Fetch from S3")
    obj = s3.get_object(Bucket=BUCKET, Key=KEY)
    body = obj["Body"].read().decode("utf-8")
    r.log(f"  Pulled {len(body)}B  mod={obj['LastModified']}")

    # ─── B. Inspect for problematic URLs ───────────────────────────────
    r.section("B. Inspect for HTTP / dead-URL references")
    http_urls = re.findall(r'http://[^\s"\'<>]+', body)
    api_urls = re.findall(r'https?://[a-zA-Z0-9-]+\.execute-api\.[^\s"\'<>]+', body)
    lambda_urls = re.findall(r'https?://[a-zA-Z0-9]+\.lambda-url\.[^\s"\'<>]+', body)
    s3_website_urls = re.findall(r's3-website[^"\'<>]*', body)

    r.log(f"  HTTP (non-HTTPS) URLs: {len(http_urls)}")
    for u in http_urls[:10]: r.log(f"    {u[:120]}")
    r.log(f"\n  API Gateway URLs: {len(api_urls)}")
    for u in api_urls[:5]: r.log(f"    {u[:120]}")
    r.log(f"\n  Lambda Function URLs: {len(lambda_urls)}")
    for u in lambda_urls[:5]: r.log(f"    {u[:120]}")
    r.log(f"\n  S3 website endpoint refs: {len(s3_website_urls)}")
    for u in s3_website_urls[:5]: r.log(f"    {u[:120]}")

    # ─── C. Patch HTTP S3 URLs → HTTPS REST endpoint ────────────────────
    r.section("C. Patch problematic URLs")
    patched = body

    # http://<bucket>.s3-website-us-east-1.amazonaws.com/<path>
    # → https://<bucket>.s3.amazonaws.com/<path>
    pattern_http_s3 = r'http://([a-zA-Z0-9.-]+)\.s3-website-us-east-1\.amazonaws\.com'
    new_pattern = r'https://\1.s3.amazonaws.com'
    n_subs = len(re.findall(pattern_http_s3, patched))
    patched = re.sub(pattern_http_s3, new_pattern, patched)
    r.log(f"  Patched {n_subs} HTTP S3 website → HTTPS REST endpoints")

    # ─── D. Write ───────────────────────────────────────────────────────
    r.section("D. Write to repo")
    repo_root = os.environ.get("GITHUB_WORKSPACE", "/home/claude/si")
    dest_path = os.path.join(repo_root, DEST)
    with open(dest_path, "w") as f:
        f.write(patched)
    r.log(f"  Wrote {len(patched)}B to {dest_path}")
    r.log(f"  GitHub Actions will auto-commit on success.")
    r.log(f"  GitHub Pages will serve the new ath.html directly from /ath.html")

    r.kv(
        original_size=len(body),
        patched_size=len(patched),
        http_subs=n_subs,
        n_http_urls=len(http_urls),
        n_api_urls=len(api_urls),
        n_lambda_urls=len(lambda_urls),
    )
    r.log("Done")
