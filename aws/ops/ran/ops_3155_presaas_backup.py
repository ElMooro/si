"""ops 3155 — PRE-SAAS BACKUP (Khalid's order: snapshot before multi-user work).

Git branch+tag `pre-saas-2026-07-12` already pushed (site + all engine
sources + worker + ops history — instant rollback).

THIS OP captures the state git does NOT hold:
  1. DynamoDB on-demand backups for every justhodl-* table (signals,
     api-keys, api-rate, llm-cost, …) — point-in-time restorable.
  2. Full Lambda fleet export (config + ENV VARS — the secrets that
     live only on AWS) → NEW private bucket justhodl-backups-857687956942
     (PublicAccessBlock ALL + SSE), key backups/2026-07-12/.
  3. Enable S3 versioning on justhodl-dashboard-live (additive: every
     data/*.json + page overwrite becomes recoverable).
  4. SSM /justhodl inventory — NAMES ONLY (no values) — flagging
     supabase / stripe / service-key hits for the SaaS wiring.
"""

import gzip
import json
import sys
from datetime import datetime, timezone

import boto3

from ops_report import report

REGION = "us-east-1"
LIVE_BUCKET = "justhodl-dashboard-live"
BK_BUCKET = "justhodl-backups-857687956942"
STAMP = "2026-07-12"

S3 = boto3.client("s3", region_name=REGION)
DDB = boto3.client("dynamodb", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
SSM = boto3.client("ssm", region_name=REGION)

with report("3155_presaas_backup") as rep:
    fails, warns = [], []
    rep.heading("ops 3155 — pre-SaaS backup")

    rep.section("1. Private backup bucket")
    try:
        S3.head_bucket(Bucket=BK_BUCKET)
        rep.log("bucket exists")
    except Exception:
        S3.create_bucket(Bucket=BK_BUCKET)
        rep.ok("bucket created")
    S3.put_public_access_block(
        Bucket=BK_BUCKET,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True, "IgnorePublicAcls": True,
            "BlockPublicPolicy": True, "RestrictPublicBuckets": True})
    S3.put_bucket_encryption(
        Bucket=BK_BUCKET,
        ServerSideEncryptionConfiguration={"Rules": [
            {"ApplyServerSideEncryptionByDefault":
             {"SSEAlgorithm": "AES256"}}]})
    rep.ok("public access blocked + SSE-AES256 enforced")

    rep.section("2. DynamoDB on-demand backups")
    tables = [t for t in DDB.list_tables()["TableNames"]
              if t.startswith("justhodl")]
    n_bk = 0
    for t in tables:
        try:
            DDB.create_backup(TableName=t,
                              BackupName=f"{t}-presaas-{STAMP}")
            n_bk += 1
        except Exception as e:
            warns.append(f"ddb {t}: {str(e)[:80]}")
    rep.kv(ddb_tables=len(tables), ddb_backups=n_bk)
    rep.log("tables: " + ", ".join(tables))
    if n_bk < len(tables):
        rep.warn(f"{len(tables) - n_bk} table backups failed (see warns)")
    else:
        rep.ok(f"all {n_bk} tables backed up")

    rep.section("3. Lambda fleet export (config + env)")
    fleet, marker = [], None
    while True:
        kw = {"Marker": marker} if marker else {}
        page = LAM.list_functions(MaxItems=50, **kw)
        for f in page.get("Functions", []):
            if not f["FunctionName"].startswith("justhodl"):
                continue
            fleet.append({
                "name": f["FunctionName"],
                "runtime": f.get("Runtime"),
                "memory": f.get("MemorySize"),
                "timeout": f.get("Timeout"),
                "role": f.get("Role"),
                "handler": f.get("Handler"),
                "env": (f.get("Environment") or {}).get("Variables") or {},
                "last_modified": f.get("LastModified"),
            })
        marker = page.get("NextMarker")
        if not marker:
            break
    body = gzip.compress(json.dumps(
        {"exported_at": datetime.now(timezone.utc).isoformat(),
         "n": len(fleet), "functions": fleet}).encode())
    key = f"backups/{STAMP}/lambda-fleet-config.json.gz"
    S3.put_object(Bucket=BK_BUCKET, Key=key, Body=body,
                  ServerSideEncryption="AES256")
    rep.kv(lambdas_exported=len(fleet),
           export_bytes=len(body), export_key=key)
    if len(fleet) < 500:
        warns.append(f"fleet export {len(fleet)} < 500 — pagination check")
    else:
        rep.ok(f"{len(fleet)} function configs + env vars secured")

    rep.section("4. Versioning on live bucket")
    v = S3.get_bucket_versioning(Bucket=LIVE_BUCKET).get("Status")
    if v != "Enabled":
        S3.put_bucket_versioning(
            Bucket=LIVE_BUCKET,
            VersioningConfiguration={"Status": "Enabled"})
        rep.ok(f"versioning {v or 'Off'} → Enabled (every overwrite "
               "now recoverable)")
    else:
        rep.ok("versioning already Enabled")

    rep.section("5. SSM /justhodl inventory (names only)")
    names, nt = [], None
    while True:
        kw = {"NextToken": nt} if nt else {}
        pg = SSM.describe_parameters(
            ParameterFilters=[{"Key": "Name", "Option": "BeginsWith",
                               "Values": ["/justhodl"]}],
            MaxResults=50, **kw)
        names += [p["Name"] for p in pg.get("Parameters", [])]
        nt = pg.get("NextToken")
        if not nt:
            break
    rep.kv(ssm_params=len(names))
    hits = [n for n in names if any(k in n.lower() for k in
            ("supabase", "stripe", "service", "jwt"))]
    rep.log("all: " + ", ".join(sorted(names))[:900])
    if hits:
        rep.ok("SaaS-relevant params found: " + ", ".join(hits))
    else:
        rep.warn("no supabase/stripe params under /justhodl — service "
                 "keys live elsewhere (checkout lambda env? — fleet "
                 "export above now holds every env var)")

    for w in warns[:6]:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
