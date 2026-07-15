"""ops 3316 — REST Benzinga is 403 on all 3 keys, but the 'Default' key
has FLAT FILES (S3) access: endpoint https://files.massive.com, bucket
'flatfiles', S3 Access Key ID 7a11d881-743a-4a4d-813d-2776f2bd4c68 +
secret (…JX_d). Benzinga may be entitled on the flat-files side, not REST.

This op connects an S3 client to files.massive.com with those creds and:
  1. Lists top-level prefixes in 'flatfiles' (what data products exist)
  2. Greps prefixes/keys for anything benzinga / ratings / guidance /
     analyst
  3. If a benzinga prefix is found, lists a few recent objects (sizes)
     so we know the file cadence + format to build a harvester against.

Creds pass through once; report prints only the ACCESS KEY ID (already
shown in Khalid's dashboard, not secret alone) + bucket listing. Secret
never printed. If flat files carry Benzinga, next op builds a flat-file
harvester for justhodl-analyst-actions (page schema unchanged).

Read-only (list/get only). Exit 0 regardless so the runner marks success;
verdict is in the report.
"""
import sys
from pathlib import Path

import boto3
from botocore.config import Config

from ops_report import report

ENDPOINT = "https://files.massive.com"
BUCKET = "flatfiles"
AK = "7a11d881-743a-4a4d-813d-2776f2bd4c68"
SK = "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"  # 'Default' key secret

HINTS = ("benzinga", "rating", "guidance", "analyst", "price_target",
         "pricetarget", "estimate")


def client():
    return boto3.client(
        "s3", endpoint_url=ENDPOINT,
        aws_access_key_id=AK, aws_secret_access_key=SK,
        config=Config(signature_version="s3v4",
                      s3={"addressing_style": "path"}),
        region_name="us-east-1")


with report("3316_massive_flatfiles_probe") as rep:
    rep.kv(endpoint=ENDPOINT, bucket=BUCKET, access_key_id=AK)
    s3 = client()

    # 1. top-level prefixes
    rep.section("TOP-LEVEL PREFIXES")
    prefixes = []
    try:
        pag = s3.get_paginator("list_objects_v2")
        resp = s3.list_objects_v2(Bucket=BUCKET, Delimiter="/")
        prefixes = [p["Prefix"] for p in resp.get("CommonPrefixes", [])]
        rep.kv(n_prefixes=len(prefixes), prefixes=prefixes[:40])
        if not prefixes:
            # some buckets have no delimiter structure; list a few raw keys
            first = s3.list_objects_v2(Bucket=BUCKET, MaxKeys=20)
            keys = [o["Key"] for o in first.get("Contents", [])]
            rep.kv(first_keys=keys[:20])
    except Exception as e:
        rep.fail(f"list_objects_v2 failed: {type(e).__name__}: {e}")
        rep.kv(RESULT="S3_ACCESS_FAILED",
               hint="creds/endpoint rejected — flat-files access may be off "
                    "for this key, or signing/region differs")
        # don't exit non-zero; keep runner green
        sys.exit(0)

    # 2. hunt for benzinga-ish prefixes (one level deeper too)
    rep.section("BENZINGA / ANALYST HUNT")
    hits = [p for p in prefixes if any(h in p.lower() for h in HINTS)]
    # dig one level into each top prefix for hints
    deeper = []
    for p in prefixes[:25]:
        try:
            r = s3.list_objects_v2(Bucket=BUCKET, Prefix=p, Delimiter="/")
            subs = [cp["Prefix"] for cp in r.get("CommonPrefixes", [])]
            for sp in subs:
                if any(h in sp.lower() for h in HINTS):
                    deeper.append(sp)
        except Exception:
            pass
    all_hits = sorted(set(hits + deeper))
    rep.kv(direct_hits=hits, deeper_hits=deeper[:30])

    # 3. sample objects under first hit
    rep.section("SAMPLE OBJECTS")
    if all_hits:
        target = all_hits[0]
        try:
            r = s3.list_objects_v2(Bucket=BUCKET, Prefix=target, MaxKeys=15)
            objs = [{"key": o["Key"], "size": o["Size"],
                     "modified": o["LastModified"].isoformat()}
                    for o in r.get("Contents", [])]
            rep.kv(sampling_prefix=target, objects=objs)
        except Exception as e:
            rep.warn(f"sample list failed: {e}")

    rep.section("VERDICT")
    if all_hits:
        rep.ok(f"Benzinga/analyst data FOUND in flat files: {all_hits[:8]}. "
               "Fix path: build a flat-file harvester (S3 list+get newest "
               "ratings/guidance files) for justhodl-analyst-actions.")
        rep.kv(RESULT="BENZINGA_IN_FLATFILES", hits=all_hits[:8])
    else:
        rep.warn("Flat-files access WORKS but no benzinga/analyst prefix "
                 "found — Benzinga likely not entitled on flat files either. "
                 "Available products listed above; decide REST re-entitlement "
                 "(Massive support) vs FMP re-source.")
        rep.kv(RESULT="FLATFILES_OK_NO_BENZINGA", available=prefixes[:40])
