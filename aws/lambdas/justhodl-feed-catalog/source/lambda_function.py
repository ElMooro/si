"""
justhodl-feed-catalog — Arch #5: Feed Catalog + JSON Schemas
=============================================================
Daily-generated authoritative inventory of every data feed on the platform.

For each key under s3://justhodl-dashboard-live/data/:
  - last_modified, size_bytes
  - inferred JSON Schema (best-effort, top 2 levels)
  - writer Lambdas (from a manifest + heuristic source-scan)
  - consuming pages (set in a separate page-scan stage by ops)
  - refresh cadence (from EventBridge tick membership)

Writes to: data/feed-catalog.json (consumed by future tooling + UI).

USES jhcore LAYER.
"""
import json
import os
import re
import time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from botocore.exceptions import ClientError

from jhcore import s3io, notify

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
# ops 3886: was DATA_PREFIX = "data/" alone, which structurally excluded every
# feed under etf-flows/, screener/, sentiment/, macro/ (e.g. this whole arc's
# own etf-flows/daily.json + constituent-pressure.json never appeared here).
# Same prefix set now used in scripts/gen_engine_manifest.py, kept consistent.
DATA_PREFIXES = ("data/", "etf-flows/", "screener/", "sentiment/", "macro/", "config/")

_s3 = boto3.client("s3", region_name=REGION)
_lam = boto3.client("lambda", region_name=REGION)


def list_data_keys():
    """All JSON-like keys under every known top-level feed prefix."""
    keys = []
    seen = set()
    for prefix in DATA_PREFIXES:
        paginator = _s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
            for o in page.get("Contents", []) or []:
                k = o["Key"]
                if k.endswith("/") or k.endswith(".tmp") or k in seen:
                    continue
                seen.add(k)
                keys.append({
                    "key": k,
                    "size": o["Size"],
                    "last_modified": o["LastModified"].isoformat(),
                })
    return keys


def infer_schema(obj, depth=0, max_depth=2):
    """Best-effort JSON Schema (subset) for an object.
    Returns dict like {"type": "object", "properties": {...}} or {"type": "array", "items": {...}}.
    """
    if depth > max_depth:
        return {"type": "?"}
    if obj is None:
        return {"type": "null"}
    if isinstance(obj, bool):
        return {"type": "boolean"}
    if isinstance(obj, int):
        return {"type": "integer"}
    if isinstance(obj, float):
        return {"type": "number"}
    if isinstance(obj, str):
        return {"type": "string"}
    if isinstance(obj, list):
        if not obj:
            return {"type": "array", "items": {}}
        # Sample first item only
        return {"type": "array", "items": infer_schema(obj[0], depth + 1, max_depth)}
    if isinstance(obj, dict):
        props = {}
        for k in list(obj.keys())[:30]:  # cap to keep schema tractable
            props[k] = infer_schema(obj[k], depth + 1, max_depth)
        return {"type": "object", "properties": props}
    return {"type": "?"}


def fetch_schema_for(key):
    """Pull the object (head-only-sized capped), parse JSON, infer schema."""
    try:
        r = _s3.get_object(Bucket=BUCKET, Key=key, Range="bytes=0-262143")  # 256KB cap
        body = r["Body"].read()
        try:
            obj = json.loads(body)
        except Exception:
            # Maybe truncated — try to find last complete }
            return {"_inferable": False, "_reason": "non-JSON or truncated"}
        return infer_schema(obj)
    except ClientError:
        return {"_inferable": False, "_reason": "fetch failed"}
    except Exception as e:
        return {"_inferable": False, "_reason": str(e)[:120]}


def writer_map_from_lambdas():
    """Scan Lambda env vars / descriptions for hints about which S3 keys they write.
    Best-effort: looks for justhodl-* and jhk-* Lambdas, builds a map of name -> [keys mentioned].
    Weaker fallback source — see writer_map_from_engine_manifest() for the primary one."""
    writers = {}  # key -> [lambda names]
    paginator = _lam.get_paginator("list_functions")
    for page in paginator.paginate():
        for fn in page.get("Functions", []) or []:
            name = fn["FunctionName"]
            if not (name.startswith("justhodl") or name.startswith("jhk")):
                continue
            desc = fn.get("Description", "") or ""
            mentioned = re.findall(
                r"(?:data|etf-flows|screener|sentiment|macro|config)/[a-zA-Z0-9_\-./]+\.json", desc)
            for m in mentioned:
                writers.setdefault(m, []).append(name)
    return writers


def writer_map_from_engine_manifest():
    """Primary writer-attribution source (ops 3886): engine-manifest.json is
    regenerated fresh on every ops run via a windowed put_object(/.put_json(
    source scan (79.1% fleet coverage, up from 50.3%) — a real static-analysis
    result, not a hopeful regex over a Lambda's free-text Description field.
    Reads the S3 copy (always current) rather than a repo checkout."""
    writers = {}
    try:
        manifest = s3io.get_json("data/engine-manifest.json", default=None)
        if not manifest:
            return writers
        for e in manifest.get("engines", []):
            fn_name = e.get("engine")
            for k in (e.get("keys") or []):
                writers.setdefault(k, []).append(fn_name)
    except Exception as ex:
        print(f"[feed-catalog] engine-manifest writer source unavailable: {str(ex)[:120]}")
    return writers


def schedule_for_lambda(scheduler_manifest):
    """Build {lambda_name: cadence} from the schedule manifest."""
    out = {}
    for tick, fns in (scheduler_manifest.get("ticks") or {}).items():
        for fn in fns:
            out[fn] = tick
    return out


def lambda_handler(event=None, context=None):
    started = time.time()
    print("[feed-catalog] starting")

    keys = list_data_keys()
    print(f"[feed-catalog] {len(keys)} keys across {len(DATA_PREFIXES)} prefixes")

    def _strip_prefix(key):
        for p in DATA_PREFIXES:
            if key.startswith(p):
                return key[len(p):]
        return key

    # Filter for top-level data feeds (skip noisy subdirs like interpretations/_summary etc)
    feeds = []
    for k in keys:
        rel = _strip_prefix(k["key"])
        if "/" in rel:
            # one-level-nested ok (e.g. interpretations/yield-curve.json)
            if rel.count("/") > 1:
                continue
        if not (rel.endswith(".json") or rel.endswith(".geojson")):
            continue
        feeds.append(k)

    # ops 3886: schema sampling was capped at feeds[:300] with NO priority —
    # S3 list_objects_v2 returns lexicographic order, so only feeds starting
    # with an early letter ever got sampled. That's exactly why
    # data/rebalance-radar.json ('r') and data/earnings-tracker.json ('e')
    # both showed "_reason": "not sampled" investigating the semi flow/price
    # divergence. Fix: sample by RECENCY (most-recently-touched first — a
    # real priority signal, not an alphabet accident), raise the cap, and
    # widen the worker pool. Config timeout/memory bumped to match.
    SAMPLE_CAP = 4000
    feeds_by_recency = sorted(feeds, key=lambda f: f["last_modified"], reverse=True)
    schemas = {}
    with ThreadPoolExecutor(max_workers=24) as ex:
        future_map = {ex.submit(fetch_schema_for, f["key"]): f["key"]
                      for f in feeds_by_recency[:SAMPLE_CAP]}
        for fut in as_completed(future_map):
            k = future_map[fut]
            try:
                schemas[k] = fut.result()
            except Exception as e:
                schemas[k] = {"_inferable": False, "_reason": str(e)[:120]}
    print(f"[feed-catalog] sampled {len(schemas)}/{len(feeds)} feeds "
          f"(cap={SAMPLE_CAP}, by-recency)")

    # Writer hints — engine-manifest.json (real source-scan, 79.1% coverage)
    # is primary; the weaker description-text heuristic fills any remaining gaps.
    writers_primary = writer_map_from_engine_manifest()
    writers_fallback = writer_map_from_lambdas()
    writers = {k: v for k, v in writers_primary.items()}
    for k, v in writers_fallback.items():
        for name in v:
            if name not in writers.get(k, []):
                writers.setdefault(k, []).append(name)
    print(f"[feed-catalog] writers: {len(writers_primary)} keys from engine-manifest, "
          f"{len(writers_fallback)} keys from description-scan, {len(writers)} keys total")

    # Cadence map (read schedule-manifest.json)
    manifest = s3io.get_json("config/schedule-manifest.json", default={})
    fn_cadence = schedule_for_lambda(manifest)

    # Assemble catalog
    catalog_entries = []
    for f in feeds:
        k = f["key"]
        rel = _strip_prefix(k)
        writers_for_key = writers.get(k, [])
        # Best cadence: most-specific writer cadence
        cadences = sorted(set(fn_cadence.get(fn) for fn in writers_for_key if fn_cadence.get(fn)))
        catalog_entries.append({
            "key": k,
            "name": rel,
            "size_bytes": f["size"],
            "size_kb": round(f["size"] / 1024, 1),
            "last_modified": f["last_modified"],
            "schema": schemas.get(k, {"_inferable": False, "_reason": "not sampled"}),
            "writers": writers_for_key,
            "cadences": cadences,
            "stale_days": _stale_days(f["last_modified"]),
        })

    # Sort: newest first
    catalog_entries.sort(key=lambda e: e["last_modified"], reverse=True)

    catalog = {
        "version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "bucket": BUCKET,
        "total_feeds": len(catalog_entries),
        "feeds": catalog_entries,
        "summary": {
            "with_writers": sum(1 for e in catalog_entries if e["writers"]),
            "with_cadence": sum(1 for e in catalog_entries if e["cadences"]),
            "schemas_inferred": sum(1 for e in catalog_entries if e["schema"].get("type")),
            "stale_gt_7d": sum(1 for e in catalog_entries if (e.get("stale_days") or 0) > 7),
        },
    }

    s3io.put_json("data/feed-catalog.json", catalog, cache_control="public, max-age=3600")

    duration = round(time.time() - started, 2)
    print(f"[feed-catalog] OK — {len(catalog_entries)} feeds, {duration}s")

    # Telegram alert if many stale feeds
    if catalog["summary"]["stale_gt_7d"] > 10:
        notify.alert("WARN", "Feed Catalog",
                     f"{catalog['summary']['stale_gt_7d']} feeds stale >7d (of {len(catalog_entries)}).")

    return {"statusCode": 200, "body": json.dumps({
        "feeds": len(catalog_entries),
        "duration_s": duration,
        **catalog["summary"],
    })}


def _stale_days(iso):
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return round((datetime.now(timezone.utc) - dt).total_seconds() / 86400, 1)
    except Exception:
        return None
