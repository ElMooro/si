"""Add-only internals publication from S3 warehouse observations, runner only."""
from datetime import date, datetime, timezone
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws/ops"))
sys.path.insert(0, str(ROOT / "scripts"))
from ops_report import report
from compile_jh_internals import FRED_LEGS, compute

BUCKET = "justhodl-dashboard-live"
OUTPUT = "data/jh-internals.json"
UNITS = {"DGS10": "Percent", "DGS2": "Percent", "WALCL": "Millions of USD",
         "WTREGEN": "Millions of USD", "RRPONTSYD": "Billions of USD", "NFCI": "Index"}


def number(value):
    if isinstance(value, bool):
        return None
    try:
        n = float(value)
        return n if math.isfinite(n) else None
    except (TypeError, ValueError):
        return None


def last_observation(doc):
    rows = doc if isinstance(doc, list) else doc.get("observations", [])
    valid = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        value = number(row.get("value"))
        try:
            day = date.fromisoformat(str(row.get("date")))
        except ValueError:
            continue
        if value is not None and day <= datetime.now(timezone.utc).date():
            valid.append((day.isoformat(), value))
    return max(valid, key=lambda x: x[0]) if valid else None


def breadth(doc):
    """Count the entire declared universe, without ranked lists or truncation."""
    rows = doc.get("by_ticker")
    n = number(doc.get("n_tickers"))
    if not isinstance(rows, dict) or n is None or n != len(rows) or n <= 200:
        return None, {"reason": "complete declared by_ticker universe unavailable"}
    if doc.get("capped") or doc.get("truncated") or doc.get("stale") or doc.get("data_unavailable"):
        return None, {"reason": "universe explicitly capped, truncated, stale or unavailable"}
    changes = [number(row.get("change_pct")) if isinstance(row, dict) else None for row in rows.values()]
    missing = sum(x is None for x in changes)
    counts = {"n_univ": int(n), "n_with_change": len(changes)-missing, "n_missing_change": missing,
              "basis": "all by_ticker rows; n_tickers must equal full row count; no slicing"}
    if missing:
        return None, {**counts, "reason": "up/down cannot cover the full declared universe"}
    up = sum(x > 0 for x in changes)
    down = sum(x < 0 for x in changes)
    counts.update(n_up=up, n_down=down, n_unchanged=int(n)-up-down, breadth_basis="uncapped")
    if up == down == 100:
        return None, {**counts, "reason": "100/100 is forbidden"}
    return {"n_up": up, "n_down": down, "n_univ": int(n)}, counts


def main():
    if os.environ.get("GITHUB_ACTIONS") != "true":
        sys.exit(1)
    import boto3
    from botocore.exceptions import ClientError

    proof = {"op": 5501, "started_at": datetime.now(timezone.utc).isoformat(),
             "source_mode": "S3 warehouse only", "objects": {}, "fred": {}, "mutated_keys": []}
    receipt = ROOT / "aws/ops/reports/5501_jh_internals_receipt.json"
    with report("ops_5501_compile_jh_internals") as r:
        try:
            s3 = boto3.client("s3", region_name="us-east-1")

            def read(key, optional=False):
                try:
                    obj = s3.get_object(Bucket=BUCKET, Key=key)
                except ClientError as exc:
                    if optional and exc.response["Error"]["Code"] in ("NoSuchKey", "404"):
                        return None, None
                    raise
                raw = obj["Body"].read()
                doc = json.loads(gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw)
                proof["objects"][key] = {"last_modified": obj["LastModified"].isoformat(),
                                          "bytes": obj["ContentLength"], "etag": obj["ETag"]}
                return doc, obj

            # A shallow prefix inventory locates banked scoped series without
            # traversing or consuming the FRED import queue.
            prefixes = []
            for page in s3.get_paginator("list_objects_v2").paginate(
                    Bucket=BUCKET, Prefix="data/warm/fred-scoped/", Delimiter="/"):
                prefixes.extend(p["Prefix"] for p in page.get("CommonPrefixes", []))
            cache, cache_obj = read("data/fred-cache.json", optional=True)
            legs = {}
            for leg, sid in FRED_LEGS.items():
                candidates = []
                if isinstance(cache, dict) and sid in cache:
                    last = last_observation(cache[sid])
                    if last:
                        candidates.append((last, "data/fred-cache.json", cache_obj))
                for prefix in prefixes:
                    key = prefix + sid + ".json"
                    doc, obj = read(key, optional=True)
                    if doc is None:
                        continue
                    identity = (doc.get("meta") or {}).get("id") or doc.get("series_id")
                    if identity and identity != sid:
                        raise RuntimeError("Warehouse series identity mismatch: " + sid)
                    last = last_observation(doc)
                    if last:
                        candidates.append((last, key, obj))
                if not candidates:
                    raise RuntimeError("No banked finite dated observation for " + sid)
                last, key, obj = max(candidates, key=lambda c: (c[0][0], c[2]["LastModified"]))
                # The compiler subtracts three USD-million legs, then divides
                # by 1000. RRPONTSYD's native FRED unit is USD billions.
                scale = 1000.0 if sid == "RRPONTSYD" else 1.0
                legs[leg] = last[1] * scale
                proof["fred"][sid] = {"key": key, "as_of": last[0], "value": last[1],
                    "units": UNITS[sid], "compiler_multiplier": scale, "compiler_value": legs[leg],
                    "last_modified": obj["LastModified"].isoformat(),
                    "selection": "latest valid observation date, then newest warehouse object",
                    "candidates": [{"key": k, "as_of": x[0], "value": x[1],
                                    "last_modified": o["LastModified"].isoformat()} for x,k,o in candidates]}
                r.ok("%s last obs %s = %s %s from %s" % (sid, last[0], last[1], UNITS[sid], key))

            universe, _ = read("data/finviz-universe.json", optional=True)
            extra, detail = breadth(universe) if isinstance(universe, dict) else (None, {"reason": "warehouse universe key missing"})
            proof["breadth"] = {**detail, "key": "data/finviz-universe.json",
                                "as_of": universe.get("generated_at") if isinstance(universe, dict) else None}
            if extra:
                legs.update(extra)
                r.ok("uncapped breadth " + json.dumps(detail))
            else:
                r.log("ad_breadth omitted: " + json.dumps(detail))

            compiled = compute(legs)
            required = {"twos_tens", "liq_proxy_bn", "nfci"}
            if not required.issubset(compiled["fields"]) or any(number(v) is None for v in compiled["fields"].values()):
                raise RuntimeError("Compiler output lacks required finite fields")
            if extra and compiled["fields"].get("ad_breadth") != round((extra["n_up"]-extra["n_down"])/extra["n_univ"], 4):
                raise RuntimeError("Uncapped breadth formula mismatch")
            previous, previous_obj = read(OUTPUT, optional=True)
            if previous is not None:
                if previous.get("schema_version") != compiled["schema_version"] or not isinstance(previous.get("fields"), dict):
                    raise RuntimeError("Existing internals schema differs; refusing replacement")
                compiled = {**previous, **compiled, "fields": {**previous["fields"], **compiled["fields"]}}
                proof["preserved_existing_fields"] = sorted(set(previous["fields"])-required-set(["ad_breadth"] if extra else []))
            body = (json.dumps(compiled, indent=2, allow_nan=False)+"\n").encode()
            proof["compiler_sha256"] = hashlib.sha256((ROOT / "scripts/compile_jh_internals.py").read_bytes()).hexdigest()
            proof["payload"] = compiled
            proof["body_sha256"] = hashlib.sha256(body).hexdigest()
            condition = {"IfMatch": previous_obj["ETag"]} if previous_obj else {"IfNoneMatch": "*"}
            s3.put_object(Bucket=BUCKET, Key=OUTPUT, Body=body, ContentType="application/json",
                          CacheControl="public, max-age=60", **condition)
            proof["mutated_keys"].append(OUTPUT)
            live, obj = read(OUTPUT)
            if live != compiled:
                raise RuntimeError("Live internals readback differs from compiler output")
            proof["live"] = {"key": OUTPUT, "last_modified": obj["LastModified"].isoformat(),
                              "fields_present": sorted(live["fields"]), "fields": live["fields"],
                              "schema_version": live["schema_version"], "source": live["source"]}
            proof["status"] = "PASS"
            r.ok("LastModified=%s fields=%s" % (proof["live"]["last_modified"], json.dumps(live["fields"])))
        except Exception as exc:
            proof["status"] = "FAIL"
            proof["error"] = {"type": type(exc).__name__, "message": str(exc)[:300]}
            r.fail(json.dumps(proof["error"]))
            receipt.write_text(json.dumps(proof, indent=2, default=str)+"\n")
            sys.exit(1)
        proof["finished_at"] = datetime.now(timezone.utc).isoformat()
        receipt.write_text(json.dumps(proof, indent=2, default=str)+"\n")


if __name__ == "__main__":
    main()
