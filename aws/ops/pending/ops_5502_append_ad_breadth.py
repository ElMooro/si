"""Merge warehouse breadth into live internals; preserve all unrelated fields."""
from datetime import date, datetime, timedelta, timezone
import copy
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws/ops"))
from ops_report import report

BUCKET = "justhodl-dashboard-live"
KEY = "data/jh-internals.json"
POLYGON_PREFIX = "data/warm/us-equities-daily/"
OWNED_FIELDS = {"ad_breadth", "n_up", "n_down", "n_univ", "n_missing"}


def price(value):
    if isinstance(value, bool):
        return None
    try:
        n = float(value)
        return n if math.isfinite(n) and n > 0 else None
    except (TypeError, ValueError):
        return None


def count_pairs(pairs):
    up = down = unchanged = missing = 0
    for current, previous in pairs:
        current, previous = price(current), price(previous)
        if current is None or previous is None:
            missing += 1
        elif current > previous:
            up += 1
        elif current < previous:
            down += 1
        else:
            unchanged += 1
    n = up + down + unchanged
    total = n + missing
    return {"n_up": up, "n_down": down, "n_unchanged": unchanged,
            "n_univ": n, "n_missing": missing, "n_total": total,
            "coverage": n / total if total else 0,
            "eligible": bool(n and n * 5 >= total * 4),
            "denominator": "rows with both finite positive prices; includes unchanged rows",
            "breadth_basis": "uncapped; all source rows; no price, volume or ranking filter"}


def finviz_counts(doc):
    rows = doc.get("by_ticker")
    if not isinstance(rows, dict) or doc.get("n_tickers") != len(rows) or not rows:
        return {"eligible": False, "reason": "full declared Finviz universe unavailable"}
    if any(doc.get(k) for k in ("capped", "truncated", "data_unavailable", "stale")):
        return {"eligible": False, "reason": "Finviz source flagged capped, truncated, stale or unavailable"}
    pairs = [(r.get("price"), r.get("prev_close")) if isinstance(r, dict) else (None, None) for r in rows.values()]
    return {**count_pairs(pairs), "current_price_field": "by_ticker.*.price",
            "previous_price_field": "by_ticker.*.prev_close", "as_of": doc.get("generated_at")}


def polygon_counts(current, previous):
    def indexed(doc):
        rows = doc.get("results")
        if not isinstance(rows, list) or doc.get("n_tickers") != len(rows) or not rows:
            raise ValueError("incomplete Polygon grouped-daily universe")
        if any(not isinstance(r, dict) or not r.get("T") for r in rows):
            raise ValueError("Polygon row lacks ticker identity")
        mapped = {r["T"]: r for r in rows}
        if len(mapped) != len(rows):
            raise ValueError("duplicate Polygon ticker rows")
        return mapped
    now_rows, prev_rows = indexed(current), indexed(previous)
    pairs = [(row.get("c"), prev_rows.get(ticker, {}).get("c")) for ticker, row in now_rows.items()]
    return {**count_pairs(pairs), "current_price_field": "results[T].c",
            "previous_price_field": "previous session results[T].c", "as_of": current["date"],
            "previous_session": previous["date"]}


def main():
    if os.environ.get("GITHUB_ACTIONS") != "true":
        sys.exit(1)
    import boto3
    from botocore.exceptions import ClientError

    proof = {"op": 5502, "started_at": datetime.now(timezone.utc).isoformat(),
             "source_mode": "warehouse only", "objects": {}, "candidates": [], "mutated_keys": []}
    receipt = ROOT / "aws/ops/reports/5502_ad_breadth_receipt.json"
    with report("ops_5502_append_ad_breadth") as r:
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

            before, before_obj = read(KEY)
            if before.get("schema_version") != 1 or not isinstance(before.get("fields"), dict):
                raise RuntimeError("Live internals schema must be version 1")
            if not {"twos_tens", "liq_proxy_bn", "nfci"}.issubset(before["fields"]):
                raise RuntimeError("Existing core fields must be present before append")
            proof["before"] = {"last_modified": before_obj["LastModified"].isoformat(), "fields": before["fields"]}

            fv_key = "data/finviz-universe.json"
            fv, _ = read(fv_key, optional=True)
            candidate = {"source": "finviz-universe", "keys": [fv_key],
                **(finviz_counts(fv) if isinstance(fv, dict) else {"eligible": False, "reason": "Finviz warehouse key missing"})}
            proof["candidates"].append(candidate)
            chosen = candidate if candidate["eligible"] else None

            if chosen is None:
                # Only complete daily warehouse blobs count; summary samples
                # and the filtered market-internals output are not universes.
                banked = {}
                for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=POLYGON_PREFIX):
                    for obj in page.get("Contents", []):
                        match = re.fullmatch(re.escape(POLYGON_PREFIX)+r"(\d{4}-\d{2}-\d{2})\.json\.gz", obj["Key"])
                        if match and match[1] <= datetime.now(timezone.utc).date().isoformat():
                            banked[match[1]] = obj["Key"]
                pc = {"source": "polygon-grouped-daily", "eligible": False, "keys": []}
                if banked:
                    session = max(banked)
                    previous = date.fromisoformat(session) - timedelta(days=1)
                    while previous.weekday() >= 5:
                        previous -= timedelta(days=1)
                    prev_session = previous.isoformat()
                    if prev_session in banked:
                        pc["keys"] = [banked[session], banked[prev_session]]
                        curr, _ = read(banked[session])
                        prev, _ = read(banked[prev_session])
                        if curr.get("date") != session or prev.get("date") != prev_session:
                            raise RuntimeError("Polygon object session does not match its key")
                        pc.update(polygon_counts(curr, prev))
                    else:
                        pc["reason"] = "immediately prior weekday is not banked; no older session substituted"
                else:
                    pc["reason"] = "no full Polygon grouped-daily warehouse blobs"
                proof["candidates"].append(pc)
                if pc["eligible"]:
                    chosen = pc

            # Preserve the complete document and every unrelated field.
            out = copy.deepcopy(before)
            stamp = datetime.now(timezone.utc).isoformat()
            if chosen:
                if chosen["n_up"] == chosen["n_down"] == 100:
                    raise RuntimeError("Refusing a 100/100 breadth result")
                n = chosen["n_univ"]
                out["fields"].update({k: chosen[k] for k in ("n_up", "n_down", "n_univ", "n_missing")})
                out["fields"]["ad_breadth"] = round((chosen["n_up"]-chosen["n_down"])/n, 4)
                out["breadth"] = {**chosen, "status": "LIVE", "generated_at": stamp, "minimum_coverage": 0.8}
                r.ok("breadth=" + json.dumps(out["breadth"]))
            else:
                if "ad_breadth" in before["fields"]:
                    raise RuntimeError("Coverage failed but an existing breadth value is present; refusing to erase or relabel it")
                counted = [c for c in proof["candidates"] if c.get("n_total")]
                best = max(counted, key=lambda c:c["coverage"]) if counted else None
                if best:
                    out["fields"].update({k: best[k] for k in ("n_up", "n_down", "n_univ", "n_missing")})
                reason = ("only %s/%s rows have both prices (%.2f%%); minimum 80%%" %
                          (best["n_univ"], best["n_total"], best["coverage"]*100)) if best else "no usable full warehouse universe with current and previous closes"
                out["breadth"] = {**(best or {}), "status": "SKIPPED", "skip_reason": reason,
                                  "generated_at": stamp, "minimum_coverage": 0.8}
                r.log("ad_breadth omitted: " + reason)
            out["generated_at"] = stamp
            untouched = set(before["fields"]) - OWNED_FIELDS
            if any(out["fields"][k] != before["fields"][k] for k in untouched) or out["schema_version"] != 1:
                raise RuntimeError("Merge altered existing unrelated fields or schema")
            proof["preserved_fields"] = {k: before["fields"][k] for k in sorted(untouched)}
            proof["payload"] = out
            body = (json.dumps(out, indent=2, allow_nan=False)+"\n").encode()
            proof["body_sha256"] = hashlib.sha256(body).hexdigest()
            s3.put_object(Bucket=BUCKET, Key=KEY, Body=body, ContentType="application/json",
                          CacheControl="public, max-age=60", IfMatch=before_obj["ETag"])
            proof["mutated_keys"].append(KEY)
            live, obj = read(KEY)
            if live != out:
                raise RuntimeError("S3 readback differs from merged internals")
            proof["live"] = {"key": KEY, "last_modified": obj["LastModified"].isoformat(),
                              "schema_version": live["schema_version"], "fields": live["fields"], "breadth": live["breadth"]}
            proof["status"] = "PASS_BREADTH" if chosen else "PASS_SKIP"
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
