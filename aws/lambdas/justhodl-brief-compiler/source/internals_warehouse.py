"""Scheduled internals: S3 reads only, conditional merge into schema version 1."""

from datetime import date, datetime, timezone

import copy, gzip, json, math

from compile_jh_internals import FRED_LEGS, compute

BUCKET = "justhodl-dashboard-live"

KEY = "data/jh-internals.json"

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


def merge_sma(before, universe, last_modified, generated_at=None):
    """Merge breadth from finite, signed Finviz SMA percentage distances.

    A finite percentage is sufficient: >0 is above, while zero is in the
    denominator but not the numerator. Prices and SMA levels are not required.
    The two windows have independent 80% gates and no universe cap or filter.
    """
    if before.get("schema_version") != 1 or not isinstance(before.get("fields"), dict):
        raise ValueError("Existing internals must have schema_version 1")
    rows = universe.get("by_ticker") if isinstance(universe, dict) else None
    if not isinstance(rows, dict) or not rows or universe.get("n_tickers") != len(rows):
        raise ValueError("Full declared Finviz universe required for SMA coverage")
    if any(universe.get(k) for k in ("capped", "truncated", "data_unavailable", "stale")):
        raise ValueError("Finviz SMA source flagged capped, truncated, stale or unavailable")
    out = copy.deepcopy(before)
    metadata = {
        "source": "finviz-universe", "key": "data/finviz-universe.json",
        "as_of": universe.get("generated_at"), "last_modified": last_modified,
        "generated_at": generated_at or datetime.now(timezone.utc).isoformat(),
        "minimum_coverage": 0.8, "unit": "fraction", "n_total": len(rows),
        "denominator": "all rows with finite sma50_pct or sma200_pct, respectively",
        "basis": "uncapped full universe; signed SMA percentage > 0; zero is not above",
        "windows": {},
    }
    for window in (50, 200):
        source_field = f"sma{window}_pct"
        usable = above = 0
        for row in rows.values():
            if not isinstance(row, dict):
                continue
            value = number(row.get(source_field))
            if value is None:
                continue
            usable += 1
            above += value > 0
        total = len(rows)
        eligible = bool(usable and usable * 5 >= total * 4)
        counts = {f"n_above_{window}": above, f"n_sma{window}": usable}
        out["fields"].update(counts)
        field = f"pct_above_{window}"
        result = {
            **counts, "n_total": total, "n_missing": total - usable,
            "coverage": usable / total, "source_field": "by_ticker.*." + source_field,
            "status": "LIVE" if eligible else "SKIPPED",
        }
        if eligible:
            out["fields"][field] = above / usable
        else:
            if field in before["fields"]:
                raise ValueError("SMA refresh skipped; preserving existing publication: " + field)
            result["skip_reason"] = (
                f"Only {usable}/{total} rows ({usable/total:.2%}) have finite "
                f"{source_field}; minimum 80%."
            )
        metadata["windows"][str(window)] = result
    out["sma_breadth"] = metadata
    return out


def read(s3, key, optional=False):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
    except Exception as exc:
        code = getattr(exc, "response", {}).get("Error", {}).get("Code")
        if optional and code in ("NoSuchKey", "404"):
            return None, None
        raise
    raw = obj["Body"].read()
    return json.loads(gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw), obj


def build(s3):
    """Read-only build, also used by runner verification with HTTP blocked.

    Freshness refers to object LastModified, not weekly observation dates.
    There is deliberately no FRED/vendor HTTP client or fallback in this mode.
    """
    before, obj = read(s3, KEY)
    if before.get("schema_version") != 1 or not isinstance(before.get("fields"), dict):
        raise ValueError("Existing internals must have schema_version 1")
    stamp = datetime.now(timezone.utc)
    cache, cache_obj = read(s3, "data/fred-cache.json", optional=True)
    legs, provenance = {}, {}
    prefixes = None
    for leg, sid in FRED_LEGS.items():
        candidates = []
        if isinstance(cache, dict) and sid in cache:
            last = last_observation(cache[sid])
            if last:
                candidates.append((last, "data/fred-cache.json", cache_obj))
        # A fresh full cache is the warehouse's scheduled refresh. Only locate
        # scoped banks if this leg is missing or its cache object is >36h old.
        if not candidates or (stamp-cache_obj["LastModified"]).total_seconds() > 36*3600:
            if prefixes is None:
                prefixes = []
                for page in s3.get_paginator("list_objects_v2").paginate(
                        Bucket=BUCKET, Prefix="data/warm/fred-scoped/", Delimiter="/"):
                    prefixes.extend(x["Prefix"] for x in page.get("CommonPrefixes", []))
            for prefix in prefixes:
                key = prefix + sid + ".json"
                doc, source_obj = read(s3, key, optional=True)
                if doc is None:
                    continue
                identity = (doc.get("meta") or {}).get("id") or doc.get("series_id")
                if identity and identity != sid:
                    raise ValueError("Warehouse identity mismatch: " + sid)
                last = last_observation(doc)
                if last:
                    candidates.append((last, key, source_obj))
        if not candidates:
            raise ValueError("No banked finite dated observation: " + sid)
        last, key, source_obj = max(candidates, key=lambda c: (c[0][0], c[2]["LastModified"]))
        age_h = (stamp-source_obj["LastModified"]).total_seconds()/3600
        scale = 1000 if sid == "RRPONTSYD" else 1
        legs[leg] = last[1]*scale
        provenance[sid] = {"key": key, "as_of": last[0], "value": last[1],
                           "last_modified": source_obj["LastModified"].isoformat(),
                           "age_h": round(age_h, 4), "fresh_36h": age_h <= 36,
                           "compiler_multiplier": scale}
    universe, universe_obj = read(s3, "data/finviz-universe.json", optional=True)
    counts = finviz_counts(universe) if isinstance(universe, dict) else {
        "eligible": False, "reason": "Finviz warehouse universe missing"}
    if counts.get("n_up") == counts.get("n_down") == 100:
        raise ValueError("Refusing 100/100 breadth")
    out = copy.deepcopy(before)
    compiled = compute(legs)
    out.update({k: compiled[k] for k in ("source", "generated_at")})
    out["fields"].update(compiled["fields"])
    if counts["eligible"]:
        out["fields"].update({k: counts[k] for k in ("n_up", "n_down", "n_univ", "n_missing")})
        out["fields"]["ad_breadth"] = round((counts["n_up"]-counts["n_down"])/counts["n_univ"], 4)
        out["breadth"] = {**counts, "source": "finviz-universe", "keys": ["data/finviz-universe.json"],
                          "status": "LIVE", "minimum_coverage": 0.8, "generated_at": stamp.isoformat(),
                          "last_modified": universe_obj["LastModified"].isoformat()}
    else:
        reason = counts.get("reason") or "fewer than 80% of universe rows have both prices"
        # Keep the prior publication intact on a failed coverage gate.
        if "ad_breadth" in before["fields"]:
            raise ValueError("Breadth refresh skipped; preserving existing publication: " + reason)
        out["breadth"] = {**counts, "status": "SKIPPED", "skip_reason": reason,
                          "minimum_coverage": 0.8, "generated_at": stamp.isoformat()}
    if not {"twos_tens", "liq_proxy_bn", "nfci"}.issubset(out["fields"]):
        raise ValueError("Required internals absent")
    out["warehouse"] = {"mode": "S3 only; no vendor HTTP fallback", "fred": provenance,
                        "fred_http_requests": 0,
                        "fresh_fred_legs": sum(x["fresh_36h"] for x in provenance.values())}
    out = merge_sma(out, universe, universe_obj["LastModified"].isoformat(), stamp.isoformat())
    return out, obj["ETag"]


def run(s3):
    out, etag = build(s3)
    body = (json.dumps(out, indent=2, allow_nan=False)+"\n").encode()
    s3.put_object(Bucket=BUCKET, Key=KEY, Body=body, ContentType="application/json",
                  CacheControl="public, max-age=60", IfMatch=etag)
    live, obj = read(s3, KEY)
    if live != out:
        raise ValueError("Internals readback differs")
    receipt = {"ok": True, "key": KEY, "last_modified": obj["LastModified"].isoformat(),
               "fields": live["fields"], "warehouse": live["warehouse"],
               "sma_breadth": live["sma_breadth"]}
    print(json.dumps({"internals_receipt": receipt}))
    return receipt
