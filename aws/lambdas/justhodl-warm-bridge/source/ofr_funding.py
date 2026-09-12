"""Build the OFR funding feed from bounded warehouse reads, with no HTTP.

Dataset layout and units verified by runner inspections ops 5441 and 5443.
TRI includes Federal Reserve transactions; TRIV1 excludes them and is not an
alias. The existing generic triparty fields retain the TRI market definition.
"""
import hashlib
import math
from datetime import date, datetime, timezone

DATASET_KEYS = tuple(f"data/warm/ofr/dataset-{name}.json.gz"
                     for name in ("repo", "nypd", "mmf"))
FIELDS = {
    "triparty_rate": ("REPO-TRI_AR_TOT-P",),
    "triparty_volume": ("REPO-TRI_TV_TOT-P",),
    "dvp_rate": ("REPO-DVP_AR_TOT-P",),
    "gcf_rate": ("REPO-GCF_AR_TOT-P",),
    "sofr": ("FNYR-SOFR-A",),
}


def observation(row):
    """A dated finite measurement. Zero is valid; booleans/NaN are not."""
    if isinstance(row, dict):
        day, value = row.get("date") or row.get("d"), row.get("value")
    elif isinstance(row, (list, tuple)) and len(row) >= 2:
        day, value = row[:2]
    else:
        return None
    if not isinstance(day, str) or len(day) < 10:
        return None
    try:
        parsed = date.fromisoformat(day[:10])
        if isinstance(value, bool) or value is None:
            return None
        value = float(value)
        if not math.isfinite(value):
            return None
    except (TypeError, ValueError, OverflowError):
        return None
    return parsed.isoformat(), value


def latest(node, today):
    """Read known OFR containers; choose by observation date, not row order."""
    if isinstance(node, dict):
        if node.get("data_unavailable") is True:
            return None
        if "date" in node or "d" in node:
            rows = [node]
        else:
            choices = []
            for key in ("timeseries", "aggregation", "data", "observations",
                        "values", "last_observation"):
                if key in node:
                    found = latest(node[key], today)
                    if found:
                        choices.append(found)
            # Some cached series use a date -> value map.
            if not choices:
                rows = [[k, v] for k, v in node.items()
                        if isinstance(k, str) and len(k) == 10]
            else:
                return max(choices, key=lambda item: item[0])
    elif isinstance(node, (list, tuple)):
        rows = [node] if observation(node) else node
    else:
        return None
    points = [point for row in rows if (point := observation(row))
              and point[0] <= today]
    return max(points, key=lambda item: item[0]) if points else None


def series_node(doc, mnemonic, single=False):
    if not isinstance(doc, dict) or doc.get("data_unavailable") is True:
        return None
    payload = doc.get("payload", doc)
    if isinstance(payload, dict):
        for container in (payload.get("timeseries"), payload.get("series"),
                          payload):
            if isinstance(container, dict) and mnemonic in container:
                return container[mnemonic]
    # Only the exact series object key can identify an unkeyed payload.
    if single and doc.get("mnemonic", mnemonic) == mnemonic:
        return payload
    return None


def series_unit(node, field):
    if isinstance(node, dict):
        metadata = node.get("metadata")
        description = metadata.get("description") if isinstance(metadata, dict) else None
        for meta in (metadata, description, node.get("description"), node):
            if isinstance(meta, dict):
                unit = meta.get("unit") or meta.get("units")
                if isinstance(unit, dict):
                    # Actual OFR units are objects, not strings. Preserve raw
                    # base-unit values; refuse an unreviewed scale change.
                    if unit.get("magnitude") != 0:
                        return None
                    unit = unit.get("name")
                if isinstance(unit, str) and unit.strip():
                    return unit
    # AR and SOFR are rates in percent. Volume needs explicit stored units.
    return None if field == "triparty_volume" else "Percent"


def build_funding(get_document, now):
    """At most three datasets plus five exact series objects per invocation."""
    published = datetime.fromisoformat(now.replace("Z", "+00:00"))
    if published.tzinfo is None:
        published = published.replace(tzinfo=timezone.utc)
    today = published.date().isoformat()
    documents, errors = {}, {}

    def read(key):
        if key not in documents and key not in errors:
            try:
                documents[key] = get_document(key)
            except Exception as exc:
                # Do not put signed URLs, credentials, or full AWS errors in hot data.
                errors[key] = type(exc).__name__
        return documents.get(key)

    for key in DATASET_KEYS:
        read(key)
    out = {"as_of": now, "source": "OFR Short-Term Funding Monitor",
           "schema_version": "ofr-funding.v2", "source_mode": "warehouse"}
    for field, mnemonics in FIELDS.items():
        candidates = []
        for priority, mnemonic in enumerate(mnemonics):
            single_key = f"data/warm/ofr/series/{mnemonic}.json.gz"
            read(single_key)
            for key in (*DATASET_KEYS, single_key):
                doc = documents.get(key)
                node = series_node(doc, mnemonic, single=key == single_key)
                point = latest(node, today)
                unit = series_unit(node, field)
                if point and unit and (field != "triparty_volume" or point[1] >= 0):
                    candidates.append((point[0], -priority, key, mnemonic,
                                       point[1], unit, doc))
        if not candidates:
            out[field] = {
                "field": field, "value": None, "unit": None,
                "as_of": None, "data_unavailable": True, "confidence": 0.0,
                "reason": "No dated finite warehouse observation with known units",
                "source": {"kind": "cache", "provider": "ofr",
                           "series_id": None, "fetched_at": None},
            }
            continue
        day, _, key, mnemonic, value, unit, doc = max(
            candidates, key=lambda item: (item[0], item[1]))
        age_days = (published.date() - date.fromisoformat(day)).days
        stale = age_days > 7
        fetched_at = doc.get("as_of") or doc.get("fetched_at")
        source_url = doc.get("source_url")
        out[field] = {
            "field": field, "value": value, "unit": unit, "as_of": day,
            "observed": day, "data_unavailable": False, "stale": stale,
            "observation_age_days": age_days,
            "freshness_policy": "stale when observation is more than 7 calendar days old",
            "confidence": 1.0, "provider": "ofr", "series": mnemonic,
            "source_url": source_url, "raw_snapshot_key": key,
            "fetched_at": fetched_at,
            "source": {
                "kind": "cache-stale" if stale else "cache", "provider": "ofr",
                "series_id": mnemonic, "url": source_url,
                "fetched_at": fetched_at, "fetched_by": "justhodl-warm-bridge",
                "raw_snapshot_key": key,
                "upstream_raw_snapshot_key": doc.get("raw_snapshot_key"),
            },
            "trace_id": hashlib.sha256(
                f"{field}|{mnemonic}|{key}|{day}".encode()).hexdigest()[:12],
        }
    out["available_fields"] = sum(
        not out[field]["data_unavailable"] for field in FIELDS)
    out["stale_fields"] = [field for field in FIELDS if out[field].get("stale")]
    out["fresh_fields"] = out["available_fields"] - len(out["stale_fields"])
    out["warehouse_read_errors"] = errors
    return out
