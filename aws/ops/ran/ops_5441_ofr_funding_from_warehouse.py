"""ops_5441: inspect warehouse last-observation maps before repairing OFR funding.

Runner-only discovery stage: bounded S3 reads, no vendor calls or hot-key writes.
"""
from __future__ import annotations

import gzip
import json
import math
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report

BUCKET = "justhodl-dashboard-live"
DATASETS = tuple(f"data/warm/ofr/dataset-{name}.json.gz"
                 for name in ("nypd", "mmf", "repo"))
CANDIDATES = (
    "FNYR-SOFR-A", "REPO-DVP_AR_TOT-F", "REPO-DVP_AR_TOT-P",
    "REPO-GCF_AR_TOT-F", "REPO-GCF_AR_TOT-P",
    "REPO-TRI_AR_TOT-F", "REPO-TRIV1_AR_TOT-F",
    "REPO-TRI_AR_TOT-P", "REPO-TRIV1_AR_TOT-P",
    "REPO-TRI_TV_TOT-F", "REPO-TRIV1_TV_TOT-F",
    "REPO-TRI_TV_TOT-P", "REPO-TRIV1_TV_TOT-P",
)


def shape(node, depth=0):
    if isinstance(node, dict):
        out = {"type": "dict", "count": len(node), "keys": list(node)[:25]}
        if depth < 4:
            out["sample"] = {k: shape(v, depth + 1)
                             for k, v in list(node.items())[:2]
                             if k not in ("source_url", "raw_snapshot_key")}
        return out
    if isinstance(node, list):
        return {"type": "list", "count": len(node),
                "sample": [shape(v, depth + 1) for v in node[:2]]
                if depth < 4 else []}
    return node


def matches(node, path="", depth=0):
    if depth > 7:
        return
    if isinstance(node, dict):
        for key, value in node.items():
            here = f"{path}/{key}"
            if key in CANDIDATES:
                yield {"path": here, "shape": shape(value),
                       "latest_dated_value": latest_dated_value(value)}
            elif isinstance(value, (dict, list)):
                yield from matches(value, here, depth + 1)
    elif isinstance(node, list):
        for i, value in enumerate(node[:500]):
            yield from matches(value, f"{path}/{i}", depth + 1)


def latest_dated_value(node, depth=0):
    """Report real observation dates in known containers, bounded by depth."""
    if depth > 6:
        return None
    if isinstance(node, dict):
        values = [latest_dated_value(node[k], depth + 1)
                  for k in ("timeseries", "aggregation", "observations", "data",
                            "values", "last_observation") if k in node]
        if "date" in node:
            values.append(latest_dated_value([node["date"], node.get("value")], depth + 1))
        values = [v for v in values if v]
        return max(values, key=lambda v: v[0]) if values else None
    if isinstance(node, list):
        if len(node) >= 2 and isinstance(node[0], str):
            try:
                day = datetime.fromisoformat(node[0].replace("Z", "+00:00")).date()
                if node[1] is not None and not isinstance(node[1], bool):
                    value = float(node[1])
                    if math.isfinite(value):
                        return [day.isoformat(), value]
            except (TypeError, ValueError, OverflowError):
                pass
        values = [v for row in node
                  if (v := latest_dated_value(row, depth + 1))]
        return max(values, key=lambda v: v[0]) if values else None
    return None


def main():
    if os.environ.get("GITHUB_ACTIONS") != "true":
        raise RuntimeError("ops_5441 must run in GitHub Actions with runner credentials")
    import boto3
    s3 = boto3.client("s3", region_name="us-east-1")
    now = datetime.now(timezone.utc)
    with report("ops_5441_ofr_funding_from_warehouse") as r:
        r.heading("ops_5441 OFR warehouse schema inspection (read-only)")
        state = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/warm/ofr/state.json")["Body"].read())
        r.kv(catalog_count=len(state.get("catalog") or []),
             done_count=len(set(state.get("done") or [])),
             candidate_series=[mn for mn in CANDIDATES if mn in (state.get("done") or [])])
        for key in DATASETS:
            try:
                obj = s3.get_object(Bucket=BUCKET, Key=key)
                doc = json.loads(gzip.decompress(obj["Body"].read()))
            except Exception as exc:
                r.warn(f"{key}: {type(exc).__name__}")
                continue
            r.log(f"{key}: age_h={(now - obj['LastModified']).total_seconds()/3600:.2f}")
            r.log("top_level_keys=" + json.dumps(list(doc) if isinstance(doc, dict) else []))
            r.log("schema=" + json.dumps(shape(doc)))
            if isinstance(doc, dict) and "payload" in doc:
                r.log("payload_schema=" + json.dumps(shape(doc["payload"])))
            r.log("candidate_maps=" + json.dumps(list(matches(doc))))
        hot = json.loads(s3.get_object(Bucket=BUCKET, Key="data/ofr-funding.json")["Body"].read())
        r.log("hot_keys=" + json.dumps(list(hot)))
        for name in ("triparty_rate", "triparty_volume", "dvp_rate", "gcf_rate", "sofr"):
            field = hot.get(name, {})
            r.kv(field=name, value=field.get("value"), unit=field.get("unit"),
                 data_unavailable=field.get("data_unavailable"),
                 as_of=field.get("as_of"), source_kind=(field.get("source") or {}).get("kind")
                 if isinstance(field.get("source"), dict) else None)
        r.log("INSPECTION ONLY: no objects written; review the schema before applying ops_5441.")


if __name__ == "__main__":
    main()
