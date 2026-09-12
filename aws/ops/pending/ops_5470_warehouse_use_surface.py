"""ops_5470 -- surface unused warehouse prefixes as a hot join. No vendor HTTP."""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"
OUT = "data/warehouse-use.json"
PREFIXES = [
    "data/warm/finra/",
    "data/warm/census-us/",
    "data/warm/sec-midas/",
    "data/warm/sec-dera/",
    "data/warm/sec-bulk/",
    "data/warm/sec-edgar/",
    "data/warm/nyfed-research/",
    "data/warm/imf-full/",
    "data/warm/ofr/",
    "data/warm/bls/",
    "data/warm/eurostat/",
    "data/warm/gdelt/",
]


def _age_h(lm):
    if not lm:
        return None
    now = datetime.now(timezone.utc)
    if lm.tzinfo is None:
        lm = lm.replace(tzinfo=timezone.utc)
    return round((now - lm).total_seconds() / 3600.0, 1)


def main():
    with report("ops_5470_warehouse_use_surface") as R:
        R.heading("ops 5470 warehouse-use surface")
        s3 = boto3.client("s3", region_name="us-east-1")
        rows = []
        for pref in PREFIXES:
            listed = s3.list_objects_v2(Bucket=B, Prefix=pref, MaxKeys=50)
            contents = listed.get("Contents") or []
            if not contents:
                rows.append({"prefix": pref, "n": 0, "status": "EMPTY"})
                R.warn("empty %s" % pref)
                continue
            newest = max(contents, key=lambda o: o["LastModified"])
            state_key = None
            for cand in (
                pref + "_state/state.json",
                pref + "state.json",
                pref + "latest.json",
                pref + "latest-summary.json",
            ):
                try:
                    s3.head_object(Bucket=B, Key=cand)
                    state_key = cand
                    break
                except Exception:
                    pass
            row = {
                "prefix": pref,
                "n_listed": len(contents),
                "truncated": bool(listed.get("IsTruncated")),
                "newest_key": newest["Key"],
                "newest_lm": newest["LastModified"].astimezone(timezone.utc).isoformat(),
                "age_h": _age_h(newest["LastModified"]),
                "state_key": state_key,
                "status": "LIVE" if (_age_h(newest["LastModified"]) or 9e9) < 48 else "STALE",
            }
            rows.append(row)
            R.ok("%s n=%s age_h=%s state=%s" % (pref, row["n_listed"], row["age_h"], state_key))
        body = {
            "schema": "warehouse-use.v1",
            "source": "ops_5470",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "n_prefixes": len(rows),
            "n_hot": sum(1 for r in rows if r.get("status") == "LIVE"),
            "rows": rows,
        }
        s3.put_object(
            Bucket=B,
            Key=OUT,
            Body=json.dumps(body, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        R.ok("wrote %s live=%s/%s" % (OUT, body["n_hot"], body["n_prefixes"]))


if __name__ == "__main__":
    main()
