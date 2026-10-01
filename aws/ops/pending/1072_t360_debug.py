"""
Ops 1072: debug ticker-360 NVDA coverage (read-only).

1071 showed 14/20 domains available but NVDA coverage_count=0.
Inspect the raw packet shapes for dark-pool, finra-short, short-interest
to see where per-ticker rows live (view vs raw packet).
Writes: aws/ops/reports/1072_t360_debug.json. Read-only. Never raises.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone

OUT_PATH = os.path.join("aws", "ops", "reports", "1072_t360_debug.json")
BUCKET = "justhodl-dashboard-live"
REGION = "us-east-1"

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "..", "shared"))
import boto3  # noqa: E402

s3 = boto3.client("s3", region_name=REGION)


def _read(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def _tk(x):
    if isinstance(x, dict):
        return str(x.get("ticker") or x.get("symbol") or "").upper()
    return ""


def probe(key, ctx_mod):
    """Check view vs raw packet for NVDA rows. Never raises."""
    out = {"key": key, "packet_ok": False}
    try:
        pkt = _read(key)
        if not isinstance(pkt, dict):
            return out
        out["packet_ok"] = True
        out["packet_keys"] = sorted(pkt.keys())[:15]
        out["generated_at"] = pkt.get("generated_at")
        # raw packet scan for NVDA
        raw_hits = []
        for k, v in pkt.items():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                hits = [x for x in v[:50] if _tk(x) == "NVDA"]
                if hits:
                    raw_hits.append({"list_key": k, "hit_keys": sorted(hits[0].keys())[:10]})
            elif isinstance(v, dict):
                for tk in list(v.keys())[:200]:
                    if str(tk).upper() == "NVDA":
                        raw_hits.append({"dict_key": k, "hit": True})
                        break
        out["raw_nvda_hits"] = raw_hits[:4]
        # decision_view scan
        if ctx_mod:
            try:
                view = __import__(ctx_mod).decision_view(pkt)
                vh = []
                for k, v in (view or {}).items():
                    if isinstance(v, list) and v and isinstance(v[0], dict):
                        if any(_tk(x) == "NVDA" for x in v[:50]):
                            vh.append(k)
                    elif isinstance(v, dict) and any(str(tk).upper() == "NVDA" for tk in list(v.keys())[:200]):
                        vh.append(k)
                out["view_nvda_hits"] = vh
                out["view_keys"] = sorted((view or {}).keys())[:20]
            except Exception as e:  # noqa: BLE001
                out["view_error"] = str(e)[:80]
        return out
    except Exception as e:  # noqa: BLE001
        out["error"] = str(e)[:80]
        return out


def main():
    """Probe three domains, write the report."""
    rep = {"script": "1072_t360_debug",
           "read_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
           "probes": {}}
    for key, mod in [("data/dark-pool.json", "offexchange_context"),
                     ("data/finra-short.json", "short_volume_context"),
                     ("data/short-interest.json", "short_interest_context"),
                     ("data/short-interest-tickers.json", None)]:
        rep["probes"][key] = probe(key, mod)
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(rep, f, indent=2)
    print(json.dumps(rep, indent=2)[:3500])


if __name__ == "__main__":
    main()
