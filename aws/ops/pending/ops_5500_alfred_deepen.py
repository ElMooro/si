"""ops_5500 -- deepen ALFRED cubes. Merge-append. Last 36 vintages + one per year."""
from __future__ import annotations

import gzip
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"
WARM = "data/warm/inst-public/alfred/"
SERIES = ["GDP", "PAYEMS", "CPIAUCSL", "PCEPI", "UNRATE"]


def _now():
    return datetime.now(timezone.utc).isoformat()


def _get(url, timeout=90):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodlOps/5500"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def _load_gz(s3, key):
    try:
        raw = gzip.decompress(s3.get_object(Bucket=B, Key=key)["Body"].read())
        return json.loads(raw)
    except Exception:
        return {}


def _pick(dates):
    if not dates:
        return []
    tail = dates[-36:]
    by_year = {}
    for d in dates:
        y = d[:4]
        by_year[y] = d
    yearly = [by_year[y] for y in sorted(by_year)]
    out = []
    seen = set()
    for d in yearly + tail:
        if d not in seen:
            seen.add(d)
            out.append(d)
    return out[-80:]


def main():
    with report("ops_5500_alfred_deepen") as R:
        R.heading("ops 5500 ALFRED deepen")
        s3 = boto3.client("s3", region_name="us-east-1")
        ssm = boto3.client("ssm", region_name="us-east-1")
        key = ssm.get_parameter(Name="/justhodl/fred-api-key", WithDecryption=True)["Parameter"]["Value"]
        indexed = []
        for sid in SERIES:
            wkey = WARM + sid + ".json.gz"
            prev = _load_gz(s3, wkey)
            have = {v.get("vintage_date"): v for v in (prev.get("vintages") or []) if isinstance(v, dict) and v.get("vintage_date")}
            vurl = "https://api.stlouisfed.org/fred/series/vintagedates?series_id=%s&api_key=%s&file_type=json" % (sid, key)
            vdoc = json.loads(_get(vurl))
            raw_v = vdoc.get("vintage_dates") or []
            dates = [x if isinstance(x, str) else (x.get("vintage_date") if isinstance(x, dict) else None) for x in raw_v]
            dates = [d for d in dates if d]
            want = [d for d in _pick(dates) if d not in have]
            R.ok("%s avail=%s have=%s fetch=%s" % (sid, len(dates), len(have), len(want)))
            for vd in want:
                ourl = ("https://api.stlouisfed.org/fred/series/observations?series_id=%s&api_key=%s"
                        "&file_type=json&vintage_dates=%s&sort_order=desc&limit=1" % (sid, key, vd))
                try:
                    odoc = json.loads(_get(ourl))
                    obs = odoc.get("observations") or []
                    if obs:
                        have[vd] = {"vintage_date": vd, "as_of": obs[0].get("date"), "value": obs[0].get("value")}
                except Exception as e:
                    R.warn("%s %s %s" % (sid, vd, e))
                time.sleep(1.15)
            vintages = [have[d] for d in sorted(have)]
            payload = {
                "series_id": sid,
                "n_vintages_available": len(dates),
                "n_vintages_banked": len(vintages),
                "first_vintage": vintages[0]["vintage_date"] if vintages else None,
                "last_vintage": vintages[-1]["vintage_date"] if vintages else None,
                "vintages": vintages,
                "source": "alfred",
                "fetched_at": _now(),
            }
            raw = json.dumps(payload).encode("utf-8")
            s3.put_object(Bucket=B, Key=wkey, Body=gzip.compress(raw),
                          ContentType="application/json", ContentEncoding="gzip")
            indexed.append({
                "key": wkey, "series_id": sid,
                "n_vintages_available": len(dates),
                "n_vintages_banked": len(vintages),
                "first_vintage": payload["first_vintage"],
                "last_vintage": payload["last_vintage"],
                "source": "alfred",
            })
            R.ok("%s banked now %s" % (sid, len(vintages)))
            time.sleep(1.5)
        hot = {
            "schema": "alfred-vintages.v1",
            "source": "ops_5500",
            "generated_at": _now(),
            "n_objects": len(indexed),
            "n_indexed": len(indexed),
            "series": indexed,
        }
        s3.put_object(Bucket=B, Key="data/alfred-vintages.json", Body=json.dumps(hot).encode("utf-8"), ContentType="application/json")
        R.ok("hot index refreshed")


if __name__ == "__main__":
    main()
