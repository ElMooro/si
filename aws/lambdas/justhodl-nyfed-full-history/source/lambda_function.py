"""justhodl-nyfed-full-history — E8 v1 (ops 4444).

Nightly full-history pull of the NY Fed reference rates that anchor the
plumbing stack: SOFR, EFFR, OBFR (unsecured) + TGCR, BGCR (secured) — up to
10y of daily observations each from the official markets.newyorkfed.org API
-> data/warm/nyfed/{rate}.json.gz + latest-summary.json (current level,
1d change, n_obs, span) for pages. F4 raw snapshots per rate. Rates the API
doesn't serve are explicit absences, never zero-filled."""
import gzip
import json
import os
import urllib.request
from datetime import datetime, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
s3 = boto3.client("s3", region_name="us-east-1")
try:
    from raw_snapshot import snapshot
except Exception:
    snapshot = None

RATES = {"sofr": "secured", "tgcr": "secured", "bgcr": "secured",
         "effr": "unsecured", "obfr": "unsecured"}


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    summary = {"as_of": now.isoformat(timespec="seconds"), "rates": {}}
    for rate, kind in RATES.items():
        url = (f"https://markets.newyorkfed.org/api/rates/{kind}/"
               f"{rate}/last/2600.json")
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "JustHodl research admin@justhodl.ai"})
            with urllib.request.urlopen(req, timeout=45) as r:
                raw = r.read()
            raw_key = snapshot("nyfed", url, raw) if snapshot else None
            obs = (json.loads(raw).get("refRates") or [])
            rows = [{"date": o.get("effectiveDate"),
                     "rate": o.get("percentRate"),
                     "volume_bn": o.get("volumeInBillions")}
                    for o in obs if o.get("effectiveDate")]
            rows.sort(key=lambda x: x["date"])
            s3.put_object(Bucket=BUCKET,
                          Key=f"data/warm/nyfed/{rate}.json.gz",
                          Body=gzip.compress(json.dumps(
                              {"rate": rate, "kind": kind,
                               "source_url": url,
                               "raw_snapshot_key": raw_key,
                               "n_obs": len(rows),
                               "observations": rows}).encode()),
                          ContentType="application/gzip")
            cur = rows[-1] if rows else {}
            prev = rows[-2] if len(rows) > 1 else {}
            summary["rates"][rate] = {
                "current": cur.get("rate"), "date": cur.get("date"),
                "chg_1d_bp": (round((cur.get("rate") - prev.get("rate"))
                                    * 100, 1)
                              if cur.get("rate") is not None
                              and prev.get("rate") is not None else None),
                "volume_bn": cur.get("volume_bn"), "n_obs": len(rows),
                "span": (f"{rows[0]['date']}..{rows[-1]['date']}"
                         if rows else None)}
        except Exception as e:
            summary["rates"][rate] = {"data_unavailable": True,
                                      "reason": f"{type(e).__name__}: "
                                                f"{str(e)[:80]}"}
    s3.put_object(Bucket=BUCKET, Key="data/warm/nyfed/latest-summary.json",
                  Body=json.dumps(summary).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    res = {"ok": True,
           "loaded": {k: v.get("n_obs") for k, v in
                      summary["rates"].items() if v.get("n_obs")},
           "failed": [k for k, v in summary["rates"].items()
                      if v.get("data_unavailable")]}
    print(json.dumps(res))
    return {"statusCode": 200, "body": json.dumps(res)}
