"""ops_5483 -- publish data/macro-tape.json from banked inst-public last prints."""
from __future__ import annotations

import gzip
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"
JOIN = "data/inst-public-join.json"
OUT = "data/macro-tape.json"
WANT = {
    "VIXCLS": "vix",
    "DTWEXBGS": "dxy_broad",
    "DGS10": "us10y",
    "CPIAUCSL": "us_cpi",
    "SOFR": "sofr",
    "NFCI": "nfci",
    "GDPNOW": "gdpnow",
    "T10Y3M": "curve_10y3m",
    "PAYEMS": "payrolls",
    "UNRATE": "unrate",
    "DCOILWTICO": "wti",
    "BAMLH0A0HYM2": "hy_oas",
}


def main():
    with report("ops_5483_macro_tape") as R:
        R.heading("ops 5483 macro-tape")
        s3 = boto3.client("s3", region_name="us-east-1")
        join = json.loads(s3.get_object(Bucket=B, Key=JOIN)["Body"].read())
        fields = {}
        for f in join.get("feeds") or []:
            sid = f.get("id")
            if sid in WANT and f.get("status") == "LIVE" and f.get("last"):
                fields[WANT[sid]] = {
                    "series_id": sid,
                    "date": (f.get("last") or {}).get("date"),
                    "value": (f.get("last") or {}).get("value"),
                    "n": f.get("n"),
                    "start": f.get("start"),
                    "end": f.get("end"),
                }
                R.ok("%s %s %s" % (sid, fields[WANT[sid]]["date"], fields[WANT[sid]]["value"]))
        body = {
            "schema": "macro-tape.v1",
            "source": "ops_5483",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "n_fields": len(fields),
            "fields": fields,
        }
        s3.put_object(Bucket=B, Key=OUT, Body=json.dumps(body).encode("utf-8"), ContentType="application/json")
        R.ok("wrote %s fields=%s" % (OUT, len(fields)))
        if len(fields) < 8:
            R.fail("too few tape fields")
            sys.exit(1)


if __name__ == "__main__":
    main()
