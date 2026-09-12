"""ops_5491 -- verify 5490 hot keys exist. No new compute."""
from __future__ import annotations

import sys
from pathlib import Path

import boto3

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"
KEYS = [
    "data/real-economy-summary.json",
    "data/treasury-auctions-composite.json",
    "data/dtcc-fails-agency.json",
    "data/tic-state.json",
    "data/fiscaldata-state.json",
    "data/census-us-state.json",
    "data/bls-full-state.json",
    "data/macro-tape.json",
    "data/inst-public-join.json",
    "data/cftc-join.json",
]


def main():
    with report("ops_5491_data_html_card_note") as R:
        R.heading("ops 5491 verify hot keys")
        s3 = boto3.client("s3", region_name="us-east-1")
        ok = 0
        for k in KEYS:
            try:
                h = s3.head_object(Bucket=B, Key=k)
                R.ok("%s %s %s" % (k, h["ContentLength"], h["LastModified"]))
                ok += 1
            except Exception as e:
                R.warn("%s %s" % (k, e))
        if ok < 8:
            R.fail("missing hot keys")
            sys.exit(1)


if __name__ == "__main__":
    main()
