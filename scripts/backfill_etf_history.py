#!/usr/bin/env python3
"""Backfill daily bars for the six drill symbols from FMP into data/warm/etf-history/grouped/<year>/<day>.json.gz
(2026-09-18). Polygon's grouped-daily entitlement is a rolling ~5-year window, so the COVID-2020 holdout block can never be
banked from it; this prefix holds ONLY the drill symbols, source-tagged, create-if-absent, in the grouped-file row shape
(T/o/h/l/c/v) so scripts/factory_holdout.py reads it through grouped_row's fallback unchanged.

  python3 scripts/backfill_etf_history.py --from 2020-01-02 --to 2020-05-29 [--dry-run]

Real data only: a session is written only when every symbol has a bar for it; nothing is interpolated.
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "aws" / "shared"))
sys.path.insert(0, str(ROOT / "scripts"))

PUBLIC = os.environ.get("JH_PUBLIC_BUCKET", "justhodl-dashboard-live")
PREFIX = "data/warm/etf-history/grouped/"
SYMBOLS = ("SPY", "QQQ", "IWM", "TLT", "GLD")            # the drill symbols (BTC is not a drill instrument)
FMP = "https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=%s&from=%s&to=%s&apikey=%s"


def fetch(symbol: str, start: str, end: str, key: str) -> dict:
    req = urllib.request.Request(FMP % (symbol, start, end, urllib.parse.quote(key)), headers={"User-Agent": "justhodl-backfill/1.0", "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as r:
        rows = json.loads(r.read())
    out = {}
    for row in rows if isinstance(rows, list) else []:
        d = str(row.get("date") or "")[:10]
        try:
            o, h, l, c, v = float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"]), float(row.get("volume") or 0)
        except Exception:  # noqa: BLE001
            continue
        if d and o > 0 and c > 0:
            out[d] = {"T": symbol, "o": o, "h": h, "l": l, "c": c, "v": v}
    return out


def sessions(start: str, end: str, by_symbol: dict) -> list:
    """Days on which EVERY symbol traded (the providers' own calendar, no interpolation)."""
    days = sorted(set.intersection(*(set(d.keys()) for d in by_symbol.values()))) if by_symbol else []
    return [d for d in days if start <= d <= end]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="start", required=True)
    ap.add_argument("--to", dest="end", required=True)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)
    import boto3
    from managed_secret import managed_secret
    key = managed_secret(("FMP_API_KEY",), ("/justhodl/fmp/api-key",))
    s3 = boto3.client("s3", region_name="us-east-1")
    by_symbol = {s: fetch(s, args.start, args.end, key) for s in SYMBOLS}
    days = sessions(args.start, args.end, by_symbol)
    print(json.dumps({"symbols": {s: len(v) for s, v in by_symbol.items()}, "sessions_with_every_symbol": len(days), "first": days[:1], "last": days[-1:]}))
    if not days:
        return 2
    written, existed = 0, 0
    for d in days:
        doc = {"source": "fmp:historical-price-eod/full", "scope": list(SYMBOLS), "date": d, "results": [by_symbol[s][d] for s in SYMBOLS],
               "note": "drill symbols only; banked because Polygon grouped-daily entitlement does not reach this date"}
        k = PREFIX + "%s/%s.json.gz" % (d[:4], d)
        if args.dry_run:
            continue
        try:
            s3.put_object(Bucket=PUBLIC, Key=k, Body=gzip.compress(json.dumps(doc, separators=(",", ":")).encode()), ContentType="application/json", ContentEncoding="gzip", IfNoneMatch="*")
            written += 1
        except Exception as exc:  # noqa: BLE001
            if "PreconditionFailed" in str(exc) or "412" in str(exc):
                existed += 1
            else:
                raise
    print(json.dumps({"written": written, "existed": existed, "prefix": PREFIX}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
