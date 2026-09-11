#!/usr/bin/env python3
"""Verify a deploy from anywhere, no AWS credentials needed (deploy lane v2).

Reads the receipt the deploy transaction published and answers the only
question that matters: is the function on AWS running the bytes from the
commit you think it is -- and is its data fresh?

  python3 scripts/verify_release.py justhodl-stock-buying
  python3 scripts/verify_release.py justhodl-stock-buying --commit e219b4b --data data/stock-buying.json --max-age-h 30

Exit 0 = verified, 1 = mismatch/stale, 2 = receipt unavailable.
Receipts: https://justhodl.ai/data/ops/releases/<function>.json
"""
from __future__ import annotations

import argparse
import email.utils
import json
import sys
import time
import urllib.error
import urllib.request

HOSTS = ("https://justhodl.ai", "https://justhodl-data-proxy.raafouis.workers.dev")
UA = "justhodl-verify-release/1.0"


def fetch(path: str, head: bool = False):
    last = None
    for host in HOSTS:
        req = urllib.request.Request(f"{host}/{path}", headers={"User-Agent": UA, "Cache-Control": "no-cache"},
                                     method="HEAD" if head else "GET")
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.headers, (None if head else resp.read())
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
            last = exc
    raise RuntimeError(f"unreachable on all hosts: {path} ({last})")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("function")
    ap.add_argument("--commit", help="expected commit (prefix ok)")
    ap.add_argument("--data", help="engine output key to freshness-check, e.g. data/stock-buying.json")
    ap.add_argument("--max-age-h", type=float, default=26.0)
    args = ap.parse_args(argv)

    try:
        _, body = fetch(f"data/ops/releases/{args.function}.json")
    except RuntimeError as exc:
        print(f"NO RECEIPT for {args.function}: {exc}")
        return 2
    r = json.loads(body)
    ok = True
    print(f"{args.function}: deployed {r['deployed_at']} from commit {r['commit'][:10]} "
          f"(run {r.get('run_id') or '-'}), CodeSha256 {r['code_sha256'][:12]}…, zip {r['zip_bytes']} bytes")
    for name, meta in r.get("source", {}).items():
        print(f"   {name}: {meta['bytes']} bytes sha256 {meta['sha256'][:12]}")
    if args.commit and not r["commit"].startswith(args.commit.lower()):
        print(f"MISMATCH: live commit {r['commit'][:10]} != expected {args.commit}")
        ok = False
    if args.data:
        try:
            headers, _ = fetch(args.data, head=True)
            lm = headers.get("Last-Modified")
            age_h = (time.time() - email.utils.parsedate_to_datetime(lm).timestamp()) / 3600 if lm else None
            state = "fresh" if age_h is not None and age_h <= args.max_age_h else "STALE"
            print(f"   {args.data}: Last-Modified {lm} ({age_h:.1f}h old) -> {state}" if age_h is not None
                  else f"   {args.data}: no Last-Modified header")
            if state == "STALE":
                ok = False
        except RuntimeError as exc:
            print(f"   {args.data}: {exc}")
            ok = False
    print("VERIFIED" if ok else "NOT VERIFIED")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
