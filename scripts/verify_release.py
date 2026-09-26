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
from datetime import datetime, timezone
import email.utils
import json
import sys
import time
import urllib.error
import urllib.request

HOSTS = ("https://justhodl.ai", "https://justhodl-data-proxy.raafouis.workers.dev")
UA = "justhodl-verify-release/1.0"


def data_freshness(headers, body, max_age_h, now=None, expected_contract=None):
    """Prefer publication time: rewriting a stale last-good object is not freshness.

    Reviewed-artifact gateways deliberately omit Last-Modified. GET verifies the
    actual public JSON and supports those gateways without weakening the check.
    """
    payload = json.loads(body)
    if not isinstance(payload, dict):
        raise ValueError("engine data must be a JSON object")
    if expected_contract is not None and payload.get("contract") != expected_contract:
        raise ValueError(f"native output contract mismatch: expected {expected_contract}, got {payload.get('contract')!r}; deployed code is not proof of a new publication")
    quality = payload.get("quality") or {}
    if not isinstance(quality, dict):
        raise ValueError("invalid quality object")
    status = quality.get("status")
    if status in ("stale", "unavailable", "invalid") or payload.get("ok") is False:
        raise ValueError(f"engine reports unusable data: {status or 'ok=false'}")
    stamps = [("generated_at", payload.get("generated_at")),
              ("meta.generated_at", (payload.get("meta") or {}).get("generated_at")),
              ("quality.publication_date", quality.get("publication_date"))]
    present = [(name, stamp) for name, stamp in stamps if stamp is not None]
    parsed = []
    for name, stamp in present:
        try:
            dt = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
            if dt.tzinfo is None or "T" not in stamp:
                raise ValueError("publication time needs a timezone and time")
            parsed.append((dt.timestamp(), name, stamp))
        except (ValueError, TypeError, AttributeError) as exc:
            raise ValueError(f"invalid {name}") from exc
    if not parsed:
        lm = headers.get("Last-Modified")
        if not lm:
            raise ValueError("no publication timestamp or Last-Modified header")
        try:
            parsed = [(email.utils.parsedate_to_datetime(lm).timestamp(), "Last-Modified", lm)]
        except (ValueError, TypeError, AttributeError) as exc:
            raise ValueError("invalid Last-Modified header") from exc
    current = time.time() if now is None else now
    if any(stamp > current + 300 for stamp, _, _ in parsed):
        raise ValueError("publication timestamp is in the future")
    # A fresh top-level wrapper cannot hide an old nested publication timestamp.
    stamp, basis, raw = min(parsed)
    age_h = max(0, (current - stamp) / 3600)
    state = "fresh" if age_h <= max_age_h else "STALE"
    detail = f"{basis} {raw} ({age_h:.1f}h old) -> {state}"
    if status:
        detail += f"; quality={status}"
    if expected_contract is not None:
        detail += f"; contract={expected_contract}"
    return state == "fresh", detail


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
    ap.add_argument("--data-contract", help="exact expected top-level data contract; rejects a fresh predecessor packet (requires --data)")
    ap.add_argument("--max-age-h", type=float, default=26.0)
    args = ap.parse_args(argv)
    if args.data_contract is not None and (not args.data or not args.data_contract.strip()):
        ap.error("--data-contract requires --data and a non-empty contract")

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
            headers, data_body = fetch(args.data)
            fresh, detail = data_freshness(headers, data_body, args.max_age_h, expected_contract=args.data_contract)
            print(f"   {args.data}: {detail}")
            ok = ok and fresh
        except (RuntimeError, ValueError, TypeError, AttributeError) as exc:
            print(f"   {args.data}: {exc}")
            ok = False
    print("VERIFIED" if ok else "NOT VERIFIED")
    if ok and args.data:
        print("Scope: code receipt and publication freshness" + ("/contract" if args.data_contract else "") +
              "; original-source and compiler replay remain separate acceptance checks.")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
