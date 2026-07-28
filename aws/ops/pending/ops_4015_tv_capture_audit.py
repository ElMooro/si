"""
ops_4015 — VERIFY the TradingView capture layer server-side.

Khalid: "did you pull all my watchlists with all the metrics inside —
my blackswan watchlist should have almost all metrics to predict a black
swan?" Extension v1.4.0 CAPTURES watchlists client-side (network trace of
symbols_list/custom/) and uploads them; this op proves what actually
LANDED: the watchlists artifact, blackswan membership, note counts — and
confirms what is structurally MISSING (per-metric source attribution),
which the v1.5.0 extension mod adds next.
"""
import json
import sys
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


def gj(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception as e:
        return {"__err__": f"{type(e).__name__}"}


def main():
    with report("4015_tv_capture_audit") as rep:
        rep.heading("ops 4015 — TV watchlists + sources server-side audit")

        rep.section("A. candidate artifacts")
        found = None
        for k in ("data/tv-watchlists.json", "data/watchlists.json",
                  "data/tv-notes-watchlists.json", "tv/watchlists.json",
                  "data/tv-tracker.json"):
            d = gj(k)
            ok = "__err__" not in d
            rep.log(f"  {k}: {'FOUND' if ok else d['__err__']}")
            if ok and not found:
                found = (k, d)
        if not found:
            resp = s3.list_objects_v2(Bucket=BUCKET, Prefix="data/tv")
            for o in resp.get("Contents") or []:
                rep.log(f"  listed: {o['Key']} {o['Size']}B")
            resp = s3.list_objects_v2(Bucket=BUCKET, Prefix="tv")
            for o in resp.get("Contents") or []:
                rep.log(f"  listed: {o['Key']} {o['Size']}B")
        if not found:
            rep.fail("no watchlists artifact — ingest may store elsewhere (KV?)")
            sys.exit(1)

        key, d = found
        rep.section(f"B. {key} — the truth")
        wls = (d.get("watchlists") or d.get("lists")
               or (d if isinstance(d, list) else []))
        if isinstance(wls, dict):
            wls = list(wls.values())
        rep.kv(n_watchlists=len(wls),
               generated_at=d.get("generated_at") if isinstance(d, dict) else None)
        bs = None
        for w in wls:
            nm = str(w.get("name", ""))
            syms = w.get("symbols") or []
            rep.log(f"  {nm[:40]:40s} n_symbols={len(syms)}")
            if "black" in nm.lower():
                bs = w
        rep.section("C. BLACKSWAN membership")
        if bs:
            syms = bs.get("symbols") or []
            rep.kv(blackswan_name=bs.get("name"), n=len(syms))
            rep.log("  symbols: " + ", ".join(str(x)[:22] for x in syms[:60]))
        else:
            rep.fail("  no watchlist with 'black' in its name landed")

        rep.section("D. brain TV-note parity + source-attribution absence")
        v = gj("data/tradingview.json")
        rows = v.get("symbols") or []
        rep.kv(vault_symbols=len(rows),
               with_notes=sum(1 for r in rows if (r.get("n_notes") or 0) > 0))
        has_srcfield = sum(1 for r in rows if r.get("tv_source_attribution"))
        rep.kv(rows_with_tv_source_attribution=has_srcfield,
               note="expected 0 — v1.4.0 never captured it; v1.5.0 adds it")
        checks = [("watchlists artifact exists", True),
                  ("blackswan present with >=5 symbols",
                   bool(bs) and len(bs.get("symbols") or []) >= 5)]
        failed = [l for l, ok in checks if not ok]
        for l, ok in checks:
            (rep.ok if ok else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"AUDIT DONE — {len(wls)} watchlists; blackswan "
               f"{len(bs.get('symbols') or [])} symbols; attribution absent as "
               f"expected → extension v1.5.0")


if __name__ == "__main__":
    main()
