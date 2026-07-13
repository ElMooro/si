"""ops 3186 — probe BEFORE paying: do free sources already cover his
foreign listings?

Khalid has an EODHD token in hand. Before I spend a euro of it (or let him
justify the $59/$99 tiers), the honest test: LSE/Frankfurt/Euronext/Shanghai
listings are usually FREE on Yahoo via exchange suffixes (LSE:VOD → VOD.L).
This op REAL-FETCHES a sample of his actual unmapped foreign symbols through
the free path and reports the hit rate per exchange.

Whatever Yahoo misses is the ONLY thing worth a vendor line item — and the
FTSE bucket (his largest, 448 symbols) is licensed INDEX product, not
exchange data, so it may not be purchasable at the €19.99 tier at all.
"""

import json
import sys
import time
from collections import defaultdict
from pathlib import Path

import boto3

from ops_report import report

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
import series_source as SS  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3186_yahoo_probe") as rep:
    fails, warns = [], []
    rep.heading("ops 3186 — probe the free path before spending")

    wl = s3_json("data/tv-watchlists.json") or {}
    lists = [l for l in (wl.get("lists") or [])
             if not str(l.get("id", "")).startswith("e2e-")]
    smap = (s3_json("data/symbol-map.json") or {}).get("map") or {}
    uniq = sorted({s.upper() for l in lists for s in (l.get("symbols") or [])})

    by_ex = defaultdict(list)
    for s in uniq:
        if ":" not in s:
            continue
        ex = s.split(":", 1)[0]
        if ex in SS.YAHOO_SUFFIX or ex in ("FTSE", "CBOEEU", "ICEEUR",
                                           "EUREX", "USI", "INTOTHEBLOCK",
                                           "GLASSNODE", "COT3"):
            by_ex[ex].append(s)

    rep.section("1. Real fetches through the FREE path (sampled)")
    results = {}
    for ex, syms in sorted(by_ex.items(), key=lambda kv: -len(kv[1])):
        sample = syms[:6]
        hits, obs_total, examples = 0, 0, []
        for sym in sample:
            src, sid, _, _ = SS.map_symbol(sym)
            if src != "MARKET":
                continue
            ser = SS.fetch("MARKET", sid, "2015-01-01")
            if len(ser) > 200:
                hits += 1
                obs_total += len(ser)
                ks = sorted(ser)
                if len(examples) < 2:
                    examples.append(f"{sym}→{sid} ({len(ser)} obs, "
                                    f"{ks[0][:4]}–{ks[-1][:4]})")
            time.sleep(0.15)
        rate = round(100 * hits / max(1, len(sample)))
        results[ex] = {"total": len(syms), "sampled": len(sample),
                       "hit_pct": rate}
        rep.log(f"  {ex:14s} {len(syms):4d} symbols · free-path hit "
                f"{rate:3d}%  {'; '.join(examples)[:64]}")

    rep.section("2. Verdict per bucket")
    free_ok, needs_vendor, unbuyable = [], [], []
    for ex, r in results.items():
        if ex in ("FTSE",):
            unbuyable.append((ex, r["total"],
                              "FTSE Russell licensed INDEX product — not "
                              "exchange data; EODHD's EOD tier likely does "
                              "NOT carry it"))
        elif ex in ("INTOTHEBLOCK", "GLASSNODE"):
            unbuyable.append((ex, r["total"],
                              "on-chain — Glassnode API is $799+/mo; not "
                              "worth 1-2 engines"))
        elif ex in ("USI",):
            free_ok.append((ex, r["total"], "already COMPUTED by "
                            "justhodl-market-internals ($0)"))
        elif ex in ("COT3",):
            free_ok.append((ex, r["total"], "CFTC is free; justhodl-cot-"
                            "tracker already owns this"))
        elif r["hit_pct"] >= 60:
            free_ok.append((ex, r["total"],
                            f"Yahoo covers it ({r['hit_pct']}% of sample)"))
        else:
            needs_vendor.append((ex, r["total"],
                                 f"free path only {r['hit_pct']}% — EODHD "
                                 "would genuinely add these"))
    rep.log("── FREE (do not pay):")
    for ex, n, why in free_ok:
        rep.log(f"  ✅ {ex:12s} {n:4d} symbols — {why}")
    rep.log("── VENDOR WOULD HELP:")
    for ex, n, why in needs_vendor:
        rep.log(f"  💰 {ex:12s} {n:4d} symbols — {why}")
    rep.log("── NOT WORTH BUYING:")
    for ex, n, why in unbuyable:
        rep.log(f"  ❌ {ex:12s} {n:4d} symbols — {why}")

    n_free = sum(n for _, n, _ in free_ok)
    n_vendor = sum(n for _, n, _ in needs_vendor)
    n_never = sum(n for _, n, _ in unbuyable)
    rep.kv(symbols_free=n_free, symbols_vendor_helps=n_vendor,
           symbols_not_worth_buying=n_never)
    if n_vendor < 150:
        rep.ok(f"only {n_vendor} symbols genuinely need a vendor — the "
               "€19.99 EOD tier is the ceiling of what is justified, and "
               "the $59/$99 tiers buy fundamentals his engines do not read")
    else:
        rep.ok(f"{n_vendor} symbols would come from EODHD's EOD tier")

    for w in warns:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
