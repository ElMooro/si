"""ops 3239 — two census families closed with existing machinery:

  A. INDEX breadth tiles (S5TH/MMFI/R3TH/NCTH/S5FI/HLUS…) — barchart-
     style '% above MA' and high-low series we ALREADY COMPUTE from the
     Polygon grouped feed. MMFI maps exactly (PCT_ABOVE_50DMA); the
     index-scoped %>200d tiles map to the all-market computed series
     with an explicit universe-proxy note (the 0.6-confidence precedent
     ops 3194 set for exchange-scoped tiles); HLUS = NEW_HIGHS −
     NEW_LOWS via the cross-source minus transform. Internals keys are
     LISTED first; nothing maps to a key that isn't there.
  B. *CCI consumer confidence — direct drill on the now-proven OECD/MEI
     dataset (skip the outer dataset-search that mismatched), hard
     ISO3-in-code reject, probe-gated.

Fleet run; wakes by name.
"""
import json
import re
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
import series_source as SS  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
UA = {"User-Agent": "jh-ops-3239"}
CCI_CTY = {"JP": ("Japan", "JPN"), "FR": ("France", "FRA"),
           "ES": ("Spain", "ESP"), "SE": ("Sweden", "SWE"),
           "DE": ("Germany", "DEU"), "GB": ("United Kingdom", "GBR"),
           "KR": ("Korea", "KOR"), "NL": ("Netherlands", "NLD"),
           "IT": ("Italy", "ITA"), "CH": ("Switzerland", "CHE"),
           "AU": ("Australia", "AUS"), "CN": ("China", "CHN")}


def s3_json(key, default=None, gz=False):
    try:
        import gzip
        b = S3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        if gz:
            b = gzip.decompress(b)
        return json.loads(b)
    except Exception:
        return default


def mei_drill(country_name, iso3):
    url = ("https://api.db.nomics.world/v22/series/OECD/MEI?limit=8"
           "&observations=0&q="
           + urllib.parse.quote(f"consumer confidence {country_name}"))
    try:
        j = json.loads(urllib.request.urlopen(
            urllib.request.Request(url, headers=UA), timeout=20).read())
    except Exception:
        return []
    docs = ((j.get("series") or {}).get("docs") or j.get("docs") or [])
    out = []
    for d in docs:
        sc = str(d.get("series_code") or "")
        if iso3 in sc.replace("-", ".").split("."):
            out.append(f"OECD/MEI/{sc}")
    return out[:3]


with report("3239_breadth_cci") as rep:
    fails, warns = [], []
    rep.heading("ops 3239 — breadth tiles from our own internals + CCI "
                "drill")

    prev = s3_json("data/symbol-map.json") or {}
    mapped = dict(prev.get("map") or {})
    curated = dict(prev.get("curated") or {})
    dry = dict(prev.get("dry") or {})
    idx = s3_json("data/wl-engines.json") or {}
    prev_active = {e["engine_id"] for e in (idx.get("engines") or [])
                   if str(e.get("state")) == "ACTIVE"}
    names_doc = s3_json("data/symbol-dictionary.json") or {}
    names = names_doc.get("dictionary") or names_doc.get("symbols") or {}
    landed = 0

    # ── A. internals keys listed, then honest mappings ────────────────
    rep.section("A1. What the internals feed actually serves")
    internals = (s3_json("data/market-internals.json") or {})\
        .get("series") or {}
    for k in sorted(internals):
        rep.log(f"  {k:<22} {len(internals[k] or {})} pts")

    rep.section("A2. INDEX tiles — names + honest curations")
    PLAN = []
    if "PCT_ABOVE_50DMA" in internals:
        PLAN.append(("INDEX:MMFI", "INTERNALS", "PCT_ABOVE_50DMA",
                     0.8, "% above 50d — computed (exact concept)"))
        PLAN.append(("INDEX:S5FI", "INTERNALS", "PCT_ABOVE_50DMA",
                     0.55, "S&P %>50d tile → all-market computed "
                           "(universe proxy)"))
    if "PCT_ABOVE_200DMA" in internals:
        for t in ("INDEX:S5TH", "INDEX:R3TH", "INDEX:NCTH",
                  "INDEX:MMTH"):
            PLAN.append((t, "INTERNALS", "PCT_ABOVE_200DMA",
                         0.55 if t != "INDEX:MMTH" else 0.8,
                         "%>200d tile → all-market computed"
                         + ("" if t == "INDEX:MMTH"
                            else " (universe proxy)")))
    if "NEW_HIGHS" in internals and "NEW_LOWS" in internals:
        PLAN.append(("INDEX:HLUS", "DERIVED",
                     "INTERNALS~NEW_HIGHS~minus~INTERNALS~NEW_LOWS",
                     0.7, "US net new highs (computed H−L)"))
    for sym, src, sid, conf, note in PLAN:
        nm = names.get(sym)
        if isinstance(nm, dict):
            nm = nm.get("name")
        rep.log(f"  {sym:<14} dict='{str(nm)[:38]}'")
        if sym in curated:
            continue
        try:
            n = len(SS.fetch(src, sid))
        except Exception:
            n = 0
        if n >= 150:
            e = {"source": src, "id": sid, "confidence": conf,
                 "note": note + " (ops 3239)"}
            mapped[sym] = e
            curated[sym] = e
            landed += 1
            rep.ok(f"{sym} → {sid[:48]}  ({n})")
        else:
            rep.log(f"    ✗ probe {n} pts — skipped")

    # ── B. CCI direct drill ────────────────────────────────────────────
    rep.section("B. Consumer-confidence drill on OECD/MEI")
    t0 = time.time()
    for cc, (cname, iso3) in CCI_CTY.items():
        if time.time() - t0 > 200:
            warns.append("CCI budget reached")
            break
        sym = f"ECONOMICS:{cc}CCI"
        if sym in curated:
            continue
        for sid in mei_drill(cname, iso3):
            if dry.get(sym) == sid:
                continue
            try:
                n = len(SS.fetch("DBNOMICS", sid))
            except Exception:
                n = 0
            if n >= 60:
                e = {"source": "DBNOMICS", "id": sid, "confidence": 0.8,
                     "note": f"{cname} consumer confidence (ops 3239)"}
                mapped[sym] = e
                curated[sym] = e
                landed += 1
                rep.ok(f"{sym} → {sid[:52]}  ({n})")
                break
    rep.kv(curations=landed)

    # ── fleet ──────────────────────────────────────────────────────────
    if landed:
        rep.section("Fleet — wakes by name")
        wl = s3_json("data/tv-watchlists.json") or {}
        uniq = sorted({s.upper() for l in (wl.get("lists") or [])
                       if not str(l.get("id", "")).startswith("e2e-")
                       for s in (l.get("symbols") or [])})
        cov = round(100 * len(mapped) / len(uniq), 1) if uniq else 0
        S3.put_object(Bucket=BUCKET, Key="data/symbol-map.json",
                      Body=json.dumps({**{k: prev.get(k) for k in
                                          ("licensed_econ",
                                           "usi_intraday_only", "retired",
                                           "search_cache") if k in prev},
                                       "generated_at":
                                           datetime.now(timezone.utc)
                                           .isoformat(),
                                       "coverage_pct": cov,
                                       "map": mapped, "curated": curated,
                                       "dry": dry, "note": "ops 3239"}),
                      ContentType="application/json")
        rep.kv(coverage_now=cov)
        mark = datetime.now(timezone.utc).isoformat()
        try:
            LAM.invoke(FunctionName="justhodl-wl-engines",
                       InvocationType="Event", Payload=b"{}")
        except Exception as e:
            fails.append(f"invoke: {str(e)[:70]}")
        idx2 = None
        for _ in range(70):
            time.sleep(10)
            d = s3_json("data/wl-engines.json") or {}
            if str(d.get("generated_at", "")) > mark:
                idx2 = d
                break
        if idx2:
            eng2 = idx2.get("engines") or []
            act2 = {e["engine_id"] for e in eng2
                    if str(e.get("state")) == "ACTIVE"}
            woken = sorted(act2 - prev_active)
            rep.kv(active_before=len(prev_active),
                   active_now=len(act2), woken=len(woken))
            for w in woken[:10]:
                nm2 = next((e.get("name") for e in eng2
                            if e.get("engine_id") == w), w)
                rep.log(f"  ⏰ WOKE: {nm2}")
            if woken:
                rep.ok(f"{len(woken)} panels WOKEN")
        else:
            warns.append("index not fresh in window")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
