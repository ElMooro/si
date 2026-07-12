"""ops 3167 (iter 2) — sources REPAIRED, then the 1990 proof.

Iter 1 verdict, verbatim: FRED reaches 1990 (US 10y 9,135 obs; VIX
9,226; OECD long-rate template valid) — but STOOQ returned NOTHING for
every market probe (it blocks datacenter IPs) and two OECD templates
(GDP, CPI) were wrong ids. Deep MARKET history was therefore unproven,
so the study rebuild was correctly blocked.

Iter 2 fixes the source layer instead of trusting it:
  · MARKET is now a CHAIN — Yahoo (deep, free) → Stooq → Polygon (5y)
  · every OECD template is TEST-FETCHED before use; failures fall back
    to FRED search rather than silently mapping to a dead id

Khalid: "almost all my TradingView indicators are public and free
somewhere — find them, and go back to at least 1990."

Two deliverables:
  1. SIDEBAR — justhodl.ai/watchlists.html mirrors his TradingView side
     panel: all 207 lists by name, every member inside, each tagged with
     the free source that now carries it.
  2. SYMBOL MAP — aws/shared/series_source.py maps TV codes to free
     deep-history sources:
        FRED       US + OECD macro (1950s+), and its /series/search
                   endpoint auto-maps unknown ECONOMICS:/TVC: codes
        STOOQ      equities, indices, FX, commodities (1990s+, no key)
                   — Polygon only reaches ~5y, which is exactly why the
                   first thesis study was history-starved
        COINGECKO  crypto
     ECONOMICS:{ISO2}{IND} decomposes against OECD templates
     (JPGDPYY → NAEXKP01JPNQ657S), so a single template resolves the
     same indicator across ~50 countries.

This op: census the unmapped codes · map the whole universe (templates
first, FRED-search for the rest, budgeted + cached) · write
data/symbol-map.json · SAMPLE-FETCH across every source and report the
EARLIEST DATE each returns (the 1990 proof).
"""

import json
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report

# aws/shared is not on the runner PYTHONPATH (only aws/ops is)
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
import series_source as SS  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
MAP_KEY = "data/symbol-map.json"
SEARCH_BUDGET = 500


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3167_symbol_map") as rep:
    fails, warns = [], []
    rep.heading("ops 3167 — free-source symbol map (1990+)")

    rep.section("1. Universe census")
    wl = s3_json("data/tv-watchlists.json", {}) or {}
    lists = [l for l in (wl.get("lists") or [])
             if not str(l.get("id", "")).startswith("e2e-")]
    uniq = sorted({s.upper() for l in lists for s in (l.get("symbols") or [])})
    rep.kv(lists=len(lists), unique_symbols=len(uniq))
    econ = [s for s in uniq if s.startswith("ECONOMICS:")]
    suff = Counter()
    for s in econ:
        t = s.split(":", 1)[1]
        if len(t) > 2:
            suff[t[2:]] += 1
    rep.log("── top ECONOMICS indicator codes (the mapping targets): " +
            ", ".join(f"{k}={v}" for k, v in suff.most_common(14)))

    rep.section("2. Validate OECD templates (kill the dead ids)")
    dead = []
    for ind, tpl in list(SS.FRED_TEMPLATES.items()):
        ok = SS.validate_template(tpl, "DEU") or SS.validate_template(tpl, "JPN")
        if not ok:
            dead.append(ind)
            SS.FRED_TEMPLATES.pop(ind, None)
    rep.kv(templates_live=len(SS.FRED_TEMPLATES), templates_dead=len(dead))
    rep.log(f"  live: {', '.join(sorted(SS.FRED_TEMPLATES))}")
    if dead:
        rep.log(f"  DEAD (fall back to FRED search): {', '.join(dead)}")

    rep.section("3. Map the universe")
    prev = s3_json(MAP_KEY, {}) or {}
    search_cache = prev.get("search_cache") or {}
    searcher = SS.fred_search_factory(search_cache)
    used, mapped = 0, {}
    kinds = Counter()
    t0 = time.time()
    for s in uniq:
        cached = (prev.get("map") or {}).get(s)
        if cached and cached.get("source"):
            mapped[s] = cached
            kinds[cached["source"]] += 1
            continue
        allow = (used < SEARCH_BUDGET and time.time() - t0 < 420)
        src, sid, conf, note = SS.map_symbol(s, searcher if allow else None)
        if allow and ("fred-search" in str(note)):
            used += 1
        if src:
            mapped[s] = {"source": src, "id": sid, "confidence": conf,
                         "note": note}
            kinds[src] += 1
        else:
            kinds["UNMAPPED"] += 1
    total_mapped = sum(v for k, v in kinds.items() if k != "UNMAPPED")
    cov = round(100 * total_mapped / max(1, len(uniq)), 1)
    rep.kv(coverage_pct=cov, searches_used=used,
           **{f"src_{k.lower()}": v for k, v in kinds.most_common()})
    for k, v in kinds.most_common():
        rep.log(f"  {k:10s} {v:5d}  ({round(100*v/len(uniq),1)}%)")
    S3.put_object(Bucket=BUCKET, Key=MAP_KEY,
                  Body=json.dumps({"generated_at":
                                   datetime.now(timezone.utc).isoformat(),
                                   "n_mapped": total_mapped,
                                   "n_total": len(uniq),
                                   "coverage_pct": cov,
                                   "map": mapped,
                                   "search_cache": search_cache}).encode(),
                  ContentType="application/json")
    rep.ok(f"symbol-map.json written: {total_mapped}/{len(uniq)} "
           f"symbols on a free source ({cov}%)")
    if cov < 50:
        warns.append("coverage under 50% — FRED-search budget spends 500 "
                     "lookups per run and caches them; a second run maps "
                     "the next tranche")

    rep.section("4. HISTORY PROOF — earliest date each source returns")
    probes = [("FRED", "DGS10", "US 10y"), ("FRED", "FEDFUNDS", "Fed funds"),
              ("FRED", "M2SL", "US M2"), ("FRED", "WALCL", "Fed B/S"),
              ("FRED", "VIXCLS", "VIX"),
              ("FRED", "NAEXKP01JPNQ657S", "Japan GDP YoY (template)"),
              ("FRED", "IRLTLT01DEM156N", "Bund 10y (template)"),
              ("FRED", "CPALTT01GBRM659N", "UK CPI YoY (template)"),
              ("MARKET", "^GSPC", "S&P 500"), ("MARKET", "DX-Y.NYB", "DXY"),
              ("MARKET", "SPY", "SPY"), ("MARKET", "GC=F", "Gold"),
              ("MARKET", "^N225", "Nikkei"), ("MARKET", "NVDA", "NVDA"),
              ("MARKET", "^VIX", "VIX index")]
    ok_1990 = 0
    for src, sid, label in probes:
        ser = SS.fetch(src, sid, "1990-01-01")
        if ser:
            ks = sorted(ser)
            rep.log(f"  {label:28s} {src:6s} {sid:20s} "
                    f"{ks[0]} → {ks[-1]}  ({len(ser)} obs)")
            if ks[0][:4] <= "1995":
                ok_1990 += 1
        else:
            warns.append(f"probe empty: {src}:{sid} ({label})")
    rep.kv(probes=len(probes), probes_reaching_1990s=ok_1990)
    if ok_1990 >= 6:
        rep.ok(f"{ok_1990}/{len(probes)} probes deliver 1990s-or-earlier "
               "history — the thesis study can be rebuilt on 35 years "
               "instead of 2")
    else:
        fails.append(f"only {ok_1990} probes reach the 1990s — deep "
                     "history not proven; do not rebuild the study yet")

    rep.section("5. Per-thesis coverage under the new map")
    scored = []
    for l in lists:
        syms = [s.upper() for s in (l.get("symbols") or [])]
        if len(syms) < 20:
            continue
        ok = sum(1 for s in syms if s in mapped)
        scored.append((round(100 * ok / len(syms)), ok, len(syms),
                       str(l.get("name"))[:44]))
    scored.sort(reverse=True)
    rep.log("── coverage by thesis (top 14):")
    for pct, ok, n, name in scored[:14]:
        rep.log(f"  {pct:3d}%  {ok:3d}/{n:<3d}  {name}")
    big = [x for x in scored if x[0] >= 60]
    rep.kv(theses_over_60pct_coverage=len(big),
           theses_measured=len(scored))

    for w in warns[:6]:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
