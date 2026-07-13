"""ops 3236 — reuse the PROVEN searcher: dbn_search_factory (dataset
/search → per-dataset series drill, hard country-token reject — the same
machinery that landed the CLI and INTR families in 3190-3192) pointed at
the four blockers with euro-area/DE tokens. Probe-gated; fleet; wakes."""
import json
import sys
import time
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
TARGETS = [
    ("ECONOMICS:EUBCOI", "industrial confidence indicator",
     "euro area", ("EA19", "EA20", "EA", "U2"), "EA industry confidence"),
    ("ECONOMICS:EUMPRYY", "monetary aggregate M3 annual growth",
     "euro area", ("U2", "EA19", "EA20"), "EA M3 growth"),
    ("ECONOMICS:DEIFOE", "ifo business climate expectations",
     "Germany", ("DE", "DEU"), "Ifo expectations"),
    ("ECONOMICS:DEZCC", "ZEW economic sentiment situation",
     "Germany", ("DE", "DEU"), "ZEW current conditions"),
]


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3236_proven_search") as rep:
    fails, warns = [], []
    rep.heading("ops 3236 — the proven searcher, four blockers")

    prev = s3_json("data/symbol-map.json") or {}
    mapped = dict(prev.get("map") or {})
    curated = dict(prev.get("curated") or {})
    dry = dict(prev.get("dry") or {})
    cache = dict(prev.get("search_cache") or {})
    idx = s3_json("data/wl-engines.json") or {}
    prev_active = {e["engine_id"] for e in (idx.get("engines") or [])
                   if str(e.get("state")) == "ACTIVE"}
    searcher = SS.dbn_search_factory(cache)
    landed = 0

    rep.section("1. Search → probe")
    for sym, term, country, toks, note in TARGETS:
        if sym in mapped and (mapped[sym].get("note") or "")\
                .startswith(note):
            continue
        sids = searcher(term, country=country, iso=toks)
        rep.log(f"  [{sym}] {len(sids)} candidates")
        for sid in sids:
            if dry.get(sym) == sid:
                continue
            try:
                n = len(SS.fetch("DBNOMICS", sid))
            except Exception:
                n = 0
            rep.log(f"    {sid[:62]:<62} {n}")
            if n >= 60:
                e = {"source": "DBNOMICS", "id": sid, "confidence": 0.8,
                     "note": f"{note} (ops 3236)"}
                mapped[sym] = e
                curated[sym] = e
                landed += 1
                rep.ok(f"{sym} → {sid[:58]}")
                break
    rep.kv(curations=landed)

    if landed:
        rep.section("2. Write + fleet — wakes")
        wl = s3_json("data/tv-watchlists.json") or {}
        uniq = sorted({s.upper() for l in (wl.get("lists") or [])
                       if not str(l.get("id", "")).startswith("e2e-")
                       for s in (l.get("symbols") or [])})
        cov = round(100 * len(mapped) / len(uniq), 1) if uniq else 0
        S3.put_object(Bucket=BUCKET, Key="data/symbol-map.json",
                      Body=json.dumps({**{k: prev.get(k) for k in
                                          ("licensed_econ",
                                           "usi_intraday_only",
                                           "retired") if k in prev},
                                       "generated_at":
                                           datetime.now(timezone.utc)
                                           .isoformat(),
                                       "coverage_pct": cov,
                                       "map": mapped, "curated": curated,
                                       "dry": dry,
                                       "search_cache": cache,
                                       "note": "ops 3236"}),
                      ContentType="application/json")
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
            for w in woken[:8]:
                nm = next((e.get("name") for e in eng2
                           if e.get("engine_id") == w), w)
                rep.log(f"  ⏰ WOKE: {nm}")
            if woken:
                rep.ok(f"{len(woken)} panels WOKEN")
        else:
            warns.append("index not fresh in window")
    else:
        warns.append("all four stay open — search cache carries the "
                     "attempts; these need bespoke provider work")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
