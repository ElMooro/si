"""ops 3235 — shape-agnostic browse: 3234's parsers assumed response
shapes DBnomics doesn't use. This version walks ANY JSON recursively for
the dicts that matter ('series_code' docs; 'dimensions_codes_order'
metadata), dumps each endpoint's top-level skeleton for the record, then
probes and curates exactly as before. Wakes by name."""
import json
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
API = "https://api.db.nomics.world/v22"
UA = {"User-Agent": "jh-ops-3235"}


def get_json(url):
    req = urllib.request.Request(url, headers=UA)
    return json.loads(urllib.request.urlopen(req, timeout=20).read())


def walk(o):
    if isinstance(o, dict):
        yield o
        for v in o.values():
            yield from walk(v)
    elif isinstance(o, list):
        for v in o:
            yield from walk(v)


def skeleton(o, depth=0):
    if depth > 1:
        return type(o).__name__
    if isinstance(o, dict):
        return {k: skeleton(v, depth + 1) for k, v in list(o.items())[:6]}
    if isinstance(o, list):
        return [skeleton(o[0], depth + 1)] if o else []
    return type(o).__name__


def search(q, must=(), reject=()):
    out = []
    for ep in (f"{API}/search?q={urllib.parse.quote(q)}&limit=14",
               f"{API}/series?q={urllib.parse.quote(q)}&limit=14"):
        try:
            j = get_json(ep)
        except Exception:
            continue
        for d in walk(j):
            if "series_code" in d and "provider_code" in d:
                nm = str(d.get("series_name") or d.get("name") or "")
                low = nm.lower()
                if must and not all(m in low for m in must):
                    continue
                if any(r in low for r in reject):
                    continue
                sid = (f"{d['provider_code']}/{d.get('dataset_code')}/"
                       f"{d['series_code']}")
                if "None" not in sid and (sid, nm) not in out:
                    out.append((sid, nm))
        if out:
            return out, ep
    return out, None


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3235_shape_agnostic") as rep:
    fails, warns = [], []
    rep.heading("ops 3235 — shape-agnostic browse-then-probe")

    prev = s3_json("data/symbol-map.json") or {}
    mapped = dict(prev.get("map") or {})
    curated = dict(prev.get("curated") or {})
    dry = dict(prev.get("dry") or {})
    idx = s3_json("data/wl-engines.json") or {}
    prev_active = {e["engine_id"] for e in (idx.get("engines") or [])
                   if str(e.get("state")) == "ACTIVE"}
    landed = 0

    rep.section("0. Endpoint skeletons (for the record)")
    for u in (f"{API}/datasets/Eurostat/ei_bssi_m_r2",
              f"{API}/search?q=ifo&limit=2"):
        try:
            rep.log(f"  {u.split('v22/')[1][:44]}: "
                    + json.dumps(skeleton(get_json(u)))[:180])
        except Exception as e:
            rep.log(f"  {u[-40:]}: ERR {str(e)[:60]}")

    def try_curate(sym, q, must, reject, note):
        global landed
        cands, ep = search(q, must, reject)
        rep.log(f"  [{sym}] {len(cands)} candidates"
                + (f" via {ep.split('v22/')[1][:20]}" if ep else ""))
        for sid, nm in cands[:6]:
            if dry.get(sym) == sid:
                continue
            try:
                n = len(SS.fetch("DBNOMICS", sid))
            except Exception:
                n = 0
            rep.log(f"    {sid[:56]:<56} {n:>5}  {nm[:38]}")
            if n >= 60:
                e = {"source": "DBNOMICS", "id": sid, "confidence": 0.8,
                     "note": f"{note}: {nm[:56]} (ops 3235)"}
                mapped[sym] = e
                curated[sym] = e
                landed += 1
                rep.ok(f"{sym} → {sid[:54]}")
                return

    rep.section("1-4. Search-probe the four blockers")
    try_curate("ECONOMICS:EUBCOI",
               "industrial confidence indicator euro area",
               ("confidence",), ("turkey", "poland", "czech", "sweden"),
               "EA industry confidence")
    try_curate("ECONOMICS:EUMPRYY",
               "M3 monetary aggregate annual growth euro area",
               ("m3",), ("m1", "denmark", "poland", "czech"),
               "EA M3 growth")
    try_curate("ECONOMICS:DEIFOE", "ifo business expectations",
               ("ifo",), (), "Ifo expectations")
    try_curate("ECONOMICS:DEZCC", "ZEW economic situation germany",
               ("zew",), (), "ZEW current conditions")
    rep.kv(curations=landed)

    if landed:
        rep.section("5. Write + fleet — wakes")
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
                                       "dry": dry,
                                       "note": "ops 3235"}),
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
        warns.append("nothing landed — skeletons above show what the API "
                     "actually returns")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
