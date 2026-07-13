"""ops 3234 — browse, don't guess. The last two dimension-guesses were
dry, so this ops reads DBnomics' OWN structures: the ei_bssi dataset's
dimension codes are browsed and the EUBCOI key ASSEMBLED from what
actually exists; EUMPRYY / DEIFOE / DEZCC go through the series-search
index with the candidate NAMES printed as evidence before any probe.
Anything that lands is curated; the fleet runs; wakes by name.

Targets (each is the last blocker of a named engine):
  · ECONOMICS:EUBCOI  → Eurostat ei_bssi_m_r2, key assembled from dims
  · ECONOMICS:EUMPRYY → ECB M3 annual growth via search
  · ECONOMICS:DEIFOE  → Ifo business expectations via search
  · ECONOMICS:DEZCC   → ZEW current conditions via search
"""
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
UA = {"User-Agent": "jh-ops-3234"}


def get_json(url):
    req = urllib.request.Request(url, headers=UA)
    return json.loads(urllib.request.urlopen(req, timeout=20).read())


def search(q, must=(), reject=(), limit=12):
    """DBnomics series search → [(sid, name)] filtered by name tokens."""
    url = f"{API}/search?q={urllib.parse.quote(q)}&limit={limit}"
    try:
        docs = ((get_json(url).get("results") or {}).get("docs")
                or get_json(f"{API}/series?q={urllib.parse.quote(q)}"
                            f"&limit={limit}")
                .get("series", {}).get("docs") or [])
    except Exception:
        try:
            docs = get_json(f"{API}/series?q={urllib.parse.quote(q)}"
                            f"&limit={limit}") \
                .get("series", {}).get("docs") or []
        except Exception:
            return []
    out = []
    for d in docs:
        nm = str(d.get("series_name") or d.get("name") or "")
        low = nm.lower()
        if must and not all(m in low for m in must):
            continue
        if any(r in low for r in reject):
            continue
        sid = (f"{d.get('provider_code')}/{d.get('dataset_code')}/"
               f"{d.get('series_code')}")
        if "None" not in sid:
            out.append((sid, nm))
    return out


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3234_browse_probe") as rep:
    fails, warns = [], []
    rep.heading("ops 3234 — browse DBnomics' own structures, then probe")

    prev = s3_json("data/symbol-map.json") or {}
    mapped = dict(prev.get("map") or {})
    curated = dict(prev.get("curated") or {})
    dry = dict(prev.get("dry") or {})
    idx = s3_json("data/wl-engines.json") or {}
    prev_active = {e["engine_id"] for e in (idx.get("engines") or [])
                   if str(e.get("state")) == "ACTIVE"}
    landed = 0

    def try_curate(sym, cands, note_prefix):
        global landed
        for sid, nm in cands[:6]:
            if dry.get(sym) == sid:
                continue
            try:
                n = len(SS.fetch("DBNOMICS", sid))
            except Exception:
                n = 0
            rep.log(f"    {sid[:58]:<58} {n:>5}  {nm[:40]}")
            if n >= 60:
                e = {"source": "DBNOMICS", "id": sid, "confidence": 0.8,
                     "note": f"{note_prefix}: {nm[:60]} (ops 3234)"}
                mapped[sym] = e
                curated[sym] = e
                landed += 1
                rep.ok(f"{sym} → {sid[:56]}")
                return True
        return False

    # ── 1. EUBCOI: assemble from the dataset's own dims ───────────────
    rep.section("1. EUBCOI — ei_bssi_m_r2 dims browsed")
    try:
        ds = get_json(f"{API}/datasets/Eurostat/ei_bssi_m_r2")
        meta = (ds.get("datasets") or {}).get("Eurostat/ei_bssi_m_r2") \
            or next(iter((ds.get("datasets") or {}).values()), {})
        order = meta.get("dimensions_codes_order") or []
        vals = meta.get("dimensions_values_labels") or {}
        rep.log(f"  dims order: {order}")
        pick = {}
        for dim in order:
            dv = vals.get(dim) or {}
            codes = list(dv.keys()) if isinstance(dv, dict) else \
                [c[0] for c in dv]
            choice = None
            for want in (("M",), ("BS-ICI", "BS-ICI-BAL"), ("SA",),
                         ("EA19", "EA20", "EA")):
                pass
            if dim.lower().startswith("freq"):
                choice = "M" if "M" in codes else codes[0]
            elif "indic" in dim.lower():
                choice = next((c for c in codes if "ICI" in c), None) \
                    or next((c for c in codes if "COF" in c or "IND" in c),
                            codes[0] if codes else None)
            elif "s_adj" in dim.lower() or "adj" in dim.lower():
                choice = "SA" if "SA" in codes else \
                    ("NSA" if "NSA" in codes else codes[0])
            elif "geo" in dim.lower():
                choice = next((c for c in ("EA19", "EA20", "EA")
                               if c in codes), None)
            else:
                choice = codes[0] if codes else None
            pick[dim] = choice
            rep.log(f"    {dim}: {choice}  (of {len(codes)} codes)")
        if all(pick.values()):
            key = ".".join(pick[d] for d in order)
            try_curate("ECONOMICS:EUBCOI",
                       [(f"Eurostat/ei_bssi_m_r2/{key}",
                         "assembled from dims")],
                       "EA industry confidence")
    except Exception as e:
        warns.append(f"ei_bssi browse: {str(e)[:70]}")
    if "ECONOMICS:EUBCOI" not in mapped:
        try_curate("ECONOMICS:EUBCOI",
                   search("industrial confidence indicator euro area",
                          must=("confidence",), reject=("turkey", "poland",
                                                        "czech")),
                   "EA industry confidence (search)")

    # ── 2-4. searches with names as evidence ──────────────────────────
    rep.section("2. EUMPRYY — ECB M3 growth via search")
    try_curate("ECONOMICS:EUMPRYY",
               search("monetary aggregate M3 annual growth rate euro",
                      must=("m3",), reject=("m1", "m2 ", "poland",
                                            "czech", "denmark")),
               "EA M3 growth")

    rep.section("3. DEIFOE — Ifo expectations via search")
    try_curate("ECONOMICS:DEIFOE",
               search("ifo business expectations germany",
                      must=("ifo",), reject=()),
               "Ifo expectations")

    rep.section("4. DEZCC — ZEW current conditions via search")
    try_curate("ECONOMICS:DEZCC",
               search("ZEW current economic situation germany",
                      must=("zew",), reject=()),
               "ZEW current conditions")

    rep.kv(curations=landed)

    # ── 5. fleet if anything landed ────────────────────────────────────
    if landed:
        rep.section("5. Write + fleet — wakes by name")
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
                                       "note": "ops 3234: browse-probe"}),
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
            for w in woken[:8]:
                nm = next((e.get("name") for e in eng2
                           if e.get("engine_id") == w), w)
                rep.log(f"  ⏰ WOKE: {nm}")
            if woken:
                rep.ok(f"{len(woken)} panels WOKEN")
        else:
            warns.append("index not fresh in window")
    else:
        warns.append("nothing landed — all four stay open with the "
                     "browse evidence above")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
