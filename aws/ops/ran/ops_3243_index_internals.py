"""ops 3243 — the INDEX: cluster through the internals table + fusion
re-kicked onto the 131-active index.

  A. 136 INDEX: misses are mapped-to-dead-ids tiles whose mnemonics are
     the SAME ones the USI: branch already routes to our Polygon-computed
     internals (ADVN→ADVANCERS, DECN→DECLINERS, TRIN, UVOL/DVOL,
     HIGH/LOW, ADVDEC, MCCL→derived McClellan…). Apply that table to the
     INDEX: cluster, probe-gated ≥200 pts, unmatched names printed for
     the record.
  B. wl-fusion cron is TUE-SAT and active grew 121→131 since its last
     kick — re-kick + verify fresh so the 18 consumers, panels boards
     and the site-wide rail chip carry tonight's full truth.

Fleet run if anything curated; wakes by name.
"""
import gzip
import json
import re
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
MNEMO = {"ADV": "ADVANCERS", "ADVN": "ADVANCERS", "ADVQ": "ADVANCERS",
         "ACTV": "ADVANCERS", "DECL": "DECLINERS", "DECN": "DECLINERS",
         "DECQ": "DECLINERS", "DEC": "DECLINERS", "UNCH": "UNCHANGED",
         "ADVDEC": "ADVDEC_LINE", "UVOL": "UP_VOLUME",
         "DVOL": "DOWN_VOLUME", "TRIN": "TRIN",
         "HIGH": "NEW_HIGHS", "LOW": "NEW_LOWS",
         "NEWHI": "NEW_HIGHS", "NEWLO": "NEW_LOWS",
         "MMFI": "PCT_ABOVE_50DMA", "MMTH": "PCT_ABOVE_200DMA"}


def s3_json(key, default=None, gz=False):
    try:
        b = S3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        if gz:
            b = gzip.decompress(b)
        return json.loads(b)
    except Exception:
        return default


with report("3243_index_internals") as rep:
    fails, warns = [], []
    rep.heading("ops 3243 — INDEX: cluster through the internals table "
                "+ fusion re-kick")

    prev = s3_json("data/symbol-map.json") or {}
    mapped = dict(prev.get("map") or {})
    curated = dict(prev.get("curated") or {})
    dry = dict(prev.get("dry") or {})
    st = s3_json("data/thesis-state-v2.json.gz", {}, gz=True) or {}
    misses = st.get("misses") or {}
    names_doc = s3_json("data/symbol-dictionary.json") or {}
    names = names_doc.get("dictionary") or names_doc.get("symbols") or {}
    idx = s3_json("data/wl-engines.json") or {}
    prev_active = {e["engine_id"] for e in (idx.get("engines") or [])
                   if str(e.get("state")) == "ACTIVE"}

    def nm(s):
        v = names.get(s)
        return (v.get("name") if isinstance(v, dict) else v) or ""

    rep.section("A. INDEX: misses → internals mnemonics")
    landed, unmatched = 0, []
    idx_miss = sorted(s for s in misses if s.startswith("INDEX:"))
    rep.kv(index_misses=len(idx_miss))
    for s in idx_miss:
        t = s.split(":", 1)[1]
        base = re.sub(r"\.(US|NY|NQ)$", "", t)
        m = MNEMO.get(base)
        tr = None
        if not m and ("MCCL" in base or base in ("MCO", "MCSUM")
                      or "SUMMATION" in base):
            tr = "mcclellan_sum" if "SUM" in base else "mcclellan_osc"
        if not m and not tr:
            b2 = base[:-1] if len(base) > 3 and base[-1] in "QNA" else base
            m = MNEMO.get(b2)
        if not m and not tr:
            unmatched.append(f"{t}'{nm(s)[:18]}'")
            continue
        if tr:
            src, sid = "DERIVED", f"INTERNALS~ADVDEC_LINE~{tr}"
            note = f"{tr} over computed A/D line"
            conf = 0.7
        else:
            src, sid = "INTERNALS", m
            note = "INDEX tile → all-market computed internals"
            conf = 0.6
        cur = (mapped.get(s) or {}).get("id")
        if cur == sid or dry.get(s) == sid:
            continue
        try:
            n = len(SS.fetch(src, sid))
        except Exception:
            n = 0
        if n >= 200:
            mapped[s] = {"source": src, "id": sid, "confidence": conf,
                         "note": note + " (ops 3243)"}
            curated[s] = mapped[s]
            landed += 1
            rep.log(f"  ✓ {s:<18} → {sid[:36]:<36} ({n})")
    rep.kv(curated_now=landed, unmatched=len(unmatched))
    if unmatched:
        rep.log("  unmatched (record): " + " | ".join(unmatched[:14]))

    rep.section("B. Fusion re-kicked onto the 131-active index")
    mark_f = datetime.now(timezone.utc).isoformat()
    try:
        LAM.invoke(FunctionName="justhodl-wl-fusion",
                   InvocationType="Event", Payload=b"{}")
    except Exception as e:
        warns.append(f"fusion invoke: {str(e)[:60]}")
    fus = None
    for _ in range(20):
        time.sleep(5)
        d = s3_json("data/wl-fusion.json") or {}
        if str(d.get("generated_at", "")) > mark_f:
            fus = d
            break
    if fus:
        th = fus.get("themes") or {}
        top = max(th.items(),
                  key=lambda kv: kv[1].get("pressure_pctile") or 0) \
            if th else None
        rep.ok("fusion fresh — top theme "
               f"{top[0]} {top[1].get('pressure_pctile')}p"
               if top else "fusion fresh")
    else:
        warns.append("fusion not fresh in window")

    if landed:
        rep.section("C. Write + fleet — wakes by name")
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
                                       "dry": dry, "note": "ops 3243"}),
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
            for w in woken[:12]:
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
