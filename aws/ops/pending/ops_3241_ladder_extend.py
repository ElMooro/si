"""ops 3241 — 3240's report truncated at the fleet header (the 3228
class: CI success, write unconfirmed). Confirm-or-repair the 9 rescues
in the LIVE map first, then extend the now-proven ladders to the bulk:

  · NASDAQ:NQ* index tiles → ^{t}  (3240's alphabetical cap spent its 25
    slots on CRSP*, all of which LANDED — the NQ* family is untried)
  · SSE:* → {t}.SS  (Shanghai's Yahoo suffix)

Probe-gated ≥200 pts, budgeted, curated with winning path named; fleet
run; wakes by name; active-count printed so 3240's unread effect is
recovered too.
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
R3240 = {  # the nine 3240 rescues, verbatim
    "BER:DX2Z": "DX2Z.DE",
    "NASDAQ:CRSPLCG1": "^CRSPLCG1", "NASDAQ:CRSPLCGT": "^CRSPLCGT",
    "NASDAQ:CRSPLCV1": "^CRSPLCV1", "NASDAQ:CRSPLCVT": "^CRSPLCVT",
    "NASDAQ:CRSPMC1": "^CRSPMC1", "NASDAQ:CRSPMIG1": "^CRSPMIG1",
    "NASDAQ:CRSPMIGT": "^CRSPMIGT", "NASDAQ:CRSPMT1": "^CRSPMT1"}


def s3_json(key, default=None, gz=False):
    try:
        b = S3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        if gz:
            b = gzip.decompress(b)
        return json.loads(b)
    except Exception:
        return default


with report("3241_ladder_extend") as rep:
    fails, warns = [], []
    rep.heading("ops 3241 — confirm 3240, extend the ladders to the bulk")

    prev = s3_json("data/symbol-map.json") or {}
    mapped = dict(prev.get("map") or {})
    curated = dict(prev.get("curated") or {})
    dry = dict(prev.get("dry") or {})
    st = s3_json("data/thesis-state-v2.json.gz", {}, gz=True) or {}
    misses = st.get("misses") or {}
    idx = s3_json("data/wl-engines.json") or {}
    prev_active = {e["engine_id"] for e in (idx.get("engines") or [])
                   if str(e.get("state")) == "ACTIVE"}
    rep.kv(active_at_start=len(prev_active),
           index_generated=str(idx.get("generated_at"))[:19])

    rep.section("1. 3240's nine rescues — confirm or repair")
    repaired = 0
    for sym, cid in R3240.items():
        if (mapped.get(sym) or {}).get("id") == cid:
            continue
        e = {"source": "MARKET", "id": cid, "confidence": 0.6,
             "note": "3240 rescue (re-affirmed 3241)"}
        mapped[sym] = e
        curated[sym] = e
        repaired += 1
    rep.kv(already_in_map=9 - repaired, repaired=repaired)
    if repaired == 0:
        rep.ok("3240's write DID land — truncation was cosmetic again")

    rep.section("2. NQ* and SSE ladders (probe-gated, budgeted)")
    landed, t0 = 0, time.time()

    def ladder(sym, cands, note):
        global landed
        for cid in cands:
            try:
                n = len(SS.fetch("MARKET", cid))
            except Exception:
                n = 0
            if n >= 200:
                e = {"source": "MARKET", "id": cid, "confidence": 0.6,
                     "note": f"{note} (ops 3241)"}
                mapped[sym] = e
                curated[sym] = e
                landed += 1
                rep.log(f"  ✓ {sym:<20} → {cid:<14} ({n})")
                return True
        return False

    nq = sorted(s for s in misses
                if s.startswith("NASDAQ:NQ") and s not in mapped)[:70]
    hit_nq = 0
    for s in nq:
        if time.time() - t0 > 260:
            warns.append("NQ budget reached — remainder next pass")
            break
        t = s.split(":", 1)[1]
        if ladder(s, [f"^{t}"], "Nasdaq index tile → Yahoo ^"):
            hit_nq += 1
    sse = sorted(s for s in misses
                 if s.startswith("SSE:") and s not in mapped)[:30]
    hit_sse = 0
    for s in sse:
        if time.time() - t0 > 380:
            warns.append("SSE budget reached")
            break
        t = s.split(":", 1)[1]
        if ladder(s, [f"{t}.SS"], "Shanghai → Yahoo .SS"):
            hit_sse += 1
    rep.kv(nq_tried=len(nq), nq_hit=hit_nq,
           sse_tried=len(sse), sse_hit=hit_sse,
           curations=landed + repaired)

    rep.section("3. Write + fleet — wakes by name")
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
                                   "dry": dry, "note": "ops 3241"}),
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
        rep.kv(active_before=len(prev_active), active_now=len(act2),
               woken=len(woken))
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
