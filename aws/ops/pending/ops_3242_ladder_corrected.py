"""ops 3242 — 3241's selector bug corrected: misses are BY DEFINITION
mapped-but-dry (that is how they got attempted), so `s not in mapped`
filtered every candidate to zero. The right skip is 'already rescued':
current mapped id == the ladder candidate. NQ* ^-ladder and SSE .SS
ladder re-run with the corrected selector; fleet; wakes by name."""
import gzip
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


def s3_json(key, default=None, gz=False):
    try:
        b = S3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        if gz:
            b = gzip.decompress(b)
        return json.loads(b)
    except Exception:
        return default


with report("3242_ladder_corrected") as rep:
    fails, warns = [], []
    rep.heading("ops 3242 — NQ*/SSE ladders, selector corrected")

    prev = s3_json("data/symbol-map.json") or {}
    mapped = dict(prev.get("map") or {})
    curated = dict(prev.get("curated") or {})
    dry = dict(prev.get("dry") or {})
    st = s3_json("data/thesis-state-v2.json.gz", {}, gz=True) or {}
    misses = st.get("misses") or {}
    idx = s3_json("data/wl-engines.json") or {}
    prev_active = {e["engine_id"] for e in (idx.get("engines") or [])
                   if str(e.get("state")) == "ACTIVE"}
    landed, t0 = 0, time.time()

    def ladder(sym, cands, note):
        global landed
        cur = (mapped.get(sym) or {}).get("id")
        for cid in cands:
            if cid == cur or dry.get(sym) == cid:
                continue
            try:
                n = len(SS.fetch("MARKET", cid))
            except Exception:
                n = 0
            if n >= 200:
                e = {"source": "MARKET", "id": cid, "confidence": 0.6,
                     "note": f"{note} (ops 3242)"}
                mapped[sym] = e
                curated[sym] = e
                landed += 1
                rep.log(f"  ✓ {sym:<20} → {cid:<14} ({n})")
                return True
        return False

    rep.section("1. Ladders (corrected selector)")
    nq = sorted(s for s in misses if s.startswith("NASDAQ:NQ"))[:80]
    hit_nq = 0
    for s in nq:
        if time.time() - t0 > 280:
            warns.append("NQ budget reached — remainder next pass")
            break
        if ladder(s, [f"^{s.split(':', 1)[1]}"],
                  "Nasdaq index tile → Yahoo ^"):
            hit_nq += 1
    sse = sorted(s for s in misses if s.startswith("SSE:"))[:30]
    hit_sse = 0
    for s in sse:
        if time.time() - t0 > 400:
            warns.append("SSE budget reached")
            break
        if ladder(s, [f"{s.split(':', 1)[1]}.SS"],
                  "Shanghai → Yahoo .SS"):
            hit_sse += 1
    rep.kv(nq_tried=len(nq), nq_hit=hit_nq,
           sse_tried=len(sse), sse_hit=hit_sse, curations=landed)

    if landed:
        rep.section("2. Write + fleet — wakes by name")
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
                                       "dry": dry, "note": "ops 3242"}),
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
    else:
        warns.append("zero hits — NQ*/SSE families are not on Yahoo's "
                     "free surface; census stands")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
