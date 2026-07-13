"""ops 3233 — the growth pair, final: PX1 mapped to its TRUE series (PX1
is the CAC40's Euronext ticker — correct mapping, Khalid's panel lists
both), EUGDPGA → EA19 QoQ (pct1, noted non-annualized), EUMPRYY → ECB BSI
M3 annual growth. Probes decide; fleet; wakes by name."""
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
CURATE = {
    "EURONEXT:PX1": [("MARKET", "^FCHI",
                      "CAC40 — PX1 is its Euronext ticker")],
    "ECONOMICS:EUGDPGA": [("DERIVED", "FRED~CLVMNACSCAB1GQEA19~pct1",
                           "EA19 real GDP QoQ% (non-annualized)")],
    "ECONOMICS:EUMPRYY": [("DBNOMICS",
                           "ECB/BSI/M.U2.Y.V.M30.X.I.U2.2300.Z01.A",
                           "EA M3 annual growth (ECB BSI)")],
}


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3233_growth_final") as rep:
    fails, warns = [], []
    rep.heading("ops 3233 — growth pair, final closes")
    prev = s3_json("data/symbol-map.json") or {}
    mapped = dict(prev.get("map") or {})
    curated = dict(prev.get("curated") or {})
    dry = dict(prev.get("dry") or {})
    idx = s3_json("data/wl-engines.json") or {}
    prev_active = {e["engine_id"] for e in (idx.get("engines") or [])
                   if str(e.get("state")) == "ACTIVE"}

    rep.section("1. Probes")
    landed = 0
    for sym, cands in CURATE.items():
        for src, sid, note in cands:
            try:
                n = len(SS.fetch(src, sid))
            except Exception:
                n = 0
            rep.log(f"  {sym[:20]:<20} {sid[:48]:<48} {n}")
            if n >= 60:
                e = {"source": src, "id": sid, "confidence": 0.9,
                     "note": note + " (ops 3233)"}
                mapped[sym] = e
                curated[sym] = e
                dry.pop(sym, None)
                landed += 1
                break
    rep.kv(landed=landed)
    if not landed:
        fails.append("nothing landed")

    if landed:
        rep.section("2. Write + fleet")
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
                                       "note": "ops 3233"}),
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
            for nm in ("Europe Growth", "France"):
                e = next((x for x in eng2 if str(x.get("name", ""))
                          .lower().startswith(nm.lower())), None)
                if e:
                    rep.log(f"  → {nm:<14} {e.get('state')} "
                            f"({str(e.get('reason') or 'ACTIVE')[:46]})")
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
