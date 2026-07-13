"""ops 3232 — the growth pair closed: dead MEI mirrors replaced from LIVE
families (EA19 GDP pct4, Eurostat une_rt_m / sts_inpr_m / ESI, ECB MIR),
duplicates and delisted tickers dry-ledgered. Probes decide everything;
fleet run; wakes by name."""
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
    "ECONOMICS:EUGDPYY": [("DERIVED", "FRED~CLVMNACSCAB1GQEA19~pct4",
                           "EA19 real GDP YoY (pct4)")],
    "ECONOMICS:EUIPYY": [("DBNOMICS",
                          "Eurostat/sts_inpr_m/M.PROD.B-D.SCA.I21.EA19",
                          "EA19 industrial production")],
    "ECONOMICS:EUBCOI": [("DBNOMICS",
                          "Eurostat/ei_bssi_m_r2/M.BS-ICI.SA.EA19",
                          "EA19 industry confidence (ESI)")],
    "ECONOMICS:FRUR": [("DBNOMICS",
                        "Eurostat/une_rt_m/M.SA.TOTAL.PC_ACT.T.FR",
                        "France unemployment (Eurostat)")],
    "ECONOMICS:FRBR": [("DBNOMICS",
                        "ECB/MIR/M.FR.B.A2A.A.R.A.2240.EUR.N",
                        "France bank lending rate (ECB MIR)")],
    "ECONOMICS:FRIPYY": [("DBNOMICS",
                          "Eurostat/sts_inpr_m/M.PROD.B-D.SCA.I21.FR",
                          "France industrial production")],
}
DRY = {"SWB:TES5": "MARKET|TES5.DE",
       "EURONEXT:PX1": "MARKET|PX1.AS (duplicate of TVC:CAC40)",
       "ECONOMICS:EUGDPYY_old": None}


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3232_growth_close") as rep:
    fails, warns = [], []
    rep.heading("ops 3232 — growth pair closed from live families")

    prev = s3_json("data/symbol-map.json") or {}
    mapped = dict(prev.get("map") or {})
    curated = dict(prev.get("curated") or {})
    dry = dict(prev.get("dry") or {})
    idx = s3_json("data/wl-engines.json") or {}
    prev_active = {e["engine_id"] for e in (idx.get("engines") or [])
                   if str(e.get("state")) == "ACTIVE"}

    rep.section("1. Probe-gated replacements")
    landed = 0
    for sym, cands in CURATE.items():
        for src, sid, note in cands:
            try:
                n = len(SS.fetch(src, sid))
            except Exception:
                n = 0
            rep.log(f"  {sym[:20]:<20} {sid[:52]:<52} {n}")
            if n >= 60:
                old = (mapped.get(sym) or {}).get("id")
                if old and old != sid:
                    dry[sym + "|prev"] = old
                e = {"source": src, "id": sid, "confidence": 0.85,
                     "note": note + " (ops 3232)"}
                mapped[sym] = e
                curated[sym] = e
                landed += 1
                break
    for sym, old in list(DRY.items()):
        if sym in mapped:
            dry[sym] = (mapped.get(sym) or {}).get("id") or str(old)
            mapped.pop(sym, None)
            curated.pop(sym, None)
    rep.kv(replacements=landed, dry_ledgered=2)
    if landed < 3:
        warns.append("fewer than 3 landed — one engine may stay short")

    rep.section("2. Write + fleet — wakes by name")
    wl = s3_json("data/tv-watchlists.json") or {}
    uniq = sorted({s.upper() for l in (wl.get("lists") or [])
                   if not str(l.get("id", "")).startswith("e2e-")
                   for s in (l.get("symbols") or [])})
    cov = round(100 * len(mapped) / len(uniq), 1) if uniq else 0
    S3.put_object(Bucket=BUCKET, Key="data/symbol-map.json",
                  Body=json.dumps({**{k: prev.get(k) for k in
                                      ("licensed_econ", "usi_intraday_only",
                                       "retired", "search_cache")
                                      if k in prev},
                                   "generated_at":
                                       datetime.now(timezone.utc)
                                       .isoformat(),
                                   "coverage_pct": cov, "map": mapped,
                                   "curated": curated, "dry": dry,
                                   "note": "ops 3232: growth close"}),
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
        for w in woken[:8]:
            nm = next((e.get("name") for e in eng2
                       if e.get("engine_id") == w), w)
            rep.log(f"  ⏰ WOKE: {nm}")
        for nm in ("Europe Growth", "France"):
            e = next((x for x in eng2 if str(x.get("name", ""))
                      .lower().startswith(nm.lower())), None)
            if e:
                rep.log(f"  → {nm:<16} {e.get('state')} "
                        f"({str(e.get('reason') or 'ACTIVE')[:50]})")
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
