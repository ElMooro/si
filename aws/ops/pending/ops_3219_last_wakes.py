"""ops 3219 — the last wakes: 3218's member-by-member evidence converted
to curations.

  · Europe Liquidity's dry members are TV EXPRESSION tiles
    (DE10Y-IT10Y, FR10Y-IT10Y, ES10Y-IT10Y) sitting on a failing FORMULA
    path — the two-base 'minus' transform curates them directly from the
    proven FRED IRLTLT bases. 3 wet + 3 spreads = 6 → wake.
  · Global Deposit Rates: EUDIR → FRED ECBDFR (the ECB deposit facility
    rate — the semantically right series, 0.9 confidence), GBDIR →
    candidate ladder starting at the proven-alive IR3TCD family.
  · CME_MINI:DVE2! retired honestly (no free daily source) — Developed
    Markets sleeps at 5/6 until one exists; named, not faked.
All probe-gated; fleet re-run; wakes by name vs 117 ACTIVE.
"""
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
IT = "FRED~IRLTLT01ITM156N"
CURATE = {
    "TVC:DE10Y-TVC:IT10Y": [("DERIVED",
                             f"FRED~IRLTLT01DEM156N~minus~{IT.replace('~', '~')}"
                             .replace("minus~FRED~", "minus~FRED~"),
                             "DE−IT 10Y spread")],
    "TVC:FR10Y-TVC:IT10Y": [("DERIVED",
                             "FRED~IRLTLT01FRM156N~minus~FRED~IRLTLT01ITM156N",
                             "FR−IT 10Y spread")],
    "TVC:ES10Y-TVC:IT10Y": [("DERIVED",
                             "FRED~IRLTLT01ESM156N~minus~FRED~IRLTLT01ITM156N",
                             "ES−IT 10Y spread")],
    "ECONOMICS:EUDIR": [("FRED", "ECBDFR",
                         "ECB deposit facility rate")],
    "ECONOMICS:GBDIR": [("FRED", "IR3TCD01GBM156N",
                         "UK 3M CD rate (OECD MEI, probe-gated)"),
                        ("FRED", "IR3TIB01GBM156N",
                         "UK 3M interbank (fallback)")],
}
CURATE["TVC:DE10Y-TVC:IT10Y"] = [("DERIVED",
                                  "FRED~IRLTLT01DEM156N~minus~FRED~IRLTLT01ITM156N",
                                  "DE−IT 10Y spread")]


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3219_last_wakes") as rep:
    fails, warns = [], []
    rep.heading("ops 3219 — expression tiles curated via minus, deposit "
                "rates laddered, DVE retired")

    prev = s3_json("data/symbol-map.json") or {}
    mapped = dict(prev.get("map") or {})
    curated = dict(prev.get("curated") or {})
    retired = dict(prev.get("retired") or {})
    idx = s3_json("data/wl-engines.json") or {}
    prev_active = {e["engine_id"] for e in (idx.get("engines") or [])
                   if str(e.get("state")) == "ACTIVE"}

    rep.section("1. Probe-gated curations")
    n_ok = 0
    for sym, cands in CURATE.items():
        for src, sid, note in cands:
            try:
                n = len(SS.fetch(src, sid))
            except Exception:
                n = 0
            if n >= 150:
                e = {"source": src, "id": sid, "confidence": 0.9,
                     "note": note + " (ops 3219)"}
                mapped[sym] = e
                curated[sym] = e
                n_ok += 1
                rep.ok(f"{sym[:30]} → {sid[:52]}  ({n})")
                break
        else:
            rep.log(f"  ✗ {sym}: all candidates dry")
    retired["CME_MINI:DVE2!"] = "no_free_daily_source"
    mapped.pop("CME_MINI:DVE2!", None)
    rep.kv(curated_now=n_ok, dve="retired (no free source)")
    if n_ok < 4:
        warns.append("fewer curations landed than expected — wakes may "
                     "fall short")

    rep.section("2. Fleet re-run — wakes by name")
    wl = s3_json("data/tv-watchlists.json") or {}
    uniq = sorted({s.upper() for l in (wl.get("lists") or [])
                   if not str(l.get("id", "")).startswith("e2e-")
                   for s in (l.get("symbols") or [])})
    cov = round(100 * len(mapped) / len(uniq), 1) if uniq else 0
    S3.put_object(Bucket=BUCKET, Key="data/symbol-map.json",
                  Body=json.dumps({**{k: prev.get(k) for k in
                                      ("licensed_econ", "usi_intraday_only",
                                       "dry", "search_cache") if k in prev},
                                   "generated_at":
                                       datetime.now(timezone.utc)
                                       .isoformat(),
                                   "coverage_pct": cov, "map": mapped,
                                   "curated": curated, "retired": retired,
                                   "note": "ops 3219: last wakes"}),
                  ContentType="application/json")
    rep.kv(coverage_now=cov)
    mark = datetime.now(timezone.utc).isoformat()
    try:
        LAM.invoke(FunctionName="justhodl-wl-engines",
                   InvocationType="Event", Payload=b"{}")
    except Exception as e:
        fails.append(f"invoke: {str(e)[:70]}")
    idx2 = None
    for _ in range(60):
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
        if woken:
            rep.ok(f"{len(woken)} panels WOKEN")
        else:
            warns.append("no wakes — read the two engines' fresh reasons")
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
