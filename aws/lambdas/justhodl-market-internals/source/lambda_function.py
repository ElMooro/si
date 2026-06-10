"""
justhodl-market-internals v1.0 — Breadth / A-D / McClellan (brain-gap)
======================================================================
Brain 34×: "narrow breadth, heavy mega-cap reliance ... ahead of corrections";
11×: indices at new highs while individual stocks lag. Built market-wide from
Polygon grouped-daily bars (one call per session, every listed stock):

  • Advancers/decliners (vs prior close), filtered px>$3 & vol>100k
  • A/D cumulative line · RANA = (A−D)/(A+D)×1000
  • McClellan Oscillator (EMA19−EMA39 of RANA) + Summation Index
  • Zweig Breadth Thrust (10d EMA of A/(A+D): <0.40 → >0.615 within 10 sessions)

History self-accumulates at data/_internals/history.json (first run backfills
~150 sessions). Oscillator extremes and thrusts log to the closed loop.
"""
import json, os, time, urllib.request
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import boto3

S3 = boto3.client("s3", region_name="us-east-1")
DDB = boto3.resource("dynamodb", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/market-internals.json"
HIST_KEY = "data/_internals/history.json"
POLY_KEY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
VERSION = "1.0.0"
BACKFILL_SESSIONS = 150


def grouped(date):
    u = (f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date}"
         f"?adjusted=true&apiKey={POLY_KEY}")
    try:
        j = json.loads(urllib.request.urlopen(u, timeout=50).read())
        return {r["T"]: (float(r["c"]), float(r.get("v") or 0))
                for r in (j.get("results") or []) if r.get("c")}
    except Exception as e:
        print(f"[grouped] {date}: {str(e)[:50]}")
        return {}


def ema(series, n):
    k = 2.0 / (n + 1)
    out, e = [], None
    for v in series:
        e = v if e is None else v * k + e * (1 - k)
        out.append(e)
    return out


def lambda_handler(event=None, context=None):
    t0 = time.time()
    hist = {"rows": []}
    try:
        hist = json.loads(S3.get_object(Bucket=BUCKET, Key=HIST_KEY)["Body"].read())
    except Exception:
        pass
    have = {r["date"] for r in hist["rows"]}

    # sessions to (back)fill
    today = datetime.now(timezone.utc).date()
    cands = []
    d = today
    need = BACKFILL_SESSIONS if len(hist["rows"]) < 30 else 6
    while len(cands) < need + 2:
        d -= timedelta(days=1)
        if d.weekday() < 5:
            cands.append(d.isoformat())
    cands = [c for c in reversed(cands) if c not in have]

    prev_map = None
    if hist["rows"]:
        prev_map = None  # will fetch the session before the first new one
    new_rows, fetched = [], 0
    for i, ds in enumerate(cands):
        if time.time() - t0 > 460:
            print("[internals] time budget — resuming next run")
            break
        cur = grouped(ds)
        fetched += 1
        if not cur:
            continue
        if prev_map is None:
            # find prior session map
            pd_ = datetime.fromisoformat(ds).date()
            for _ in range(6):
                pd_ -= timedelta(days=1)
                if pd_.weekday() < 5:
                    prev_map = grouped(pd_.isoformat())
                    fetched += 1
                    if prev_map:
                        break
            if not prev_map:
                prev_map = cur
                continue
        adv = dec = 0
        for t, (c, v) in cur.items():
            p = prev_map.get(t)
            if not p:
                continue
            pc, _pv = p
            if c < 3 or v < 100_000 or pc <= 0:
                continue
            if c > pc:
                adv += 1
            elif c < pc:
                dec += 1
        tot = adv + dec
        if tot > 800:
            new_rows.append({"date": ds, "adv": adv, "dec": dec,
                              "rana": round((adv - dec) / tot * 1000, 1),
                              "pct_adv": round(adv / tot, 4)})
        prev_map = cur

    if new_rows:
        hist["rows"] = sorted(hist["rows"] + new_rows, key=lambda r: r["date"])[-420:]
        S3.put_object(Bucket=BUCKET, Key=HIST_KEY, Body=json.dumps(hist).encode(),
                      ContentType="application/json")

    rows = hist["rows"]
    out = {"engine": "market-internals", "version": VERSION,
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "duration_s": round(time.time() - t0, 1),
           "sessions": len(rows), "fetched_this_run": fetched,
           "brain_audit_source": "ops-1580"}
    if len(rows) >= 40:
        rana = [r["rana"] for r in rows]
        e19, e39 = ema(rana, 19), ema(rana, 39)
        osc = [round(a - b, 1) for a, b in zip(e19, e39)]
        summ = []
        s_ = 0.0
        for o_ in osc:
            s_ += o_
            summ.append(round(s_, 0))
        pa10 = ema([r["pct_adv"] for r in rows], 10)
        # Zweig thrust: <0.40 → >0.615 within 10 sessions
        thrust_date = None
        for i in range(len(pa10) - 1, max(0, len(pa10) - 40), -1):
            if pa10[i] > 0.615:
                for j in range(max(0, i - 10), i):
                    if pa10[j] < 0.40:
                        thrust_date = rows[i]["date"]
                        break
            if thrust_date:
                break
        ad_line = []
        c_ = 0
        for r in rows:
            c_ += r["adv"] - r["dec"]
            ad_line.append([r["date"], c_])
        latest = rows[-1]
        osc_now = osc[-1]
        state = ("WASHOUT" if osc_now <= -100 else "OVERSOLD" if osc_now <= -70
                  else "OVERBOUGHT" if osc_now >= 70 else "NEUTRAL")
        out.update({
            "latest": {"date": latest["date"], "advancers": latest["adv"],
                        "decliners": latest["dec"], "rana": latest["rana"]},
            "mcclellan": {"oscillator": osc_now, "summation": summ[-1], "state": state,
                           "spec": "±70 stretch, ±100 washout/blowoff (brain: breadth "
                                   "narrows ahead of corrections)"},
            "zweig_thrust": {"fired": bool(thrust_date), "date": thrust_date,
                              "ema10_pct_adv": round(pa10[-1], 3),
                              "spec": "<0.40→>0.615 in ≤10 sessions = rare bull thrust"},
            "ad_line_tail": ad_line[-180:],
            "oscillator_tail": [[rows[i]["date"], osc[i]] for i in range(max(0, len(osc) - 180), len(osc))]})

        # closed loop: washout (contrarian UP) or fresh thrust (UP)
        sig = None
        if osc_now <= -100:
            sig = ("internals-washout", "UP", 0.58,
                   f"McClellan {osc_now} washout — breadth capitulation, contrarian long")
        elif thrust_date and thrust_date >= rows[-3]["date"]:
            sig = ("internals-thrust", "UP", 0.62,
                   f"Zweig breadth thrust {thrust_date} — historically near-perfect 6-12m record")
        elif osc_now >= 100:
            sig = ("internals-blowoff", "DOWN", 0.55, f"McClellan {osc_now} blowoff stretch")
        if sig:
            try:
                spy = grouped(latest["date"]).get("SPY")
                px0 = spy[0] if spy else None
                if px0:
                    nowt = datetime.now(timezone.utc)
                    kind, dr, cf, why = sig
                    DDB.Table("justhodl-signals").put_item(Item={
                        "signal_id": f"{kind}#US#{latest['date']}",
                        "signal_type": "market_internals", "signal_value": str(osc_now),
                        "predicted_direction": dr, "confidence": Decimal(str(cf)),
                        "measure_against": "ticker", "baseline_price": str(px0),
                        "benchmark": "SPY", "check_windows": ["day_5", "day_21", "day_63"],
                        "check_timestamps": {f"day_{w}": (nowt + timedelta(days=w)).isoformat()
                                              for w in (5, 21, 63)},
                        "outcomes": {}, "accuracy_scores": {},
                        "logged_at": nowt.isoformat(), "logged_epoch": int(nowt.timestamp()),
                        "status": "pending", "schema_version": "2",
                        "horizon_days_primary": 21, "regime_at_log": state,
                        "ttl": int(nowt.timestamp()) + 120 * 86400,
                        "metadata": {"engine": "market-internals", "v": VERSION,
                                     "kind": kind},
                        "rationale": why})
                    out["signal_logged"] = kind
            except Exception as e:
                print(f"[loop] {str(e)[:70]}")
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[internals] sessions={len(rows)} osc={out.get('mcclellan',{}).get('oscillator')} {out['duration_s']}s")
    return {"statusCode": 200, "body": json.dumps({"sessions": len(rows)})}
