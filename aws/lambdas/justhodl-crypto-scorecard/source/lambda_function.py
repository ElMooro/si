"""
justhodl-crypto-scorecard  ·  v1.0  —  CRYPTO EXCESS-vs-BTC LEDGER
================================================================================
The crypto sibling of justhodl-signal-scorecard. Grades crypto-ma200 signals on
forward EXCESS return over BTC — the right benchmark for a crypto long (did this
coin beat just holding bitcoin?), computed straight from the crypto-ma200 close
buffer (which already holds every coin's daily closes + BTC), so zero extra API
calls. Mirrors the equity discipline exactly: Wilson lower bound on the hit-rate
(share that beat BTC), forward-excess alpha stats, Benjamini-Hochberg FDR across
signal families. CRYPTO_PROVEN only when the edge survives all three.

This is measure-before-trust for crypto. Until ~20 signals mature at the 21-day
horizon, every family reads WARMING — and that is the honest state.
"""
import json, math, time
from datetime import datetime, timezone, date, timedelta
from decimal import Decimal
import boto3
from boto3.dynamodb.conditions import Attr

S3 = boto3.client("s3", "us-east-1")
ddb = boto3.resource("dynamodb", "us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/crypto-scorecard.json"
BUF_KEY = "data/_ma200/crypto-closes.json"
TABLE = "justhodl-signals"
BENCH = "BTC"
MIN_ALPHA_N = 20
FDR_Q = 0.10
PROMOTE_LB = 0.57
DEPRECATE_LB = 0.45
DEPRECATE_N = 20
PRIMARY = 21
WIN = [5, 21, 63]


def norm_sf(z):
    return 0.5 * math.erfc(z / math.sqrt(2))


def wilson_lower(hits, n, z=1.96):
    if n == 0:
        return 0.0
    p = hits / n
    d = 1 + z * z / n
    c = p + z * z / (2 * n)
    m = z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n)
    return max(0.0, (c - m) / d)


def bh_fdr(pairs, q=FDR_Q):
    m = len(pairs)
    if not m:
        return set()
    s = sorted(pairs, key=lambda x: x[1])
    rej = set()
    for i, (k, p) in enumerate(s, 1):
        if p <= i / m * q:
            rej = {kk for kk, _ in s[:i]}
    return rej


def alpha_stats(ex):
    n = len(ex)
    if n < MIN_ALPHA_N:
        return None
    mean = sum(ex) / n
    var = sum((x - mean) ** 2 for x in ex) / (n - 1) if n > 1 else 0.0
    sd = math.sqrt(var)
    se = sd / math.sqrt(n) if sd > 0 else 0.0
    t = mean / se if se > 0 else 0.0
    p = norm_sf(t) if t > 0 else (1.0 - norm_sf(-t))
    hit = sum(1 for x in ex if x > 0) / n
    return {"alpha_n": n, "mean_excess_pct": round(mean, 3), "sd_pct": round(sd, 3),
            "t_stat": round(t, 2), "hit_rate_vs_btc": round(hit, 3), "p_value": round(p, 5)}


def scan_crypto():
    t = ddb.Table(TABLE)
    items, kw = [], {"FilterExpression": Attr("signal_type").begins_with("crypto_ma200")}
    while True:
        r = t.scan(**kw)
        items += r.get("Items", [])
        lek = r.get("LastEvaluatedKey")
        if not lek:
            break
        kw["ExclusiveStartKey"] = lek
    return items


def lambda_handler(event=None, context=None):
    t0 = time.time()
    try:
        buf = json.loads(S3.get_object(Bucket=BUCKET, Key=BUF_KEY)["Body"].read())
    except Exception as e:
        print(f"[crypto-scorecard] no buffer: {e}")
        return {"statusCode": 200, "body": "no buffer"}
    dates = buf.get("dates", [])
    series = buf.get("series", {})
    didx = {d: i for i, d in enumerate(dates)}
    latest = dates[-1] if dates else None

    def close_at(tk, d):
        i = didx.get(d)
        s = series.get(tk)
        return s[i] if (s and i is not None and i < len(s) and s[i] is not None) else None

    def close_on_or_after(tk, target):
        for d in dates:
            if d >= target:
                v = close_at(tk, d)
                if v is not None:
                    return v
        return None

    rows = scan_crypto()
    tbl = ddb.Table(TABLE)
    by_type, graded, pending = {}, 0, 0

    for it in rows:
        st = it.get("signal_type")
        dr = it.get("predicted_direction")
        md = it.get("metadata") or {}
        log_date = md.get("log_date")
        tk = it["signal_id"].split("#")[1] if "#" in it["signal_id"] else None
        if not log_date or log_date not in didx or not tk:
            continue
        base = close_at(tk, log_date)
        bbase = close_at("BTC", log_date)
        if base is None or bbase is None:
            continue
        outcomes = dict(it.get("outcomes") or {})
        acc = dict(it.get("accuracy_scores") or {})
        changed = False
        for w in WIN:
            key = f"day_{w}"
            if key in outcomes:
                continue
            target = (date.fromisoformat(log_date) + timedelta(days=w)).isoformat()
            if latest < target:
                continue                                  # not matured yet
            fwd = close_on_or_after(tk, target)
            bfwd = close_on_or_after("BTC", target)
            if fwd is None or bfwd is None:
                continue
            ex = ((fwd / base - 1) - (bfwd / bbase - 1)) * 100   # excess vs BTC, %
            outcomes[key] = Decimal(str(round(ex, 4)))
            acc[key] = 1 if ((ex > 0) == (dr == "UP")) else 0
            changed = True
        if changed:
            status = "graded" if f"day_{PRIMARY}" in outcomes else it.get("status", "pending")
            try:
                tbl.update_item(Key={"signal_id": it["signal_id"]},
                                UpdateExpression="SET outcomes=:o, accuracy_scores=:a, #s=:st",
                                ExpressionAttributeNames={"#s": "status"},
                                ExpressionAttributeValues={":o": outcomes, ":a": acc, ":st": status})
            except Exception as e:
                print(f"[update] {str(e)[:50]}")
        pk = f"day_{PRIMARY}"
        if pk in outcomes:
            graded += 1
            by_type.setdefault(st, []).append(float(outcomes[pk]))
        else:
            pending += 1

    pairs, type_stats = [], {}
    for st, ex in by_type.items():
        d = {"n": len(ex)}
        s = alpha_stats(ex)
        if s:
            d.update(s)
            d["wilson_lb"] = round(wilson_lower(sum(1 for x in ex if x > 0), len(ex)), 3)
            pairs.append((st, s["p_value"]))
        type_stats[st] = d
    rej = bh_fdr(pairs)
    for st, d in type_stats.items():
        if "wilson_lb" not in d:
            d["alpha_status"] = "WARMING"
        elif d["wilson_lb"] >= PROMOTE_LB and d.get("mean_excess_pct", 0) > 0 and st in rej:
            d["alpha_status"] = "CRYPTO_PROVEN"
        elif d["wilson_lb"] < DEPRECATE_LB and d["n"] >= DEPRECATE_N:
            d["alpha_status"] = "ALPHA_NEGATIVE"
        else:
            d["alpha_status"] = "WARMING"

    out = {"engine": "crypto-scorecard", "version": "1.0",
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "benchmark": BENCH, "primary_horizon_days": PRIMARY, "min_alpha_n": MIN_ALPHA_N,
           "n_signals": len(rows), "n_graded_primary": graded, "n_pending": pending,
           "by_type": type_stats,
           "note": ("crypto-ma200 signals graded on forward excess-vs-BTC from the close buffer (zero extra "
                    "API calls). CRYPTO_PROVEN requires Wilson LB>=0.57 on the share that beat BTC, positive "
                    "mean excess, and surviving Benjamini-Hochberg FDR. WARMING until n>=20 mature at 21 days."),
           "duration_s": round(time.time() - t0, 1)}
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    # mirror a compact alpha-status map crypto-ma200 can consume
    S3.put_object(Bucket=BUCKET, Key="data/crypto-engine-trust.json",
                  Body=json.dumps({"generated_at": out["generated_at"],
                                   "alpha": {st: d.get("alpha_status") for st, d in type_stats.items()}},
                                  default=str).encode(), ContentType="application/json")
    print(f"[crypto-scorecard] signals={len(rows)} graded={graded} pending={pending} types={list(type_stats.keys())}")
    return {"statusCode": 200, "body": json.dumps({"graded": graded, "pending": pending})}
