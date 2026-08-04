"""justhodl-signal-optimizer v0.1 — the hand-tuned constant dies.
Nightly: scan the graded ledger, learn per-engine and per-
(engine,regime) win-lift tables, publish data/learned-weights.json.
Consumers (fabric first) prefer learned weights over any prior.
Google's rule, made local: every weight is an experiment's output."""
import json, time
from datetime import datetime, timezone

import boto3

ddb = boto3.resource("dynamodb", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"
OUT = "data/learned-weights.json"


def F(v):
    try:
        return float(v)
    except Exception:
        return None


def eng_of(it):
    st0 = str(it.get("signal_type") or "")
    if st0.startswith("eng:"):
        return st0[4:]
    md = it.get("metadata")
    md = md if isinstance(md, dict) else {}
    return str(md.get("engine") or it.get("engine") or st0 or "?")


def is_down(pd):
    return any(t in str(pd).upper() for t in
               ("DOWN", "UNDER", "SHORT", "SELL", "BEAR"))


def outcome(it):
    oc = it.get("outcomes")
    if not isinstance(oc, dict) or not oc:
        return None
    prim = str(it.get("horizon_days_primary") or "")
    wkeys = sorted(oc.keys())
    wk = ("day_" + prim if ("day_" + prim) in oc
          else prim if prim in oc else (wkeys[-1] if wkeys else None))
    w0 = oc.get(wk)
    if not isinstance(w0, dict):
        return None
    hit = w0.get("correct")
    if isinstance(hit, str):
        hit = hit.lower() in ("true", "1")
    if hit is None:
        ret = None
        for k, v in w0.items():
            if "return" in str(k).lower():
                ret = F(v)
                break
        if ret is None:
            return None
        hit = (ret < 0) if is_down(
            it.get("predicted_direction")) else (ret > 0)
    return bool(hit)


def lambda_handler(event=None, context=None):
    t0 = time.time()
    tbl = ddb.Table("justhodl-signals")
    items = []
    kw = {}
    while len(items) < 60000:
        resp = tbl.scan(**kw)
        items += resp.get("Items", [])
        if "LastEvaluatedKey" not in resp:
            break
        kw["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    BE, BER = {}, {}
    for it in items:
        h = outcome(it)
        if h is None:
            continue
        e = eng_of(it)
        rg = str(it.get("regime_at_log") or "UNKNOWN").upper()
        a = BE.setdefault(e, [0, 0])
        a[0] += 1
        a[1] += 1 if h else 0
        b = BER.setdefault(e + "|" + rg, [0, 0])
        b[0] += 1
        b[1] += 1 if h else 0

    def pack(d, min_n):
        out2 = {}
        for k, (n, w) in d.items():
            if n < min_n:
                continue
            win = round(100.0 * w / n, 1)
            out2[k] = {"n": n, "win": win,
                       "lift": round(win - 50.0, 1)}
        return out2
    by_e = pack(BE, 15)
    by_er = pack(BER, 8)
    movers = sorted(by_er.items(),
                    key=lambda kv: -abs(kv[1]["lift"]))[:12]
    out = {"engine": "justhodl-signal-optimizer",
           "version": "0.1",
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "elapsed_s": round(time.time() - t0, 1),
           "rules": {"by_engine_min_n": 15,
                     "by_engine_regime_min_n": 8,
                     "weight_formula":
                         "clip(0.6 + lift/50, 0.2, 1.6)"},
           "n_engines": len(by_e),
           "n_engine_regime_pairs": len(by_er),
           "by_engine": by_e,
           "by_engine_regime": by_er,
           "top_regime_movers": [
               {"key": k, **v} for k, v in movers]}
    s3.put_object(Bucket=B, Key=OUT,
                  Body=json.dumps(out).encode(),
                  ContentType="application/json",
                  CacheControl="no-cache")
    print(json.dumps({"ok": True, "engines": len(by_e),
                      "pairs": len(by_er)}))
    return {"ok": True}
