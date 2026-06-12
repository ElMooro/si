"""
justhodl-meta-labeler v1.0 — the gatekeeper (Lopez de Prado meta-labeling)
==========================================================================
Doesn't predict direction. Predicts whether a primary signal should be TRUSTED.
Trains a pure-python logistic model on the harness's per-signal graded rows
(label: 21d excess vs SPY > 0) with features: signal confidence, direction,
signal-type one-hots, and SPY regime context at signal date (21d momentum,
20d vol, above-50dma). Chronological 70/30 split — train strictly precedes
test (no leakage). Applies the model to every PENDING live signal: TAKE/SKIP
at threshold. Output: data/meta-labeler.json · daily 21:50 UTC.
"""
import json, gzip, math, os, time, urllib.request
from datetime import datetime, timezone
import boto3

S3 = boto3.client("s3", region_name="us-east-1")
DDB = boto3.resource("dynamodb", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
GRADED_KEY = "data/_backtest/graded.json.gz"
OUT_KEY = "data/meta-labeler.json"
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
THRESH = 0.55
VERSION = "1.0.1"
DIAG = []


def jget(url, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl admin@justhodl.ai"})
    return json.loads(urllib.request.urlopen(req, timeout=timeout).read())


def spy_context():
    j = jget(f"https://financialmodelingprep.com/stable/historical-price-eod/light"
              f"?symbol=SPY&apikey={FMP_KEY}")
    rows = j if isinstance(j, list) else (j.get("historical") or [])
    ser = sorted(((x["date"], x.get("price") or x.get("close")) for x in rows
                   if x.get("date") and (x.get("price") or x.get("close"))))
    dates = [d for d, _ in ser]
    px = [p for _, p in ser]
    import bisect

    def ctx(d):
        i = bisect.bisect_right(dates, d) - 1
        if i < 55:
            return None
        mom = px[i] / px[i - 21] - 1
        rets = [px[k] / px[k - 1] - 1 for k in range(i - 19, i + 1)]
        m = sum(rets) / 20
        vol = math.sqrt(sum((x - m) ** 2 for x in rets) / 20)
        ma50 = sum(px[i - 49:i + 1]) / 50
        return {"mom21": mom, "vol20": vol, "above50": 1.0 if px[i] > ma50 else 0.0}
    return ctx


def featurize(row, ctx_fn, top_types):
    c = ctx_fn(row["date"]) if row.get("date") else None
    if c is None:
        return None
    x = [1.0, float(row.get("conf") or 0.5), float(row.get("dir", 1)),
          c["mom21"] * 10, c["vol20"] * 100, c["above50"]]
    ty = row.get("type") or "unknown"
    for t in top_types:
        x.append(1.0 if ty == t else 0.0)
    return x


def train_logistic(X, Y, iters=400, lr=0.12, l2=1e-3):
    k = len(X[0])
    w = [0.0] * k
    n = len(X)
    for _ in range(iters):
        g = [0.0] * k
        for x, y in zip(X, Y):
            z = sum(wi * xi for wi, xi in zip(w, x))
            p = 1 / (1 + math.exp(-max(-30, min(30, z))))
            e = p - y
            for j in range(k):
                g[j] += e * x[j]
        for j in range(k):
            w[j] -= lr * (g[j] / n + l2 * w[j])
    return w


def predict(w, x):
    z = sum(wi * xi for wi, xi in zip(w, x))
    return 1 / (1 + math.exp(-max(-30, min(30, z))))


def lambda_handler(event=None, context=None):
    t0 = time.time()
    DIAG.clear()
    rows = json.loads(gzip.decompress(
        S3.get_object(Bucket=BUCKET, Key=GRADED_KEY)["Body"].read()))
    rows = [r for r in rows if r.get("date")]
    rows.sort(key=lambda r: r["date"])
    counts = {}
    for r in rows:
        counts[r.get("type") or "unknown"] = counts.get(r.get("type") or "unknown", 0) + 1
    top_types = [t for t, _ in sorted(counts.items(), key=lambda x: -x[1])[:5]]
    ctx_fn = spy_context()
    X, Y, META = [], [], []
    for r in rows:
        x = featurize(r, ctx_fn, top_types)
        if x is None:
            continue
        X.append(x)
        Y.append(1.0 if r["ex"] > 0 else 0.0)
        META.append(r)
    n = len(X)
    MIN_N = 300
    warming = n < MIN_N
    cut = int(n * 0.7)
    DIAG.append(f"{n} graded rows featurized; train {cut} (<= {META[cut-1]['date']}) "
                 f"test {n-cut}; types {top_types}")
    mus, sds = [0.0] * len(X[0]), [1.0] * len(X[0])
    for j in (1, 2, 3, 4):
        vals = [X[i][j] for i in range(cut)]
        mu = sum(vals) / cut
        sd = math.sqrt(sum((v - mu) ** 2 for v in vals) / cut) or 1.0
        mus[j], sds[j] = mu, sd
        for i in range(n):
            X[i][j] = (X[i][j] - mu) / sd
    w = train_logistic(X[:cut], Y[:cut])
    test_p = [predict(w, X[i]) for i in range(cut, n)]
    test_y = Y[cut:]
    base = sum(test_y) / len(test_y)
    takes = [(p, y) for p, y in zip(test_p, test_y) if p >= THRESH]
    take_rate = len(takes) / len(test_y)
    precision = (sum(y for _, y in takes) / len(takes)) if takes else None
    brier = sum((p - y) ** 2 for p, y in zip(test_p, test_y)) / len(test_y)
    ex_test = [META[i]["ex"] for i in range(cut, n)]
    avg_all = sum(ex_test) / len(ex_test)
    taken_ex = [META[cut + i]["ex"] for i, p in enumerate(test_p) if p >= THRESH]
    avg_taken = (sum(taken_ex) / len(taken_ex)) if taken_ex else None
    per_type = {}
    for i, p in enumerate(test_p):
        ty = META[cut + i].get("type") or "unknown"
        d = per_type.setdefault(ty, {"n": 0, "hits": 0, "take_n": 0, "take_hits": 0})
        d["n"] += 1
        d["hits"] += test_y[i]
        if p >= THRESH:
            d["take_n"] += 1
            d["take_hits"] += test_y[i]
    pt = [{"type": t, "n_test": d["n"], "base_hit": round(d["hits"] / d["n"] * 100, 1),
            "gated_hit": (round(d["take_hits"] / d["take_n"] * 100, 1) if d["take_n"] else None),
            "take_rate": round(d["take_n"] / d["n"] * 100, 1)}
           for t, d in sorted(per_type.items(), key=lambda x: -x[1]["n"])]
    names = ["bias", "confidence", "direction", "spy_mom21", "spy_vol20", "spy_above50"] \
             + [f"type:{t}" for t in top_types]
    T = DDB.Table("justhodl-signals")
    gates, lek = [], None
    while True:
        kw = {}
        if lek:
            kw["ExclusiveStartKey"] = lek
        r = T.scan(**kw)
        for it in r.get("Items") or []:
            if (it.get("status") == "pending" and it.get("baseline_price")
                    and it.get("check_windows")):
                ep = int(it.get("logged_epoch") or 0)
                d0 = (datetime.fromtimestamp(ep, tz=timezone.utc).date().isoformat()
                       if ep else None)
                sid = it.get("signal_id") or ""
                ty = (it.get("signal_type")
                       or (sid.split("#")[0] if "#" in sid else "unknown"))
                ty = str(ty).replace("-", "_")
                row = {"type": ty, "date": d0,
                        "conf": float(it.get("confidence") or 0.5),
                        "dir": 1 if (it.get("predicted_direction") or "UP") == "UP" else 0}
                x = featurize(row, ctx_fn, top_types)
                if x is None:
                    continue
                for j in (1, 2, 3, 4):
                    x[j] = (x[j] - mus[j]) / sds[j]
                p = predict(w, x)
                gates.append({"signal_id": sid[:70], "type": ty,
                                "ticker": it.get("ticker"), "date": d0,
                                "conf": row["conf"], "meta_p": round(p, 3),
                                "verdict": "TAKE" if p >= THRESH else "SKIP"})
        lek = r.get("LastEvaluatedKey")
        if not lek:
            break
    gates.sort(key=lambda g: -g["meta_p"])
    if warming:
        DIAG.append(f"WARM-UP: only {n} dated graded rows (<{MIN_N}); model published "
                     "for transparency but gating is advisory-only until pending "
                     "signals age in (~140/day crossing 21 trading days)")
    out = {"engine": "meta-labeler", "version": VERSION, "status": ("warming_up" if warming else "active"),
            "n_training_rows": n, "min_rows_to_activate": MIN_N,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "duration_s": round(time.time() - t0, 1), "threshold": THRESH,
            "model": {"n_train": cut, "n_test": n - cut,
                       "coefficients": {nm: round(wi, 3) for nm, wi in zip(names, w)},
                       "test_base_hit": round(base * 100, 1),
                       "test_take_precision": (round(precision * 100, 1) if precision else None),
                       "test_take_rate": round(take_rate * 100, 1),
                       "uplift_pp": (round((precision - base) * 100, 1) if precision else None),
                       "avg_excess_all_pct": round(avg_all * 100, 2),
                       "avg_excess_taken_pct": (round(avg_taken * 100, 2)
                                                  if avg_taken is not None else None),
                       "brier": round(brier, 4)},
            "per_type_test": pt, "n_pending_gated": len(gates),
            "n_take": sum(1 for g in gates if g["verdict"] == "TAKE"),
            "gates": gates[:120], "diagnostics": list(DIAG),
            "methodology": ("Meta-labeling (Lopez de Prado): a logistic gatekeeper that "
              "predicts whether a primary signal will produce positive 21d excess vs "
              "SPY, trained on the harness's graded rows with a strict chronological "
              "70/30 split (train precedes test, no leakage). Features: confidence, "
              "direction, signal-type one-hots, SPY regime at signal date (21d momentum, "
              "20d vol, above-50dma). Live pending signals are gated TAKE/SKIP at "
              f"p>={THRESH}. The money metric is excess-return uplift of taken vs all. "
              "Research, not advice.")}
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print(f"[meta] uplift {out['model']['uplift_pp']}pp · take {out['model']['test_take_rate']}% "
           f"· {out['n_take']}/{len(gates)} pending TAKE · {out['duration_s']}s")
    return {"statusCode": 200, "body": json.dumps({"uplift_pp": out["model"]["uplift_pp"]})}
