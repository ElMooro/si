#!/usr/bin/env python3
"""The engine's own market model (2026-09-23): a model it RUNS and RETRAINS, graded exactly like the LLM.

The owned LLM reads 20 anonymized bars at the naive line (holdout 0.48-0.50 vs prior 0.51). This is the other half of
"runs financial models": a small, transparent quantitative model trained on every 20-session window of the five drill ETFs
since 2005 (FMP daily bars), labelled by the SAME shared rule the drills use (factory_core.labels_from_prices on the next 5
sessions), with the three held-out crisis blocks EMBARGOED (+/- 60 days) -- then graded on the frozen holdout drills with
factory_core.score_prediction against the same prior and momentum baselines. Features are scale-free (rebased bars).

  python3 scripts/factory_market_model.py [--since 2005-01-01] [--dry-run]

Outputs (private): factory/models/market/quant-<ts>.json (coefficients + feature spec + training counts),
factory/exams/market/quant-latest.json (holdout scores); the engine projects the aggregate onto data/ai.json.market_model.
The read may use its probabilities as a TOOL line -- the LLM's own calls stay its own.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
import urllib.parse
import urllib.request
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "aws" / "shared"))
sys.path.insert(0, str(ROOT / "scripts"))
from factory_core import labels_from_prices, score_prediction  # noqa: E402

PRIVATE = os.environ.get("AI_PRIVATE_BUCKET", "justhodl-ai-857687956942")
SYMBOLS = ("SPY", "QQQ", "IWM", "TLT", "GLD")
HOLDOUT = [("2020-02-17", "2020-04-03"), ("2023-03-06", "2023-03-24"), ("2024-07-29", "2024-08-16")]
EMBARGO_DAYS = 60
WINDOW, HORIZON = 20, 5
DIRS, REGS = ("UP", "DOWN", "FLAT"), ("RANGE", "TRANSITION", "TREND")
FMP = "https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=%s&from=%s&to=%s&apikey=%s"
FEATURES = ("ret1", "ret5", "ret20", "vol5", "vol20", "vol_ratio", "eff20", "eff5", "dd20", "range_pos", "vrel5", "gap_mean", "hl_mean")


def rebase(rows):
    base = rows[0]["o"] or 1.0
    vols = [r.get("v") or 0.0 for r in rows]
    mv = (sum(vols) / len(vols)) if any(vols) else 1.0
    return [{"o": 100 * r["o"] / base, "h": 100 * r["h"] / base, "l": 100 * r["l"] / base, "c": 100 * r["c"] / base, "v_rel": (v / mv) if mv else 1.0} for r, v in zip(rows, vols)]


def features(bars) -> list:
    """Scale-free features of 20 rebased bars (identical for generated windows and the frozen drills)."""
    c = np.array([b["c"] for b in bars], dtype=float)
    o = np.array([b["o"] for b in bars], dtype=float)
    h = np.array([b["h"] for b in bars], dtype=float)
    l_ = np.array([b["l"] for b in bars], dtype=float)
    v = np.array([b.get("v_rel") or 1.0 for b in bars], dtype=float)
    lr = np.diff(np.log(np.maximum(c, 1e-9)))
    def eff(x):
        path = np.abs(np.diff(x)).sum()
        return abs(x[-1] - x[0]) / path if path else 0.0
    peak = np.maximum.accumulate(c)
    vol5, vol20 = float(np.std(lr[-5:])), float(np.std(lr))
    rng = h.max() - l_.min()
    return [float(c[-1] / c[-2] - 1), float(c[-1] / c[-6] - 1), float(c[-1] / c[0] - 1), vol5, vol20, vol5 / vol20 if vol20 else 1.0,
            eff(c), eff(c[-6:]), float((c / peak - 1).min()), float((c[-1] - l_.min()) / rng) if rng else 0.5,
            float(v[-5:].mean()), float(np.mean(o[1:] / c[:-1] - 1)), float(np.mean((h - l_) / c))]


def softmax_fit(X, y, k, l2=1e-2, iters=600, lr=0.5):
    """Multinomial logistic regression, full-batch gradient descent on standardized features (numpy only)."""
    n, d = X.shape
    W = np.zeros((d + 1, k))
    Xb = np.hstack([X, np.ones((n, 1))])
    Y = np.eye(k)[y]
    for _ in range(iters):
        Z = Xb @ W
        Z -= Z.max(axis=1, keepdims=True)
        P = np.exp(Z); P /= P.sum(axis=1, keepdims=True)
        G = Xb.T @ (P - Y) / n
        G[:-1] += l2 * W[:-1]
        W -= lr * G
    return W


def softmax_predict(W, X):
    Z = np.hstack([X, np.ones((X.shape[0], 1))]) @ W
    Z -= Z.max(axis=1, keepdims=True)
    P = np.exp(Z)
    return P / P.sum(axis=1, keepdims=True)


def embargoed(day: str) -> bool:
    d = date.fromisoformat(day)
    return any(date.fromisoformat(a) - timedelta(days=EMBARGO_DAYS) <= d <= date.fromisoformat(b) + timedelta(days=EMBARGO_DAYS) for a, b in HOLDOUT)


def windows_from_history(series: dict, season: dict):
    """(features, labels, symbol, first_day) for every 20+5 window whose 25 sessions avoid the embargo."""
    out = []
    for sym, rows in series.items():
        for i in range(0, len(rows) - WINDOW - HORIZON + 1):
            win, fut = rows[i:i + WINDOW], rows[i + WINDOW:i + WINDOW + HORIZON]
            if embargoed(win[0]["date"]) or embargoed(fut[-1]["date"]):
                continue
            try:
                lab = labels_from_prices(sym, fut[0]["o"], [r["c"] for r in fut], season)
            except Exception:  # noqa: BLE001
                continue
            out.append((features(rebase(win)), lab, sym, win[0]["date"]))
    return out


class Model:
    def __init__(self, mu, sd, Wd, Wr, Wc):
        self.mu, self.sd, self.Wd, self.Wr, self.Wc = mu, sd, Wd, Wr, Wc

    def predict(self, feats):
        x = (np.array([feats]) - self.mu) / self.sd
        pd_, pr, pc = softmax_predict(self.Wd, x)[0], softmax_predict(self.Wr, x)[0], softmax_predict(self.Wc, x)[0]
        return {"direction": DIRS[int(pd_.argmax())], "regime": REGS[int(pr.argmax())], "crisis_probability": float(pc[1]),
                "direction_probabilities": {d: float(p) for d, p in zip(DIRS, pd_)}, "regime_probabilities": {r: float(p) for r, p in zip(REGS, pr)}}

    def to_json(self):
        return {"features": list(FEATURES), "mu": self.mu.tolist(), "sd": self.sd.tolist(), "direction": self.Wd.tolist(), "regime": self.Wr.tolist(), "crisis": self.Wc.tolist(),
                "classes": {"direction": list(DIRS), "regime": list(REGS), "crisis": [False, True]}}


def train(samples, crisis_weight: float = 1.0) -> Model:
    X = np.array([f for f, _, _, _ in samples], dtype=float)
    mu, sd = X.mean(axis=0), X.std(axis=0) + 1e-9
    Xs = (X - mu) / sd
    yd = np.array([DIRS.index(l["direction"]) for _, l, _, _ in samples])
    yr = np.array([REGS.index(l["regime"]) for _, l, _, _ in samples])
    yc = np.array([int(bool(l["crisis"])) for _, l, _, _ in samples])
    return Model(mu, sd, softmax_fit(Xs, yd, 3), softmax_fit(Xs, yr, 3), softmax_fit(Xs, yc, 2))


def grade(model: Model, drills, season):
    """Model vs the exam's OWN prior + momentum baselines (imported from factory_market_exam) on the frozen drills."""
    import factory_market_exam as mx
    labels = [d["labels"] for d in drills]
    crisis_rate = sum(1 for l in labels if l.get("crisis")) / max(1, len(labels))
    rows = {"model": [], "prior": [], "momentum": []}
    for d in drills:
        rows["model"].append(score_prediction(model.predict(features(d["bars"])), d["labels"], season))
        rows["prior"].append(score_prediction(mx.prior_baseline(labels, crisis_rate), d["labels"], season))
        rows["momentum"].append(score_prediction(mx.momentum_baseline(d, crisis_rate), d["labels"], season))

    def agg(rs):
        n = len(rs)
        return {"n": n, "score": round(sum(r["score"] for r in rs) / n, 4) if n else None,
                **{k + "_acc": round(sum(r["components"][k] for r in rs) / n, 4) if n else None for k in ("direction", "regime", "crisis")},
                "crisis_brier": round(sum(r.get("crisis_brier", 0) for r in rs) / n, 4) if n else None}
    return {k: agg(v) for k, v in rows.items()}


def fetch_fmp(sym: str, since: str, key: str):
    url = FMP % (sym, since, date.today().isoformat(), urllib.parse.quote(key))
    with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "justhodl-market-model/1.0"}), timeout=90) as r:
        rows = json.loads(r.read())
    out = []
    for x in rows if isinstance(rows, list) else []:
        try:
            out.append({"date": str(x["date"])[:10], "o": float(x["open"]), "h": float(x["high"]), "l": float(x["low"]), "c": float(x["close"]), "v": float(x.get("volume") or 0)})
        except Exception:  # noqa: BLE001
            continue
    out = [r for r in out if r["o"] > 0 and r["c"] > 0]
    out.sort(key=lambda r: r["date"])
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2005-01-01")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)
    import boto3
    from managed_secret import managed_secret
    from factory_official_prints import Warehouse, load_season
    s3 = boto3.client("s3", region_name="us-east-1")
    season = load_season(Warehouse(s3, private=PRIVATE, public="justhodl-dashboard-live"))
    key = managed_secret(("FMP_API_KEY",), ("/justhodl/fmp/api-key",))
    series = {s: fetch_fmp(s, args.since, key) for s in SYMBOLS}
    samples = windows_from_history(series, season)
    drills = []
    token = None
    while True:
        kw = {"Bucket": PRIVATE, "Prefix": "factory/curriculum/charts/holdout/", "MaxKeys": 1000}
        if token:
            kw["ContinuationToken"] = token
        page = s3.list_objects_v2(**kw)
        for o in page.get("Contents", []):
            d = json.loads(s3.get_object(Bucket=PRIVATE, Key=o["Key"])["Body"].read())
            if d.get("bars") and d.get("labels"):
                drills.append(d)
        token = page.get("NextContinuationToken")
        if not token:
            break
    model = train(samples)
    graded = grade(model, drills, season)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    counts = {"windows": len(samples), "per_symbol": dict(Counter(s for _, _, s, _ in samples)), "crisis_windows": sum(1 for _, l, _, _ in samples if l["crisis"]),
              "first": min((d for _, _, _, d in samples), default=None), "last": max((d for _, _, _, d in samples), default=None), "holdout_drills": len(drills)}
    result = {"schema_version": "factory-market-model.v1", "model_id": "quant-" + stamp, "at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
              "training": counts, "holdout": graded, "beats_prior": bool(graded["model"]["score"] and graded["prior"]["score"] and graded["model"]["score"] > graded["prior"]["score"]),
              "embargo_days": EMBARGO_DAYS, "features": list(FEATURES), "source": "fmp:historical-price-eod/full"}
    print(json.dumps({k: result[k] for k in ("model_id", "training", "holdout", "beats_prior")}, default=str))
    if not args.dry_run:
        s3.put_object(Bucket=PRIVATE, Key="factory/models/market/%s.json" % result["model_id"], Body=json.dumps({**result, "weights": model.to_json()}).encode(), ContentType="application/json")
        s3.put_object(Bucket=PRIVATE, Key="factory/models/market/latest.json", Body=json.dumps({**result, "weights": model.to_json()}).encode(), ContentType="application/json")
        s3.put_object(Bucket=PRIVATE, Key="factory/exams/market/quant-latest.json", Body=json.dumps(result, indent=1).encode(), ContentType="application/json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
