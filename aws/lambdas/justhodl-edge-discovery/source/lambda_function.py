"""
justhodl-edge-discovery — THE RESEARCH FACTORY (autonomous edge discovery)
═══════════════════════════════════════════════════════════════════════════════════════════════
Until now Khalid hand-builds every engine. This is the loop that lets the SYSTEM find its own edges
— and, crucially, refuses to fool itself. It hypothesizes candidate signals from cross-asset feature
interactions, backtests each strictly OUT-OF-SAMPLE, then applies the two corrections that separate
real alpha from data-mined noise:
  • Deflated Sharpe Ratio (Bailey & López de Prado) — discounts the best Sharpe for the number of
    trials run, so "we tried 160 things and one looked great" is correctly penalised.
  • Benjamini-Hochberg FDR (q=0.10) across all candidates — controls false-discovery rate.
Only candidates that clear OOS + DSR + FDR + minimum trade count GRADUATE (written to a watchlist for
the measure-before-trust pipeline). Previously-graduated signals are re-tested every run and RETIRED
when their out-of-sample Sharpe decays. No look-ahead: feature thresholds & directions are fit on the
in-sample half only; all reported statistics are out-of-sample and net of cost.

Methodology is intentionally conservative and HONEST — most candidates will (correctly) fail. A run
that graduates nothing is a feature, not a bug: it means nothing cleared the multiple-testing bar.
"""
import json
import math
import os
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta

import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
POLY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
S3 = boto3.client("s3", REGION)
OUT_KEY = "data/edge-discovery.json"
GRAD_KEY = "data/edge-discovery-graduated.json"

TARGETS = ["SPY", "QQQ", "IWM", "XLK", "XLF", "XLE", "GLD", "TLT", "HYG", "EEM"]
AUX = ["RSP", "IEF", "UUP", "DBC", "USO", "SLV", "LQD"]
HORIZONS = [10, 21]
COST = 0.0010          # round-trip cost per trade
EULER = 0.5772156649
Z_EXTREME = 0.84       # act only on ~top/bottom 20% feature readings


# ── stats helpers ──
def norm_cdf(x):
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def norm_ppf(p):  # Acklam inverse normal
    p = min(max(p, 1e-9), 1 - 1e-9)
    a = [-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02, 1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00]
    b = [-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02, 6.680131188771972e+01, -1.328068155288572e+01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00, -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00, 3.754408661907416e+00]
    pl = 0.02425
    if p < pl:
        q = math.sqrt(-2 * math.log(p))
        return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
    if p <= 1 - pl:
        q = p - 0.5; r = q * q
        return (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q / (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1)
    q = math.sqrt(-2 * math.log(1 - p))
    return -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)


def mean(x):
    return sum(x) / len(x) if x else 0.0


def std(x):
    if len(x) < 2:
        return 0.0
    m = mean(x)
    return math.sqrt(sum((v - m) ** 2 for v in x) / (len(x) - 1))


def skewkurt(x):
    n = len(x)
    if n < 4:
        return 0.0, 3.0
    m, s = mean(x), std(x)
    if s == 0:
        return 0.0, 3.0
    sk = sum(((v - m) / s) ** 3 for v in x) / n
    ku = sum(((v - m) / s) ** 4 for v in x) / n
    return sk, ku


def deflated_sharpe(sr_pt, T, sk, ku, sr0):
    """Probabilistic/Deflated Sharpe — prob the per-trade SR exceeds the multiple-testing null sr0."""
    den = math.sqrt(max(1e-9, 1 - sk * sr_pt + (ku - 1) / 4.0 * sr_pt * sr_pt))
    return norm_cdf((sr_pt - sr0) * math.sqrt(max(1, T - 1)) / den)


# ── data ──
def fetch(t):
    frm = (datetime.now(timezone.utc) - timedelta(days=1100)).strftime("%Y-%m-%d")
    to = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    url = f"https://api.polygon.io/v2/aggs/ticker/{t}/range/1/day/{frm}/{to}?adjusted=true&sort=asc&limit=1200&apiKey={POLY}"
    try:
        r = json.loads(urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "jh"}), timeout=25).read())
        return t, {x["t"]: x["c"] for x in r.get("results", []) if x.get("c")}
    except Exception:
        return t, {}


def build_features(C, dates):
    """All features computed from TRAILING data only (no look-ahead)."""
    def s(t):
        return [C[t][d] for d in dates]
    out = {}
    def momser(arr, n):
        return [arr[i] / arr[i - n] - 1 if i >= n else 0.0 for i in range(len(arr))]
    def ratio_mom(a, b, n):
        ra = [a[i] / b[i] for i in range(len(a))]
        return momser(ra, n)
    spy, rsp, qqq, iwm = s("SPY"), s("RSP"), s("QQQ"), s("IWM")
    out["credit_hyg_ief"] = ratio_mom(s("HYG"), s("IEF"), 21)
    out["duration_tlt"] = momser(s("TLT"), 21)
    out["gold_gld"] = momser(s("GLD"), 21)
    out["dollar_uup"] = momser(s("UUP"), 21)
    out["commod_dbc"] = momser(s("DBC"), 21)
    out["oil_uso"] = momser(s("USO"), 21)
    out["em_eem"] = momser(s("EEM"), 21)
    out["growth_v_small"] = ratio_mom(qqq, iwm, 21)
    out["concentration"] = ratio_mom(spy, rsp, 63)
    # SPY realized vol (21d) — inverse risk-on
    rv = []
    rets = [spy[i] / spy[i - 1] - 1 if i else 0.0 for i in range(len(spy))]
    for i in range(len(spy)):
        w = rets[max(0, i - 20):i + 1]
        rv.append(std(w) if len(w) > 3 else 0.0)
    out["spy_realized_vol"] = rv
    return out


def zscore_is(series, is_end):
    seg = series[:is_end]
    m, sd = mean(seg), std(seg)
    if sd == 0:
        return [0.0] * len(series), 0.0, 1.0
    return [(v - m) / sd for v in series], m, sd


def backtest(feat_z, tgt_closes, is_end, H, direction):
    """OOS only, non-overlapping H-day trades on feature extremes. Returns trade list (net of cost)."""
    trades = []
    i = is_end
    n = len(tgt_closes)
    while i < n - H:
        fz = feat_z[i]
        if abs(fz) > Z_EXTREME:
            pos = direction * (1 if fz > 0 else -1)
            r = pos * (tgt_closes[i + H] / tgt_closes[i] - 1) - COST
            trades.append(r)
            i += H
        else:
            i += 1
    return trades


def lambda_handler(event=None, context=None):
    t0 = time.time()
    tickers = list(set(TARGETS + AUX))
    with ThreadPoolExecutor(max_workers=18) as ex:
        C = dict(ex.map(fetch, tickers))
    common = set.intersection(*[set(C[t].keys()) for t in tickers if C[t]]) if all(C[t] for t in tickers) else set()
    dates = sorted(common)
    if len(dates) < 400:
        out = {"engine": "justhodl-edge-discovery", "ok": False, "error": f"insufficient history ({len(dates)})",
               "generated_at": datetime.now(timezone.utc).isoformat()}
        S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out).encode(), ContentType="application/json")
        return {"statusCode": 200, "body": json.dumps(out)}

    feats = build_features(C, dates)
    is_end = int(len(dates) * 0.55)   # in-sample for thresholds/direction; OOS = remaining 45%

    # ── hypothesise + backtest every (feature, target, horizon) candidate ──
    cands = []
    for fname, fseries in feats.items():
        fz, _, _ = zscore_is(fseries, is_end)
        for tgt in TARGETS:
            tgt_closes = [C[tgt][d] for d in dates]
            # forward returns over IS to pick DIRECTION (no OOS peeking)
            for H in HORIZONS:
                is_pairs = [(fz[i], tgt_closes[i + H] / tgt_closes[i] - 1) for i in range(is_end - H) if abs(fz[i]) > Z_EXTREME]
                if len(is_pairs) < 10:
                    continue
                # direction = does high feature precede higher target? sign of IS mean(signed return)
                is_signed = [(1 if a > 0 else -1) * b for a, b in is_pairs]
                direction = 1 if mean(is_signed) >= 0 else -1
                trades = backtest(fz, tgt_closes, is_end, H, direction)
                if len(trades) < 12:
                    continue
                m, sd = mean(trades), std(trades)
                if sd == 0:
                    continue
                sr_pt = m / sd                                   # per-trade Sharpe
                sharpe_ann = sr_pt * math.sqrt(252.0 / H)        # annualised
                tstat = m / (sd / math.sqrt(len(trades)))
                hit = 100 * sum(1 for r in trades if r > 0) / len(trades)
                cands.append({"feature": fname, "target": tgt, "horizon": H, "direction": int(direction),
                              "n_trades": len(trades), "mean_ret_pct": round(m * 100, 3),
                              "sharpe_ann": round(sharpe_ann, 2), "sr_pt": sr_pt, "tstat": round(tstat, 2),
                              "hit_pct": round(hit, 1), "trades": trades})

    N = len(cands)
    result = {"engine": "justhodl-edge-discovery", "version": "1.0.0", "ok": True,
              "generated_at": datetime.now(timezone.utc).isoformat(),
              "thesis": "Autonomous research factory — hypothesise → backtest OOS → Deflated-Sharpe + FDR multiple-"
                        "testing correction → graduate survivors, retire decayed. Honest by construction.",
              "n_candidates_tested": N, "oos_fraction": 0.45, "cost_per_trade_pct": COST * 100,
              "history_days": len(dates)}
    if N == 0:
        result.update({"survivors": [], "top_candidates": [], "note": "no testable candidates this run"})
        S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(result).encode(), ContentType="application/json")
        return {"statusCode": 200, "body": json.dumps({"ok": True, "n": 0})}

    # ── multiple-testing correction ──
    sr_list = [c["sr_pt"] for c in cands]
    var_sr = std(sr_list) ** 2 if len(sr_list) > 1 else 0.01
    sr0 = math.sqrt(max(1e-9, var_sr)) * ((1 - EULER) * norm_ppf(1 - 1.0 / N) + EULER * norm_ppf(1 - 1.0 / (N * math.e)))
    # p-values (one-sided, positive edge) for BH-FDR
    for c in cands:
        c["p_value"] = 1 - norm_cdf(c["tstat"])
        sk, ku = skewkurt(c["trades"])
        c["dsr"] = round(deflated_sharpe(c["sr_pt"], c["n_trades"], sk, ku, sr0), 3)
    # Benjamini-Hochberg at q=0.10
    q = 0.10
    ordered = sorted(cands, key=lambda c: c["p_value"])
    bh_k = 0
    for idx, c in enumerate(ordered, 1):
        if c["p_value"] <= q * idx / N:
            bh_k = idx
    fdr_survivors = set(id(c) for c in ordered[:bh_k])

    # ── graduate ──
    survivors = []
    for c in cands:
        c["expected_max_sharpe_null"] = round(sr0 * math.sqrt(252.0 / c["horizon"]), 2)
        c["fdr_pass"] = id(c) in fdr_survivors
        c["graduate"] = bool(c["fdr_pass"] and c["dsr"] >= 0.90 and c["sharpe_ann"] >= 0.5 and c["n_trades"] >= 12)
        c.pop("trades", None); c.pop("sr_pt", None)
        if c["graduate"]:
            survivors.append(c)

    # ── retire previously-graduated whose OOS decayed ──
    try:
        prior = json.loads(S3.get_object(Bucket=BUCKET, Key=GRAD_KEY)["Body"].read())
    except Exception:
        prior = {"graduated": []}
    cand_by_sig = {f"{c['feature']}|{c['target']}|{c['horizon']}": c for c in cands}
    retired, still_live = [], []
    for g in prior.get("graduated", []):
        sig = f"{g['feature']}|{g['target']}|{g['horizon']}"
        now = cand_by_sig.get(sig)
        if now and now["sharpe_ann"] >= 0.2 and now["dsr"] >= 0.5:
            still_live.append({**g, "current_sharpe_ann": now["sharpe_ann"], "current_dsr": now["dsr"]})
        else:
            retired.append({**g, "current_sharpe_ann": (now or {}).get("sharpe_ann"), "reason": "OOS Sharpe/DSR decayed"})
    # add new survivors to graduated set (dedup by signature)
    live_sigs = {f"{g['feature']}|{g['target']}|{g['horizon']}" for g in still_live}
    for s in survivors:
        sig = f"{s['feature']}|{s['target']}|{s['horizon']}"
        if sig not in live_sigs:
            still_live.append({"feature": s["feature"], "target": s["target"], "horizon": s["horizon"],
                               "direction": s["direction"], "graduated_at": datetime.now(timezone.utc).isoformat(),
                               "graduation_sharpe_ann": s["sharpe_ann"], "graduation_dsr": s["dsr"],
                               "current_sharpe_ann": s["sharpe_ann"], "current_dsr": s["dsr"]})
            live_sigs.add(sig)
    S3.put_object(Bucket=BUCKET, Key=GRAD_KEY,
                  Body=json.dumps({"graduated": still_live, "updated_at": datetime.now(timezone.utc).isoformat()}).encode(),
                  ContentType="application/json")

    top = sorted(cands, key=lambda c: -c["dsr"])[:20]
    result.update({
        "multiple_testing": {"n_trials": N, "expected_max_sharpe_per_trade_null": round(sr0, 4),
                             "fdr_q": q, "fdr_survivors": bh_k, "method": "Deflated Sharpe (Bailey-LdP) + Benjamini-Hochberg"},
        "n_graduated_this_run": len(survivors),
        "survivors": sorted(survivors, key=lambda c: -c["dsr"]),
        "graduated_live": still_live, "retired_this_run": retired,
        "top_candidates": top,
        "note": ("Direction & thresholds fit IN-SAMPLE (first 55%); all stats OUT-OF-SAMPLE & net of cost. "
                 "Graduation bar: FDR-survivor AND Deflated-Sharpe≥0.90 AND OOS annualised Sharpe≥0.5 AND ≥12 trades. "
                 "Most candidates SHOULD fail — that is the multiple-testing discipline working."),
        "elapsed_s": round(time.time() - t0, 1),
    })
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(result, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    print(f"[edge-discovery] tested {N} | sr0_null={sr0:.4f} | FDR survivors {bh_k} | graduated {len(survivors)} "
          f"| live {len(still_live)} retired {len(retired)} | {result['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "tested": N, "graduated": len(survivors),
            "live": len(still_live), "retired": len(retired)})}
