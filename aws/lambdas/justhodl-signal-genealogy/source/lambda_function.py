"""
justhodl-signal-genealogy — WHICH SIGNALS LEAD, WHICH CONFIRM
================================================================
Thesis: EDGE-ACCURACY (signal-scorecard) measures whether a signal is RIGHT.
Nothing on the platform measures whether a signal is EARLY. A signal can be
100% accurate and still be worthless for catching something before the crowd
if it only fires after five other signals already moved. This engine adds
the missing axis: earliness, not just accuracy.

HONEST SCOPE — read this before trusting the output
════════════════════════════════════════════════════
The raw material is `justhodl-outcomes` (the same ledger signal-scorecard
grades): ~40,000 records, 271 signal_types, ~3.5 months of history. That
ledger tracks WHEN each signal_type fired and whether the call was right —
it does NOT retain which ticker each firing was about. So this engine
cannot answer "does patent-velocity firing on ticker X precede estimate-
revisions firing on the SAME ticker X" — that per-ticker join isn't
possible with the data that exists today.

What IS real and computable: treating each signal_type's daily FIRING
INTENSITY (how many times it fired, net-directional) as its own time
series, and cross-correlating that series against every other signal_type's
series (and against SPY's own return series) across a range of lags. This
is the same methodology used for factor-momentum research (treating a
factor's aggregate behavior as a series, not tracking individual stock
pairs) and is exactly the lead/lag + t-stat math already proven correct
and shipped in justhodl-liquidity-inflection's lead_curve function — same
approach, applied to signal families instead of assets.

A signal family with n < MIN_N firings in the window is excluded from
grading — this is the same Wilson/MIN_SAMPLE discipline signal-scorecard
already applies, just for a correlation instead of a hit-rate.

OUTPUT  data/signal-genealogy.json   SCHEDULE daily 06:40 UTC.
"""
import json
import math
import os
import time
import urllib.request as request
from datetime import datetime, timezone, timedelta
from statistics import mean, stdev

import boto3

VERSION = "1.1.0"
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/signal-genealogy.json"
OUTCOMES_TABLE = "justhodl-outcomes"
BENCH = "SPY"
MIN_N = 20              # minimum firings in-window to grade a signal_type at all
MIN_PAIR_N = 15         # minimum overlapping periods to grade a pair's lead-lag
MAX_LAG_DAYS = 21        # how far to search for a lead/lag relationship
FDR_Q = 0.10             # same false-discovery-rate threshold signal-scorecard uses
UP_DIRS = {"UP", "OUTPERFORM", "BULLISH", "LONG"}
DOWN_DIRS = {"DOWN", "UNDERPERFORM", "BEARISH", "SHORT"}

ddb = boto3.resource("dynamodb", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def norm_sf(z):
    """One-sided upper-tail of the standard normal (survival function). Same as
    signal-scorecard's — normal approximation is fine at our n (>=15)."""
    return 0.5 * math.erfc(z / math.sqrt(2.0))


def pval(t):
    """Two-tailed p-value from a t-stat via the normal approximation."""
    return 2 * norm_sf(abs(t))


def bh_fdr(pairs, q=FDR_Q):
    """Benjamini-Hochberg, identical to signal-scorecard's. pairs = [(key, p_value), ...].
    Returns the set of keys whose null is rejected controlling FDR at q.

    THIS IS THE FIX for a real bug caught before this ever reached a page: searching
    29-43 lags per test and reporting only the best |corr| inflates apparent
    significance hard — a genuinely null pair still produces a non-trivial max-of-many
    correlation by chance. Without this correction, the first live run showed 1,455/3,501
    pairs (41.6%) as 'significant' at |t|>=2.0, when pure chance predicts ~5%. Every
    (pair, lag) combination actually tested is treated as one hypothesis in one shared
    correction, not just the cherry-picked best-per-pair result."""
    m = len(pairs)
    if m == 0:
        return set()
    ordered = sorted(pairs, key=lambda kv: kv[1])
    k_max = 0
    for i, (_, p) in enumerate(ordered, start=1):
        if p <= (i / m) * q:
            k_max = i
    return {ordered[i][0] for i in range(k_max)}


def scan_outcomes():
    table = ddb.Table(OUTCOMES_TABLE)
    items, kwargs = [], {}
    while True:
        resp = table.scan(**kwargs)
        items.extend(resp.get("Items", []))
        lek = resp.get("LastEvaluatedKey")
        if not lek:
            break
        kwargs["ExclusiveStartKey"] = lek
        if len(items) > 100000:
            break
    return items


def _spy_history():
    """SPY daily closes -> {YYYY-MM-DD: close}. Same proven pattern as signal-scorecard."""
    start = (datetime.now(timezone.utc) - timedelta(days=130)).strftime("%Y-%m-%d")
    out = {}
    fmp = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
    try:
        u = (f"https://financialmodelingprep.com/stable/historical-price-eod/full"
             f"?symbol={BENCH}&from={start}&apikey={fmp}")
        d = json.loads(request.urlopen(request.Request(u, headers={"User-Agent": "jh-genealogy"}), timeout=25).read())
        rows = d if isinstance(d, list) else d.get("historical", [])
        for r in rows:
            dt, c = r.get("date"), r.get("adjClose", r.get("close"))
            if dt and c is not None:
                out[str(dt)[:10]] = float(c)
    except Exception as e:
        print(f"[genealogy] FMP SPY history failed: {e}")
    return out


def _corr(xs, ys):
    n = len(xs)
    if n < 3:
        return 0.0
    mx, my = mean(xs), mean(ys)
    sx = sum((a - mx) ** 2 for a in xs) ** 0.5
    sy = sum((b - my) ** 2 for b in ys) ** 0.5
    return (sum((a - mx) * (b - my) for a, b in zip(xs, ys)) / (sx * sy)) if sx and sy else 0.0


def _tstat(c, n):
    if abs(c) >= 0.999 or n < 4:
        return 0.0
    return c * (((n - 2) / (1 - c * c)) ** 0.5)


def build_series(items):
    """Per signal_type: {date: (count, net_directional_intensity)}, daily-binned."""
    per_type = {}
    for it in items:
        st = it.get("signal_type")
        logged = it.get("logged_at")
        if not st or not logged:
            continue
        try:
            d = str(logged)[:10]
        except Exception:
            continue
        pd = str(it.get("predicted_dir") or "").strip().upper()
        direction = 1 if pd in UP_DIRS else (-1 if pd in DOWN_DIRS else 0)
        per_type.setdefault(st, {}).setdefault(d, [0, 0])
        per_type[st][d][0] += 1
        per_type[st][d][1] += direction
    return per_type


def dense_series(day_map, all_days):
    """Fill gaps with 0 so every signal_type's series is aligned on the same day axis."""
    counts = [day_map.get(d, [0, 0])[0] for d in all_days]
    net = [day_map.get(d, [0, 0])[1] for d in all_days]
    return counts, net


def lead_lag(seriesA, seriesB, max_lag=MAX_LAG_DAYS):
    """Positive lag => A leads B by `lag` days (A[t] correlates with B[t+lag]).
    Returns the lag with peak |corr|, its t-stat, n, and the full curve."""
    n = len(seriesA)
    best = {"lag": 0, "corr": 0.0, "t": 0.0, "n": 0}
    curve = []
    for lag in range(-max_lag, max_lag + 1):
        if lag >= 0:
            a = seriesA[: n - lag] if lag else seriesA
            b = seriesB[lag:]
        else:
            a = seriesA[-lag:]
            b = seriesB[: n + lag]
        if len(a) < MIN_PAIR_N:
            continue
        c = _corr(a, b)
        t = _tstat(c, len(a))
        curve.append({"lag": lag, "corr": round(c, 3), "t": round(t, 2), "n": len(a)})
        if abs(c) > abs(best["corr"]):
            best = {"lag": lag, "corr": round(c, 3), "t": round(t, 2), "n": len(a)}
    return best, curve


def sibling_root(signal_type):
    """Signals like crisis_dfii10_vs_gld / crisis_dfii10_vs_spy share the SAME underlying
    input (dfii10) paired against different assets — they will correlate near-instantly for
    mechanical reasons, not because one genuinely leads the other. Same for _vs_ pairs
    sharing a root generally. Strip the '_vs_X' suffix to detect siblings."""
    if "_vs_" in signal_type:
        return signal_type.split("_vs_")[0]
    return None


def lambda_handler(event=None, context=None):
    t0 = time.time()
    items = scan_outcomes()
    print(f"[genealogy] scanned {len(items)} outcome records")

    per_type = build_series(items)
    counts_total = {st: sum(v[0] for v in day_map.values()) for st, day_map in per_type.items()}
    qualifying = sorted([st for st, n in counts_total.items() if n >= MIN_N],
                        key=lambda st: -counts_total[st])
    print(f"[genealogy] {len(qualifying)}/{len(per_type)} signal_types clear MIN_N={MIN_N}")

    all_days_set = set()
    for st in qualifying:
        all_days_set.update(per_type[st].keys())
    all_days = sorted(all_days_set)

    spy_hist = _spy_history()
    spy_ret = []
    for i, d in enumerate(all_days):
        if i == 0 or d not in spy_hist:
            spy_ret.append(0.0)
            continue
        prev_d = all_days[i - 1]
        p0, p1 = spy_hist.get(prev_d), spy_hist.get(d)
        spy_ret.append((p1 / p0 - 1) * 100 if (p0 and p1) else 0.0)

    dense = {}
    for st in qualifying:
        counts, net = dense_series(per_type[st], all_days)
        dense[st] = {"counts": counts, "net": net}

    # ── each qualifying signal vs SPY: is it a genuine leading indicator of the market? ──
    # Collect EVERY lag tested (not just the best) so FDR correction sees the true
    # multiple-comparisons burden — each signal searches ~43 lags (-21..+21).
    spy_tests = []           # (key, p_value) for every (signal, lag) actually tested
    spy_curves = {}          # signal_type -> {lag: {corr,t,n}}
    for st in qualifying:
        best, curve = lead_lag(dense[st]["net"], spy_ret)
        spy_curves[st] = {c["lag"]: c for c in curve}
        for c in curve:
            spy_tests.append(((st, c["lag"]), pval(c["t"])))

    spy_survivors = bh_fdr(spy_tests, q=FDR_Q)
    vs_spy = {}
    for st in qualifying:
        curve = spy_curves.get(st, {})
        surviving_lags = [lag for lag in curve if (st, lag) in spy_survivors]
        if not surviving_lags:
            continue
        best_lag = max(surviving_lags, key=lambda l: abs(curve[l]["corr"]))
        vs_spy[st] = curve[best_lag]
        vs_spy[st]["lag"] = best_lag

    # ── pairwise signal-vs-signal lead/lag (capped pair count for a bounded runtime) ──
    # Same discipline: collect every (pair, lag) tested, FDR-correct across the whole
    # family, then only keep a pair's SURVIVING best lag — not its raw best-of-29.
    MAX_PAIRS = 3500
    pair_curves = {}         # (a,b) -> {lag: {corr,t,n}}
    all_tests = []           # ((a,b,lag), p_value)
    checked = 0
    for i, a in enumerate(qualifying):
        for b in qualifying[i + 1:]:
            checked += 1
            if checked > MAX_PAIRS:
                break
            _, curve = lead_lag(dense[a]["net"], dense[b]["net"], max_lag=14)
            pair_curves[(a, b)] = {c["lag"]: c for c in curve}
            for c in curve:
                all_tests.append(((a, b, c["lag"]), pval(c["t"])))
        if checked > MAX_PAIRS:
            break

    survivors = bh_fdr(all_tests, q=FDR_Q)
    n_sibling_excluded = 0
    pairs = []
    for (a, b), curve in pair_curves.items():
        root_a, root_b = sibling_root(a), sibling_root(b)
        if root_a and root_a == root_b:
            n_sibling_excluded += 1
            continue                     # same underlying input, different pairing -- not a real cascade
        surviving_lags = [lag for lag in curve if (a, b, lag) in survivors and lag != 0
                          and curve[lag]["n"] >= MIN_PAIR_N]
        if not surviving_lags:
            continue
        best_lag = max(surviving_lags, key=lambda l: abs(curve[l]["corr"]))
        c = curve[best_lag]
        leader, follower = (a, b) if best_lag > 0 else (b, a)
        pairs.append({"leader": leader, "follower": follower,
                     "lag_days": abs(best_lag), "corr": c["corr"], "t": c["t"], "n": c["n"]})
    pairs.sort(key=lambda p: -abs(p["t"]))
    print(f"[genealogy] excluded {n_sibling_excluded} sibling pairs (shared underlying variable)")

    lead_score, follow_score = {}, {}
    for p in pairs:
        w = min(abs(p["t"]), 6.0)
        lead_score[p["leader"]] = lead_score.get(p["leader"], 0) + w
        follow_score[p["follower"]] = follow_score.get(p["follower"], 0) + w

    leaderboard = []
    for st in qualifying:
        ls, fs = lead_score.get(st, 0.0), follow_score.get(st, 0.0)
        spy_lead = vs_spy.get(st, {})
        leads_spy = spy_lead.get("lag", 0) > 0 and abs(spy_lead.get("t", 0)) >= 2.0
        leaderboard.append({
            "signal_type": st, "n_firings": counts_total[st],
            "lead_score": round(ls, 1), "follow_score": round(fs, 1),
            "earliness_index": round(ls - fs, 1),
            "vs_spy_lag_days": spy_lead.get("lag"), "vs_spy_corr": spy_lead.get("corr"),
            "vs_spy_t": spy_lead.get("t"), "leads_spy": leads_spy,
        })
    leaderboard.sort(key=lambda r: -r["earliness_index"])

    out = {
        "engine": "signal-genealogy", "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "thesis": "Which of the platform's own signal families fire early (before other "
                  "signals and before price), and which only confirm what's already moved. "
                  "Extends EDGE-ACCURACY from 'is this signal right' to 'is this signal right "
                  "AND early.'",
        "scope_note": "The outcomes ledger tracks signal_type and firing date, not ticker — so "
                      "this measures FAMILY-LEVEL firing-intensity lead/lag (does a surge in one "
                      "signal family's activity precede a surge in another's, or in SPY returns), "
                      "not per-ticker sequencing. A true per-ticker version needs a ticker field "
                      "added to future outcome-ledger writes.",
        "window": {"start": all_days[0] if all_days else None, "end": all_days[-1] if all_days else None,
                   "n_days": len(all_days), "n_signal_types_total": len(per_type),
                   "n_signal_types_qualifying": len(qualifying), "min_n_firings": MIN_N},
        "leaderboard": leaderboard,
        "earliest_signals": leaderboard[:20],
        "most_confirmatory_signals": sorted(leaderboard, key=lambda r: r["earliness_index"])[:20],
        "leads_spy": sorted([r for r in leaderboard if r["leads_spy"]],
                            key=lambda r: -(r["vs_spy_t"] or 0))[:20],
        "significant_cascades": pairs[:60],
        "n_pairs_tested": checked, "n_hypothesis_tests": len(all_tests),
        "n_sibling_pairs_excluded": n_sibling_excluded,
        "n_significant_pairs": len(pairs),
        "fdr_note": f"{len(all_tests)} total (pair, lag) hypotheses tested across {checked} pairs "
                   f"and up to 29 lags each; Benjamini-Hochberg FDR at q={FDR_Q} applied across the "
                   f"WHOLE family before any pair is called significant — not just its cherry-picked "
                   f"best lag. {n_sibling_excluded} pairs were further excluded as 'siblings' (e.g. "
                   f"crisis_dfii10_vs_gld and crisis_dfii10_vs_spy share the dfii10 input and "
                   f"correlate near-instantly for mechanical reasons, not genuine lead-lag). Without "
                   f"the FDR correction the first run showed 1,455/3,501 pairs (41.6%) as "
                   f"'significant' at a raw |t|>=2.0 threshold, when independent noise predicts ~5%.",
        "methodology": {
            "series": "daily net-directional firing intensity per signal_type (UP-coded minus "
                     "DOWN-coded firings per day), zero-filled on days with no firing",
            "lead_lag": "cross-correlation at every lag from -21 to +21 days (or -14..+14 for "
                       "pairwise signal-vs-signal tests); every (pair, lag) combination actually "
                       "tested is treated as one hypothesis in a single Benjamini-Hochberg FDR "
                       "correction at q=0.10 — a pair only counts as a significant cascade if its "
                       "best SURVIVING lag clears FDR, not just its raw best-of-many correlation.",
            "earliness_index": "lead_score minus follow_score, where each significant pairwise "
                              "relationship contributes its (capped) t-stat to whichever signal "
                              "was the leader or the follower. Positive = net leader, negative = "
                              "net confirmatory.",
        },
        "elapsed_s": round(time.time() - t0, 2),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    print(f"[genealogy] qualifying={len(qualifying)} pairs_tested={checked} "
          f"significant={len(pairs)} {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "n_qualifying": len(qualifying),
            "n_significant_pairs": len(pairs)})}
