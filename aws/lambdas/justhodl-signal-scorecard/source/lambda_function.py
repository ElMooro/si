"""justhodl-signal-scorecard — signal quality grading + decay enforcement.

The platform tracks 200+ signals and a calibrator already learns weights.
What was missing is the institutional discipline layer: a visible scoreboard
of every signal's REALISED edge, and ACTIVE deprecation of dead signals.
Funds kill the bottom half of their signals — this engine does it on evidence.

WHAT COUNTS AS A SCORABLE OUTCOME
═════════════════════════════════
A signal is graded ONLY on outcomes that can actually confirm or deny it.
Three classes of outcome are excluded from grading, because scoring them
would punish a signal for a data problem rather than for being wrong:

  1. NEUTRAL prediction — the signal made no directional call. predicted_dir
     not in {UP,DOWN,OUTPERFORM,UNDERPERFORM,...}. Counted as n_neutral.
  2. LEGACY records — flagged is_legacy=1 (legacy_reason pre_baseline_fix_*).
     correct=None, actual_direction=UNKNOWN, price_at_signal=0. Never validly
     scored. Counted as n_legacy.
  3. UNRESOLVED outcomes — the outcome-checker could not establish a real
     realised move (actual_direction UNKNOWN/NEUTRAL, or a no-op price where
     price_at_signal == price_at_check). Counted as n_unresolved — this also
     SURFACES upstream data-pipeline gaps (e.g. an unpriceable instrument).

Only SCORED outcomes (directional prediction + resolved + non-legacy) drive
the hit-rate, Wilson LB, grade and status.

CORRECTNESS IS DERIVED, NOT TRUSTED
═══════════════════════════════════
For every scored outcome the engine recomputes correctness from ground
truth — predicted direction vs the sign of the realised (relative or
absolute) return — rather than trusting the stored `correct` flag. The
agreement rate with the stored flag is reported as an integrity metric.

METHOD
══════
For each signal_type, over its SCORED outcomes only:
  • n_scored, raw hit-rate
  • Wilson 95% lower bound on the hit-rate — a 90%-hit signal with n=4 is
    NOT graded above a 56%-hit signal with n=300. Small samples earn it.
  • average realised return
  • a letter GRADE (A-F) and a STATUS:
       PROMOTED     — proven edge          (Wilson LB >= 0.57)
       ACTIVE       — performing           (LB 0.45-0.57, adequate sample)
       INSUFFICIENT — too few scored outcomes to grade (n_scored < MIN)
       DEPRECATED   — proven no edge        (LB < 0.45 with n_scored >= DEPRECATE_N)
  • performance_multiplier: PROMOTED 1.25, ACTIVE 1.0, INSUFFICIENT 1.0
    (no evidence -> no penalty), DEPRECATED 0.0 — enforced, not cosmetic.

OUTPUTS
═══════
  s3://.../data/signal-scorecard.json   — the visible board
  SSM /justhodl/calibration/scorecard   — {signal_type: multiplier}
Schedule: daily.
"""
import json, os, time, math
from datetime import datetime, timezone
from urllib import request
import boto3
from boto3.dynamodb.conditions import Attr

S3_BUCKET = "justhodl-dashboard-live"
S3_KEY = "data/signal-scorecard.json"
OUTCOMES_TABLE = "justhodl-outcomes"
SSM_PARAM = "/justhodl/calibration/scorecard"

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

MIN_SAMPLE = 15        # below this many SCORED outcomes -> INSUFFICIENT
DEPRECATE_N = 25       # need this many scored outcomes before we kill a signal
PROMOTE_LB = 0.57      # Wilson LB to earn PROMOTED
DEPRECATE_LB = 0.45    # Wilson LB below this (with enough n) -> DEPRECATED

# ── Alpha attribution (benchmark-relative edge) ──
# Directional hit-rate rewards beta in a melt-up. The honest edge of an engine
# is the forward EXCESS return of its picks over SPY. We compute it from the
# prices already stored on every outcome row + a SPY daily-history lookup, so
# the entire historical ledger becomes alpha-gradable with no migration.
BENCH = "SPY"
MIN_ALPHA_N = 20       # min scored excess observations to test an engine for alpha
FDR_Q = 0.10           # Benjamini-Hochberg false-discovery rate across engines
ALPHA_SSM_PARAM = "/justhodl/calibration/alpha"
ALPHA_S3_KEY = "data/engine-alpha.json"

# predicted_dir buckets — a prediction only scores if it made a directional call
UP_DIRS = {"UP", "OUTPERFORM", "LONG", "BULLISH"}
DOWN_DIRS = {"DOWN", "UNDERPERFORM", "SHORT", "BEARISH"}
DIRECTIONAL = UP_DIRS | DOWN_DIRS

MULTIPLIER = {"PROMOTED": 1.25, "ACTIVE": 1.0, "INSUFFICIENT": 1.0, "DEPRECATED": 0.0}

s3 = boto3.client("s3", region_name="us-east-1")
ddb = boto3.resource("dynamodb", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")


def maybe_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"[tg] no creds: {msg[:80]}")
        return
    try:
        body = json.dumps({"chat_id": TELEGRAM_CHAT_ID, "text": msg,
                            "parse_mode": "HTML", "disable_web_page_preview": True}).encode("utf-8")
        req = request.Request(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                               data=body, headers={"Content-Type": "application/json"})
        request.urlopen(req, timeout=10).read()
    except Exception as e:
        print(f"[tg] err: {e}")


def wilson_lower(hits, n, z=1.96):
    """Wilson score interval lower bound — confidence-adjusted hit rate."""
    if n == 0:
        return 0.0
    p = hits / n
    denom = 1 + z * z / n
    centre = p + z * z / (2 * n)
    margin = z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n)
    return max(0.0, (centre - margin) / denom)


def norm_sf(z):
    """One-sided upper-tail of the standard normal (survival function)."""
    return 0.5 * math.erfc(z / math.sqrt(2.0))


def bh_fdr(pairs, q=FDR_Q):
    """Benjamini-Hochberg. pairs = [(key, p_value), ...].
    Returns the set of keys whose null is rejected controlling FDR at q."""
    m = len(pairs)
    if m == 0:
        return set()
    ordered = sorted(pairs, key=lambda kv: kv[1])
    k_max = 0
    for i, (_, p) in enumerate(ordered, start=1):
        if p <= (i / m) * q:
            k_max = i
    return {ordered[i][0] for i in range(k_max)}


def alpha_stats(excess):
    """Distribution of forward excess returns (%, vs SPY) -> edge statistics.
    Returns dict or None if too few observations."""
    n = len(excess)
    if n < MIN_ALPHA_N:
        return None
    mean = sum(excess) / n
    var = sum((x - mean) ** 2 for x in excess) / (n - 1) if n > 1 else 0.0
    sd = math.sqrt(var)
    se = sd / math.sqrt(n) if sd > 0 else 0.0
    t = mean / se if se > 0 else 0.0
    ir = mean / sd if sd > 0 else 0.0          # per-observation information ratio
    p_one = norm_sf(t) if t > 0 else (1.0 - norm_sf(-t))  # H1: mean > 0
    hit = sum(1 for x in excess if x > 0) / n   # share of picks that beat SPY
    return {"alpha_n": n, "alpha_mean_excess_pct": round(mean, 3),
            "alpha_sd_pct": round(sd, 3), "alpha_t_stat": round(t, 2),
            "info_ratio": round(ir, 3), "alpha_hit_rate": round(hit, 3),
            "alpha_p_value": round(p_one, 5)}


def _spy_history():
    """SPY daily closes -> {YYYY-MM-DD: close} over ~420 days. FMP /stable/ then Polygon."""
    import datetime as _dt
    start = (datetime.now(timezone.utc) - _dt.timedelta(days=430)).strftime("%Y-%m-%d")
    out = {}
    fmp = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
    try:
        u = (f"https://financialmodelingprep.com/stable/historical-price-eod/full"
             f"?symbol={BENCH}&from={start}&apikey={fmp}")
        d = json.loads(request.urlopen(request.Request(u, headers={"User-Agent": "jh-sc"}), timeout=25).read())
        rows = d if isinstance(d, list) else d.get("historical", [])
        for r in rows:
            dt, c = r.get("date"), r.get("adjClose", r.get("close"))
            if dt and c is not None:
                out[str(dt)[:10]] = float(c)
    except Exception as e:
        print(f"[scorecard] FMP SPY history failed: {e}")
    if len(out) < 30:
        try:
            pk = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            u = (f"https://api.polygon.io/v2/aggs/ticker/{BENCH}/range/1/day/{start}/{today}"
                 f"?adjusted=true&sort=asc&limit=500&apiKey={pk}")
            d = json.loads(request.urlopen(request.Request(u, headers={"User-Agent": "jh-sc"}), timeout=25).read())
            for r in (d.get("results") or []):
                dt = datetime.fromtimestamp(r["t"] / 1000, timezone.utc).strftime("%Y-%m-%d")
                out[dt] = float(r["c"])
        except Exception as e:
            print(f"[scorecard] Polygon SPY history failed: {e}")
    print(f"[scorecard] SPY history points: {len(out)}")
    return out


def _spy_on(hist, iso):
    """SPY close on the date of an ISO timestamp; walk back up to 6 days to the
    nearest prior trading day."""
    if not iso:
        return None
    import datetime as _dt
    try:
        d = datetime.fromisoformat(str(iso).replace("Z", "+00:00")).date()
    except Exception:
        try:
            d = _dt.date.fromisoformat(str(iso)[:10])
        except Exception:
            return None
    for _ in range(7):
        k = d.isoformat()
        if k in hist:
            return hist[k]
        d -= _dt.timedelta(days=1)
    return None


def num(v):
    """Return float(v) or None if v is not a real number."""
    if v is None:
        return None
    try:
        f = float(v)
        return f if f == f else None   # reject NaN
    except (TypeError, ValueError):
        return None


def predicted_dir(o):
    """Normalised predicted direction; '' when no directional call was made."""
    pd = (o.get("predicted_dir") or o.get("predicted_direction") or "").strip().upper()
    return pd if pd in DIRECTIONAL else ""


def classify(o, pd):
    """Decide how an outcome should be treated. Returns (state, correct).

    state ∈ {legacy, unresolved, scored}; correct is bool only when scored.
    Correctness is DERIVED from ground truth, not read from the stored flag.
    """
    # 1 — legacy: explicitly flagged pre-baseline records, never validly scored
    if o.get("is_legacy") or o.get("legacy_reason"):
        return "legacy", None

    oc = o.get("outcome") or {}
    up_pred = pd in UP_DIRS

    # 2 — relative outcome (benchmark-relative signals e.g. screener_top_pick)
    er = num(oc.get("excess_return"))
    if er is not None:
        if er == 0.0:
            return "unresolved", None        # no measurable relative move
        correct = (er > 0) if up_pred else (er < 0)
        return "scored", correct

    # 3 — absolute outcome: needs a real realised direction
    ad = str(oc.get("actual_direction") or "").strip().upper()
    rp = num(oc.get("return_pct"))
    p_sig = num(oc.get("price_at_signal"))
    p_chk = num(oc.get("price_at_check"))
    # no-op price artifact: checker could not price the instrument
    if (p_sig is not None and p_chk is not None
            and p_sig == p_chk) or p_sig == 0:
        return "unresolved", None
    if ad in ("UP", "DOWN") and rp is not None:
        correct = (ad == "UP") if up_pred else (ad == "DOWN")
        return "scored", correct
    # fall back to return sign when actual_direction missing but return is real
    if rp is not None and rp != 0.0:
        correct = (rp > 0) if up_pred else (rp < 0)
        return "scored", correct

    return "unresolved", None


def grade_for(lb, n_scored):
    if n_scored < MIN_SAMPLE:
        return "—"
    if lb >= 0.60:
        return "A"
    if lb >= 0.53:
        return "B"
    if lb >= 0.47:
        return "C"
    if lb >= 0.40:
        return "D"
    return "F"


def status_for(lb, n_scored):
    """Status is decided on the SCORED sample only."""
    if n_scored < MIN_SAMPLE:
        return "INSUFFICIENT"          # cannot grade -> no penalty, no boost
    if lb >= PROMOTE_LB:
        return "PROMOTED"
    if lb < DEPRECATE_LB and n_scored >= DEPRECATE_N:
        return "DEPRECATED"            # proven it cannot beat a coin flip
    return "ACTIVE"


def scan_outcomes():
    """Full scan of the outcomes table with pagination."""
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


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[signal-scorecard] starting {datetime.now(timezone.utc).isoformat()}")

    outcomes = scan_outcomes()
    print(f"[signal-scorecard] scanned {len(outcomes)} outcome records")

    spy_hist = _spy_history()   # for benchmark-relative (alpha) attribution

    # group by signal_type, classifying every outcome
    by_sig = {}
    for o in outcomes:
        st = o.get("signal_type")
        if not st:
            continue
        g = by_sig.setdefault(st, {"n": 0, "n_neutral": 0, "n_legacy": 0,
                                   "n_unresolved": 0, "n_scored": 0, "hits": 0,
                                   "stored_agree": 0, "rets": [], "excess": [], "windows": {},
                                   "by_regime": {}})
        g["n"] += 1
        pd = predicted_dir(o)
        if not pd:
            g["n_neutral"] += 1
            continue

        state, correct = classify(o, pd)
        if state == "legacy":
            g["n_legacy"] += 1
            continue
        if state == "unresolved":
            g["n_unresolved"] += 1
            continue

        # state == "scored"
        g["n_scored"] += 1
        if correct:
            g["hits"] += 1
        # integrity check vs the stored flag
        stored = o.get("correct")
        if isinstance(stored, bool) or stored in (0, 1):
            if bool(stored) == bool(correct):
                g["stored_agree"] += 1
        oc = o.get("outcome") or {}
        r = num(oc.get("return_pct"))
        if r is None:
            r = num(oc.get("excess_return"))
        if r is not None:
            g["rets"].append(r)

        # ── benchmark-relative excess (alpha) ──────────────────────────────
        # excess = asset_return - SPY_return over the SAME window, computed from
        # the prices already stored on the outcome row. Signed by the call so a
        # correct short scores positive alpha too.
        sx = None
        p_sig = num(oc.get("price_at_signal"))
        p_chk = num(oc.get("price_at_check"))
        if p_sig and p_chk and p_sig > 0:
            spy0 = _spy_on(spy_hist, o.get("logged_at"))
            spy1 = _spy_on(spy_hist, oc.get("checked_at") or o.get("checked_at"))
            if spy0 and spy1 and spy0 > 0:
                asset_ret = (p_chk / p_sig - 1.0) * 100.0
                spy_ret = (spy1 / spy0 - 1.0) * 100.0
                ex = asset_ret - spy_ret
                sx = ex if pd in UP_DIRS else -ex
                g["excess"].append(sx)
        wk = o.get("window_key") or "all"
        w = g["windows"].setdefault(wk, {"n": 0, "hits": 0})
        w["n"] += 1
        if correct:
            w["hits"] += 1
        # regime-conditioned tally (institutional: edges are not stationary)
        rg = str(o.get("regime_at_log") or "UNKNOWN")
        rgd = g["by_regime"].setdefault(rg, {"n": 0, "hits": 0, "rets": [], "excess": []})
        rgd["n"] += 1
        if correct:
            rgd["hits"] += 1
        if r is not None:
            rgd["rets"].append(r)
        if sx is not None:
            rgd["excess"].append(sx)

    scorecard = []
    for st, g in by_sig.items():
        n_scored = g["n_scored"]
        hits = g["hits"]
        hit_rate = hits / n_scored if n_scored else 0.0
        lb = wilson_lower(hits, n_scored)
        avg_ret = round(sum(g["rets"]) / len(g["rets"]), 2) if g["rets"] else None
        grade = grade_for(lb, n_scored)
        status = status_for(lb, n_scored)
        agree = round(g["stored_agree"] / n_scored, 3) if n_scored else None
        astats = alpha_stats(g["excess"])
        windows = {wk: {"n": w["n"],
                        "hit_rate": round(w["hits"] / w["n"], 3) if w["n"] else None,
                        "wilson_lb": round(wilson_lower(w["hits"], w["n"]), 3)}
                   for wk, w in g["windows"].items()}
        row = {
            "signal_type": st,
            "n_total": g["n"],
            "n_scored": n_scored,
            "n_neutral": g["n_neutral"],
            "n_legacy": g["n_legacy"],
            "n_unresolved": g["n_unresolved"],
            "hits": hits,
            "hit_rate": round(hit_rate, 3),
            "wilson_lb": round(lb, 3),
            "edge_vs_coinflip_pct": round((lb - 0.5) * 100, 1),
            "avg_return_pct": avg_ret,
            "stored_flag_agreement": agree,
            "grade": grade,
            "status": status,
            "performance_multiplier": MULTIPLIER[status],
            # alpha (benchmark-relative) — populated when MIN_ALPHA_N met
            "alpha_status": "INSUFFICIENT",
            "alpha_n": (astats or {}).get("alpha_n", len(g["excess"])),
            "alpha_mean_excess_pct": (astats or {}).get("alpha_mean_excess_pct"),
            "alpha_t_stat": (astats or {}).get("alpha_t_stat"),
            "info_ratio": (astats or {}).get("info_ratio"),
            "alpha_hit_rate": (astats or {}).get("alpha_hit_rate"),
            "alpha_p_value": (astats or {}).get("alpha_p_value"),
            "by_window": windows,
            "by_regime": {rg: {"n": v["n"],
                               "hit_rate": round(v["hits"] / v["n"], 3) if v["n"] else None,
                               "wilson_lb": round(wilson_lower(v["hits"], v["n"]), 3),
                               "avg_return_pct": round(sum(v["rets"]) / len(v["rets"]), 2) if v["rets"] else None,
                               "alpha_mean_excess_pct": round(sum(v["excess"]) / len(v["excess"]), 2) if len(v["excess"]) >= 5 else None}
                          for rg, v in sorted(g["by_regime"].items(), key=lambda kv: -kv[1]["n"])[:6]
                          if v["n"] >= 3},
        }
        scorecard.append(row)

    # ── Multiple-testing control: Benjamini-Hochberg FDR across all engines ──
    # Testing ~100+ engines at once, some clear any single threshold by chance.
    # We control the false-discovery rate on the alpha t-stats. Two one-sided
    # families: proven POSITIVE alpha and proven NEGATIVE (value-destroying) alpha.
    alpha_rows = [r for r in scorecard
                  if r["alpha_t_stat"] is not None and r["alpha_n"] >= MIN_ALPHA_N]
    proven_pos = bh_fdr([(r["signal_type"], norm_sf(r["alpha_t_stat"])) for r in alpha_rows], FDR_Q)
    proven_neg = bh_fdr([(r["signal_type"], norm_sf(-r["alpha_t_stat"])) for r in alpha_rows], FDR_Q)
    for r in scorecard:
        if r["alpha_t_stat"] is None or r["alpha_n"] < MIN_ALPHA_N:
            r["alpha_status"] = "INSUFFICIENT"
        elif r["signal_type"] in proven_pos:
            r["alpha_status"] = "ALPHA_PROVEN"        # FDR-significant edge over SPY
        elif r["signal_type"] in proven_neg:
            r["alpha_status"] = "ALPHA_NEGATIVE"      # FDR-significant value destruction
        else:
            r["alpha_status"] = "NO_ALPHA"            # tested, indistinguishable from beta

    alpha_proven = [r["signal_type"] for r in scorecard if r["alpha_status"] == "ALPHA_PROVEN"]
    alpha_negative = [r["signal_type"] for r in scorecard if r["alpha_status"] == "ALPHA_NEGATIVE"]
    alpha_tested = sum(1 for r in scorecard if r["alpha_status"] in
                       ("ALPHA_PROVEN", "ALPHA_NEGATIVE", "NO_ALPHA"))

    # rank: proven alpha first (by t-stat), then directional status, then Wilson LB
    order = {"PROMOTED": 0, "ACTIVE": 1, "INSUFFICIENT": 2, "DEPRECATED": 3}
    a_order = {"ALPHA_PROVEN": 0, "NO_ALPHA": 1, "INSUFFICIENT": 1, "ALPHA_NEGATIVE": 2}
    scorecard.sort(key=lambda r: (a_order.get(r["alpha_status"], 1),
                                  -(r["alpha_t_stat"] or -99),
                                  order[r["status"]], -r["wilson_lb"]))

    promoted = [r["signal_type"] for r in scorecard if r["status"] == "PROMOTED"]
    deprecated = [r["signal_type"] for r in scorecard if r["status"] == "DEPRECATED"]
    insufficient = [r["signal_type"] for r in scorecard if r["status"] == "INSUFFICIENT"]
    graded = [r for r in scorecard if r["n_scored"] >= MIN_SAMPLE]
    portfolio_lb = (round(sum(r["wilson_lb"] for r in graded) / len(graded), 3)
                    if graded else None)

    # data-quality flags: signals losing most of their outcomes to legacy/unresolved
    dq_flags = []
    for r in scorecard:
        lost = r["n_legacy"] + r["n_unresolved"]
        if r["n_total"] >= 20 and lost / r["n_total"] >= 0.5:
            dq_flags.append({"signal_type": r["signal_type"], "n_total": r["n_total"],
                             "n_legacy": r["n_legacy"], "n_unresolved": r["n_unresolved"],
                             "n_scored": r["n_scored"],
                             "note": ("mostly legacy — pre-baseline backfill"
                                      if r["n_legacy"] >= r["n_unresolved"]
                                      else "outcome-checker cannot resolve a realised move")})

    out = {
        "schema_version": "2.1",
        "method": "signal_scorecard_wilson_lb_scored_only",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": round(time.time() - t0, 1),
        "n_outcomes_scanned": len(outcomes),
        "n_outcomes_scored": sum(r["n_scored"] for r in scorecard),
        "n_outcomes_neutral": sum(r["n_neutral"] for r in scorecard),
        "n_outcomes_legacy": sum(r["n_legacy"] for r in scorecard),
        "n_outcomes_unresolved": sum(r["n_unresolved"] for r in scorecard),
        "n_signals_tracked": len(scorecard),
        "n_signals_graded": len(graded),
        "n_promoted": len(promoted),
        "n_deprecated": len(deprecated),
        "n_insufficient": len(insufficient),
        "avg_graded_wilson_lb": portfolio_lb,
        "promoted_signals": promoted,
        "deprecated_signals": deprecated,
        "alpha": {
            "benchmark": BENCH,
            "method": "forward excess return vs SPY over each pick's window; Benjamini-Hochberg FDR across engines",
            "fdr_q": FDR_Q,
            "min_alpha_n": MIN_ALPHA_N,
            "n_engines_tested": alpha_tested,
            "n_alpha_proven": len(alpha_proven),
            "n_alpha_negative": len(alpha_negative),
            "alpha_proven_signals": alpha_proven,
            "alpha_negative_signals": alpha_negative,
            "leaderboard": sorted(
                [{"signal_type": r["signal_type"], "alpha_status": r["alpha_status"],
                  "mean_excess_pct": r["alpha_mean_excess_pct"], "t_stat": r["alpha_t_stat"],
                  "info_ratio": r["info_ratio"], "alpha_hit_rate": r["alpha_hit_rate"], "n": r["alpha_n"]}
                 for r in scorecard if r["alpha_t_stat"] is not None and r["alpha_n"] >= MIN_ALPHA_N],
                key=lambda x: -(x["t_stat"] or -99))[:25],
        },
        "data_quality_flags": dq_flags,
        "scorecard": scorecard,
        "thresholds": {"min_sample": MIN_SAMPLE, "deprecate_n": DEPRECATE_N,
                       "promote_lb": PROMOTE_LB, "deprecate_lb": DEPRECATE_LB},
        "interpretation": (
            "Signals are graded ONLY on SCORED outcomes — a directional "
            "prediction with a resolved realised move. NEUTRAL predictions, "
            "legacy pre-baseline records, and unresolved outcomes (the "
            "outcome-checker could not price the instrument) are counted and "
            "reported but never scored as misses. Correctness is recomputed "
            "from ground truth, not trusted from the stored flag; "
            "stored_flag_agreement reports how often the two agree. DEPRECATED "
            "signals proved (n_scored>=25) they cannot beat a coin flip — "
            "their multiplier is 0. PROMOTED signals get 1.25x. INSUFFICIENT "
            "signals lack the scored sample to grade and keep a neutral 1.0x. "
            "data_quality_flags lists signals losing most outcomes to legacy "
            "or unresolved records — an upstream pipeline gap to fix."
        ),
    }

    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                   Body=json.dumps(out, default=str).encode("utf-8"),
                   ContentType="application/json", CacheControl="public, max-age=3600")

    # publish the enforcement map to SSM for downstream consumers
    multipliers = {r["signal_type"]: r["performance_multiplier"] for r in scorecard}
    try:
        ssm.put_parameter(Name=SSM_PARAM, Type="String", Overwrite=True,
                          Value=json.dumps({"updated_at": out["generated_at"],
                                            "multipliers": multipliers,
                                            "deprecated": deprecated}))
        print(f"[signal-scorecard] SSM {SSM_PARAM} updated ({len(multipliers)} signals)")
    except Exception as e:
        print(f"[signal-scorecard] SSM write failed: {e}")

    # publish the per-engine ALPHA map (benchmark-relative truth) for the
    # calibrator / best-setups / master-ranker to consume.
    alpha_map = {r["signal_type"]: {"alpha_status": r["alpha_status"],
                                    "t_stat": r["alpha_t_stat"],
                                    "mean_excess_pct": r["alpha_mean_excess_pct"],
                                    "info_ratio": r["info_ratio"],
                                    "alpha_n": r["alpha_n"]}
                 for r in scorecard if r["alpha_t_stat"] is not None}
    alpha_doc = {"generated_at": out["generated_at"], "benchmark": BENCH, "fdr_q": FDR_Q,
                 "n_alpha_proven": len(alpha_proven), "n_alpha_negative": len(alpha_negative),
                 "alpha_proven_signals": alpha_proven, "alpha_negative_signals": alpha_negative,
                 "engines": alpha_map}
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=ALPHA_S3_KEY,
                      Body=json.dumps(alpha_doc, default=str).encode("utf-8"),
                      ContentType="application/json", CacheControl="public, max-age=3600")
        ssm.put_parameter(Name=ALPHA_SSM_PARAM, Type="String", Overwrite=True,
                          Value=json.dumps({"updated_at": out["generated_at"],
                                            "alpha_proven": alpha_proven,
                                            "alpha_negative": alpha_negative}))
        print(f"[signal-scorecard] alpha map -> {ALPHA_S3_KEY} ({len(alpha_proven)} proven, {len(alpha_negative)} negative)")
    except Exception as e:
        print(f"[signal-scorecard] alpha map write failed: {e}")

    # alert on newly deprecated / promoted vs last run
    prior = {}
    try:
        prev = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY + ".prev")["Body"].read())
        prior = {r["signal_type"]: r["status"] for r in prev.get("scorecard", [])}
    except Exception:
        pass
    newly_dep = [s for s in deprecated if prior.get(s) and prior.get(s) != "DEPRECATED"]
    newly_pro = [s for s in promoted if prior.get(s) and prior.get(s) != "PROMOTED"]
    if newly_dep or newly_pro:
        lines = []
        if newly_dep:
            lines.append("DEPRECATED (no proven edge): " + ", ".join(newly_dep))
        if newly_pro:
            lines.append("PROMOTED (proven edge): " + ", ".join(newly_pro))
        maybe_telegram("[signal-scorecard] <b>Signal status changes</b>\n" + "\n".join(lines))
        
        # Emit one event per newly-changed signal so downstream engines
        # (engine-signal-map, miss-calibrator) can refresh immediately.
        # Fire-and-forget — never blocks scorecard run.
        try:
            from system_events import (
                publish_many, EVT_SIGNAL_DEPRECATED, EVT_SIGNAL_PROMOTED,
            )
            events_to_pub = []
            for s in newly_dep:
                # Find this signal's stats for richer event payload
                row = next((r for r in scorecard if r.get("signal_type") == s), {})
                events_to_pub.append((EVT_SIGNAL_DEPRECATED, {
                    "signal_type":   s,
                    "wilson_lb":     row.get("wilson_lb"),
                    "n_scored":      row.get("n_scored"),
                    "previous_status": prior.get(s),
                    "reason":        "scorecard moved below deprecate threshold",
                }))
            for s in newly_pro:
                row = next((r for r in scorecard if r.get("signal_type") == s), {})
                events_to_pub.append((EVT_SIGNAL_PROMOTED, {
                    "signal_type":   s,
                    "wilson_lb":     row.get("wilson_lb"),
                    "n_scored":      row.get("n_scored"),
                    "previous_status": prior.get(s),
                    "reason":        "scorecard reached promote threshold",
                }))
            # EventBridge limit: 10 per batch
            for i in range(0, len(events_to_pub), 10):
                publish_many(events_to_pub[i:i+10])
        except Exception as e:
            print(f"[signal-scorecard] event publish failed: {e}")
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY + ".prev",
                       Body=json.dumps(out, default=str).encode("utf-8"),
                       ContentType="application/json")
    except Exception:
        pass

    print(f"[signal-scorecard] done {out['elapsed_s']}s tracked={len(scorecard)} "
          f"graded={len(graded)} promoted={len(promoted)} deprecated={len(deprecated)} "
          f"insufficient={len(insufficient)} dq_flags={len(dq_flags)}")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "n_signals_tracked": len(scorecard), "n_signals_graded": len(graded),
        "n_promoted": len(promoted), "n_deprecated": len(deprecated),
        "n_insufficient": len(insufficient), "avg_graded_wilson_lb": portfolio_lb,
        "n_outcomes_scored": out["n_outcomes_scored"],
        "n_outcomes_legacy": out["n_outcomes_legacy"],
        "n_outcomes_unresolved": out["n_outcomes_unresolved"]})}
