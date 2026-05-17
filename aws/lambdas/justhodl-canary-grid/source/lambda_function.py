"""
justhodl-canary-grid — Global Early-Warning Grid  (Phase 1)

WHAT IT DOES
A leading, ex-US early-warning layer that front-runs the crisis-composite.
The crisis-composite is largely US / coincident plumbing; this engine
watches the canaries that crack FIRST — trade-exposed economies, cyclical
commodities, funding plumbing and labour hours — weeks to months before
US stress shows up.

THE GRID — 4 sub-grids, each a set of cross-confirming leading signals:
  • Trade & Shipping   — Korea & China exports (Korea reports first; its
                         exports are a pure global semiconductor-cycle read)
  • Commodity Cycle    — copper (Dr. Copper) and lumber, classic real-economy
                         leads
  • Funding Plumbing   — Swiss-franc haven bid + ingest of the eurodollar-
                         stress composite (no recompute of plumbing)
  • Labour & Industrial— US manufacturing weekly hours + temp employment +
                         Swiss unemployment (employers cut hours/temps and
                         small open economies wobble before the US labour
                         market turns)

METHOD (how a global-macro desk would build it)
Each signal is transformed (YoY / momentum / level change), z-scored against
its own 5-10y history, and mapped to a 0-100 STRESS score (higher = worse).
Sub-grids average their available signals; the composite Early-Warning Level
is a lead-time-weighted blend of the sub-grids, banded CALM -> CRITICAL.
Missing signals degrade gracefully — the grid never crashes on one bad feed.

DATA  FRED (the platform's own source) for all 9 Phase-1 signals + ingest of
      data/eurodollar-stress.json.  (DBnomics — dbnomics.py — is bundled and
      reserved for Phase 3 FRED-gaps: Taiwan export orders, KOF, Cu output.)
OUTPUT  data/canary-grid.json        SCHEDULE  daily 12:30 UTC

Research / education only — not financial advice.
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timezone

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
FRED_KEY = os.environ.get("FRED_KEY", "2f057499936072679d8843d7fce99989")
OUT_KEY = "data/canary-grid.json"
s3 = boto3.client("s3", region_name="us-east-1")

# data-freshness guard — an early-warning engine must be FORWARD-looking, so a
# signal whose latest reading is older than ~3 months is dropped from the
# composite entirely (it would only describe the past). A signal past the
# warn line is still used but flagged so the staleness is always visible.
STALE_WARN_DAYS = 65    # ~2 months — getting old, flag it
STALE_HARD_DAYS = 95    # ~3 months — exclude from the grid entirely

# ── signal definitions ──────────────────────────────────────────────
# kind: yoy=12-period %chg · mom=window %chg · diff=window abs change · level
# dir : "fall" = falling is stress · "rise" = rising is stress
SIGNALS = [
    dict(key="korea_exports", name="South Korea exports", grid="trade_shipping",
         fred="XTEXVA01KRM664S", kind="yoy", win=12, dir="fall", lead=3,
         limit=160, unit="%YoY",
         hot="South Korea's exports are contracting — Korea reports first and "
             "its exports track the global semiconductor cycle, so this is an "
             "early read on a worldwide trade slowdown.",
         cool="South Korea's exports are holding up — global trade demand "
              "looks intact for now."),
    dict(key="china_exports", name="China exports", grid="trade_shipping",
         fred="XTEXVA01CNM664S", kind="yoy", win=12, dir="fall", lead=3,
         limit=160, unit="%YoY",
         hot="China's exports are shrinking — global goods demand is weakening.",
         cool="China's exports are growing — global goods demand looks steady."),
    dict(key="copper", name="Copper price (Dr. Copper)", grid="commodity_cycle",
         fred="PCOPPUSDM", kind="yoy", win=12, dir="fall", lead=2,
         limit=160, unit="%YoY",
         hot="Copper is falling hard — 'Dr. Copper' has a long record of "
             "sniffing out industrial slowdowns before the data confirms them.",
         cool="Copper is firm — industrial demand looks healthy."),
    dict(key="lumber", name="Lumber & wood (PPI)", grid="commodity_cycle",
         fred="WPU081", kind="yoy", win=12, dir="fall", lead=2,
         limit=160, unit="%YoY",
         hot="Lumber prices are sliding — a classic early sign of housing and "
             "real-economy demand cooling.",
         cool="Lumber prices are stable — housing-linked demand looks okay."),
    dict(key="chf_haven", name="Swiss franc (haven bid)", grid="funding_plumbing",
         fred="DEXSZUS", kind="mom", win=63, dir="fall", lead=1,
         limit=1300, unit="%3m (CHF/USD)",
         hot="The Swiss franc is strengthening sharply — a flight into the "
             "classic haven currency signals global risk-off.",
         cool="The Swiss franc is steady — no haven panic in the currency "
              "market."),
    dict(key="mfg_hours", name="US mfg average weekly hours",
         grid="labor_industrial", fred="AWHMAN", kind="yoy", win=12,
         dir="fall", lead=2, limit=160, unit="%YoY",
         hot="US factories are cutting hours — employers trim the workweek "
             "before they cut jobs, so this leads the labour market.",
         cool="US factory hours are steady — no pre-layoff trimming yet."),
    dict(key="temp_help", name="US temp-help employment",
         grid="labor_industrial", fred="TEMPHELPS", kind="yoy", win=12,
         dir="fall", lead=2, limit=160, unit="%YoY",
         hot="US temp employment is falling — temps are the first workers let "
             "go, a reliable lead on broader job losses.",
         cool="US temp employment is holding — no early labour-market cracks."),
    dict(key="swiss_unemp", name="Switzerland unemployment", grid="labor_industrial",
         fred=["LMUNRRTTCHM156S", "LRUNTTTTCHM156S", "LRHUTTTTCHM156S"],
         kind="diff", win=6, dir="rise", lead=2, limit=160, unit="ppt 6m",
         hot="Swiss unemployment is rising — Switzerland is a sensitive "
             "global-risk bellwether and rising joblessness there has often "
             "preceded wider trouble.",
         cool="Swiss unemployment is flat to lower — the bellwether is calm."),
]
GRID_WEIGHT = {"trade_shipping": 0.30, "commodity_cycle": 0.20,
               "funding_plumbing": 0.30, "labor_industrial": 0.20}
GRID_LABEL = {"trade_shipping": "Trade & Shipping",
              "commodity_cycle": "Commodity Cycle",
              "funding_plumbing": "Funding Plumbing",
              "labor_industrial": "Labour & Industrial"}


# ── helpers ──────────────────────────────────────────────────────────
def fred(series_id, limit):
    """Return [(date, value|None), ...] newest-first, or [] on failure."""
    try:
        url = ("https://api.stlouisfed.org/fred/series/observations"
               f"?series_id={series_id}&api_key={FRED_KEY}&file_type=json"
               f"&sort_order=desc&limit={limit}")
        with urllib.request.urlopen(url, timeout=25) as r:
            obs = json.loads(r.read()).get("observations", [])
        out = []
        for o in obs:
            try:
                v = float(o.get("value"))
            except (TypeError, ValueError):
                v = None
            out.append((o.get("date"), v))
        return out
    except Exception as e:
        print(f"[canary] FRED {series_id}: {e}")
        return []


def fetch_observations(sid, limit):
    """Source-agnostic fetch -> [(date, value), ...] newest-first.
    An id containing '/' is DBnomics (PROVIDER/DATASET/SERIES); otherwise
    it is a FRED series id. Lets one signal try multiple sources."""
    if "/" in str(sid):
        try:
            from dbnomics import fetch_series
            pts = [(p, v) for p, v in fetch_series(sid) if v is not None]
            return list(reversed(pts))  # dbnomics returns oldest-first
        except Exception as e:
            print(f"[canary] DBnomics {sid}: {e}")
            return []
    return fred(sid, limit)


def mean(xs):
    xs = [x for x in xs if x is not None]
    return sum(xs) / len(xs) if xs else None


def stdev(xs, mu):
    xs = [x for x in xs if x is not None]
    if len(xs) < 2:
        return None
    return (sum((x - mu) ** 2 for x in xs) / (len(xs) - 1)) ** 0.5


def transform(obs, kind, win):
    """obs newest-first -> transformed series [(date, value), ...] newest-first."""
    vals = [(d, v) for d, v in obs if v is not None]
    if kind == "level":
        return list(vals)
    w = 12 if kind == "yoy" else win
    out = []
    for i in range(len(vals) - w):
        d, v = vals[i]
        vo = vals[i + w][1]
        if kind == "diff":
            out.append((d, v - vo))
        elif vo not in (0, None):
            out.append((d, v / vo - 1.0))
    return out


def to_stress(z, direction):
    s = 50 + z * 22 if direction == "rise" else 50 - z * 22
    return round(max(0.0, min(100.0, s)), 1)


def band(score):
    if score is None:
        return "NO DATA"
    if score >= 80:
        return "CRITICAL"
    if score >= 60:
        return "WARNING"
    if score >= 40:
        return "ELEVATED"
    if score >= 20:
        return "WATCH"
    return "CALM"


def age_days(date_str):
    """Days between today and an ISO date string; None if unparseable."""
    try:
        d = datetime.fromisoformat(str(date_str)[:10]).date()
        return (datetime.now(timezone.utc).date() - d).days
    except Exception:
        return None


# ── per-signal evaluation ────────────────────────────────────────────
def eval_signal(sig):
    base = {"key": sig["key"], "name": sig["name"], "sub_grid": sig["grid"],
            "lead_months": sig["lead"], "unit": sig["unit"]}
    ids = sig["fred"] if isinstance(sig["fred"], list) else [sig["fred"]]
    valid = []  # (age_rank, preference_idx, series, sid)
    for idx, sid in enumerate(ids):
        cand = transform(fetch_observations(sid, sig["limit"]),
                         sig["kind"], sig["win"])
        if len(cand) >= 24:
            a = age_days(cand[0][0])
            valid.append((a if a is not None else 99999, idx, cand, sid))
    if not valid:
        return {**base, "available": False,
                "reason": f"no series resolved ({', '.join(map(str, ids))})"}
    # prefer the first source in preference order that is FRESH (<= hard
    # limit); only if none are fresh fall back to the freshest stale one.
    fresh = [v for v in valid if v[0] <= STALE_HARD_DAYS]
    pick = (min(fresh, key=lambda v: v[1]) if fresh
            else min(valid, key=lambda v: v[0]))
    series, used = pick[2], pick[3]
    latest_date, latest_val = series[0]
    hist = [v for _, v in series]
    mu = mean(hist)
    sd = stdev(hist, mu)
    if sd in (None, 0):
        return {**base, "available": False, "reason": "zero variance"}
    z = (latest_val - mu) / sd
    stress = to_stress(z, sig["dir"])
    disp = (round(latest_val * 100, 2) if sig["kind"] in ("yoy", "mom")
            else round(latest_val, 2))
    read = (sig["hot"] if stress >= 60 else
            sig["cool"] if stress <= 40 else
            f"{sig['name']} is near its historical norm — neutral signal.")
    age = age_days(latest_date)
    if age is not None and age > STALE_HARD_DAYS:
        return {**base, "available": False, "as_of": latest_date,
                "age_days": age, "fred_series": used,
                "reason": (f"stale — latest reading is {age}d old "
                           f"(>{STALE_HARD_DAYS}d); excluded to keep the grid "
                           f"forward-looking")}
    return {**base, "available": True, "value": disp, "as_of": latest_date,
            "age_days": age,
            "stale_warning": bool(age is not None and age > STALE_WARN_DAYS),
            "fred_series": used, "transform": sig["kind"],
            "zscore": round(z, 2), "stress": stress, "read": read}


def ingest_eurodollar():
    """Plumbing — reuse the eurodollar-stress composite rather than recompute."""
    base = {"key": "eurodollar_stress", "name": "Eurodollar / USD funding stress",
            "sub_grid": "funding_plumbing", "lead_months": 0.5,
            "unit": "0-100 composite"}
    try:
        d = json.loads(s3.get_object(Bucket=S3_BUCKET,
                       Key="data/eurodollar-stress.json")["Body"].read())
        score = d.get("composite_score")
        if score is None:
            return {**base, "available": False, "reason": "no composite_score"}
        score = float(score)
        read = ("USD funding plumbing is under stress — cross-currency and "
                "repo signals are tightening." if score >= 60 else
                "USD funding plumbing looks orderly." if score <= 40 else
                "USD funding plumbing is mildly firm — worth watching.")
        as_of = d.get("as_of") or d.get("generated_at")
        age = age_days(as_of)
        if age is not None and age > STALE_HARD_DAYS:
            return {**base, "available": False, "as_of": as_of,
                    "age_days": age,
                    "reason": f"stale — eurodollar feed is {age}d old"}
        return {**base, "available": True, "value": round(score, 1),
                "as_of": as_of, "age_days": age,
                "stale_warning": bool(age is not None and age > STALE_WARN_DAYS),
                "transform": "ingest", "zscore": None,
                "stress": round(score, 1), "read": read}
    except Exception as e:
        return {**base, "available": False, "reason": f"feed unavailable: {e}"}


# ── handler ──────────────────────────────────────────────────────────
def lambda_handler(event, context):
    t0 = time.time()
    signals = [eval_signal(s) for s in SIGNALS]
    signals.append(ingest_eurodollar())

    # data-freshness audit — keep the grid forward-looking, never stale
    ages = [(s["key"], s["age_days"]) for s in signals
            if s.get("age_days") is not None]
    excluded_stale = [s["key"] for s in signals if not s.get("available")
                      and "stale" in str(s.get("reason", ""))]
    oldest = max(ages, key=lambda x: x[1]) if ages else None
    freshness = {
        "stale_hard_days": STALE_HARD_DAYS, "stale_warn_days": STALE_WARN_DAYS,
        "n_fresh": sum(1 for s in signals if s.get("available")
                       and not s.get("stale_warning")),
        "n_stale_warning": sum(1 for s in signals if s.get("stale_warning")),
        "n_excluded_stale": len(excluded_stale),
        "excluded_for_staleness": excluded_stale,
        "oldest_signal": ({"key": oldest[0], "age_days": oldest[1]}
                          if oldest else None),
    }

    # sub-grid scores
    sub_grids = {}
    for g, label in GRID_LABEL.items():
        live = [s for s in signals if s["sub_grid"] == g and s.get("available")]
        score = round(mean([s["stress"] for s in live]), 1) if live else None
        sub_grids[g] = {"label": label, "score": score, "band": band(score),
                        "n_signals": len(live),
                        "lead_months": round(mean([s["lead_months"] for s in
                                       signals if s["sub_grid"] == g]) or 0, 1)}

    # lead-time-weighted composite over available sub-grids
    num = den = 0.0
    for g, sg in sub_grids.items():
        if sg["score"] is not None:
            w = GRID_WEIGHT[g]
            num += w * sg["score"]
            den += w
    level = round(num / den, 1) if den > 0 else None
    lvl_band = band(level)

    live = [s for s in signals if s.get("available")]
    top = sorted(live, key=lambda s: s["stress"], reverse=True)[:4]

    headlines = {
        "CRITICAL": "Multiple global early-warning canaries are flashing red — "
                    "elevated danger of a developing crisis.",
        "WARNING": "Several leading canaries are deteriorating — global risk is "
                   "building ahead of the US data.",
        "ELEVATED": "Early-warning signals are mixed and somewhat elevated — "
                    "worth close monitoring.",
        "WATCH": "Most canaries are calm with a few soft spots — low but "
                 "non-zero early-warning risk.",
        "CALM": "Global early-warning canaries are calm — no leading sign of "
                "crisis in the trade, commodity, plumbing or labour data.",
        "NO DATA": "Insufficient data to compute the grid.",
    }

    out = {
        "schema_version": "1.0",
        "method": "leading_canary_zscore_grid",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": round(time.time() - t0, 2),
        "early_warning_level": level,
        "band": lvl_band,
        "headline": headlines.get(lvl_band, ""),
        "sub_grids": sub_grids,
        "freshness": freshness,
        "signals": signals,
        "top_deteriorating": [{"key": s["key"], "name": s["name"],
                               "stress": s["stress"], "read": s["read"]}
                              for s in top],
        "n_available": len(live),
        "n_total": len(signals),
        "methodology": ("Each signal is transformed (YoY, momentum or level "
                        "change), z-scored against its own 5-10 year history "
                        "and mapped to a 0-100 stress score. Sub-grids average "
                        "their available signals; the Early-Warning Level is a "
                        "lead-time-weighted blend (Trade & Plumbing 30% each, "
                        "Commodity & Labour 20% each). Faster signals lead by "
                        "days-weeks, trade/labour by 1-3 months. Missing feeds "
                        "are excluded, not guessed."),
        "disclaimer": ("Research and education only — not financial advice. "
                       "Leading indicators reduce but do not eliminate "
                       "uncertainty; false signals occur."),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, default=str).encode("utf-8"),
                  ContentType="application/json",
                  CacheControl="public, max-age=3600")
    print(f"[canary] level={level} {lvl_band} · {len(live)}/{len(signals)} "
          f"signals live · {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "early_warning_level": level, "band": lvl_band,
        "n_available": len(live), "n_total": len(signals)})}
