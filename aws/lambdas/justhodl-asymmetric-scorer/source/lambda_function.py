"""
justhodl-asymmetric-scorer — Phase 2B.

Reads screener/data.json, scores stocks on 4 dimensions:
  quality, safety, value, momentum.
Filters for stocks ranking well on ≥3 of 4 with quality gate passed.
Outputs top 30 setups + value_traps list to S3.
"""
import json
import os
import statistics
import urllib.request
import urllib.parse
import ssl
from datetime import datetime, timezone, timedelta
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
TG_TOKEN_PARAM = "/justhodl/telegram/bot_token"
TG_CHAT_ID_PARAM = "/justhodl/telegram/chat_id"

s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


# Quality gate — minimum to be considered a setup
MIN_PIOTROSKI = 6      # 6 of 9 — improving company
MAX_DEBT_EQUITY = 1.5  # not over-leveraged
MIN_CURRENT_RATIO = 1.0  # can pay short-term bills
MIN_PRICE = 5.0        # avoid penny stocks
MIN_MARKET_CAP = 1_000_000_000  # $1B minimum (liquidity)

# Setup filter — must rank well on ≥3 of 4 dimensions
DIMS_REQUIRED = 3
TOP_PCT_PER_DIM = 0.40  # rank within top 40% on the dimension


def get_s3_json(key, default=None):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as e:
        print(f"[S3] {key}: {e}")
        return default


def put_s3_json(key, body, cache="public, max-age=900"):
    s3.put_object(
        Bucket=BUCKET, Key=key,
        Body=json.dumps(body, default=str).encode("utf-8"),
        ContentType="application/json", CacheControl=cache,
    )


def safe_float(v, default=None):
    try:
        if v is None: return default
        f = float(v)
        return f if f == f else default  # NaN check
    except Exception:
        return default


def passes_quality_gate(s):
    """Return (passes, fail_reason). Hard filters before any scoring."""
    pe = safe_float(s.get("peRatio"))
    pio = safe_float(s.get("piotroski"), 0)
    de = safe_float(s.get("debtToEquity"))
    cr = safe_float(s.get("currentRatio"))
    pr = safe_float(s.get("price"), 0)
    mc = safe_float(s.get("marketCap"), 0)
    fcfg = safe_float(s.get("fcfGrowth"))

    if pr is None or pr < MIN_PRICE:
        return False, "price_too_low"
    if mc < MIN_MARKET_CAP:
        return False, "market_cap_small"
    if pio is None or pio < MIN_PIOTROSKI:
        return False, "piotroski_low"
    if de is not None and de > MAX_DEBT_EQUITY:
        return False, "debt_high"
    if cr is not None and cr < MIN_CURRENT_RATIO:
        return False, "liquidity_weak"
    if pe is None or pe <= 0:
        return False, "no_earnings"
    if fcfg is None or fcfg < 0:
        return False, "fcf_negative"
    return True, None


def quality_score(s):
    """Quality dimension: Piotroski + margins + ROE."""
    pio = safe_float(s.get("piotroski"), 0)
    roe = safe_float(s.get("roe"), 0)
    om = safe_float(s.get("operatingMargin"), 0)
    nm = safe_float(s.get("netMargin"), 0)
    # Normalize Piotroski 0-9 → 0-100
    pio_norm = (pio / 9) * 100
    # Margins: cap at sensible upper bounds
    om_norm = max(0, min(100, om * 250)) if om else 0  # 40% margin → 100
    nm_norm = max(0, min(100, nm * 333)) if nm else 0  # 30% margin → 100
    roe_norm = max(0, min(100, roe * 333)) if roe else 0  # 30% ROE → 100
    # Average
    return round((pio_norm + om_norm + nm_norm + roe_norm) / 4, 1)


def safety_score(s):
    """Safety dimension: balance sheet."""
    de = safe_float(s.get("debtToEquity"))
    cr = safe_float(s.get("currentRatio"))
    ic = safe_float(s.get("interestCoverage"))

    # D/E: 0 = best, 1.5 = limit
    if de is None: de_norm = 50
    else: de_norm = max(0, min(100, (1.5 - de) / 1.5 * 100))

    # Current ratio: 1.0 = ok, 2.0+ = great
    if cr is None: cr_norm = 50
    else: cr_norm = max(0, min(100, (cr - 1.0) * 100))

    # Interest coverage: ≥3x = healthy, ≥10x = great
    if ic is None: ic_norm = 50
    else: ic_norm = max(0, min(100, ic * 10))

    return round((de_norm + cr_norm + ic_norm) / 3, 1)


def value_score(s, sector_medians):
    """Value dimension: rank within sector by valuation multiples.
    Lower multiples = higher value score.
    """
    sector = s.get("sector", "")
    medians = sector_medians.get(sector, {})

    pe = safe_float(s.get("peRatio"))
    ps = safe_float(s.get("psRatio"))
    ev = safe_float(s.get("evEbitda"))

    scores = []

    if pe and medians.get("pe"):
        # 50% of median = score 100; 200% of median = score 0
        ratio = pe / medians["pe"]
        sc = max(0, min(100, (2.0 - ratio) * 100))
        scores.append(sc)
    if ps and medians.get("ps"):
        ratio = ps / medians["ps"]
        sc = max(0, min(100, (2.0 - ratio) * 100))
        scores.append(sc)
    if ev and medians.get("ev") and medians["ev"] > 0:
        ratio = ev / medians["ev"]
        sc = max(0, min(100, (2.0 - ratio) * 100))
        scores.append(sc)

    if not scores:
        return None
    return round(sum(scores) / len(scores), 1)


def momentum_score(s):
    """Momentum dimension: revenue + EPS + FCF growth."""
    rg = safe_float(s.get("revenueGrowth"), 0)
    eg = safe_float(s.get("epsGrowth"), 0)
    fg = safe_float(s.get("fcfGrowth"), 0)
    # Each growth metric: 0% = 30, 20% = 70, 40%+ = 100
    def norm(g):
        if g is None or g != g: return 30
        return max(0, min(100, 30 + g * 175))
    return round((norm(rg) + norm(eg) + norm(fg)) / 3, 1)


def compute_sector_medians(stocks):
    """For each sector, compute median P/E, P/S, EV/EBITDA."""
    by_sector = {}
    for s in stocks:
        sector = s.get("sector", "")
        if not sector: continue
        by_sector.setdefault(sector, []).append(s)

    medians = {}
    for sector, group in by_sector.items():
        pes = [safe_float(s.get("peRatio")) for s in group]
        pes = [p for p in pes if p and p > 0]
        pss = [safe_float(s.get("psRatio")) for s in group]
        pss = [p for p in pss if p and p > 0]
        evs = [safe_float(s.get("evEbitda")) for s in group]
        evs = [p for p in evs if p and p > 0]
        medians[sector] = {
            "pe": statistics.median(pes) if pes else None,
            "ps": statistics.median(pss) if pss else None,
            "ev": statistics.median(evs) if evs else None,
            "n": len(group),
        }
    return medians


def get_telegram_creds():
    try:
        token = ssm.get_parameter(Name=TG_TOKEN_PARAM, WithDecryption=True)["Parameter"]["Value"]
        chat_id = ssm.get_parameter(Name=TG_CHAT_ID_PARAM)["Parameter"]["Value"]
        return token, chat_id
    except Exception:
        return None, None


def send_telegram(message):
    token, chat_id = get_telegram_creds()
    if not token or not chat_id: return False
    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = urllib.parse.urlencode({
            "chat_id": chat_id, "text": message, "parse_mode": "Markdown",
        }).encode("utf-8")
        req = urllib.request.Request(url, data=data,
            headers={"Content-Type": "application/x-www-form-urlencoded"})
        with urllib.request.urlopen(req, timeout=10, context=ctx) as r:
            return r.status == 200
    except Exception as e:
        print(f"[TG] {e}")
        return False


def lambda_handler(event, context):
    print("=== ASYMMETRIC EQUITY SCORER v1 ===")
    now = datetime.now(timezone.utc)

    # 1. Load screener output
    screener_data = get_s3_json("screener/data.json", {})
    stocks = screener_data.get("results", []) or screener_data.get("stocks", []) or []
    if not stocks:
        # Try alternate shape
        for k in ("data", "items", "rows"):
            if isinstance(screener_data.get(k), list):
                stocks = screener_data[k]
                break

    if not stocks:
        return {"statusCode": 500,
                "body": json.dumps({"error": "no_screener_data",
                                    "screener_keys": list(screener_data.keys())[:5]})}
    print(f"  Loaded {len(stocks)} stocks from screener")

    # 2. Compute sector medians
    sector_medians = compute_sector_medians(stocks)
    print(f"  Computed medians for {len(sector_medians)} sectors")

    # 3. Score each stock
    scored = []
    quality_failures = {}
    for s in stocks:
        passes, reason = passes_quality_gate(s)
        if not passes:
            quality_failures[reason] = quality_failures.get(reason, 0) + 1
            # Special case: looks cheap but fails quality? Track as value trap
            ps = safe_float(s.get("psRatio"))
            pe = safe_float(s.get("peRatio"))
            if reason in ("piotroski_low", "fcf_negative", "debt_high"):
                if pe and pe > 0 and pe < 12:  # looks cheap on P/E
                    scored.append({
                        **{k: s.get(k) for k in ("symbol", "name", "sector", "price", "marketCap", "peRatio", "psRatio", "piotroski", "debtToEquity")},
                        "category": "value_trap",
                        "trap_reason": reason,
                    })
            continue

        q = quality_score(s)
        sf_score = safety_score(s)
        v = value_score(s, sector_medians)
        m = momentum_score(s)

        # Count dimensions where this stock is top 40% in sample
        # We'll determine percentile cutoffs after scoring all
        scored.append({
            **{k: s.get(k) for k in ("symbol", "name", "sector", "price", "marketCap",
                                      "peRatio", "psRatio", "evEbitda",
                                      "roe", "operatingMargin", "netMargin",
                                      "revenueGrowth", "epsGrowth", "fcfGrowth",
                                      "debtToEquity", "currentRatio", "interestCoverage",
                                      "piotroski", "beta")},
            "quality_score": q,
            "safety_score": sf_score,
            "value_score": v,
            "momentum_score": m,
            "category": "candidate",
        })

    candidates = [s for s in scored if s.get("category") == "candidate"]
    value_traps = [s for s in scored if s.get("category") == "value_trap"]

    print(f"  Quality gate failures: {quality_failures}")
    print(f"  Candidates: {len(candidates)}, Value traps tracked: {len(value_traps)}")

    if not candidates:
        return {"statusCode": 200,
                "body": json.dumps({"warning": "no_candidates_after_quality_gate",
                                    "failures": quality_failures})}

    # 4. Compute percentile cutoffs across candidates
    def cutoff(field, pct=1 - TOP_PCT_PER_DIM):
        vals = [s.get(field) for s in candidates if s.get(field) is not None]
        if not vals: return None
        sv = sorted(vals)
        idx = int(len(sv) * pct)
        return sv[min(idx, len(sv) - 1)]

    cutoffs = {
        "quality": cutoff("quality_score"),
        "safety": cutoff("safety_score"),
        "value": cutoff("value_score"),
        "momentum": cutoff("momentum_score"),
    }
    print(f"  Cutoffs (60th pct): {cutoffs}")

    # 5. Mark how many dimensions each candidate passes
    for s in candidates:
        n_pass = 0
        passes = []
        for dim, key in [("quality", "quality_score"), ("safety", "safety_score"),
                         ("value", "value_score"), ("momentum", "momentum_score")]:
            v = s.get(key)
            if v is not None and cutoffs[dim] is not None and v >= cutoffs[dim]:
                n_pass += 1
                passes.append(dim)
        s["dims_passed"] = n_pass
        s["dims_passed_list"] = passes
        # Composite score for ranking — average of all 4 dims (None → 0)
        valid_scores = [s.get(k) for k in ("quality_score", "safety_score", "value_score", "momentum_score")
                        if s.get(k) is not None]
        s["composite_score"] = round(sum(valid_scores) / len(valid_scores), 1) if valid_scores else 0

    # 6. Filter to setups
    setups = [s for s in candidates if s.get("dims_passed", 0) >= DIMS_REQUIRED]
    setups.sort(key=lambda x: (x["dims_passed"], x["composite_score"]), reverse=True)

    print(f"  Setups passing ≥{DIMS_REQUIRED} dims: {len(setups)}")
    print(f"  Top 5: {[s['symbol'] for s in setups[:5]]}")

    # 7. Sector breakdown of setups
    sector_counts = {}
    for s in setups:
        sector_counts[s.get("sector", "Unknown")] = sector_counts.get(s.get("sector", "Unknown"), 0) + 1

    # 8. Detect new setups vs last run
    prior = get_s3_json("opportunities/asymmetric-equity.json", {})
    prior_setups_set = {s["symbol"] for s in prior.get("top_setups", [])[:30]}
    cur_setups_set = {s["symbol"] for s in setups[:30]}
    new_this_week = sorted(cur_setups_set - prior_setups_set)
    dropped_this_week = sorted(prior_setups_set - cur_setups_set)

    # 9. Build snapshot
    snapshot = {
        "as_of": now.isoformat(),
        "v": "1.0",
        "summary": {
            "n_screener_total": len(stocks),
            "n_quality_passed": len(candidates),
            "n_setups": len(setups),
            "n_value_traps": len(value_traps),
            "quality_gate_failures": quality_failures,
            "new_this_week": new_this_week,
            "dropped_this_week": dropped_this_week,
        },
        "cutoffs": cutoffs,
        "sector_breakdown": sector_counts,
        "top_setups": setups[:30],
        "value_traps": sorted(value_traps,
                              key=lambda x: safe_float(x.get("peRatio"), 999))[:15],
        "filter_logic": {
            "quality_gate": {
                "min_piotroski": MIN_PIOTROSKI,
                "max_debt_equity": MAX_DEBT_EQUITY,
                "min_current_ratio": MIN_CURRENT_RATIO,
                "min_price": MIN_PRICE,
                "min_market_cap": MIN_MARKET_CAP,
            },
            "setup_filter": {
                "dims_required": DIMS_REQUIRED,
                "top_pct_per_dim": TOP_PCT_PER_DIM,
            },
        },
    }

    put_s3_json("opportunities/asymmetric-equity.json", snapshot)

    # 10. Telegram alert: 5+ new high-conviction setups appear (rare event)
    if len(new_this_week) >= 5 and len(prior.get("top_setups", [])) > 0:
        new_top = [s for s in setups[:30] if s["symbol"] in new_this_week]
        lines = [f"🎯 *{len(new_this_week)} NEW Asymmetric Equity Setups*\n"]
        for s in new_top[:8]:
            sectors = s.get("sector", "?")
            lines.append(
                f"• *{s['symbol']}* ({sectors}) {s.get('price', '?'):.2f}\n"
                f"  composite: {s.get('composite_score', '?')} | "
                f"dims: {' '.join(s.get('dims_passed_list', []))}\n"
            )
        lines.append("\n_4-dimension filter: quality + safety + value + momentum_")
        message = "\n".join(lines)
        sent = send_telegram(message)
        snapshot["alert_sent"] = sent
        print(f"  New setups alert sent: {sent}")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "n_setups": len(setups),
            "n_value_traps": len(value_traps),
            "n_new_this_week": len(new_this_week),
            "n_dropped_this_week": len(dropped_this_week),
            "top_5_symbols": [s["symbol"] for s in setups[:5]],
            "sector_breakdown": sector_counts,
        }),
    }
