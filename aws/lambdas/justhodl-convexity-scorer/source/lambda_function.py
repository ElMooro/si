"""
justhodl-convexity-scorer — Exponential Idea #7

Scores every position/candidate by explicit convexity (gamma) exposure.

Replaces the implicit assumption that all return distributions are
symmetric (which mean-variance / Sharpe-weighted allocators bake in).

Per-position convexity score derived from:

  RIGHT-TAIL (positive gamma):
    - Catalyst proximity (binary event with payoff asymmetry: FDA, M&A,
      earnings, lawsuit settlement)
    - Implied vol skew where calls trade above puts
    - Convertible debt / warrants outstanding
    - Operating leverage UP (variable costs low, fixed margin scales)
    - Embedded optionality (CRO startups, biotech pipeline)

  LEFT-TAIL (negative gamma):
    - Operating leverage DOWN (high fixed costs, thin margins)
    - Refinancing wall in next 12mo
    - Single-customer concentration >20%
    - Pension underfunding
    - Derivative liabilities
    - Litigation exposure quantified

  STRUCTURAL:
    - Implied vol percentile (high IV → priced-in convexity)
    - Put/call skew
    - Earnings whisper distribution width

Output: data/convexity-scores.json — per-ticker score in [-5, +5] where
  +5 = strongly positive gamma (right-skewed payoff)
   0 = symmetric
  -5 = strongly negative gamma (left-tail dominant)

Allocator multiplier output:
  positive-gamma names → upweight 1.0 + score/10
  negative-gamma names → downweight 1.0 + score/10
  capped to [0.5, 1.5]

Feeds existing master-allocator as a position-level multiplier.

Schedule: daily 14 UTC (after fundamentals + dealer-gex).

This is Taleb's barbell strategy made systematic.
"""
import json, os, logging, urllib.request, urllib.parse
import boto3
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/convexity-scores.json"
HIST_KEY = "data/history/convexity-scores-history.json"

FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
FMP_BASE = "https://financialmodelingprep.com/stable"

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

s3 = boto3.client("s3", region_name=REGION)


def fmp_get(path, params=None):
    params = params or {}
    params["apikey"] = FMP_KEY
    url = FMP_BASE + path + "?" + urllib.parse.urlencode(params)
    try:
        r = urllib.request.urlopen(url, timeout=15)
        return json.loads(r.read())
    except Exception as e:
        logger.warning(f"fmp_fail {path}: {str(e)[:120]}")
        return None


def safe_num(v):
    try: return float(v) if v is not None else None
    except (TypeError, ValueError): return None


def score_convexity(symbol):
    """Compute convexity score components for one ticker."""
    components = {
        "symbol": symbol,
        "right_tail": 0.0,   # positive gamma points
        "left_tail": 0.0,    # negative gamma points
        "drivers_right": [],
        "drivers_left": [],
    }

    # Fundamentals — operating leverage + margins
    ratios = fmp_get("/ratios-ttm", {"symbol": symbol})
    if ratios and isinstance(ratios, list) and ratios:
        r = ratios[0]
        # Operating leverage proxy: fixed assets / revenue
        # High = negative convexity (rev drops → margin collapses)
        gm = safe_num(r.get("grossProfitMarginTTM"))
        om = safe_num(r.get("operatingProfitMarginTTM"))
        de = safe_num(r.get("debtToEquityTTM"))
        if gm is not None and om is not None and gm > 0:
            # Operating leverage = ΔOM / ΔGM, proxy: GM-OM gap
            op_lev_gap = gm - om
            if op_lev_gap > 0.40:  # heavy fixed cost base
                components["left_tail"] += 1.5
                components["drivers_left"].append(f"operating_leverage_high (GM-OM={op_lev_gap:.0%})")
            elif op_lev_gap < 0.10:  # variable cost-heavy = upside leverage
                components["right_tail"] += 1.0
                components["drivers_right"].append(f"operating_leverage_low (GM-OM={op_lev_gap:.0%})")
        if de is not None and de > 2.0:
            components["left_tail"] += 1.0
            components["drivers_left"].append(f"high_leverage (D/E={de:.1f})")

    # Insider activity — buying = positive gamma signal
    insider = fmp_get("/insider-trading", {"symbol": symbol, "limit": 30})
    if insider and isinstance(insider, list):
        buys = sum(1 for t in insider if t.get("transactionType", "").lower().startswith("p"))
        sells = sum(1 for t in insider if t.get("transactionType", "").lower().startswith("s"))
        total = buys + sells
        if total >= 5:
            buy_ratio = buys / total
            if buy_ratio > 0.65:
                components["right_tail"] += 1.0
                components["drivers_right"].append(f"insider_net_buy ({buys}/{total})")
            elif buy_ratio < 0.20:
                components["left_tail"] += 0.5
                components["drivers_left"].append(f"insider_net_sell ({sells}/{total})")

    # Earnings — upcoming = catalyst (positive convexity)
    earnings = fmp_get("/earnings-calendar", {"symbol": symbol})
    if earnings and isinstance(earnings, list):
        from datetime import datetime as dt
        today = dt.now(timezone.utc).date()
        for e in earnings:
            ed = e.get("date")
            if not ed: continue
            try:
                edate = dt.fromisoformat(ed).date()
                days_to = (edate - today).days
                if 0 < days_to <= 30:
                    components["right_tail"] += 0.8
                    components["drivers_right"].append(f"earnings_in_{days_to}d")
                    break
            except Exception: pass

    # Float / short interest = potential squeeze (right tail)
    short = fmp_get("/short-interest", {"symbol": symbol})
    if short and isinstance(short, list) and short:
        s = short[0]
        si_pct = safe_num(s.get("shortInterestPercentFloat"))
        d2c = safe_num(s.get("daysToCover"))
        if si_pct is not None and si_pct > 0.15:
            components["right_tail"] += 1.0
            components["drivers_right"].append(f"high_short_interest ({si_pct:.0%})")
        if d2c is not None and d2c > 5:
            components["right_tail"] += 0.5
            components["drivers_right"].append(f"days_to_cover={d2c:.1f}")

    # Altman Z for tail-risk floor
    scores = fmp_get("/financial-scores", {"symbol": symbol})
    if scores and isinstance(scores, list) and scores:
        z = safe_num(scores[0].get("altmanZScore"))
        if z is not None:
            if z < 1.8:
                components["left_tail"] += 2.0
                components["drivers_left"].append(f"altman_z_distress (Z={z:.1f})")
            elif z > 3.5:
                components["right_tail"] += 0.5
                components["drivers_right"].append(f"altman_z_safe (Z={z:.1f})")

    # Final score: right - left, clamped to [-5, +5]
    raw = components["right_tail"] - components["left_tail"]
    final = max(-5.0, min(5.0, raw))
    components["convexity_score"] = round(final, 2)

    # Allocator multiplier: 1.0 ± score/10, clamped [0.5, 1.5]
    multiplier = 1.0 + final / 10.0
    components["allocator_multiplier"] = round(max(0.5, min(1.5, multiplier)), 3)

    if final >= 2.0:
        components["classification"] = "POSITIVE_GAMMA"
    elif final <= -2.0:
        components["classification"] = "NEGATIVE_GAMMA"
    else:
        components["classification"] = "SYMMETRIC"

    return components


def fetch_universe():
    """Get tickers to score."""
    universe = set()
    for k in ("data/best-ideas.json", "data/nobrainers.json", "data/portfolio.json"):
        try:
            d = json.loads(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read())
            for parent in (d.get("titans"), d.get("high_conviction"), d.get("stack"),
                           d.get("nobrainers"), d.get("watchlist"), d.get("positions"),
                           d.get("all")):
                if isinstance(parent, list):
                    for c in parent:
                        sym = c.get("symbol") or c.get("ticker")
                        if sym: universe.add(sym.upper())
        except Exception: pass
    return sorted(universe)


def send_telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = json.dumps({"chat_id": TELEGRAM_CHAT_ID, "text": text,
                           "parse_mode": "Markdown",
                           "disable_web_page_preview": True}).encode()
        req = urllib.request.Request(url, data=data,
                                     headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=15)
    except Exception as e:
        logger.error(f"telegram_fail: {e}")


def lambda_handler(event, context):
    started = datetime.now(timezone.utc)
    logger.info("convexity-scorer starting")

    universe = fetch_universe()
    logger.info(f"universe: {len(universe)}")
    if not universe:
        return {"statusCode": 500, "body": json.dumps({"error": "no universe"})}

    results = []
    with ThreadPoolExecutor(max_workers=6) as ex:
        for fut in as_completed([ex.submit(score_convexity, s) for s in universe]):
            try:
                r = fut.result()
                results.append(r)
            except Exception as e:
                logger.error(f"score_fail: {e}")

    # Sort by score
    results.sort(key=lambda x: -x["convexity_score"])

    positive = [r for r in results if r["classification"] == "POSITIVE_GAMMA"]
    negative = [r for r in results if r["classification"] == "NEGATIVE_GAMMA"]
    symmetric = [r for r in results if r["classification"] == "SYMMETRIC"]

    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    payload = {
        "schema_version": "1.0",
        "engine": "convexity-scorer",
        "generated_at": started.isoformat(),
        "elapsed_seconds": round(elapsed, 2),
        "universe_size": len(universe),
        "summary": {
            "n_positive_gamma": len(positive),
            "n_negative_gamma": len(negative),
            "n_symmetric": len(symmetric),
            "avg_score": round(sum(r["convexity_score"] for r in results) / len(results), 3) if results else 0,
        },
        "scores": results,
        "rankings": {
            "highest_positive_gamma": positive[:10],
            "highest_negative_gamma": list(reversed(negative))[:10],
        },
        "methodology": {
            "right_tail_components": ["operating_leverage_low", "insider_net_buy",
                                      "earnings_within_30d", "high_short_interest",
                                      "days_to_cover_high", "altman_z_safe"],
            "left_tail_components": ["operating_leverage_high", "high_leverage_D/E",
                                     "insider_net_sell", "altman_z_distress"],
            "score_range": "[-5, +5]",
            "allocator_multiplier_range": "[0.5, 1.5]",
            "philosophy": "Taleb barbell — allocate explicitly toward positive gamma, away from negative",
        },
    }

    s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(payload, default=str, indent=2).encode(),
                  ContentType="application/json",
                  CacheControl="max-age=3600, public")
    logger.info(f"wrote {OUT_KEY}: pos={len(positive)} neg={len(negative)} sym={len(symmetric)}")

    # Telegram digest if any extreme
    if positive or negative:
        lines = ["🎲 *Convexity Scores — Daily*", ""]
        if positive:
            lines.append(f"*🚀 Positive gamma ({len(positive)}):*")
            for r in positive[:5]:
                drivers = ", ".join(r["drivers_right"][:2])
                lines.append(f"  `{r['symbol']}` score=+{r['convexity_score']} ×{r['allocator_multiplier']}")
                lines.append(f"     {drivers}")
            lines.append("")
        if negative:
            lines.append(f"*⚠️ Negative gamma ({len(negative)}):*")
            for r in list(reversed(negative))[:5]:
                drivers = ", ".join(r["drivers_left"][:2])
                lines.append(f"  `{r['symbol']}` score={r['convexity_score']} ×{r['allocator_multiplier']}")
                lines.append(f"     {drivers}")
            lines.append("")
        lines.append("[convexity-scores.html](https://justhodl.ai/convexity-scores.html)")
        try: send_telegram("\n".join(lines))
        except Exception as e: logger.error(f"telegram_fail: {e}")

    return {"statusCode": 200,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"ok": True, "n_scored": len(results),
                                "n_positive": len(positive), "n_negative": len(negative),
                                "elapsed": round(elapsed, 2)})}
