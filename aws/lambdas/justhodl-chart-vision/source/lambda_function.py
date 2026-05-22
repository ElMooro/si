"""
justhodl-chart-vision — Exponential Idea #6 (v1: chart pattern vision)

Most quant systems vectorize price into floats and miss patterns that
human chartists see at a glance — head-and-shoulders, broadening tops,
cup-and-handle, bull/bear flags, divergences against momentum.

This Lambda generates an actual PNG chart for each top-conviction
ticker, sends it to Claude as a vision input, and asks Claude to
identify:
  - Visible chart pattern (if any)
  - Pattern completion percentage
  - Suggested entry/stop/target if pattern is actionable
  - Confidence 1-5

Output: data/chart-vision.json — per-ticker patterns detected

This is the explicit anti-AI hunt: signals that work BECAUSE quants
can't see them. Chart patterns have been arbed away in their
algorithmic form, but visual gestalt recognition is still a moat.

Schedule: daily 16 UTC after the close of European session.

Dependencies: matplotlib (Lambda layer needed, or use API for chart
generation). v1 uses a textual ASCII chart approximation + price
statistics. v2 will add matplotlib-rendered PNG.
"""
import json, os, logging, urllib.request, urllib.parse, base64
import boto3
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/chart-vision.json"

POLYGON_KEY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")

ANTHROPIC_KEY = os.environ.get("ANTHROPIC_KEY") or os.environ.get("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = "claude-haiku-4-5-20251001"
ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

TOP_N = int(os.environ.get("TOP_N", "10"))

s3 = boto3.client("s3", region_name=REGION)


PATTERN_PROMPT = """You are a senior chart technician with 25 years of experience reading price action.

You are given the daily closing prices for {symbol} ({name}) over the last 180 days as a structured dataset.

DATASET:
{price_data}

KEY STATISTICS:
- Current price: ${current_price}
- 180-day high: ${high_180}
- 180-day low: ${low_180}
- 50-day MA: ${ma_50}
- 200-day MA: ${ma_200}
- Distance from 52w high: {pct_from_high}%
- 20-day realized volatility (annualized): {vol_20d}%

Identify the dominant chart pattern (if any). Be conservative — only call a pattern if you can clearly see it.

Patterns to consider (most to least common):
1. Trend (uptrend / downtrend / sideways consolidation)
2. Head and shoulders / inverse H&S (top or bottom)
3. Cup and handle / inverse cup and handle
4. Double top / double bottom / triple
5. Ascending / descending / symmetric triangle
6. Bull flag / bear flag (after sharp move)
7. Wedge (rising or falling)
8. Rounding bottom / saucer
9. Breakout from base / breakdown
10. Divergence (price vs simple momentum)
11. No clear pattern / chop

Output ONLY valid JSON, no markdown:
{{
  "pattern": "<pattern name from list above, or 'none'>",
  "pattern_completion_pct": <0-100, where 100 = pattern just completed/triggered>,
  "direction_implied": "BULLISH|BEARISH|NEUTRAL",
  "confidence": <1-5, where 5 = textbook-clear>,
  "key_levels": {{
    "support": <number or null>,
    "resistance": <number or null>,
    "trigger_above": <number or null>,
    "stop_below": <number or null>
  }},
  "actionable": <true if pattern is at or past completion AND confidence >= 3>,
  "rationale": "<1-2 sentences on what you see>",
  "risk_reward_target": <number — implied target if pattern completes>
}}"""


def fmp_get(path, params=None):
    params = params or {}
    params["apikey"] = FMP_KEY
    url = "https://financialmodelingprep.com/stable" + path + "?" + urllib.parse.urlencode(params)
    try:
        r = urllib.request.urlopen(url, timeout=15)
        return json.loads(r.read())
    except Exception as e:
        logger.warning(f"fmp_fail {path}: {str(e)[:120]}")
        return None


def get_historical_prices(symbol, days=180):
    """Get daily close prices for last N days from FMP /stable/historical-price-eod."""
    d = fmp_get(f"/historical-price-eod/full", {"symbol": symbol})
    if not d or not isinstance(d, dict):
        return None
    bars = d.get("historical") or []
    if not isinstance(bars, list):
        return None
    bars = sorted(bars, key=lambda x: x.get("date", ""))[-days:]
    return bars


def build_summary(bars):
    """Build the dataset string + stats."""
    if not bars or len(bars) < 30:
        return None
    closes = [b.get("close") for b in bars if b.get("close") is not None]
    if len(closes) < 30:
        return None
    # Compress prices to a compact representation for the prompt
    # Use weekly samples to keep token usage reasonable
    sampled = closes[::5]  # every 5 days (~ weekly)
    dataset = ", ".join(f"{c:.2f}" for c in sampled)
    current = closes[-1]
    high_180 = max(closes)
    low_180 = min(closes)
    ma_50 = sum(closes[-50:]) / min(50, len(closes))
    ma_200 = sum(closes[-200:]) / min(200, len(closes))
    pct_from_high = round((current - high_180) / high_180 * 100, 1) if high_180 else None
    # Simple realized vol
    rets = [(closes[i+1]/closes[i] - 1) for i in range(len(closes)-1)]
    last_20 = rets[-20:]
    if last_20:
        mean = sum(last_20) / len(last_20)
        var = sum((r - mean) ** 2 for r in last_20) / len(last_20)
        vol_daily = var ** 0.5
        vol_ann = vol_daily * (252 ** 0.5) * 100
    else:
        vol_ann = None
    return {
        "price_data": dataset,
        "current_price": round(current, 2),
        "high_180": round(high_180, 2),
        "low_180": round(low_180, 2),
        "ma_50": round(ma_50, 2),
        "ma_200": round(ma_200, 2),
        "pct_from_high": pct_from_high,
        "vol_20d": round(vol_ann, 1) if vol_ann else None,
    }


def call_claude(prompt, max_retries=2):
    if not ANTHROPIC_KEY:
        return None, "no_api_key"
    body = {
        "model": ANTHROPIC_MODEL,
        "max_tokens": 800,
        "messages": [{"role": "user", "content": prompt}],
    }
    for attempt in range(max_retries + 1):
        try:
            req = urllib.request.Request(
                ANTHROPIC_URL,
                data=json.dumps(body).encode(),
                headers={"x-api-key": ANTHROPIC_KEY,
                         "anthropic-version": "2023-06-01",
                         "Content-Type": "application/json"})
            r = urllib.request.urlopen(req, timeout=60)
            response = json.loads(r.read())
            return response.get("content", [{}])[0].get("text", ""), None
        except Exception as e:
            if attempt < max_retries:
                import time
                time.sleep(1.5 * (attempt + 1))
                continue
            return None, str(e)[:150]


def extract_json(text):
    if not text:
        return None
    t = text.strip()
    if t.startswith("```"):
        first_nl = t.find("\n")
        if first_nl > 0: t = t[first_nl+1:]
        last_fence = t.rfind("```")
        if last_fence > 0: t = t[:last_fence]
    start = t.find("{")
    end = t.rfind("}")
    if start < 0 or end <= start: return None
    try: return json.loads(t[start:end+1])
    except Exception: return None


def analyze_chart(symbol_info):
    sym = symbol_info["symbol"]
    bars = get_historical_prices(sym)
    if not bars:
        return {"symbol": sym, "error": "no_price_data"}
    summary = build_summary(bars)
    if not summary:
        return {"symbol": sym, "error": "insufficient_history"}
    prompt = PATTERN_PROMPT.format(
        symbol=sym, name=symbol_info.get("name", sym),
        **summary,
    )
    response, error = call_claude(prompt)
    if error:
        return {"symbol": sym, "error": error}
    parsed = extract_json(response)
    if not parsed:
        return {"symbol": sym, "error": "parse_fail",
                "raw_head": (response or "")[:300]}
    return {
        "symbol": sym,
        "name": symbol_info.get("name", sym),
        "analyzed_at": datetime.now(timezone.utc).isoformat(),
        "current_price": summary["current_price"],
        "vol_20d": summary["vol_20d"],
        **parsed,
    }


def fetch_universe():
    universe = []
    seen = set()
    for k in ("data/best-ideas.json", "data/nobrainers.json", "data/portfolio.json"):
        try:
            d = json.loads(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read())
            for parent in (d.get("titans"), d.get("high_conviction"), d.get("stack"),
                           d.get("nobrainers"), d.get("positions"), d.get("all")):
                if isinstance(parent, list):
                    for c in parent:
                        sym = c.get("symbol") or c.get("ticker")
                        if sym and sym not in seen:
                            seen.add(sym)
                            universe.append({
                                "symbol": sym.upper(),
                                "name": c.get("name") or c.get("company_name", sym),
                                "score": c.get("conviction_score") or c.get("asymmetric_score") or 0,
                            })
        except Exception: pass
    universe.sort(key=lambda x: -(x["score"] or 0))
    return universe[:TOP_N]


def send_telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = json.dumps({"chat_id": TELEGRAM_CHAT_ID, "text": text,
                           "parse_mode": "Markdown",
                           "disable_web_page_preview": True}).encode()
        req = urllib.request.Request(url, data=data,
                                     headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=15)
    except Exception as e: logger.error(f"telegram_fail: {e}")


def lambda_handler(event, context):
    started = datetime.now(timezone.utc)
    logger.info("chart-vision starting")

    universe = fetch_universe()
    logger.info(f"top {len(universe)} targets")
    if not universe:
        return {"statusCode": 500, "body": json.dumps({"error": "no universe"})}

    results = []
    with ThreadPoolExecutor(max_workers=3) as ex:
        for fut in as_completed([ex.submit(analyze_chart, u) for u in universe]):
            try:
                r = fut.result()
                results.append(r)
            except Exception as e:
                logger.error(f"analyze_fail: {e}")

    # Sort by confidence × actionable
    def score(r):
        if r.get("error"): return -1
        conf = r.get("confidence", 0) or 0
        completion = r.get("pattern_completion_pct", 0) or 0
        return conf * 10 + completion / 10

    results.sort(key=lambda r: -score(r))

    actionable = [r for r in results if r.get("actionable") and r.get("confidence", 0) >= 3]

    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    payload = {
        "schema_version": "1.0",
        "engine": "chart-vision",
        "generated_at": started.isoformat(),
        "elapsed_seconds": round(elapsed, 2),
        "n_analyzed": len(results),
        "n_with_pattern": sum(1 for r in results if r.get("pattern") and r["pattern"] != "none"),
        "n_actionable": len(actionable),
        "model": ANTHROPIC_MODEL,
        "results": results,
        "actionable_now": actionable,
        "methodology": {
            "version": "v1_text_based",
            "input": "180d weekly-sampled close series + key statistics",
            "patterns": [
                "trend", "head_and_shoulders", "cup_and_handle",
                "double_top_bottom", "triangle", "flag", "wedge",
                "rounding_bottom", "breakout", "divergence",
            ],
            "v2_plan": "matplotlib-rendered PNG → Claude vision (true visual gestalt)",
        },
    }

    s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(payload, default=str, indent=2).encode(),
                  ContentType="application/json",
                  CacheControl="max-age=3600, public")
    logger.info(f"wrote {OUT_KEY}: analyzed={len(results)} actionable={len(actionable)}")

    if actionable:
        lines = ["📈 *Chart Vision — Actionable patterns*", ""]
        for r in actionable[:5]:
            lines.append(f"  `{r['symbol']}` — {r['pattern']} ({r.get('direction_implied','?')}) conf={r['confidence']}/5")
            lines.append(f"     {r.get('rationale', '')[:120]}")
        lines.append("\n[chart-vision.html](https://justhodl.ai/chart-vision.html)")
        try: send_telegram("\n".join(lines))
        except Exception as e: logger.error(f"telegram_fail: {e}")

    return {"statusCode": 200,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"ok": True, "n_analyzed": len(results),
                                "n_actionable": len(actionable),
                                "elapsed": round(elapsed, 2)})}
