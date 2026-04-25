"""
justhodl-stock-ai-research

Generates AI-powered research summaries for individual stocks viewed
in the Stock Analyzer. Returns a structured JSON document with:
  - description: what the company does (1-2 sentences)
  - bull_case: thesis + reasoning + key drivers
  - bear_case: thesis + reasoning + key risks
  - scenarios: bull/base/bear scenarios for 1M, 1Q, 1Y horizons
              (NOT single-number predictions — these are reasoned scenarios)

Cache strategy:
  - 7-day S3 cache at stock-ai/<TICKER>.json
  - Companies don't change daily; weekly refresh is fine
  - On-demand only (no nightly pre-population) — keeps Anthropic costs
    bounded to actually-viewed tickers

Per-call cost (Haiku 4.5):
  Input ~1500 tokens × $0.25/M = $0.0004
  Output ~1200 tokens × $1.25/M = $0.0015
  Total ≈ $0.002 per uncached ticker
  S&P 500 once each = $1.00 worst case
"""
import os
import json
import time
import urllib3
import boto3
from datetime import datetime, timezone

http = urllib3.PoolManager()
s3 = boto3.client("s3", region_name="us-east-1")

FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
ANT_KEY = os.environ.get("ANTHROPIC_KEY", os.environ.get("ANTHROPIC_API_KEY", ""))
BUCKET  = "justhodl-dashboard-live"
CACHE_PREFIX = "stock-ai/"
CACHE_TTL = 7 * 24 * 3600  # 7 days

FMP_BASE = "https://financialmodelingprep.com"
ANT_URL  = "https://api.anthropic.com/v1/messages"
MODEL    = "claude-haiku-4-5-20251001"

# ── helpers ─────────────────────────────────────────────────────

def fmp(endpoint, params=None, retries=2):
    p = dict(params or {})
    p["apikey"] = FMP_KEY
    qs = "&".join(f"{k}={v}" for k, v in p.items())
    url = f"{FMP_BASE}/{endpoint}?{qs}"
    for attempt in range(retries + 1):
        try:
            r = http.request("GET", url, timeout=10)
            if r.status == 200:
                return json.loads(r.data.decode("utf-8"))
            if r.status == 429:
                time.sleep(1 + attempt * 2)
                continue
        except Exception as e:
            if attempt == retries:
                print(f"  FMP {endpoint} error: {e}")
    return None

def gather_facts(ticker):
    """Pull the FMP facts we'll feed to Claude."""
    ticker = ticker.upper().strip()

    profile = fmp("stable/profile", {"symbol": ticker})
    ratios  = fmp("stable/ratios-ttm", {"symbol": ticker})
    growth  = fmp("stable/income-statement-growth", {"symbol": ticker, "limit": "4"})
    metrics = fmp("stable/key-metrics-ttm", {"symbol": ticker})
    pchange = fmp("stable/stock-price-change", {"symbol": ticker})
    pt      = fmp("stable/price-target-consensus", {"symbol": ticker})
    grades  = fmp("stable/grades-consensus", {"symbol": ticker})

    p = profile[0] if isinstance(profile, list) and profile else {}
    r = ratios[0]  if isinstance(ratios, list)  and ratios  else {}
    g = growth[0]  if isinstance(growth, list)  and growth  else {}
    m = metrics[0] if isinstance(metrics, list) and metrics else {}
    c = pchange[0] if isinstance(pchange, list) and pchange else {}
    t = pt[0]      if isinstance(pt, list)      and pt      else {}
    gr= grades[0]  if isinstance(grades, list)  and grades  else {}

    def safe(v, dp=None):
        try:
            if v is None: return None
            x = float(v)
            return round(x, dp) if dp is not None else x
        except: return None

    return {
        "ticker": ticker,
        "name": p.get("companyName"),
        "sector": p.get("sector"),
        "industry": p.get("industry"),
        "exchange": p.get("exchange"),
        "country": p.get("country"),
        "ceo": p.get("ceo"),
        "employees": safe(p.get("fullTimeEmployees")),
        "description": p.get("description"),
        "ipoDate": p.get("ipoDate"),
        "website": p.get("website"),
        "price": safe(p.get("price"), 2),
        "marketCap": safe(p.get("marketCap")),
        "beta": safe(p.get("beta"), 2),
        # Valuation
        "pe": safe(r.get("priceToEarningsRatioTTM"), 1),
        "pb": safe(r.get("priceToBookRatioTTM"), 2),
        "ps": safe(r.get("priceToSalesRatioTTM"), 2),
        "evEbitda": safe(m.get("evToEBITDATTM"), 1),
        "fcfYield": safe(m.get("freeCashFlowYieldTTM"), 4),
        # Quality
        "roe": safe(m.get("returnOnEquityTTM"), 3),
        "roa": safe(m.get("returnOnAssetsTTM"), 3),
        "roic": safe(m.get("returnOnInvestedCapitalTTM"), 3),
        "grossMargin": safe(r.get("grossProfitMarginTTM"), 3),
        "netMargin": safe(r.get("netProfitMarginTTM"), 3),
        "operatingMargin": safe(r.get("operatingProfitMarginTTM"), 3),
        # Balance sheet
        "debtEquity": safe(r.get("debtToEquityRatioTTM"), 2),
        "currentRatio": safe(r.get("currentRatioTTM"), 2),
        "interestCoverage": safe(r.get("interestCoverageRatioTTM"), 1),
        # Growth
        "revenueGrowth": safe(g.get("growthRevenue"), 3),
        "epsGrowth": safe(g.get("growthEPS"), 3),
        "fcfGrowth": safe(g.get("growthFreeCashFlow"), 3),
        # Price action
        "chg1M": safe(c.get("1M"), 2),
        "chg3M": safe(c.get("3M"), 2),
        "chg6M": safe(c.get("6M"), 2),
        "chg1Y": safe(c.get("1Y"), 2),
        "chgYTD": safe(c.get("ytd"), 2),
        # Analysts
        "ptHigh": safe(t.get("targetHigh"), 2),
        "ptMedian": safe(t.get("targetMedian"), 2),
        "ptLow": safe(t.get("targetLow"), 2),
        "ptConsensus": safe(t.get("targetConsensus"), 2),
        "analystBuy": safe(gr.get("strongBuy") or 0) + safe(gr.get("buy") or 0),
        "analystHold": safe(gr.get("hold") or 0),
        "analystSell": safe(gr.get("sell") or 0) + safe(gr.get("strongSell") or 0),
    }

# ── Anthropic prompt ─────────────────────────────────────────────

SYSTEM_PROMPT = """You are a senior equity research analyst writing concise, honest research notes for a sophisticated retail user. You produce structured JSON only — no preamble, no apologies, no markdown fences.

Rules:
- Be honest about uncertainty. Use ranges, not false-precision point estimates.
- Distinguish base case from bull/bear. Each scenario must have a CONCRETE reasoning chain (not boilerplate).
- Price scenarios are forecasts based on the data provided AT THIS MOMENT, with stated assumptions. They are NOT investment advice.
- Bull and bear cases should be the strongest version of each side — not strawmen.
- "description" should be 2 sentences: what they do + how they make money.
- For scenarios, base your numbers on multiples in the data (P/E, P/S, EV/EBITDA), not on guessing.
- If data is insufficient for honest scenarios, set scenarios to null and explain in description."""

USER_PROMPT_TEMPLATE = """Generate a research note for {ticker} — {name} ({sector}/{industry}).

CURRENT DATA:
Price: ${price}  |  Market Cap: ${mcap_b}B  |  Beta: {beta}
Valuation: P/E={pe}, P/B={pb}, P/S={ps}, EV/EBITDA={evEbitda}, FCF Yield={fcfYield_pct}%
Quality: ROE={roe_pct}%, ROIC={roic_pct}%, Net Margin={netMargin_pct}%, Gross Margin={grossMargin_pct}%
Balance: D/E={debtEquity}, Current Ratio={currentRatio}, Interest Coverage={interestCoverage}x
Growth: Revenue YoY={revenueGrowth_pct}%, EPS YoY={epsGrowth_pct}%, FCF YoY={fcfGrowth_pct}%
Price action: 1M={chg1M}%, 3M={chg3M}%, 6M={chg6M}%, 1Y={chg1Y}%, YTD={chgYTD}%
Analyst targets: Low ${ptLow}, Median ${ptMedian}, High ${ptHigh}  |  Consensus: ${ptConsensus}
Analyst grades: {analystBuy} buy / {analystHold} hold / {analystSell} sell
Company description (FMP source, may be outdated): {fmp_description}

Return JSON exactly matching this schema:

{{
  "description": "<2 sentences: what the company does and how it makes money>",
  "bull_case": {{
    "thesis": "<1 sentence headline>",
    "reasoning": "<2-3 sentences citing specific data points above>",
    "key_drivers": ["<driver 1>", "<driver 2>", "<driver 3>"]
  }},
  "bear_case": {{
    "thesis": "<1 sentence headline>",
    "reasoning": "<2-3 sentences citing specific data points above>",
    "key_risks": ["<risk 1>", "<risk 2>", "<risk 3>"]
  }},
  "scenarios": {{
    "horizon_1m": {{"bull": <price>, "base": <price>, "bear": <price>, "rationale": "<1 sentence>"}},
    "horizon_1q": {{"bull": <price>, "base": <price>, "bear": <price>, "rationale": "<1 sentence>"}},
    "horizon_1y": {{"bull": <price>, "base": <price>, "bear": <price>, "rationale": "<1 sentence>"}}
  }},
  "data_quality": "high|medium|low",
  "notes": "<any caveats or important context, e.g. earnings imminent, M&A rumors, sector context>"
}}"""

def format_prompt(facts):
    """Coerce nones, format percents/billions, fill template."""
    f = dict(facts)
    # market cap → billions
    mcap = f.get("marketCap")
    f["mcap_b"] = round(mcap / 1e9, 1) if mcap else "?"
    # percentage formatters
    def pct(v): return round(v * 100, 1) if v is not None else "?"
    f["fcfYield_pct"]    = pct(f.get("fcfYield"))
    f["roe_pct"]         = pct(f.get("roe"))
    f["roic_pct"]        = pct(f.get("roic"))
    f["netMargin_pct"]   = pct(f.get("netMargin"))
    f["grossMargin_pct"] = pct(f.get("grossMargin"))
    f["revenueGrowth_pct"] = pct(f.get("revenueGrowth"))
    f["epsGrowth_pct"]   = pct(f.get("epsGrowth"))
    f["fcfGrowth_pct"]   = pct(f.get("fcfGrowth"))
    # missing → "?"
    for k in ["price","mcap_b","beta","pe","pb","ps","evEbitda","debtEquity","currentRatio",
              "interestCoverage","chg1M","chg3M","chg6M","chg1Y","chgYTD",
              "ptLow","ptMedian","ptHigh","ptConsensus","analystBuy","analystHold","analystSell"]:
        if f.get(k) is None: f[k] = "?"
    # description fallback
    desc = f.get("description") or "(no description)"
    f["fmp_description"] = desc[:600]
    return USER_PROMPT_TEMPLATE.format(**f)

def call_anthropic(facts):
    """Send to Anthropic, parse JSON response."""
    if not ANT_KEY:
        raise Exception("ANTHROPIC_KEY not set in env")

    user_prompt = format_prompt(facts)
    body = json.dumps({
        "model": MODEL,
        "max_tokens": 2000,
        "system": SYSTEM_PROMPT,
        "messages": [{"role": "user", "content": user_prompt}],
    })
    r = http.request(
        "POST", ANT_URL,
        body=body.encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "x-api-key": ANT_KEY,
            "anthropic-version": "2023-06-01",
        },
        timeout=45,
    )
    if r.status != 200:
        raise Exception(f"Anthropic {r.status}: {r.data[:500].decode('utf-8')}")
    resp = json.loads(r.data.decode("utf-8"))
    text = resp["content"][0]["text"].strip()
    # Strip code fences if model added them anyway
    if text.startswith("```"):
        text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()
        if text.startswith("json\n"):
            text = text[5:]
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        raise Exception(f"Bad JSON from Anthropic: {e}; raw={text[:300]}")

# ── cache ─────────────────────────────────────────────────────────

def cache_get(ticker):
    """Return cached payload if fresh, else None."""
    key = f"{CACHE_PREFIX}{ticker}.json"
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        cached = json.loads(obj["Body"].read())
        age = time.time() - cached.get("generated_at_unix", 0)
        if age < CACHE_TTL:
            cached["from_cache"] = True
            cached["age_hours"] = round(age / 3600, 1)
            return cached
    except Exception:
        pass
    return None

def cache_put(ticker, payload):
    key = f"{CACHE_PREFIX}{ticker}.json"
    s3.put_object(
        Bucket=BUCKET, Key=key,
        Body=json.dumps(payload, separators=(",", ":")),
        ContentType="application/json",
        CacheControl="max-age=604800",  # 7 days
    )

# ── handler ───────────────────────────────────────────────────────

def lambda_handler(event, context):
    hdrs = {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type,Authorization",
    }
    method = (event.get("requestContext", {}).get("http", {}) or {}).get("method", "GET")
    if method == "OPTIONS":
        return {"statusCode": 200, "headers": hdrs, "body": ""}

    qs = event.get("queryStringParameters") or {}
    ticker = (qs.get("ticker") or qs.get("symbol") or "").upper().strip()
    force = qs.get("force") == "true"

    if not ticker:
        return {"statusCode": 400, "headers": hdrs,
                "body": json.dumps({"error": "ticker query param required"})}

    if not ticker.replace(".", "").replace("-", "").isalpha() or len(ticker) > 8:
        return {"statusCode": 400, "headers": hdrs,
                "body": json.dumps({"error": "invalid ticker format"})}

    # Cache lookup
    if not force:
        cached = cache_get(ticker)
        if cached:
            return {"statusCode": 200, "headers": hdrs, "body": json.dumps(cached)}

    # Fresh generation
    print(f"=== AI RESEARCH: {ticker} ===")
    t0 = time.time()
    try:
        facts = gather_facts(ticker)
        if not facts.get("name"):
            return {"statusCode": 404, "headers": hdrs,
                    "body": json.dumps({"error": f"ticker {ticker} not found at FMP"})}

        ai_research = call_anthropic(facts)
        elapsed = time.time() - t0

        payload = {
            "ticker": ticker,
            "company": {
                "name": facts.get("name"),
                "sector": facts.get("sector"),
                "industry": facts.get("industry"),
                "ceo": facts.get("ceo"),
                "employees": facts.get("employees"),
                "ipo_date": facts.get("ipoDate"),
                "website": facts.get("website"),
                "country": facts.get("country"),
                "exchange": facts.get("exchange"),
            },
            "snapshot": {
                "price": facts.get("price"),
                "market_cap": facts.get("marketCap"),
                "pe": facts.get("pe"),
                "ps": facts.get("ps"),
                "pb": facts.get("pb"),
                "ev_ebitda": facts.get("evEbitda"),
                "roe": facts.get("roe"),
                "roic": facts.get("roic"),
                "net_margin": facts.get("netMargin"),
                "rev_growth": facts.get("revenueGrowth"),
                "chg_1m": facts.get("chg1M"),
                "chg_3m": facts.get("chg3M"),
                "chg_1y": facts.get("chg1Y"),
                "analyst_target_median": facts.get("ptMedian"),
            },
            "ai": ai_research,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "generated_at_unix": int(time.time()),
            "elapsed_seconds": round(elapsed, 2),
            "model": MODEL,
            "from_cache": False,
        }

        # Cache
        try: cache_put(ticker, payload)
        except Exception as e: print(f"  cache write failed: {e}")

        return {"statusCode": 200, "headers": hdrs, "body": json.dumps(payload)}

    except Exception as e:
        elapsed = time.time() - t0
        print(f"  ERROR ({elapsed:.1f}s): {e}")
        return {"statusCode": 500, "headers": hdrs,
                "body": json.dumps({"error": str(e), "ticker": ticker})}
