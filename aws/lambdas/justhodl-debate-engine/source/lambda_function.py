"""
justhodl-debate-engine — Roadmap #15 Multi-Agent Debate Engine

═══════════════════════════════════════════════════════════════════════
WHY THIS EXISTS
───────────────
The daily brief is ONE Claude voice. Sophisticated decision-making uses
MULTIPLE analyst perspectives that DISAGREE. This Lambda runs 5 specialized
Claude personas in parallel for each TIER S/A pick, then synthesizes a
consensus verdict + forced bear case.

═══════════════════════════════════════════════════════════════════════
THE 5 PERSONAS
──────────────
  💎 VALUE   (Buffett-style)
     "Would I own the whole business at this price? Moat, ROIC, owner earnings."

  🚀 GROWTH  (Tiger Global / ARK style)
     "Where will this be in 5 years? TAM, growth rate, optionality."

  🌐 MACRO   (Druckenmiller / Soros style)
     "What's the regime tailwind? Currency, rates, sector cycle."

  📊 QUANT   (Renaissance / Two Sigma style)
     "Statistical edge: factor exposure, mean reversion, momentum decay."

  🐻 SHORT   (Hindenburg / Muddy Waters / Burry style)
     "WHY IS THIS A SELL. Find every red flag. Kill the thesis."

═══════════════════════════════════════════════════════════════════════
DECISIVE DESIGN
───────────────
- Bear case is FORCED into every output (the short persona is always present)
- Consensus verdict = majority of 5 personas (BUY / HOLD / SELL)
- Dissent is highlighted separately so it's never lost
- Synthesis includes entry-zone guidance + downside scenarios

═══════════════════════════════════════════════════════════════════════
INPUTS (all sidecars from S3)
─────────────────────────────
  screener/alpha-score.json     → TIER S/A picks + components
  signals/confluence.json       → confluence tier + factors firing
  signals/regime-picks.json     → regime fit score
  signals/anomalies.json        → Macro Stress Score for regime context
  data/options-flow.json        → options-flow signal
  data/earnings-tracker.json    → upcoming earnings catalyst
  screener/data.json             → full screener row (PE, growth, margins, etc.)

OUTPUT: data/debate.json + (optional) data/debate-{symbol}.json per stock

═══════════════════════════════════════════════════════════════════════
COST
────
  Claude haiku-4-5: ~$1/M input · ~$5/M output
  Per stock:  5 personas × (~3K input + ~500 output) = $0.027/stock
  Per day:    5 stocks × $0.027 = $0.135/day ≈ $4.10/month

═══════════════════════════════════════════════════════════════════════
"""
import json
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

VERSION = "1.0.0"

S3_BUCKET = "justhodl-dashboard-live"
ALPHA_KEY = "screener/alpha-score.json"
CONFLUENCE_KEY = "signals/confluence.json"
REGIME_KEY = "signals/regime-picks.json"
ANOMALIES_KEY = "signals/anomalies.json"
OPTIONS_FLOW_KEY = "data/options-flow.json"
EARNINGS_KEY = "data/earnings-tracker.json"
SCREENER_KEY = "screener/data.json"
OUTPUT_KEY = "data/debate.json"

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# How many top stocks to debate per run
MAX_TIER_S = int(os.environ.get("MAX_TIER_S", "3"))
MAX_TIER_A = int(os.environ.get("MAX_TIER_A", "3"))

# Per-Claude call budget
MAX_TOKENS_PER_PERSONA = 600
HTTP_TIMEOUT = 30

s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")


# ═══════════════════════════════════════════════════════════════════════
# 5 PERSONA SYSTEM PROMPTS
# ═══════════════════════════════════════════════════════════════════════

PERSONAS = {
    "VALUE": {
        "icon": "💎",
        "system_prompt": """You are a Warren Buffett-style value investor. Your job is to answer ONE question: would I want to own this WHOLE business at this price?

You evaluate:
- DURABLE COMPETITIVE MOAT (brand, scale, network effects, switching costs)
- RETURN ON CAPITAL (ROE, ROIC trending)
- OWNER EARNINGS (free cash flow, predictable)
- INTRINSIC VALUE vs price (margin of safety)
- MANAGEMENT QUALITY (capital allocation track record)

You are SKEPTICAL of growth without profitability, high P/E, unproven moats.
You PRAISE companies with 15+ year track records of compounding returns.

Respond with valid JSON ONLY (no preamble, no markdown):
{
  "verdict": "BUY" | "HOLD" | "SELL",
  "conviction": 0-100,
  "thesis": "2-3 sentence core argument (≤350 chars)",
  "concerns": "1-2 specific concerns even if buying (≤200 chars)",
  "fair_value_view": "your view on current price vs intrinsic"
}"""
    },
    "GROWTH": {
        "icon": "🚀",
        "system_prompt": """You are a Tiger Global / Cathie Wood-style growth investor. Your job is to answer ONE question: where will this be in 5 years?

You evaluate:
- TOTAL ADDRESSABLE MARKET (TAM, growing or saturating?)
- REVENUE GROWTH RATE (sustainable, accelerating, or decelerating?)
- OPTIONALITY (adjacent markets, S-curve potential)
- COMPETITIVE POSITION (winner-take-most dynamics)
- INNOVATION RATE (R&D, product velocity)

You ACCEPT high multiples for genuine compounders. You IGNORE this-quarter noise.
You PRAISE companies disrupting their industries.
You are WARY of mature, low-growth incumbents.

Respond with valid JSON ONLY (no preamble, no markdown):
{
  "verdict": "BUY" | "HOLD" | "SELL",
  "conviction": 0-100,
  "thesis": "2-3 sentence core argument (≤350 chars)",
  "concerns": "1-2 growth risks (≤200 chars)",
  "tam_view": "TAM size + growth trajectory"
}"""
    },
    "MACRO": {
        "icon": "🌐",
        "system_prompt": """You are a Stanley Druckenmiller / George Soros-style macro trader. Your job is to answer ONE question: does this fit the current macro regime?

You evaluate:
- REGIME FIT (cyclical vs defensive in current Macro Stress level)
- INTEREST RATE SENSITIVITY (duration risk for valuations)
- CURRENCY EXPOSURE (USD strength tailwind/headwind)
- SECTOR CYCLE POSITION (early/mid/late expansion)
- MACRO CATALYSTS (FOMC, rate cuts, recession risk)

You ARE TACTICAL: a great business in the wrong regime is a bad trade.
You ARE DECISIVE about which side of the macro environment to be on.

Respond with valid JSON ONLY (no preamble, no markdown):
{
  "verdict": "BUY" | "HOLD" | "SELL",
  "conviction": 0-100,
  "thesis": "2-3 sentence core argument (≤350 chars)",
  "concerns": "macro headwinds (≤200 chars)",
  "regime_fit": "STRONG | OK | WEAK fit for current regime"
}"""
    },
    "QUANT": {
        "icon": "📊",
        "system_prompt": """You are a Renaissance / Two Sigma-style quant. Your job is to answer ONE question: what does the statistical edge say?

You evaluate:
- FACTOR EXPOSURES (momentum, quality, value, low-vol, profitability)
- MEAN REVERSION (overbought RSI, stretched vs MA)
- HISTORICAL FORWARD RETURNS at this signal combo
- TURNOVER & DRIFT (do these signals decay quickly?)
- BASE RATE for stocks with similar profile

You ARE EMOTIONLESS — only the data matters. You IGNORE narrative.
You CALL OUT signal decay. You QUANTIFY edge in basis points / sigma.

Respond with valid JSON ONLY (no preamble, no markdown):
{
  "verdict": "BUY" | "HOLD" | "SELL",
  "conviction": 0-100,
  "thesis": "2-3 sentence statistical argument (≤350 chars)",
  "concerns": "factor decay or crowded trade risk (≤200 chars)",
  "expected_60d_return_pct": -50 to 50
}"""
    },
    "SHORT": {
        "icon": "🐻",
        "system_prompt": """You are a Hindenburg Research / Muddy Waters / Michael Burry-style short seller. Your ONLY job is to KILL THE THESIS. Find every reason this stock should fall.

You evaluate:
- VALUATION EXCESS (P/E, P/S vs peers and history)
- ACCOUNTING RED FLAGS (declining margins, rising DSO, weird footnotes)
- COMPETITION (new entrants, commoditization risk)
- REGULATORY/LEGAL EXPOSURE (lawsuits, antitrust, FDA, China)
- INSIDER SELLING / DILUTION
- SHORT INTEREST ALREADY HIGH (too crowded)
- MANAGEMENT TURNOVER, AUDITOR CHANGES, SEC INQUIRIES
- CAPITAL STRUCTURE (debt maturities, refi risk)
- DEMAND CLIFF risk (pull-forward demand, post-COVID normalization)

You are HOSTILE TO THE STOCK. Even if you can't conclude SELL, list every disconfirming signal.
You ALWAYS find at least 2 specific risks. You are NEVER pollyannish.

Respond with valid JSON ONLY (no preamble, no markdown):
{
  "verdict": "BUY" | "HOLD" | "SELL",
  "conviction": 0-100,
  "thesis": "the bear case — be specific (≤400 chars)",
  "red_flags": ["flag 1", "flag 2", "flag 3"],
  "downside_target_pct": -50 to 0
}"""
    },
}


# ═══════════════════════════════════════════════════════════════════════
# DATA LOADERS
# ═══════════════════════════════════════════════════════════════════════

def load_s3_json(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception as e:
        print(f"  load {key} err: {str(e)[:120]}")
        return None


# ═══════════════════════════════════════════════════════════════════════
# CLAUDE INVOCATION
# ═══════════════════════════════════════════════════════════════════════

def call_claude(system_prompt, user_message, max_tokens=600, retries=2):
    """POST to Anthropic /v1/messages, return parsed text or None."""
    if not ANTHROPIC_API_KEY:
        return None, "no_api_key"
    headers = {
        "x-api-key": ANTHROPIC_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    body = json.dumps({
        "model": ANTHROPIC_MODEL,
        "max_tokens": max_tokens,
        "system": system_prompt,
        "messages": [{"role": "user", "content": user_message}],
    }).encode("utf-8")

    last_err = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                "https://api.anthropic.com/v1/messages",
                data=body, headers=headers
            )
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
                data = json.loads(r.read().decode("utf-8"))
            content = (data.get("content") or [])
            if not content: return None, "empty_response"
            text = content[0].get("text", "")
            return text, None
        except urllib.error.HTTPError as e:
            err_body = e.read().decode("utf-8", "replace")[:300] if hasattr(e, "read") else ""
            last_err = f"HTTP {e.code}: {err_body}"
            if e.code in (429, 529): time.sleep(2 ** attempt)
            elif e.code in (500, 502, 503, 504): time.sleep(1 + attempt)
            else: break
        except Exception as e:
            last_err = str(e)[:300]
            time.sleep(1)
    return None, last_err


def parse_persona_json(text):
    """Extract JSON from Claude response. Returns dict or fallback."""
    if not text: return {}
    text = text.strip()
    # Strip markdown code fences
    if text.startswith("```"):
        lines = text.split("\n")
        # Remove first line (```json or ```)
        text = "\n".join(lines[1:]).rstrip()
        if text.endswith("```"): text = text[:-3].rstrip()
    try:
        return json.loads(text)
    except Exception:
        # Try to find first {...} block
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            try: return json.loads(text[start:end + 1])
            except Exception: pass
        return {"verdict": "HOLD", "thesis": text[:300], "parse_error": True}


# ═══════════════════════════════════════════════════════════════════════
# CONTEXT BUILDER
# ═══════════════════════════════════════════════════════════════════════

def build_stock_context(symbol, alpha_row, screener_row, confluence_row,
                         regime_row, options_flow_row, anomaly_data,
                         earnings_for_sym):
    """Compose a rich JSON context block for one stock that all 5 personas see."""
    ctx = {
        "symbol": symbol,
        "as_of": datetime.now(timezone.utc).isoformat(),
        "alpha_composite": {
            "alpha_score": alpha_row.get("alpha_score"),
            "tier": alpha_row.get("tier"),
            "components": alpha_row.get("components"),
            "top_signals": alpha_row.get("top_signals"),
            "risk_flags": alpha_row.get("risk_flags"),
        },
        "confluence": {
            "tier": (confluence_row or {}).get("confluence_tier"),
            "components_firing": (confluence_row or {}).get("components_firing"),
        } if confluence_row else None,
        "regime": {
            "current_regime": (anomaly_data or {}).get("stress_interpretation"),
            "macro_stress_score_0_100": (anomaly_data or {}).get("macro_stress_score"),
            "regime_adj_score": (regime_row or {}).get("regime_adj_score"),
        },
        "options_flow": {
            "score": (options_flow_row or {}).get("score"),
            "tier": (options_flow_row or {}).get("tier"),
            "flags": (options_flow_row or {}).get("flags"),
        } if options_flow_row else None,
        "upcoming_earnings": earnings_for_sym,
    }
    # Screener fundamentals
    if screener_row:
        ctx["fundamentals"] = {
            "name": screener_row.get("name"),
            "sector": screener_row.get("sector"),
            "industry": screener_row.get("industry"),
            "marketCap": screener_row.get("marketCap"),
            "price": screener_row.get("price") or screener_row.get("currentPrice"),
            "pe": screener_row.get("pe") or screener_row.get("peRatio"),
            "forwardPE": screener_row.get("forwardPE"),
            "priceToSales": screener_row.get("priceToSales"),
            "pegRatio": screener_row.get("pegRatio"),
            "ev_to_ebitda": screener_row.get("evToEbitda"),
            "revenue": screener_row.get("revenue"),
            "revenueGrowth": screener_row.get("revenueGrowth"),
            "epsGrowth": screener_row.get("epsGrowth"),
            "operatingMargin": screener_row.get("operatingMargin"),
            "grossMargin": screener_row.get("grossMargin"),
            "fcfMargin": screener_row.get("fcfMargin"),
            "roe": screener_row.get("roe"),
            "roic": screener_row.get("roic"),
            "debt_to_equity": screener_row.get("debtToEquity"),
            "currentRatio": screener_row.get("currentRatio"),
            "piotroski": screener_row.get("piotroskiF") or screener_row.get("piotroski"),
            "altmanZ": screener_row.get("altmanZ"),
            "shortPctFloat": screener_row.get("shortPctFloat") or screener_row.get("shortRatio"),
            "shortChange30d": screener_row.get("shortChange30d"),
            "beta": screener_row.get("beta"),
            "return_1m": screener_row.get("return1M") or screener_row.get("priceReturn1M"),
            "return_3m": screener_row.get("return3M") or screener_row.get("priceReturn3M"),
            "return_6m": screener_row.get("return6M") or screener_row.get("priceReturn6M"),
            "return_1y": screener_row.get("return1Y") or screener_row.get("priceReturn1Y"),
            "from_52w_high_pct": screener_row.get("pctFrom52WHigh"),
            "from_52w_low_pct": screener_row.get("pctFrom52WLow"),
            "insiderNet90dUsd": screener_row.get("insiderNet90dUsd"),
            "insiderClusterBuy": screener_row.get("insiderClusterBuy"),
            "analystGrade": screener_row.get("analystGrade") or screener_row.get("grade"),
            "ptUpsidePct": screener_row.get("ptUpsidePct") or screener_row.get("priceTargetUpside"),
            "dcfUpsidePct": screener_row.get("dcfUpsidePct"),
            "lastSurprisePct": screener_row.get("lastSurprisePct"),
            "beatStreak": screener_row.get("beatStreak"),
        }
    return ctx


def run_persona(name, persona_cfg, stock_context):
    """Single persona evaluation for one stock."""
    user_msg = (
        f"Evaluate {stock_context['symbol']} based on the following data:\n\n"
        f"```json\n{json.dumps(stock_context, separators=(',', ':'), default=str)}\n```\n\n"
        f"Return your verdict as valid JSON only — no preamble, no markdown."
    )
    text, err = call_claude(persona_cfg["system_prompt"], user_msg,
                              max_tokens=MAX_TOKENS_PER_PERSONA)
    if err:
        return {"name": name, "icon": persona_cfg["icon"], "verdict": "ERROR",
                "error": err[:200]}
    parsed = parse_persona_json(text)
    parsed["name"] = name
    parsed["icon"] = persona_cfg["icon"]
    return parsed


def debate_stock(symbol, stock_context):
    """Run all 5 personas in parallel for one stock."""
    results = []
    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = {ex.submit(run_persona, name, cfg, stock_context): name
                    for name, cfg in PERSONAS.items()}
        for f in as_completed(futures):
            results.append(f.result())
    # Sort by canonical order (VALUE, GROWTH, MACRO, QUANT, SHORT)
    order = list(PERSONAS.keys())
    results.sort(key=lambda r: order.index(r["name"]) if r.get("name") in order else 99)
    return results


def synthesize(symbol, alpha_row, personas):
    """Build consensus verdict + bear case + synthesis."""
    verdicts = [p.get("verdict", "HOLD") for p in personas]
    buy_count = sum(1 for v in verdicts if v == "BUY")
    sell_count = sum(1 for v in verdicts if v == "SELL")
    hold_count = sum(1 for v in verdicts if v == "HOLD")
    error_count = sum(1 for v in verdicts if v == "ERROR")

    # Determine consensus
    if buy_count >= 4:        consensus = "STRONG_BUY"
    elif buy_count == 3:       consensus = "BUY"
    elif sell_count >= 3:      consensus = "SELL"
    elif sell_count == 2 and buy_count == 2:  consensus = "DIVIDED"
    else:                       consensus = "HOLD"

    # Bear case = short persona's thesis (always present)
    short_persona = next((p for p in personas if p.get("name") == "SHORT"), None)
    bear_case = (short_persona or {}).get("thesis", "Short persona unavailable.")
    red_flags = (short_persona or {}).get("red_flags") or []
    downside_pct = (short_persona or {}).get("downside_target_pct")

    # Dissent: any persona whose verdict differs from consensus
    dissenters = []
    consensus_direction = "BUY" if buy_count >= 3 else "SELL" if sell_count >= 3 else "HOLD"
    for p in personas:
        if p.get("verdict") == "ERROR": continue
        if p.get("verdict") != consensus_direction and p.get("verdict") != "HOLD":
            dissenters.append({
                "name": p.get("name"),
                "icon": p.get("icon"),
                "verdict": p.get("verdict"),
                "thesis": p.get("thesis", "")[:300],
            })

    # Average conviction (excluding errors)
    convictions = [p.get("conviction") for p in personas
                    if isinstance(p.get("conviction"), (int, float))]
    avg_conviction = round(sum(convictions) / len(convictions), 1) if convictions else None

    return {
        "consensus_verdict": consensus,
        "consensus_direction": consensus_direction,
        "verdict_counts": {"BUY": buy_count, "HOLD": hold_count,
                            "SELL": sell_count, "ERROR": error_count},
        "avg_conviction": avg_conviction,
        "bear_case": bear_case,
        "red_flags": red_flags,
        "downside_target_pct": downside_pct,
        "dissenters": dissenters,
    }


# ═══════════════════════════════════════════════════════════════════════
# MAIN HANDLER
# ═══════════════════════════════════════════════════════════════════════

def lambda_handler(event, context):
    started = time.time()
    print(f"=== DEBATE ENGINE v{VERSION} · {datetime.now(timezone.utc).isoformat()} ===")

    # ─── Load inputs ───
    alpha = load_s3_json(ALPHA_KEY) or {}
    confluence = load_s3_json(CONFLUENCE_KEY) or {}
    regime = load_s3_json(REGIME_KEY) or {}
    anomalies = load_s3_json(ANOMALIES_KEY) or {}
    options_flow = load_s3_json(OPTIONS_FLOW_KEY) or {}
    earnings_tracker = load_s3_json(EARNINGS_KEY) or {}
    screener = load_s3_json(SCREENER_KEY) or {}

    # ─── Index lookups ───
    screener_by = {s["symbol"]: s for s in (screener.get("stocks") or [])
                    if s.get("symbol")}
    confluence_by = {r["symbol"]: r for r in (confluence.get("rankings") or [])
                      if r.get("symbol")}
    regime_by = {r["symbol"]: r for r in (regime.get("regime_picks") or [])
                  if r.get("symbol")}
    flow_by = {r["symbol"]: r for r in (options_flow.get("all_qualifying") or [])
                if r.get("symbol")}
    earnings_by = {e.get("symbol") or e.get("ticker"): e
                    for e in (earnings_tracker.get("upcoming_14d") or [])}

    # ─── Pick stocks to debate ───
    explicit_symbols = event.get("symbols") if isinstance(event, dict) else None

    alpha_stocks = alpha.get("stocks") or []
    tier_s = [s for s in alpha_stocks if s.get("tier") == "S"][:MAX_TIER_S]
    tier_a = [s for s in alpha_stocks if s.get("tier") == "A"][:MAX_TIER_A]
    targets = tier_s + tier_a

    # Allow explicit symbol override (manual invoke)
    if explicit_symbols:
        targets = [s for s in alpha_stocks if s.get("symbol") in explicit_symbols]
    elif not targets:
        # Fallback: top 3 by alpha if no TIER S/A
        targets = sorted(alpha_stocks, key=lambda s: -(s.get("alpha_score") or 0))[:3]

    symbols = [s.get("symbol") for s in targets if s.get("symbol")]
    print(f"  debating {len(symbols)} stocks: {symbols}")

    if not symbols:
        payload = {"generated_at": datetime.now(timezone.utc).isoformat(),
                    "version": VERSION, "status": "no_targets",
                    "stocks_debated": 0, "debates": []}
        s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY,
                      Body=json.dumps(payload, default=str).encode("utf-8"),
                      ContentType="application/json")
        return {"statusCode": 200, "body": json.dumps({"success": True, "n": 0})}

    # ─── Run debates ───
    debates = []
    for sym in symbols:
        alpha_row = next((s for s in alpha_stocks if s.get("symbol") == sym), {})
        ctx = build_stock_context(
            sym, alpha_row,
            screener_by.get(sym), confluence_by.get(sym),
            regime_by.get(sym), flow_by.get(sym),
            anomalies, earnings_by.get(sym))

        t0 = time.time()
        personas = debate_stock(sym, ctx)
        synthesis = synthesize(sym, alpha_row, personas)
        elapsed = round(time.time() - t0, 2)

        # Build human-readable verdict lines (e.g. "Value: Buy. Pricing power...")
        verdict_lines = []
        for p in personas:
            label = p.get("name", "").title()
            verdict = p.get("verdict", "?")
            thesis = (p.get("thesis") or "")[:200]
            verdict_lines.append(f"{p.get('icon','')} {label}: {verdict}. {thesis}")

        debates.append({
            "symbol": sym,
            "alpha_score": alpha_row.get("alpha_score"),
            "tier": alpha_row.get("tier"),
            "confluence_tier": (confluence_by.get(sym) or {}).get("confluence_tier"),
            "regime_adj_score": (regime_by.get(sym) or {}).get("regime_adj_score"),
            "options_flow_score": (flow_by.get(sym) or {}).get("score"),
            "personas": personas,
            "synthesis": synthesis,
            "verdict_lines": verdict_lines,
            "debate_elapsed_seconds": elapsed,
        })
        print(f"    {sym} ✓ consensus={synthesis['consensus_verdict']} "
              f"conviction={synthesis['avg_conviction']} ({elapsed}s)")

    # ─── Build payload ───
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_at_unix": int(time.time()),
        "version": VERSION,
        "model": ANTHROPIC_MODEL,
        "elapsed_seconds": round(time.time() - started, 2),
        "stocks_debated": len(debates),
        "personas": list(PERSONAS.keys()),
        "summary": {
            "n_strong_buy": sum(1 for d in debates if d["synthesis"]["consensus_verdict"] == "STRONG_BUY"),
            "n_buy": sum(1 for d in debates if d["synthesis"]["consensus_verdict"] == "BUY"),
            "n_hold": sum(1 for d in debates if d["synthesis"]["consensus_verdict"] == "HOLD"),
            "n_divided": sum(1 for d in debates if d["synthesis"]["consensus_verdict"] == "DIVIDED"),
            "n_sell": sum(1 for d in debates if d["synthesis"]["consensus_verdict"] == "SELL"),
            "top_consensus": [d["symbol"] for d in debates
                                 if d["synthesis"]["consensus_verdict"] in ("STRONG_BUY", "BUY")][:5],
        },
        "debates": debates,
    }

    try:
        s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY,
            Body=json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8"),
            ContentType="application/json",
            CacheControl="public, max-age=1800")
        print(f"  ✓ debate.json written ({len(debates)} debates, {payload['elapsed_seconds']}s total)")
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"err": str(e)})}

    return {"statusCode": 200, "body": json.dumps({
        "success": True, "version": VERSION,
        "stocks_debated": len(debates),
        "summary": payload["summary"],
        "elapsed_seconds": payload["elapsed_seconds"],
    })}
