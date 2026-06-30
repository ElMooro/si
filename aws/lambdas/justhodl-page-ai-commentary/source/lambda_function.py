"""justhodl-page-ai-commentary

Universal AI commentary engine. Generates page-specific narratives by:
  1. Reading data sources configured per page
  2. Calling Claude API with topic-focused prompts
  3. Writing data/ai-commentary/{page}.json for the page to fetch

Each page renders a 3-card AI brief panel by fetching its commentary JSON.
Refreshes daily 10:00 ET (1h after digest-trends-ai). Some pages may
schedule more frequent refreshes.
"""
import json
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from typing import Optional, Dict, List

import boto3

S3_BUCKET = "justhodl-dashboard-live"
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = "claude-haiku-4-5-20251001"

s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")


def _read_json(key: str) -> Optional[dict]:
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def _get_anthropic_key() -> str:
    if ANTHROPIC_KEY:
        return ANTHROPIC_KEY
    for path in ["/justhodl/anthropic/api_key", "/anthropic/api_key",
                 "/justhodl/anthropic_api_key"]:
        try:
            return ssm.get_parameter(Name=path, WithDecryption=True)["Parameter"]["Value"]
        except Exception:
            continue
    return ""


def call_claude(prompt: str, system: str = "", max_tokens: int = 1200) -> str:
    try:
        import claude_compat
        _b = {"model": ANTHROPIC_MODEL, "max_tokens": max_tokens,
              "messages": [{"role": "user", "content": prompt}]}
        if system:
            _b["system"] = system
        _t = claude_compat.text(_b)
        if _t and _t.strip():
            return _t
    except Exception:
        pass
    api_key = _get_anthropic_key()
    if not api_key:
        return ""
    body = {"model": ANTHROPIC_MODEL, "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": prompt}]}
    if system:
        body["system"] = system
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json",
                  "anthropic-version": "2023-06-01", "x-api-key": api_key},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=40) as r:
            data = json.loads(r.read().decode())
        for block in data.get("content", []):
            if block.get("type") == "text":
                return block.get("text", "")
        return ""
    except Exception as e:
        print(f"[claude] {e}")
        return ""


# ═══ PAGE CONFIGS ═══════════════════════════════════════════════════════════

PAGE_CONFIGS = {
    "pre-pump-radar": {
        "data_files": [
            "data/theme-cascade.json", "data/trade-tickets.json",
            "data/cascade-validation-log.json", "data/predictions-snapshots/latest.json",
            "data/simulated-portfolio.json", "data/polygon-options-flow.json",
        ],
        "system": """You are JustHodl.AI's pre-pump trading analyst.
Generate concise, decisive commentary on today's cascade picks and trade setups.
Tone: specific tickers, specific numbers, no hedging.
Return ONLY valid JSON, no markdown fences.

Schema:
{
  "headline": "2-sentence summary of today's highest-conviction setups",
  "top_pick_analysis": "3-4 sentence analysis of the #1 pick with entry/stop/TP rationale",
  "regime_context": "2-3 sentences on what market regime is supporting/hurting these picks",
  "what_to_watch": "2-3 sentences on stops approaching, levels to monitor",
  "confidence_score": "integer 0-100 representing overall conviction in today's setup"
}""",
    },
    "signal-board": {
        "data_files": ["data/signal-board.json"],
        "system": """You are JustHodl.AI's cross-asset strategist.
Synthesize the 7-engine signal board into a coherent regime narrative.
Return ONLY valid JSON, no markdown fences.

Schema:
{
  "headline": "1-sentence regime classification (e.g., 'Risk-on with caveats: equities firm but credit widening')",
  "regime_analysis": "3-4 sentences on current cross-asset regime",
  "key_divergences": "2-3 sentences on important divergences between asset classes",
  "positioning_view": "2-3 sentences on what positioning makes sense given the regime",
  "regime_score": "integer 0-100 (0=panic, 50=neutral, 100=euphoria)"
}""",
    },
    "risk-desk": {
        "data_files": [
            "data/risk-composite.json", "data/cro-digest.json",
            "data/crisis-composite.json", "data/regime-composite.json",
            "data/dealer-gex.json",
        ],
        "system": """You are JustHodl.AI's Chief Risk Officer.
Generate the institutional risk dashboard narrative.
Tone: candid, specific, actionable. Cite specific risk metrics.
Return ONLY valid JSON, no markdown fences.

Schema:
{
  "headline": "1-sentence current risk regime (e.g., 'Elevated tail risk: credit-equity diverging')",
  "primary_risks": "3-4 sentences on top 2-3 risks RIGHT NOW with specific metrics",
  "hedge_recommendation": "2-3 sentences on what hedges make sense given current setup",
  "leading_indicators": "2-3 sentences on what to watch for regime transition",
  "risk_score": "integer 0-100 (0=calm, 50=elevated, 100=acute crisis)"
}""",
    },
    "liquidity": {
        "data_files": [
            "data/fed-liquidity.json", "data/tga.json", "data/rrp.json",
            "data/eurodollar-stress.json", "data/treasury-auction-crisis.json",
        ],
        "system": """You are JustHodl.AI's macro liquidity analyst.
Generate Fed/macro liquidity commentary with specific dollar amounts.
Return ONLY valid JSON, no markdown fences.

Schema:
{
  "headline": "1-sentence liquidity regime (e.g., 'Net liquidity declining: TGA up $X, RRP flat')",
  "liquidity_flow": "3-4 sentences on TGA, RRP, Fed balance sheet flows with $ amounts",
  "implications": "2-3 sentences on what this means for asset prices",
  "policy_outlook": "2-3 sentences on Fed/Treasury actions to watch",
  "liquidity_score": "integer 0-100 (0=draining, 50=neutral, 100=easing)"
}""",
    },
    "fundamentals": {
        "data_files": [
            "data/fundamentals-engine.json", "data/theme-cascade.json",
        ],
        "system": """You are JustHodl.AI's fundamental analyst.
Generate valuation commentary on the highest-conviction names.
Return ONLY valid JSON, no markdown fences.

Schema:
{
  "headline": "1-sentence overall valuation picture across cascade picks",
  "best_value": "3-4 sentences on the ticker with best DCF gap + financial health",
  "warning_flags": "2-3 sentences on names with concerning Altman/Piotroski scores",
  "watch_list": "2-3 sentences on names to monitor for fundamental changes",
  "value_score": "integer 0-100 (0=expensive, 50=fair, 100=undervalued)"
}""",
    },
    "crisis": {
        "data_files": [
            "data/crisis-composite.json", "data/eurodollar-stress.json",
            "data/auction-crisis.json", "data/treasury-auction-crisis.json",
            "data/credit-stress.json", "data/bank-stress.json",
        ],
        "system": """You are JustHodl.AI's tail-risk specialist focused on systemic crisis detection.
Generate candid assessment of plumbing stress + crisis probabilities.
Cite specific scores, regimes, and series IDs. No hedging.
Return ONLY valid JSON, no markdown fences.

Schema:
{
  "headline": "1-sentence current crisis regime (e.g., 'CALM with 1 watchlist: TIPS auction tail >2.5bp')",
  "crisis_assessment": "3-4 sentences on master crisis composite score + which sub-indicators driving it",
  "contagion_risks": "2-3 sentences on cross-asset contagion vectors (credit-equity, FX-rates, etc.)",
  "hedge_actions": "2-3 sentences on what hedges should already be on / what to add now",
  "crisis_score": "integer 0-100 (0=calm, 30=DEFCON 4, 60=elevated, 80=stress, 100=acute)"
}""",
    },
    "signals": {
        "data_files": [
            "data/signal-board.json", "data/cascade-validation-log.json",
            "data/predictions-snapshots/latest.json", "data/cascade-calibration.json",
        ],
        "system": """You are JustHodl.AI's signal-quality analyst.
Assess current signal quality, hit rates, and trend.
Cite specific signal counts, hit rates, and recent best/worst calls.
Return ONLY valid JSON, no markdown fences.

Schema:
{
  "headline": "1-sentence signal regime (e.g., 'Strong signal week: 7 of 12 cascade alerts pumped within 3d')",
  "signal_quality": "3-4 sentences on current signal counts, quality of setups, conviction levels",
  "hit_rate_trends": "2-3 sentences on cascade hit rate trends + calibration progress",
  "top_signals": "2-3 sentences naming specific high-conviction tickers/setups firing right now",
  "signal_score": "integer 0-100 (0=weak/noisy, 50=mixed, 100=strong/clean)"
}""",
    },
    "portfolio": {
        "data_files": [
            "data/simulated-portfolio.json", "data/pnl-stats.json",
            "data/pm-decision.json", "data/portfolio-snapshot.json",
            "data/trade-monitor-snapshots.json",
        ],
        "system": """You are JustHodl.AI's portfolio manager writing the morning book review.
Assess open positions, concentration, P&L trajectory.
Be specific: tickers, P&L $, exposure %. Recommend rebalancing if warranted.
Return ONLY valid JSON, no markdown fences.

Schema:
{
  "headline": "1-sentence book summary (e.g., '15 positions, 8 underwater, MU drag at -2.7%')",
  "position_review": "3-4 sentences on top winners + losers with specific P&L $",
  "risk_concentration": "2-3 sentences on sector/single-name concentration risks",
  "rebalance_suggestion": "2-3 sentences on what to trim/add given calibration + cascade",
  "portfolio_score": "integer 0-100 (0=in distress, 50=neutral, 100=outperforming)"
}""",
    },
    "13f": {
        "data_files": [
            "data/13f-positions.json", "data/13f-price-divergence.json",
            "data/insider-clusters.json", "data/activist-13d.json",
        ],
        "system": """You are JustHodl.AI's institutional flow analyst.
Track what smart money (13F filers, activists, insiders) is buying/selling.
Cite specific filers and tickers. Note divergences from price action.
Return ONLY valid JSON, no markdown fences.

Schema:
{
  "headline": "1-sentence smart money summary (e.g., 'Tiger Global added 8 names, sold 3; major rotation into AI semis')",
  "smart_money_moves": "3-4 sentences on top buys/sells across major funds",
  "top_buys_sells": "2-3 sentences naming specific high-conviction additions or exits",
  "divergence_signals": "2-3 sentences on stocks where smart money buys but price is down (opp) or vice versa",
  "conviction_score": "integer 0-100 (0=funds selling, 50=mixed, 100=concentrated accumulation)"
}""",
    },
    "screener": {
        "data_files": [
            "data/screener-results.json", "data/sp500-screener.json",
            "data/theme-cascade.json", "data/predictions-snapshots/latest.json",
        ],
        "system": """You are JustHodl.AI's S&P 500 screener analyst.
Surface the most actionable stocks from today's screening: best value, best momentum, best risk-reward.
Be specific: tickers, scores, key metrics.
Return ONLY valid JSON, no markdown fences.

Schema:
{
  "headline": "1-sentence S&P 500 picture (e.g., '12 names with momentum + value + insider buying confluence')",
  "top_picks": "3-4 sentences on 3-5 specific tickers with conviction rationale",
  "fundamental_strength": "2-3 sentences on names with strongest financial health screens",
  "momentum_picks": "2-3 sentences on names with strongest technical/momentum scores",
  "screener_score": "integer 0-100 (0=weak market, 50=mixed opportunities, 100=rich opportunity set)"
}""",
    },
}


def gather_page_context(page: str) -> dict:
    """Read all data files for a page into a context dict."""
    cfg = PAGE_CONFIGS.get(page)
    if not cfg:
        return {}
    ctx = {}
    for path in cfg["data_files"]:
        key = path.split("/")[-1].replace(".json", "").replace("-", "_")
        ctx[key] = _read_json(path) or {}
    return ctx


def truncate_for_prompt(data, max_chars: int = 8000) -> str:
    """Truncate nested data to fit prompt budget."""
    s = json.dumps(data, default=str)
    if len(s) <= max_chars:
        return s
    # Shrink lists to top 5
    if isinstance(data, dict):
        shrunk = {}
        for k, v in data.items():
            if isinstance(v, list) and len(v) > 5:
                shrunk[k] = v[:5] + [f"...({len(v) - 5} more)"]
            elif isinstance(v, dict):
                shrunk[k] = truncate_for_prompt(v, max_chars // 3)
            else:
                shrunk[k] = v
        s = json.dumps(shrunk, default=str)
        return s[:max_chars]
    return s[:max_chars]


def generate_commentary(page: str, context: dict) -> dict:
    """Call Claude to generate page commentary."""
    cfg = PAGE_CONFIGS.get(page)
    if not cfg:
        return {"error": "unknown_page"}

    system = cfg["system"]
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    user_prompt = f"""Today is {today}. Here is the data for {page}:

{truncate_for_prompt(context, max_chars=9000)}

Generate the JSON commentary now."""

    raw = call_claude(user_prompt, system=system, max_tokens=1200)
    if not raw:
        return {"error": "no_response", "fallback": True}

    cleaned = raw.strip()
    if cleaned.startswith("```"):
        # Strip code fences
        parts = cleaned.split("```", 2)
        if len(parts) >= 2:
            cleaned = parts[1]
            if cleaned.startswith("json"):
                cleaned = cleaned[4:].strip()

    try:
        parsed = json.loads(cleaned)
        return parsed
    except Exception as e:
        return {"narrative_raw": raw[:1500], "parse_error": str(e)}


def lambda_handler(event, context):
    t0 = time.time()
    target_page = event.get("page")  # if provided, only generate for one page

    pages = [target_page] if target_page else list(PAGE_CONFIGS.keys())
    results = {}

    for page in pages:
        if page not in PAGE_CONFIGS:
            results[page] = {"error": "no_config"}
            continue
        print(f"[ai-commentary] generating for {page}")
        ctx = gather_page_context(page)
        commentary = generate_commentary(page, ctx)
        # Validate at least one expected field
        has_content = "error" not in commentary
        results[page] = {
            "has_content": has_content,
            "keys": list(commentary.keys()),
        }

        output = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "page": page,
            "model": ANTHROPIC_MODEL,
            "commentary": commentary,
        }

        s3.put_object(
            Bucket=S3_BUCKET, Key=f"data/ai-commentary/{page}.json",
            Body=json.dumps(output, default=str).encode(),
            ContentType="application/json", CacheControl="public, max-age=600",
        )

        # Dated history
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        s3.put_object(
            Bucket=S3_BUCKET, Key=f"data/ai-commentary/history/{page}/{today}.json",
            Body=json.dumps(output, default=str).encode(),
            ContentType="application/json", CacheControl="public, max-age=86400",
        )

    elapsed = round(time.time() - t0, 1)
    print(f"[ai-commentary] DONE — {len(results)} pages in {elapsed}s")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "ok": True, "elapsed_s": elapsed,
            "pages_generated": list(results.keys()),
            "results": results,
        }),
    }
