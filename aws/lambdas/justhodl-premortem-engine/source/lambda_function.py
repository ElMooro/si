"""
justhodl-premortem-engine — Exponential Idea #1

For every TIER_A / CONVICTION_TITAN / HIGH_CONVICTION idea in best-ideas.json,
spin up Claude as the adversarial PM and have it write the KILL THESIS:

  "Here are 5 specific things that would have to be true for this trade
   to lose 50%, and here's the data feed that monitors each."

Output: data/kill-theses.json

This is structurally different from the existing debate-engine and 
investor-agents — those produce synthesis/balanced views. The pre-mortem 
ASSUMES the trade has already failed and works backwards.

Kahneman/Klein research: structured pre-mortems cut decision errors by ~30%.

Each kill-condition has:
  - description (plain English)
  - monitor_metric (specific number to watch)
  - data_source (where to fetch it)
  - break_threshold (the line that triggers the alert)
  - check_cadence (how often)
  - severity (1-5)

A follow-up tracker Lambda (justhodl-premortem-tracker) reads this file
and alerts when any condition breaks while the trade is active.

Schedule: daily 14 UTC (after best-ideas runs at 13 UTC).
"""
import json, os, logging, time, urllib.request
import boto3
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/kill-theses.json"
HIST_KEY = "data/history/kill-theses-history.json"
BEST_IDEAS_KEY = "data/best-ideas.json"

ANTHROPIC_KEY = os.environ.get("ANTHROPIC_KEY") or os.environ.get("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = "claude-haiku-4-5-20251001"
ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# How many ideas to red-team per run
TOP_N = int(os.environ.get("TOP_N", "15"))
# Max parallel Claude calls
MAX_PARALLEL = 4

s3 = boto3.client("s3", region_name=REGION)


PREMORTEM_PROMPT_TEMPLATE = """You are an ADVERSARIAL PORTFOLIO MANAGER with 20 years experience killing trades.
Your ONLY job: assume this trade has ALREADY lost 50% twelve months from now.
Now work backwards: what specifically went wrong? Be ruthless and specific.

TRADE UNDER REVIEW:
Symbol: {symbol} ({name})
Sector: {sector}
Market cap: {market_cap}
Current price: ${price}
Price target: ${price_target} ({upside_pct}% upside)
Conviction tier: {conviction_tier}
Families hit: {families}
Why the system likes this: {why}
Existing risk note: {risk}

Generate EXACTLY 5 kill conditions. For each:
1. Description — what specifically broke (concrete, not vague)
2. Monitor metric — the EXACT number/data point to watch
3. Data source — where this data comes from (FMP/Polygon/FRED/SEC EDGAR/etc.)
4. Break threshold — the specific number that triggers a "thesis broken" alert
5. Check cadence — daily/weekly/quarterly/event-driven
6. Severity 1-5 (5 = position-killing)

CRITICAL RULES:
- Be specific. Not "demand weakens" but "QoQ unit growth drops below 5%".
- Each kill condition must be MEASURABLE from publicly available data.
- Diversify the kill conditions — fundamentals, competitive, macro, technical, regulatory.
- No generic risks like "market crash" — specific to THIS company.
- Output ONLY valid JSON. No markdown, no preamble.

OUTPUT JSON SCHEMA:
{{
  "thesis_summary": "1-2 sentences naming the most likely failure mode",
  "kill_conditions": [
    {{
      "id": 1,
      "description": "...",
      "monitor_metric": "...",
      "data_source": "...",
      "break_threshold": "...",
      "check_cadence": "daily|weekly|monthly|quarterly|event_driven",
      "severity": 1-5
    }},
    ... 5 total ...
  ],
  "earliest_break_signal": "which of the 5 would fire first if the trade fails",
  "early_warning_horizon": "how many weeks/months before the 50% drawdown would we see the first break"
}}"""


def fetch_best_ideas():
    """Load best-ideas.json — fall back to nobrainers.json if absent."""
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=BEST_IDEAS_KEY)
        return json.loads(obj["Body"].read())
    except Exception as e:
        logger.warning(f"best_ideas_missing: {e}; trying nobrainers")
        try:
            obj = s3.get_object(Bucket=BUCKET, Key="data/nobrainers.json")
            return json.loads(obj["Body"].read())
        except Exception as e2:
            logger.error(f"both_inputs_missing: {e2}")
            return None


def select_targets(best_ideas):
    """Pick top-N ideas. Schema-flexible — supports best-ideas.json + nobrainers.json."""
    candidates = []
    # best-ideas.json schema
    for k in ("titans", "high_conviction", "stack", "all"):
        v = best_ideas.get(k)
        if isinstance(v, list):
            candidates.extend(v)
    # nobrainers.json schema  
    for k in ("nobrainers", "watchlist", "all_candidates"):
        v = best_ideas.get(k)
        if isinstance(v, list):
            candidates.extend(v)
    # Dedupe by symbol
    seen = set()
    unique = []
    for c in candidates:
        sym = c.get("symbol") or c.get("ticker")
        if not sym or sym in seen:
            continue
        seen.add(sym)
        # Normalize
        unique.append({
            "symbol": sym,
            "name": c.get("name") or c.get("company_name", sym),
            "sector": c.get("sector", "Unknown"),
            "market_cap": c.get("market_cap"),
            "price": c.get("price") or c.get("current_price"),
            "price_target": c.get("price_target") or c.get("target"),
            "upside_pct": c.get("upside_pct"),
            "conviction_tier": c.get("conviction_tier") or c.get("flag", "UNKNOWN"),
            "conviction_score": c.get("conviction_score") or c.get("asymmetric_score", 0),
            "families": ", ".join(c.get("families", [])) if isinstance(c.get("families"), list) else c.get("families", ""),
            "why": (c.get("why") or c.get("thesis", ""))[:1000],
            "risk": (c.get("risk") or "")[:500],
        })
    # Sort by conviction
    unique.sort(key=lambda x: x.get("conviction_score") or 0, reverse=True)
    return unique[:TOP_N]


def call_claude(payload, max_retries=2):
    """Anthropic API call with retry."""
    if not ANTHROPIC_KEY:
        return None, "no_api_key"
    req_body = {
        "model": ANTHROPIC_MODEL,
        "max_tokens": 2000,
        "messages": [{"role": "user", "content": payload}],
    }
    for attempt in range(max_retries + 1):
        try:
            req = urllib.request.Request(
                ANTHROPIC_URL,
                data=json.dumps(req_body).encode(),
                headers={
                    "x-api-key": ANTHROPIC_KEY,
                    "anthropic-version": "2023-06-01",
                    "Content-Type": "application/json",
                },
            )
            r = urllib.request.urlopen(req, timeout=60)
            response = json.loads(r.read())
            content = response.get("content", [{}])[0].get("text", "")
            return content, None
        except Exception as e:
            if attempt < max_retries:
                time.sleep(1.5 * (attempt + 1))
                continue
            return None, str(e)[:200]


def extract_json(text):
    """Robust JSON extraction from Claude response (handles ```json wrappers)."""
    if not text:
        return None
    # Strip markdown code fences if present
    t = text.strip()
    if t.startswith("```"):
        # Find the first newline, then last ```
        first_nl = t.find("\n")
        if first_nl > 0:
            t = t[first_nl+1:]
        last_fence = t.rfind("```")
        if last_fence > 0:
            t = t[:last_fence]
    t = t.strip()
    # Find first { and last }
    start = t.find("{")
    end = t.rfind("}")
    if start < 0 or end < 0 or end <= start:
        return None
    try:
        return json.loads(t[start:end+1])
    except Exception:
        return None


def generate_premortem(idea):
    """Generate kill thesis for one idea."""
    prompt = PREMORTEM_PROMPT_TEMPLATE.format(**idea)
    response, error = call_claude(prompt)
    if error:
        logger.warning(f"claude_fail symbol={idea['symbol']} err={error}")
        return {"symbol": idea["symbol"], "error": error, "raw": None}
    parsed = extract_json(response)
    if not parsed:
        logger.warning(f"parse_fail symbol={idea['symbol']} response_head={response[:200] if response else ''}")
        return {"symbol": idea["symbol"], "error": "json_parse_failed", "raw": (response or "")[:500]}
    # Sanity check the schema
    if not isinstance(parsed.get("kill_conditions"), list):
        return {"symbol": idea["symbol"], "error": "missing_kill_conditions", "raw": (response or "")[:500]}
    # Stamp metadata
    return {
        "symbol": idea["symbol"],
        "name": idea.get("name"),
        "sector": idea.get("sector"),
        "conviction_tier": idea.get("conviction_tier"),
        "price_at_premortem": idea.get("price"),
        "price_target": idea.get("price_target"),
        "upside_pct": idea.get("upside_pct"),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "thesis_summary": parsed.get("thesis_summary"),
        "kill_conditions": parsed.get("kill_conditions"),
        "earliest_break_signal": parsed.get("earliest_break_signal"),
        "early_warning_horizon": parsed.get("early_warning_horizon"),
        "status": "ACTIVE",  # tracker can set to BROKEN / EXPIRED
        "conditions_broken": [],
    }


def send_telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = json.dumps({
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        }).encode()
        req = urllib.request.Request(url, data=data,
                                     headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=15)
    except Exception as e:
        logger.error(f"telegram_fail: {e}")


def build_telegram_digest(theses):
    n_ok = sum(1 for t in theses if not t.get("error"))
    n_fail = len(theses) - n_ok
    lines = [f"🎯 *Pre-Mortem Engine — daily kill theses*", ""]
    lines.append(f"Generated kill conditions for {n_ok} top-conviction names "
                 f"({n_fail} fail).")
    lines.append("")
    # Highest severity earliest-warning
    by_warning = []
    for t in theses:
        if t.get("error") or not t.get("kill_conditions"): continue
        # Most severe kill condition
        max_sev = max((c.get("severity", 0) for c in t["kill_conditions"]), default=0)
        by_warning.append((t["symbol"], t.get("earliest_break_signal", "?"),
                           t.get("early_warning_horizon", "?"), max_sev))
    by_warning.sort(key=lambda x: -x[3])
    if by_warning:
        lines.append("*🚨 Most fragile theses (max severity kill condition):*")
        for sym, warn, horizon, sev in by_warning[:5]:
            lines.append(f"  `{sym}` sev={sev}/5 watch={horizon} ahead")
            lines.append(f"     trigger: _{warn[:90]}_")
        lines.append("")
    lines.append("[kill-theses.html](https://justhodl.ai/kill-theses.html)")
    return "\n".join(lines)


def update_history(payload):
    try:
        try:
            old = json.loads(s3.get_object(Bucket=BUCKET, Key=HIST_KEY)["Body"].read())
        except s3.exceptions.NoSuchKey:
            old = {"snapshots": []}
        snap = {
            "ts": payload["generated_at"],
            "n_theses": payload["summary"]["n_theses_ok"],
            "n_failed": payload["summary"]["n_failed"],
            "top_symbols": [t["symbol"] for t in payload["theses"][:5] if not t.get("error")],
        }
        old["snapshots"] = (old.get("snapshots") or []) + [snap]
        old["snapshots"] = old["snapshots"][-90:]  # 90 days
        s3.put_object(
            Bucket=BUCKET, Key=HIST_KEY,
            Body=json.dumps(old, indent=2).encode(),
            ContentType="application/json",
            CacheControl="max-age=3600",
        )
    except Exception as e:
        logger.error(f"history_write_fail: {e}")


def lambda_handler(event, context):
    started = datetime.now(timezone.utc)
    logger.info(f"premortem-engine starting v1")

    # 1. Fetch best ideas
    best = fetch_best_ideas()
    if not best:
        return {"statusCode": 500, "body": json.dumps({"error": "no input data"})}

    # 2. Select top-N candidates
    targets = select_targets(best)
    logger.info(f"selected {len(targets)} targets for pre-mortem")
    if not targets:
        return {"statusCode": 500, "body": json.dumps({"error": "no targets"})}

    # 3. Parallel Claude calls (red-team each)
    theses = []
    with ThreadPoolExecutor(max_workers=MAX_PARALLEL) as ex:
        futures = {ex.submit(generate_premortem, t): t for t in targets}
        for fut in as_completed(futures):
            try:
                t = fut.result()
                theses.append(t)
            except Exception as e:
                logger.error(f"pre_mortem_thread_fail: {e}")

    # Preserve original order
    sym_order = {t["symbol"]: i for i, t in enumerate(targets)}
    theses.sort(key=lambda x: sym_order.get(x["symbol"], 999))

    n_ok = sum(1 for t in theses if not t.get("error"))
    n_fail = len(theses) - n_ok

    # 4. Build payload
    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    payload = {
        "schema_version": "1.0",
        "engine": "premortem-engine",
        "generated_at": started.isoformat(),
        "elapsed_seconds": round(elapsed, 2),
        "model_used": ANTHROPIC_MODEL,
        "input_source": BEST_IDEAS_KEY,
        "n_targets": len(targets),
        "summary": {
            "n_theses_ok": n_ok,
            "n_failed": n_fail,
            "n_active_conditions": sum(len(t.get("kill_conditions") or []) for t in theses if not t.get("error")),
        },
        "theses": theses,
        "methodology": {
            "approach": "Kahneman/Klein pre-mortem — assume trade lost 50%, work backwards",
            "kill_conditions_per_idea": 5,
            "model": ANTHROPIC_MODEL,
            "tracker_lambda": "justhodl-premortem-tracker (to be deployed) reads kill_conditions and monitors break_thresholds",
        },
    }

    # 5. Write to S3
    s3.put_object(
        Bucket=BUCKET, Key=OUT_KEY,
        Body=json.dumps(payload, default=str, indent=2).encode(),
        ContentType="application/json",
        CacheControl="max-age=3600, public",
    )
    update_history(payload)
    logger.info(f"wrote {OUT_KEY}: n_ok={n_ok} n_fail={n_fail}")

    # 6. Telegram digest
    try:
        send_telegram(build_telegram_digest(theses))
    except Exception as e:
        logger.error(f"telegram_fail: {e}")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({
            "ok": True,
            "n_targets": len(targets),
            "n_theses_ok": n_ok,
            "n_failed": n_fail,
            "elapsed": round(elapsed, 2),
        }),
    }
