"""
justhodl-alpha-daily-brief — Roadmap #5

═══════════════════════════════════════════════════════════════════════
THE ONE EMAIL YOU READ EACH MORNING
─────────────────────────────────────
Bloomberg-grade synthesis of the entire JustHodl AI stack, distilled by
Claude haiku into a ~400-word morning brief. Format:

  REGIME STATUS:    SLOWING (unchanged) · LCE 38 → 41
  TOP 3 TRADES:     1. LONG NVDA · why · entry/target/stop/RR
                     2. SHORT TTD · why · entry/target/stop/RR
                     3. LONG JOE  · why · entry/target/stop/RR
  PORTFOLIO RISK:   Stocks in your watchlist with alpha falling
  MACRO FLAGS:      Yields · DXY · stress score
  KEY EVENTS:       CPI · earnings · Fed minutes

═══════════════════════════════════════════════════════════════════════
PIPELINE
────────
1. Read alpha-score.json (today's rankings)
2. Read signals/confluence.json (TIER S/A picks)
3. Read signals/regime-picks.json (regime-adjusted)
4. Read sentiment/data.json (news bullish/bearish)
5. Read smart-money-holdings.json (today's flagship positions)
6. Compose context bundle for Claude
7. Claude returns synthesized brief (markdown)
8. Write to data/alpha-brief.md + data/alpha-brief.json
9. Send to Telegram (truncated to 4000 char limit)

Schedule: 11:30 UTC daily (7:30 AM ET pre-market open)
Cost: ~\$0.01/day for one Claude haiku call
"""
import json
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone
import boto3

S3_BUCKET = "justhodl-dashboard-live"
ALPHA_KEY = "screener/alpha-score.json"
CONFLUENCE_KEY = "signals/confluence.json"
REGIME_KEY = "signals/regime-picks.json"
SENTIMENT_KEY = "sentiment/data.json"
SMART_MONEY_KEY = "screener/smart-money-holdings.json"
DEBATE_KEY = "data/debate.json"
ANOMALIES_KEY = "signals/anomalies.json"

BRIEF_MD_KEY = "data/alpha-brief.md"
BRIEF_JSON_KEY = "data/alpha-brief.json"

CLAUDE_MODEL = "claude-haiku-4-5-20251001"
# Accept either env var name — different existing Lambdas use different names:
#   justhodl-ai-brief uses ANTHROPIC_KEY
#   justhodl-telegram-bot uses ANTHROPIC_API_KEY
ANTHROPIC_KEY = (os.environ.get("ANTHROPIC_KEY")
                  or os.environ.get("ANTHROPIC_API_KEY")
                  or "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")

s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")


def get_chat_id():
    try:
        return ssm.get_parameter(Name="/justhodl/telegram/chat_id",
                                  WithDecryption=True)["Parameter"]["Value"]
    except Exception:
        return os.environ.get("TELEGRAM_CHAT_ID", "")


def load_sidecar(key, default=None):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception as e:
        print(f"  failed to read {key}: {str(e)[:80]}")
        return default


def trim(s, n=80):
    if not s: return ""
    s = str(s)
    return s[:n] + "…" if len(s) > n else s


def build_context_bundle():
    """Gather all the data Claude needs to write the brief."""
    alpha = load_sidecar(ALPHA_KEY, {})
    confluence = load_sidecar(CONFLUENCE_KEY, {})
    regime = load_sidecar(REGIME_KEY, {})
    sentiment = load_sidecar(SENTIMENT_KEY, {})
    debate = load_sidecar(DEBATE_KEY, {})
    anomalies = load_sidecar(ANOMALIES_KEY, {})

    # Top 10 alpha-ranked stocks
    top_alpha = [s for s in (alpha.get("stocks") or [])
                 if s.get("alpha_score") is not None][:10]
    top_alpha_brief = [{
        "rank": s.get("rank"),
        "sym": s["symbol"],
        "name": trim(s.get("name"), 30),
        "alpha": s.get("alpha_score"),
        "tier": s.get("tier"),
        "sector": s.get("sector"),
        "signals": s.get("top_signals", [])[:3],
        "flags": s.get("risk_flags", [])[:2],
        "price": s.get("price"),
    } for s in top_alpha]

    # TIER S + TIER A confluence
    tier_s = (confluence.get("tier_s_confluence") or [])[:5]
    tier_a = (confluence.get("tier_a_confluence") or [])[:5]
    confluence_brief = []
    for s in tier_s + tier_a:
        confluence_brief.append({
            "tier": s.get("confluence_tier"),
            "sym": s["symbol"],
            "alpha": s.get("alpha_score"),
            "firing_count": s.get("confluence_count"),
            "signals": s.get("top_signals", [])[:2],
        })

    # Regime picks
    regime_top = (regime.get("regime_picks") or [])[:8]
    regime_brief = [{
        "sym": r["symbol"],
        "sector": r.get("sector"),
        "alpha": r.get("alpha_score"),
        "regime_adj": r.get("regime_adj"),
        "regime_adj_score": r.get("regime_adj_score"),
    } for r in regime_top]

    # Diffs (changes since previous run)
    diffs = confluence.get("diffs") or {}
    diff_brief = {
        "new_tier_s": [d["symbol"] for d in (diffs.get("new_tier_s") or [])[:5]],
        "new_tier_a": [d["symbol"] for d in (diffs.get("new_tier_a_plus") or [])[:5]],
        "downgrades": [{"sym": d["symbol"], "from": d.get("from"), "to": d.get("to")}
                       for d in (diffs.get("downgrades") or [])[:5]],
    }

    # Top news (most-bullish + most-bearish from sentiment)
    sent_list = sentiment.get("sentiment") or []
    top_bulls = sorted([s for s in sent_list if s.get("sentimentSignal") == "bullish"],
                       key=lambda s: -s.get("sentimentScore", 0))[:3]
    top_bears = sorted([s for s in sent_list if s.get("sentimentSignal") == "bearish"],
                       key=lambda s: s.get("sentimentScore", 0))[:3]
    news_brief = {
        "bulls": [{"sym": s["symbol"], "score": s.get("sentimentScore"),
                  "reason": trim(s.get("sentimentReason"), 100)} for s in top_bulls],
        "bears": [{"sym": s["symbol"], "score": s.get("sentimentScore"),
                  "reason": trim(s.get("sentimentReason"), 100)} for s in top_bears],
    }

    # ─── DEBATE: 5-persona adversarial analysis (compact form for prompt) ───
    # debate.json schema: { debates: [ {symbol, alpha_score, synthesis: {consensus_verdict,
    #     bear_case, red_flags, downside_target_pct, avg_conviction}, personas: [...]} ] }
    debate_by_sym = {}
    for d in (debate.get("debates") or []):
        sym = d.get("symbol")
        if not sym: continue
        synth = d.get("synthesis") or {}
        # Compact per-persona one-liners (verdict + thesis truncated)
        persona_lines = []
        for p in (d.get("personas") or []):
            name = p.get("name")
            verdict = p.get("verdict", "?")
            conv = p.get("conviction")
            thesis = trim(p.get("thesis"), 140)
            if not name or verdict == "ERROR": continue
            persona_lines.append({
                "persona": name, "verdict": verdict,
                "conviction": conv, "thesis": thesis,
            })
        debate_by_sym[sym] = {
            "sym": sym,
            "consensus": synth.get("consensus_verdict"),
            "avg_conviction": synth.get("avg_conviction"),
            "verdict_counts": synth.get("verdict_counts"),
            "bear_case": trim(synth.get("bear_case"), 360),
            "red_flags": (synth.get("red_flags") or [])[:4],
            "downside_target_pct": synth.get("downside_target_pct"),
            "personas": persona_lines,
        }

    # Macro stress score for top-of-brief context
    macro_stress = anomalies.get("macro_stress_score")
    macro_interp = anomalies.get("stress_interpretation")

    return {
        "regime": confluence.get("regime") or regime.get("regime") or "UNKNOWN",
        "regime_confidence": confluence.get("regime_confidence", 0),
        "regime_logic": regime.get("regime_logic"),
        "macro_stress_score": macro_stress,
        "macro_stress_interp": macro_interp,
        "tier_distribution": alpha.get("tier_distribution"),
        "top_alpha": top_alpha_brief,
        "confluence": confluence_brief,
        "regime_picks": regime_brief,
        "diffs": diff_brief,
        "news": news_brief,
        "debate_by_sym": debate_by_sym,
    }


def call_claude(prompt):
    body = json.dumps({
        "model": CLAUDE_MODEL,
        "max_tokens": 1800,
        "messages": [{"role": "user", "content": prompt}],
    }).encode("utf-8")
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=body,
        headers={
            "Content-Type": "application/json",
            "x-api-key": ANTHROPIC_KEY,
            "anthropic-version": "2023-06-01",
        },
        method="POST")
    with urllib.request.urlopen(req, timeout=60) as r:
        resp = json.loads(r.read().decode("utf-8"))
    return resp["content"][0]["text"].strip()


def synthesize_brief(context):
    """Send context to Claude, return markdown brief."""
    today = datetime.now(timezone.utc).strftime("%a %b %d, %Y")
    mss = context.get("macro_stress_score")
    mss_line = (f"MACRO STRESS: {mss}/100 — {context.get('macro_stress_interp','')}"
                  if mss is not None else "")

    debate_block = context.get("debate_by_sym") or {}
    has_debates = bool(debate_block)

    if has_debates:
        debate_section = (
            f"\n═══ 5-PERSONA DEBATE OUTPUT (FORCED BEAR CASE FOR EACH) ═══\n"
            f"{json.dumps(list(debate_block.values()), indent=2)[:3500]}\n"
        )
        debate_rules = (
            "\n⚔ BEAR-CASE REQUIREMENT (forced disconfirming view):\n"
            "- If a stock you are recommending has debate output above, you MUST include "
            "a '**Bear case (Short persona):**' line under that pick with the short persona's "
            "concrete thesis + at least one red flag and the downside_target_pct.\n"
            "- If consensus is DIVIDED or SELL, recommend HOLD instead of LONG unless "
            "you can explicitly defeat the bear case.\n"
            "- The bear view is not optional — surface it even when you disagree.\n"
        )
    else:
        debate_section = (
            f"\n═══ DEBATE OUTPUT ═══\n(No debate sidecar today — explicitly note this and "
            f"write your own bear case for each LONG.)\n"
        )
        debate_rules = (
            "\n⚔ NO DEBATE DATA TODAY — write your own bear case for each LONG recommendation "
            "(specific to that stock: valuation excess, competitive threat, regime headwind, "
            "or accounting concern). Don't skip it.\n"
        )

    prompt = f"""You are a senior portfolio analyst writing the morning brief for an institutional investor.

TODAY: {today}
MACRO REGIME: {context['regime']} (confidence {context['regime_confidence']:.0%})
REGIME LOGIC: {context.get('regime_logic','')}
{mss_line}

═══ TOP 10 ALPHA-RANKED STOCKS ═══
{json.dumps(context['top_alpha'], indent=2)[:3000]}

═══ TIER S/A CONFLUENCE (multi-factor alignment) ═══
{json.dumps(context['confluence'], indent=2)[:2000]}

═══ REGIME-CONDITIONAL TOP PICKS ═══
{json.dumps(context['regime_picks'], indent=2)[:1500]}

═══ TIER CHANGES SINCE YESTERDAY ═══
{json.dumps(context['diffs'], indent=2)[:1000]}

═══ TOP NEWS-SENTIMENT MOVES ═══
{json.dumps(context['news'], indent=2)[:2000]}
{debate_section}
═══════════════════════════════════════════════════════════
TASK: Write a concise morning brief in MARKDOWN format following this EXACT structure.
Be decisive — name specific tickers + specific dollar levels where possible. Total length: 500-750 words.

```
# 🎯 JustHodl Alpha Brief · {today}

## REGIME STATUS
[1-2 sentences: current regime, confidence, what's shifted since yesterday, what it means for positioning. Include macro stress if available.]

## TODAY'S TOP 3 TRADES

### 1. LONG/SHORT $TICKER  [Tier · α=XX]
[Why this trade — 2-3 sentences citing specific signals from data above]
**Setup**: entry zone · target · stop · R/R
**Bear case (Short persona):** [the disconfirming view — specific thesis + 1-2 red flags + downside%. Required.]

### 2. ...
[Same structure incl. Bear case]

### 3. ...
[Same structure incl. Bear case]

## TIER UPGRADES & DOWNGRADES
[Brief bullet list of stocks that moved tier since yesterday — name + direction + key reason]

## NEWS SIGNAL OF THE DAY
[1 bullet for most-bullish news + 1 for most-bearish]

## DEBATE DISSENT (if any)
[If any debated stock had DIVIDED or SELL consensus, surface it here with the bear case. Otherwise note "5-persona debates aligned today."]

## MACRO FLAGS TO WATCH
[2-3 specific things — regime fragility, yield curve, sector flows]
```

Rules:
- Use ONLY data provided above. Don't invent prices, levels, or events.
- Mention TIER S confluence stocks first if any exist — they're rare.
- For each trade, cite the specific factor scores that justify it.
- Reference regime fit explicitly: "X benefits from {context['regime']} regime because..."
- Be DECISIVE. Not "could be" — "this IS" or "this ISN'T."
{debate_rules}"""
    return call_claude(prompt)


def send_telegram(text, chat_id):
    if not TELEGRAM_TOKEN or not chat_id: return False
    # Telegram has 4096 char limit per message
    if len(text) > 4000:
        text = text[:3950] + "\n\n…(truncated — full brief at justhodl.ai)"
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    body = urllib.parse.urlencode({
        "chat_id": chat_id, "text": text,
        "parse_mode": "Markdown", "disable_web_page_preview": "true",
    }).encode("utf-8")
    try:
        with urllib.request.urlopen(urllib.request.Request(url, data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"}),
            timeout=15) as r:
            resp = json.loads(r.read().decode("utf-8"))
        return resp.get("ok", False)
    except Exception as e:
        print(f"  telegram send err: {e}")
        return False


def lambda_handler(event, context):
    started = time.time()
    print(f"=== ALPHA DAILY BRIEF · {datetime.now(timezone.utc).isoformat()} ===")

    # 1. Gather context
    bundle = build_context_bundle()
    print(f"  bundled: {len(bundle.get('top_alpha',[]))} alpha · "
          f"{len(bundle.get('confluence',[]))} confluence · "
          f"{len(bundle.get('regime_picks',[]))} regime picks · "
          f"{len(bundle.get('debate_by_sym') or {})} debates")

    # 2. Synthesize with Claude
    try:
        brief_md = synthesize_brief(bundle)
        print(f"  claude returned {len(brief_md)} chars")
    except Exception as e:
        print(f"  claude failed: {e}")
        return {"statusCode": 500, "body": json.dumps({"err": f"claude: {str(e)[:200]}"})}

    # 3. Persist
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "model": CLAUDE_MODEL,
        "regime": bundle.get("regime"),
        "macro_stress_score": bundle.get("macro_stress_score"),
        "brief_markdown": brief_md,
        "context_summary": {
            "n_top_alpha": len(bundle.get("top_alpha", [])),
            "n_confluence": len(bundle.get("confluence", [])),
            "n_debates": len(bundle.get("debate_by_sym") or {}),
            "regime": bundle.get("regime"),
        },
    }
    s3.put_object(
        Bucket=S3_BUCKET, Key=BRIEF_MD_KEY,
        Body=brief_md.encode("utf-8"),
        ContentType="text/markdown; charset=utf-8",
        CacheControl="public, max-age=14400")
    s3.put_object(
        Bucket=S3_BUCKET, Key=BRIEF_JSON_KEY,
        Body=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        ContentType="application/json",
        CacheControl="public, max-age=14400")

    # 4. Send Telegram
    chat_id = get_chat_id()
    telegram_sent = False
    if chat_id:
        telegram_sent = send_telegram(brief_md, chat_id)

    elapsed = time.time() - started
    print(f"  brief written + telegram_sent={telegram_sent} · {elapsed:.2f}s")

    return {"statusCode": 200, "body": json.dumps({
        "success": True,
        "brief_length": len(brief_md),
        "telegram_sent": telegram_sent,
        "elapsed_seconds": round(elapsed, 2),
    })}
