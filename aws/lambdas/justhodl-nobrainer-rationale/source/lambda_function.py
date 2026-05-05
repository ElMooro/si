"""
justhodl-nobrainer-rationale  (Layer 5 of nobrainer hunter)
===========================================================
For each top-N candidate from Layer 4, build a structured prompt with all
underlying numbers and have Claude (haiku-4-5) write a thesis in Khalid's
voice that explains:

  • The megatrend (theme attribution from Layer 1)
  • Why the primary leg is crowded/inflated
  • The hard supply/demand inflection (Layer 2 supply signals)
  • The valuation asymmetry vs theme peers (Layer 3 z-scores + mcap_to_rev)
  • The catalyst proximity (next earnings)
  • Counter-thesis (what kills it)
  • Position recommendation (size, entry, stop, target, time horizon)

Outputs:
  • s3://justhodl-dashboard-live/data/nobrainers-rationale.json (full)
  • s3://justhodl-dashboard-live/data/nobrainer-thesis/<TICKER>_<THEME>.json (per-thesis)
  • Telegram digest of top 3 to chat 8678089260

Schedule: cron(45 13 * * ? *) — daily 13:45 UTC, ~15min after Layer 4
                                (Layer 4 runs at 13:30 UTC daily).

Reads:
  s3://justhodl-dashboard-live/data/nobrainers.json (Layer 4)

Env:
  ANTHROPIC_KEY (preferred) or SSM /justhodl/anthropic/api-key
  TELEGRAM_BOT_TOKEN or SSM /justhodl/telegram/bot_token
  SSM /justhodl/telegram/chat_id
  SKIP_TELEGRAM=1 to disable digest
  SKIP_CLAUDE=1 to dry-run without API calls
"""
import json
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
SSM = boto3.client("ssm", region_name=REGION)

ANTHROPIC_KEY = os.environ.get("ANTHROPIC_KEY", "")
ANTHROPIC_MODEL = "claude-haiku-4-5-20251001"

SKIP_TELEGRAM = os.environ.get("SKIP_TELEGRAM") == "1"
SKIP_CLAUDE = os.environ.get("SKIP_CLAUDE") == "1"

# How many top candidates to write theses for
N_THESES = int(os.environ.get("N_THESES", "10"))
# How many to send in Telegram digest
N_DIGEST = int(os.environ.get("N_DIGEST", "3"))
# Min asymmetric_score to bother writing thesis (saves tokens)
MIN_SCORE = float(os.environ.get("MIN_SCORE", "55.0"))


# ─────────────────────────────────────────────────────────────────────────────
# CRED HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def get_anthropic_key():
    if ANTHROPIC_KEY:
        return ANTHROPIC_KEY
    try:
        return SSM.get_parameter(Name="/justhodl/anthropic/api-key", WithDecryption=True)["Parameter"]["Value"]
    except Exception as e:
        print(f"[ssm-anthropic] {e}")
        return None


def get_telegram_token():
    tk = os.environ.get("TELEGRAM_BOT_TOKEN") or os.environ.get("TELEGRAM_TOKEN")
    if tk:
        return tk
    try:
        return SSM.get_parameter(Name="/justhodl/telegram/bot_token", WithDecryption=True)["Parameter"]["Value"]
    except Exception:
        return None


def get_telegram_chat_id():
    try:
        return SSM.get_parameter(Name="/justhodl/telegram/chat_id")["Parameter"]["Value"]
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# CLAUDE CALL (mirror of ai-brief envelope)
# ─────────────────────────────────────────────────────────────────────────────
def call_anthropic(prompt, key, max_tokens=1500):
    url = "https://api.anthropic.com/v1/messages"
    body = json.dumps({
        "model": ANTHROPIC_MODEL,
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": prompt}],
    }).encode()
    req = urllib.request.Request(
        url,
        data=body,
        headers={
            "Content-Type": "application/json",
            "x-api-key": key,
            "anthropic-version": "2023-06-01",
        },
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode())


def extract_text(claude_response):
    try:
        for block in claude_response.get("content", []):
            if block.get("type") == "text":
                return block.get("text", "").strip()
    except Exception:
        pass
    return ""


# ─────────────────────────────────────────────────────────────────────────────
# THESIS PROMPT BUILDER
# ─────────────────────────────────────────────────────────────────────────────
KHALID_VOICE = """You are writing in the voice of Khalid, a sharp macro/asymmetric trader who built JustHodl.AI. His style:
- Decisive, direct, no hedging language
- Cites specific numbers inline (P/S, mcap_to_rev, percentiles, supply tightness scores)
- Calls out crowdedness and market mispricing explicitly
- Treats supply/demand inflections as the unsexy edge the market misses
- Uses macro framing (this is downstream of X, market hasn't priced Y)
- Ends with a binary call: LONG / TRIM / EXIT / WAIT with explicit thresholds
- Never "consider" or "might want to" — issues actual decisions
- Format follows your existing decisive-call structure

Write the thesis as if you are Khalid himself, not as an analyst describing his thinking."""



def _insider_block(cl):
    """Format insider-cluster info for prompt injection. Returns 'no recent insider signal' if None."""
    if not cl:
        return "  (no recent insider cluster activity for this ticker)"
    lines = []
    lines.append(f"  Insider score: {cl.get('score', 0):.1f}/100 ({cl.get('signal_type')})")
    lines.append(f"  Rationale: {cl.get('rationale', '')}")
    lines.append(f"  {cl.get('n_insiders', 0)} insiders / {cl.get('n_transactions', 0)} TX / ${cl.get('total_value', 0):,.0f} total")
    lines.append(f"  Window: {cl.get('first_buy', '?')} → {cl.get('last_buy', '?')}")
    if cl.get("has_ceo"):
        lines.append("  ✓ CEO bought")
    if cl.get("has_cfo"):
        lines.append("  ✓ CFO bought")
    insiders = cl.get("insiders", [])
    if insiders:
        lines.append("  Top insiders:")
        for i in insiders[:5]:
            name = (i.get("name") or "?")[:30]
            role = (i.get("role") or "")[:35]
            val = i.get("total_value", 0) or 0
            lines.append(f"    • {name:<30} {role:<35} ${val:>10,.0f}")
    return "\n".join(lines)


def build_thesis_prompt(candidate, insider_cluster=None):
    """Build the full structured prompt with all data inputs."""
    f = candidate.get("factors", {})
    fund = candidate.get("fundamentals", {})
    val = candidate.get("valuation_components", {})
    supply_sigs = candidate.get("supply_signals", [])

    fund_lines = []
    if fund.get("market_cap"):
        fund_lines.append(f"Market cap: ${fund['market_cap']/1e9:.2f}B")
    if fund.get("revenue_ttm"):
        fund_lines.append(f"Revenue TTM: ${fund['revenue_ttm']/1e9:.2f}B")
    if fund.get("mcap_to_rev") is not None:
        fund_lines.append(f"Mcap/Revenue ratio: {fund['mcap_to_rev']:.2f}× (the asymmetry tell)")
    if fund.get("p_s") is not None:
        fund_lines.append(f"P/S: {fund['p_s']:.2f}")
    if fund.get("p_e") is not None:
        fund_lines.append(f"P/E: {fund['p_e']:.1f}")
    if fund.get("ev_ebitda") is not None:
        fund_lines.append(f"EV/EBITDA: {fund['ev_ebitda']:.1f}")
    if fund.get("fcf_yield") is not None:
        fund_lines.append(f"FCF yield: {fund['fcf_yield']*100:.1f}%")
    if fund.get("gross_margin") is not None:
        fund_lines.append(f"Gross margin: {fund['gross_margin']*100:.1f}%")
    if fund.get("rev_growth_ttm") is not None:
        fund_lines.append(f"Revenue growth TTM: {fund['rev_growth_ttm']*100:.1f}%")
    if fund.get("price"):
        fund_lines.append(f"Current price: ${fund['price']:.2f}")
    if fund.get("industry"):
        fund_lines.append(f"Industry: {fund['industry']}")

    val_lines = []
    if val.get("z_p_s_vs_theme") is not None:
        val_lines.append(f"P/S z-score vs theme median: {val['z_p_s_vs_theme']:.2f} (negative = cheap vs peers)")
    if val.get("z_p_e_vs_theme") is not None:
        val_lines.append(f"P/E z-score vs theme median: {val['z_p_e_vs_theme']:.2f}")

    supply_lines = []
    for s in supply_sigs:
        supply_lines.append(f"  • {s.get('signal')}: score {s.get('score')} ({s.get('flag')}) — {s.get('description')}")

    prompt = f"""{KHALID_VOICE}

Here is the data on a candidate the system has flagged:

CANDIDATE: {candidate.get('ticker')} ({candidate.get('name', 'n/a')})
THEME: {candidate.get('theme_etf')} — {candidate.get('theme_name')} (phase: {candidate.get('theme_phase')})
TIER: {candidate.get('tier')} (1=primary, 2=second-order, 3=industry peer not in ETF top 10)
ASYMMETRIC SCORE: {candidate.get('asymmetric_score')}/100  ({candidate.get('flag')})

5-FACTOR BREAKDOWN:
  Theme attribution:    {f.get('theme_attribution')}/100  (is the megatrend alive?)
  Primary leg inflated: {f.get('primary_inflated')}/100  (is the crowded leg expensive?)
  Supply inflection:    {f.get('supply_inflection')}/100  (hard supply tightness in inputs)
  Valuation asymmetry:  {f.get('valuation_asym')}/100  (this name vs theme peers)
  Catalyst proximity:   {f.get('catalyst_prox')}/100  (next earnings / event)
  Tier multiplier:      ×{f.get('tier_multiplier')}
  Phase multiplier:     ×{f.get('phase_multiplier')}

FUNDAMENTALS:
{chr(10).join('  ' + l for l in fund_lines) if fund_lines else '  (data unavailable)'}

VALUATION vs THEME PEERS:
{chr(10).join('  ' + l for l in val_lines) if val_lines else '  (data unavailable)'}

HARD SUPPLY/DEMAND SIGNALS (the market structurally misses these):
{chr(10).join(supply_lines) if supply_lines else '  (no supply signals mapped to this theme)'}

NEXT CATALYST: {candidate.get('next_earnings') or 'no earnings date in window'}

INSIDER CLUSTER SIGNAL (if any):
{_insider_block(insider_cluster)}

YOUR TASK:
Write a 250-350 word thesis for {candidate.get('ticker')} in your decisive-call voice. Structure:

1. **Megatrend** (1 short paragraph): What's the larger trade {candidate.get('ticker')} is downstream of, and why is the market still funding it?

2. **The mispricing** (1 short paragraph): The primary leg is crowded/expensive. Cite the theme stats. Explain why second-order names get ignored.

3. **The hard data tell** (1 short paragraph): What supply tightness or input inflection is the market not pricing in? Cite the supply scores by name.

4. **The valuation asymmetry** (1 short paragraph): Why is THIS ticker cheap vs theme peers? Cite mcap_to_rev, P/S z-score, what would happen if margins/multiples normalized.

5. **Catalyst** (1 short paragraph): What forces the market to look in 30-90 days?

6. **Counter / what kills it** (3-4 bullets): What specifically would invalidate the trade. Be honest.

7. **DECISIVE CALL** (1 short paragraph): Position size as % of portfolio (1-5%), entry zone, stop level (% below entry), target (% above), time horizon, risk:reward.

Use specific numbers throughout. No hedge language. Issue an actual call."""
    return prompt


def build_telegram_digest(theses, leader_count, mu_count):
    """Build a 1500-char Markdown digest of top theses."""
    lines = ["🎯 *Nobrainer hunter — daily digest*", ""]
    lines.append(f"_{leader_count} leaderboard candidates · {mu_count} MU-grade_")
    lines.append("")

    for i, t in enumerate(theses[:N_DIGEST], 1):
        c = t["candidate"]
        f = c.get("factors", {})
        fund = c.get("fundamentals", {})
        flag_emoji = {
            "TIER_A_NOBRAINER":     "🟢",
            "TIER_B_HIGH_CONVICTION": "🟡",
            "TIER_C_WATCHLIST":     "🟠",
            "TIER_D_MONITOR":       "⚪",
        }.get(c.get("flag"), "⚪")
        lines.append(f"{flag_emoji} *{i}. {c.get('ticker')}* — {c.get('flag')} ({c.get('asymmetric_score'):.0f}/100)")
        lines.append(f"   theme: {c.get('theme_etf')} ({c.get('theme_phase')})  ·  tier {c.get('tier')}")
        if fund.get("mcap_to_rev") is not None:
            lines.append(f"   mcap/rev: {fund.get('mcap_to_rev'):.2f}×  ·  P/S: {fund.get('p_s', 'n/a')}")
        lines.append(f"   supply={f.get('supply_inflection'):.0f}  val={f.get('valuation_asym'):.0f}  "
                     f"cat={f.get('catalyst_prox'):.0f}  earnings: {c.get('next_earnings') or 'n/a'}")
        # First sentence of thesis
        thesis = (t.get("thesis") or "").strip()
        if thesis:
            first = thesis.split("\n")[0][:200]
            lines.append(f"   _{first}_")
        lines.append("")

    lines.append("[nobrainers.html](https://justhodl.ai/nobrainers.html)  ·  "
                 "[themes.html](https://justhodl.ai/themes.html)  ·  "
                 "[brief.html](https://justhodl.ai/brief.html)")
    return "\n".join(lines)


def send_telegram(text, chat_id):
    token = get_telegram_token()
    if not token or not chat_id:
        print("[tg] missing token/chat_id")
        return False
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        body = json.dumps({
            "chat_id": chat_id,
            "text": text[:4096],
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        }).encode()
        req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read().decode())
            ok = bool(data.get("ok"))
            mid = (data.get("result") or {}).get("message_id")
            print(f"[tg] sent ok={ok} message_id={mid}")
            return ok
    except Exception as e:
        print(f"[tg] err: {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# HANDLER
# ─────────────────────────────────────────────────────────────────────────────
def lambda_handler(event=None, context=None):
    started = time.time()
    print("[rationale] Layer 5 — nobrainer-rationale starting")

    # 1. Load Layer 4 output
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/nobrainers.json")
        layer4 = json.loads(obj["Body"].read())
    except Exception as e:
        print(f"[rationale] FATAL — cannot load Layer 4: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

    summary = layer4.get("summary", {})
    leader = summary.get("top_25_overall", [])
    mu_grade = summary.get("mu_grade_top_15", [])
    print(f"[rationale] leaderboard: {len(leader)}  mu_grade: {len(mu_grade)}")

    # Load insider-cluster signals for compound-score augmentation
    insider_by_ticker = {}
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/insider-clusters.json")
        insider_data = json.loads(obj["Body"].read())
        for cl in insider_data.get("clusters", []):
            tk = cl.get("ticker")
            if tk:
                insider_by_ticker[tk] = cl
        print(f"[rationale] loaded {len(insider_by_ticker)} insider clusters")
    except Exception as e:
        print(f"[rationale] WARN — insider clusters unavailable: {e}")

    # 2. Pick top N candidates above MIN_SCORE
    candidates = [x for x in leader if x.get("asymmetric_score", 0) >= MIN_SCORE][:N_THESES]
    if not candidates:
        print(f"[rationale] no candidates >= {MIN_SCORE} — nothing to write")
        return {
            "statusCode": 200,
            "body": json.dumps({"n_theses": 0, "reason": "no candidates above threshold"}),
        }
    print(f"[rationale] writing theses for top {len(candidates)} above {MIN_SCORE}")

    # 3. Get Anthropic key
    key = get_anthropic_key()
    if not key and not SKIP_CLAUDE:
        print("[rationale] WARN — no Anthropic key, switching to SKIP_CLAUDE mode")
        skip = True
    else:
        skip = SKIP_CLAUDE

    # 4. Generate theses
    theses = []
    n_ok = 0
    n_fail = 0
    for c in candidates:
        ticker = c.get("ticker")
        theme = c.get("theme_etf")
        cl = insider_by_ticker.get(ticker)
        if cl:
            print(f"[rationale] {ticker} ALSO has insider cluster (score={cl.get('score')}, signal={cl.get('signal_type')})")
        prompt = build_thesis_prompt(c, cl)
        thesis_text = ""
        usage = {}
        err = None

        if skip:
            thesis_text = f"[SKIP_CLAUDE=1] would-be thesis for {ticker} on {theme} — score {c.get('asymmetric_score')}"
        else:
            t0 = time.time()
            try:
                resp = call_anthropic(prompt, key, max_tokens=1500)
                thesis_text = extract_text(resp)
                usage = resp.get("usage", {})
                print(f"[rationale] {ticker}/{theme} thesis ok ({len(thesis_text)} chars, "
                      f"in={usage.get('input_tokens')} out={usage.get('output_tokens')}, "
                      f"{round(time.time()-t0, 1)}s)")
                n_ok += 1
            except urllib.error.HTTPError as he:
                body = ""
                try:
                    body = he.read().decode()[:200]
                except Exception:
                    pass
                err = f"HTTP {he.code}: {body}"
                print(f"[rationale] {ticker}/{theme} ERR {err}")
                n_fail += 1
            except Exception as e:
                err = str(e)
                print(f"[rationale] {ticker}/{theme} ERR {err}")
                n_fail += 1

        thesis_block = {
            "ticker": ticker,
            "theme_etf": theme,
            "asymmetric_score": c.get("asymmetric_score"),
            "flag": c.get("flag"),
            "tier": c.get("tier"),
            "candidate": c,
            "thesis": thesis_text,
            "thesis_chars": len(thesis_text),
            "claude_usage": usage,
            "error": err,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
        theses.append(thesis_block)

        # Per-thesis S3 key
        try:
            S3.put_object(
                Bucket=BUCKET,
                Key=f"data/nobrainer-thesis/{ticker}_{theme}.json",
                Body=json.dumps(thesis_block, default=str).encode("utf-8"),
                ContentType="application/json",
                CacheControl="max-age=60, public",
            )
        except Exception as e:
            print(f"[rationale] s3 per-thesis put err: {e}")

    # 5. Compose full output
    output = {
        "schema_version": "1.0",
        "method": "nobrainer_rationale_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - started, 1),
        "n_theses": len(theses),
        "n_claude_ok": n_ok,
        "n_claude_fail": n_fail,
        "min_score_threshold": MIN_SCORE,
        "model": ANTHROPIC_MODEL,
        "skipped_claude": skip,
        "theses": theses,
        "n_layer4_leaderboard": len(leader),
        "n_layer4_mu_grade": len(mu_grade),
        "layer4_generated_at": layer4.get("generated_at"),
    }

    body = json.dumps(output, default=str)
    S3.put_object(
        Bucket=BUCKET,
        Key="data/nobrainers-rationale.json",
        Body=body.encode("utf-8"),
        ContentType="application/json",
        CacheControl="max-age=60, public",
    )
    print(f"[rationale] wrote {len(body)}b to data/nobrainers-rationale.json")

    # 6. Telegram digest of top 3
    if not SKIP_TELEGRAM and theses:
        chat_id = get_telegram_chat_id()
        if chat_id:
            digest = build_telegram_digest(theses, len(leader), len(mu_grade))
            send_telegram(digest, chat_id)
        else:
            print("[rationale] no telegram chat_id — skipping digest")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_theses": len(theses),
            "n_claude_ok": n_ok,
            "n_claude_fail": n_fail,
            "duration_s": round(time.time() - started, 1),
        }),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2))
