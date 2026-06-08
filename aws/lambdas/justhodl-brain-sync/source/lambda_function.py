"""justhodl-brain-sync — mirrors the user's "brain" (investing philosophy, rules,
theses, reminders) from the Cloudflare KV store to S3 data/brain.json, so the
platform's engines (morning intelligence, AI brief, ask) can read what's on
Khalid's mind and weight the system toward what matters to him.

The brain is authored on /brain.html → stored in KV via the worker /brain route.
This Lambda fetches the public GET /brain and writes a clean S3 copy + a
pre-formatted text block the AI engines can drop straight into a prompt.

SCHEDULE: every 15 min.
"""
import json, time
import urllib.request, os
from datetime import datetime, timezone
import boto3

REGION = "us-east-1"; BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/brain.json"
BRAIN_URL = "https://justhodl-data-proxy.raafouis.workers.dev/brain"
MODEL = "claude-haiku-4-5-20251001"
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_KEY", "") or os.environ.get("ANTHROPIC_API_KEY", "")
s3 = boto3.client("s3", region_name=REGION)

CAT_LABEL = {"philosophy": "Philosophy", "rule": "Rule", "thesis": "Thesis",
             "macro": "Macro View", "watchlist": "Watchlist", "lesson": "Lesson",
             "reminder": "Reminder"}


def _ai_extract(notes_text):
    """Have Claude READ the brain and produce a structured directive layer the
    engines can act on — real understanding, not keyword matching. Returns a dict
    with sector tilts, themes, hard rules, avoid-list, watched tickers, risk
    posture, and a one-paragraph 'investor profile'. Cached; only re-runs when
    the brain content hash changes (handled by caller)."""
    if not ANTHROPIC_KEY or not notes_text.strip():
        return None
    system = (
        "You are distilling an investor's personal notes into a STRUCTURED PROFILE that "
        "automated trading-signal engines will use to bias toward what this investor cares "
        "about. Read their notes and extract their actual investing worldview. "
        "Return STRICT JSON only, no prose: {"
        "\"investor_profile\": \"<2-3 sentence summary of how they think about markets>\", "
        "\"hard_rules\": [\"<imperative rules they live by, e.g. 'Do not treat QT ending as bullish'>\"], "
        "\"sector_tilts\": {\"<sector>\": \"<overweight|underweight|avoid> — <why>\"}, "
        "\"themes\": [\"<themes they're focused on, e.g. 'AI capex buildout', 'energy scarcity'>\"], "
        "\"watched_tickers\": [\"<tickers they explicitly track>\"], "
        "\"avoid\": [\"<things/sectors/setups they want to avoid>\"], "
        "\"risk_posture\": \"<aggressive|balanced|defensive> — <one line>\", "
        "\"signal_emphasis\": [\"<which signal types matter most to them: e.g. insider, buyback, capex, dislocation, macro>\"]}"
    )
    try:
        body = {"model": MODEL, "max_tokens": 900, "system": system,
                "messages": [{"role": "user", "content": "INVESTOR NOTES:\n" + notes_text[:9000]}]}
        req = urllib.request.Request("https://api.anthropic.com/v1/messages",
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json", "x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01"})
        r = json.loads(urllib.request.urlopen(req, timeout=30).read().decode())
        txt = "".join(b.get("text", "") for b in r.get("content", []) if b.get("type") == "text")
        import re as _re
        txt = _re.sub(r"^```(?:json)?\s*|\s*```$", "", txt.strip())
        return json.loads(txt)
    except Exception as e:
        print(f"[brain-sync] AI extract err: {str(e)[:80]}")
        return None


def read_prev():
    """Previous brain.json output (for hash-gating the AI extraction)."""
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=OUT_KEY)["Body"].read())
    except Exception:
        return {}


def read_json(key, default=None):
    """Read any S3 JSON file (used by the regime-read to pull live macro engines)."""
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return default or {}


def read_history():
    try:
        return (json.loads(s3.get_object(Bucket=BUCKET, Key="data/brain-history.json")["Body"].read()) or {}).get("history", [])
    except Exception:
        return []


def _regime_read(notes_text, regimes):
    """Top AI read: given the user's notes + the live regime, say where we are
    relative to THEIR thinking, and flag which of their notes to double-check
    (because conditions may have changed or a thesis may be playing out/breaking)."""
    if not ANTHROPIC_KEY or not notes_text.strip():
        return None
    system = (
        "You are the user's personal macro strategist & CIO. You've studied THEIR investing notes "
        "(their philosophy, frameworks, heroes, hard rules) and you see the LIVE market regime data. "
        "Speak in THEIR voice and framework. Return STRICT JSON only: {"
        "\"regime\": \"<2-4 word name of the current regime, e.g. 'Liquidity-drain / late-cycle' or 'Stagflationary risk-off'>\", "
        "\"headline\": \"<one punchy sentence: what regime we're in right now and the single biggest thing it means>\", "
        "\"assessment\": \"<3-5 sentences: connect the live data (rates, dollar, liquidity/plumbing, credit, crypto) to "
        "THEIR specific frameworks and notes. Reference their own thinking by name where it applies.>\", "
        "\"risk_assets\": \"<2-3 sentences: what should happen to risk assets (equities, crypto, credit) in THIS regime, "
        "per their playbook — up/down/chop, and why>\", "
        "\"invest_in\": [\"<4-8 specific, actionable ideas: assets/sectors/themes/tickers to favor NOW given the regime + "
        "their philosophy — e.g. 'Gold & gold miners (currency-debasement hedge)', 'Long-duration Treasuries if growth rolls over', "
        "'Quality compounders with pricing power', 'BTC on dips — post-halving + debasement'. Be concrete.>\"], "
        "\"avoid\": [\"<2-4 things to avoid/reduce in this regime per their rules>\"], "
        "\"alignment\": \"<aligned|mixed|at-odds — is the current tape with or against how they're positioned/thinking?>\", "
        "\"recheck_notes\": [\"<verbatim-ish snippets of THEIR notes to revisit NOW because a thesis is triggering "
        "or conditions changed — max 4>\"]}"
        " Ground everything in their actual notes + the live data. Be decisive, not hedged. This is research, not advice."
    )
    try:
        body = {"model": MODEL, "max_tokens": 1500, "system": system,
                "messages": [{"role": "user", "content": f"LIVE MARKET REGIME DATA:\n{json.dumps(regimes, default=str)}\n\nTHEIR INVESTING NOTES (their worldview):\n{notes_text[:14000]}"}]}
        req = urllib.request.Request("https://api.anthropic.com/v1/messages",
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json", "x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01"})
        r = json.loads(urllib.request.urlopen(req, timeout=30).read().decode())
        txt = "".join(b.get("text", "") for b in r.get("content", []) if b.get("type") == "text")
        import re as _re
        return json.loads(_re.sub(r"^```(?:json)?\s*|\s*```$", "", txt.strip()))
    except Exception as e:
        print(f"[brain-sync] regime read err: {str(e)[:70]}")
        return None


def lambda_handler(event=None, context=None):
    t0 = time.time()
    try:
        req = urllib.request.Request(BRAIN_URL + "?sync=1&uid=brain-930ffa48-60a1-4b11-8726-8848d1b827f9", headers={"User-Agent": "JustHodl-BrainSync/1.0"})
        d = json.loads(urllib.request.urlopen(req, timeout=15).read().decode())
    except Exception as e:
        print(f"[brain-sync] fetch err: {str(e)[:80]}")
        return {"statusCode": 502, "body": "fetch failed"}

    notes = d.get("notes") or []
    # newest first; pinned float to top
    notes = sorted(notes, key=lambda n: (0 if n.get("pinned") else 1, -(n.get("created") or 0)))

    # Pre-format a compact prompt block the AI engines can inject directly.
    pinned = [n for n in notes if n.get("pinned")]
    lines = []
    if pinned:
        lines.append("PINNED PRINCIPLES (highest priority — always honor these):")
        for n in pinned:
            lines.append(f"  • [{CAT_LABEL.get(n.get('cat'), n.get('cat'))}] {n.get('text','').strip()}")
        lines.append("")
    by_cat = {}
    for n in notes:
        if n.get("pinned"):
            continue
        by_cat.setdefault(n.get("cat", "reminder"), []).append(n)
    for cat in ["philosophy", "rule", "thesis", "macro", "watchlist", "lesson", "reminder"]:
        items = by_cat.get(cat)
        if items:
            lines.append(f"{CAT_LABEL.get(cat, cat).upper()}:")
            for n in items[:12]:
                lines.append(f"  • {n.get('text','').strip()}")
            lines.append("")
    prompt_block = "\n".join(lines).strip()

    # Extract watchlist-ish tickers mentioned (UPPERCASE 1-5 char tokens) for
    # engines that want to bias toward what Khalid is watching.
    import re
    tickers = set()
    for n in notes:
        for m in re.findall(r"\b[A-Z]{1,5}\b", n.get("text", "")):
            if m not in {"QT", "QE", "RRP", "SOFR", "IORB", "EFFR", "TGA", "AI", "USD", "FED", "CPI", "PPI", "ETF", "I", "A", "US", "GDP", "BTC", "DCF"}:
                tickers.add(m)

    # ── SMART layer: AI-extracted structured directive (hash-gated so we only
    # spend a Claude call when the brain actually changes) ──
    import hashlib
    # 16k notes is far more than the AI needs (and too big to process). Use pinned
    # notes (always) + the most recent ~400 for the worldview/regime distillation.
    _pinned_notes = [n for n in notes if n.get("pinned")]
    _recent_notes = [n for n in notes if not n.get("pinned")][:400]
    _ai_notes = _pinned_notes + _recent_notes
    all_text = "\n".join(f"[{'PINNED ' if n.get('pinned') else ''}{n.get('cat')}] {n.get('text','')}" for n in _ai_notes)
    content_hash = hashlib.sha256(all_text.encode()).hexdigest()[:16]
    directive = None
    prev = read_prev()
    directive_changed = False
    if notes and (prev.get("content_hash") != content_hash or not prev.get("directive")):
        directive = _ai_extract(all_text)
        directive_changed = bool(directive)
    else:
        directive = prev.get("directive")  # reuse — content unchanged

    # Top AI regime read vs the user's notes (refresh when notes change OR every
    # run if absent — regimes shift even when notes don't, but cap cost by only
    # recomputing on content change or if missing). We refresh it each run since
    # the live regime moves; it's one cheap call.
    regime_read = None
    if notes:
        # Pull live macro series so the regime-read reasons from real data, not vibes.
        def _fred_latest(series):
            try:
                u = f"https://api.stlouisfed.org/fred/series/observations?series_id={series}&api_key=2f057499936072679d8843d7fce99989&file_type=json&sort_order=desc&limit=1"
                d = json.loads(urllib.request.urlopen(u, timeout=8).read().decode())
                return float(d["observations"][0]["value"])
            except Exception:
                return None
        regimes = {
            "bond_vol": (read_json("data/bond-vol.json") or {}).get("regime"),
            "funding_plumbing": {k: (read_json("data/funding-plumbing.json") or {}).get(k) for k in ["regime", "balance_sheet_direction", "qt_ended_not_qe"]},
            "crypto_dump_risk": {k: (read_json("data/crypto-cycle-risk.json") or {}).get(k) for k in ["risk_level", "dump_risk_score"]},
            "us10y_yield": _fred_latest("DGS10"),
            "us2y_yield": _fred_latest("DGS2"),
            "real_10y_yield_TIPS": _fred_latest("DFII10"),
            "10y_breakeven_inflation": _fred_latest("T10YIE"),
            "dollar_index_DTWEXBGS": _fred_latest("DTWEXBGS"),
            "hy_credit_spread_OAS": _fred_latest("BAMLH0A0HYM2"),
            "fed_funds_rate": _fred_latest("DFF"),
            "as_of": datetime.now(timezone.utc).date().isoformat(),
        }
        regime_read = _regime_read(all_text, regimes)

    # ── TRACK: keep a history of how the brain/worldview evolves over time, so
    # the system can show what changed and when, and prove it's applying the
    # latest thinking. Only appends a snapshot when the content actually changes. ──
    if directive_changed:
        try:
            hist = read_history()
            hist.append({
                "at": datetime.now(timezone.utc).isoformat(),
                "content_hash": content_hash,
                "n_notes": len(notes), "n_pinned": len(pinned),
                "investor_profile": (directive or {}).get("investor_profile"),
                "hard_rules": (directive or {}).get("hard_rules"),
                "themes": (directive or {}).get("themes"),
                "sector_tilts": (directive or {}).get("sector_tilts"),
                "risk_posture": (directive or {}).get("risk_posture"),
            })
            hist = hist[-200:]
            s3.put_object(Bucket=BUCKET, Key="data/brain-history.json",
                          Body=json.dumps({"history": hist}, default=str).encode(),
                          ContentType="application/json")
        except Exception as e:
            print(f"[brain-sync] history write err: {str(e)[:60]}")

    out = {
        "engine": "brain-sync", "version": "2.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_updated_at": d.get("updated_at"),
        "content_hash": content_hash,
        "n_notes": len(notes), "n_pinned": len(pinned),
        "notes": notes,
        "prompt_block": prompt_block,
        "directive": directive,                # ← the smart, structured worldview
        "directive_changed_this_run": directive_changed,
        "applied_by": ["morning-intelligence", "ask", "best-setups (brain-aligned)",
                       "devils-advocate (rule checks)", "my-brief", "position-sizer (risk posture)"],
        "mentioned_tickers": sorted(tickers | set((directive or {}).get("watched_tickers", []) or [])),
        "categories": {CAT_LABEL.get(k, k): len(v) for k, v in by_cat.items()},
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=300")
    print(f"[brain-sync] DONE {round(time.time()-t0,1)}s — {len(notes)} notes ({len(pinned)} pinned), "
          f"directive={'fresh' if (directive and prev.get('content_hash') != content_hash) else 'cached' if directive else 'none'}")
    return {"statusCode": 200, "body": json.dumps({"n_notes": len(notes), "n_pinned": len(pinned)})}
