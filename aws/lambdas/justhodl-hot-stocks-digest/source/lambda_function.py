"""
justhodl-hot-stocks-digest  ·  v1.0  —  RETAIL FLOW & SENTIMENT MORNING BRIEF
================================================================================
Your personal investment-research analyst. Fuses the monitoring layer you already
built — retail-sentiment (Reddit/ApeWisdom/StockTwits mentions, velocity, rank
climbers, bull/bear), stocktwits trending, buzz-velocity, news-wire, gdelt — into
ONE institutional morning brief: what's heating up, why, the bull case (good news),
the bear case (bad news), and the net read per name. Plus crowding/exhaustion
warnings so it doesn't just chase pumps.

Design (institutional):
  • HEAT SCORE per name = mention velocity + rank-climb + trending + buzz, deduped
    across venues (multi-venue confirmation ranks higher than a single-forum spike).
  • NEWS join: news-wire / gdelt headlines mapped to each hot ticker, split into
    good (positive sentiment) vs bad (negative) — the literal good-news/bad-news ask.
  • ONE consolidated GLM pass writes the analyst narrative (market read + per-name
    bull/bear/net) — 1 call not 12, so it's reliable under Z.ai rate limits. Robust
    parse + graceful fallback: if the LLM is down, the structured brief still ships.
  • Delivery: data/hot-stocks-digest.json (page) + an HTML morning email via SES.

Honest scope: covers Reddit + StockTwits + news. X/Twitter is NOT included (paid
API). This reads the crowd; it is research, not a buy list.
"""
import json, time, urllib.request
from datetime import datetime, timezone
import boto3

VERSION = "1.0"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/hot-stocks-digest.json"
SES_SENDER = "reports@justhodl.ai"
SES_RECIPIENT = "reports@justhodl.ai"   # set env SES_DIGEST_TO to your gmail once SES-verified
s3 = boto3.client("s3", "us-east-1")

TOP_N = 12


def _read(key, default=None):
    try: return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception: return default

def _parse_json(txt):
    if not txt: return {}
    import re
    t = txt.strip()
    if "```" in t:
        m = re.search(r"```(?:json)?\s*(.*?)```", t, re.S)
        if m: t = m.group(1).strip()
    a, b = t.find("{"), t.rfind("}")
    if a >= 0 and b > a:
        try: return json.loads(t[a:b + 1])
        except Exception: return {}
    return {}

def _tk(x):
    if isinstance(x, str): return x.strip().upper()
    if isinstance(x, dict): return (x.get("ticker") or x.get("symbol") or "").upper()
    return ""

def _d(x):
    """Normalize a feed item to a dict — some feeds list bare ticker strings."""
    if isinstance(x, dict): return x
    if isinstance(x, str): return {"ticker": x}
    return {}


def build_hot_list(retail, stwt, buzz):
    """Composite heat across venues. Multi-venue confirmation ranks higher."""
    heat = {}   # ticker -> {score, mentions, velocity, rank_climb, bull_pct, venues, why[]}
    def bump(tk, pts, **kw):
        if not tk: return
        h = heat.setdefault(tk, {"ticker": tk, "score": 0.0, "venues": set(), "why": [],
                                  "bull_pct": None, "mentions": None, "velocity": None, "rank_climb": None})
        h["score"] += pts
        for k, v in kw.items():
            if v is not None and h.get(k) in (None, 0): h[k] = v

    ranked = (retail or {}).get("ranked") or {}
    for i, r in enumerate((ranked.get("hottest") or [])[:25]):
        r = _d(r); tk = _tk(r)
        bump(tk, 30 - i*0.8, mentions=r.get("mentions"), velocity=r.get("mention_velocity") or r.get("velocity"),
             bull_pct=r.get("bull_pct"))
        if tk in heat: heat[tk]["venues"].add("reddit/stwt")
    for r in (ranked.get("biggest_velocity_surges") or [])[:15]:
        r = _d(r); tk = _tk(r); bump(tk, 18, velocity=r.get("mention_velocity") or r.get("velocity"))
        if tk in heat: heat[tk]["why"].append("mention velocity surging"); heat[tk]["venues"].add("velocity")
    for r in (ranked.get("biggest_rank_climbers") or [])[:15]:
        r = _d(r); tk = _tk(r); bump(tk, 16, rank_climb=r.get("rank_velocity") or r.get("rank_delta"))
        if tk in heat: heat[tk]["why"].append("climbing the mention ranks fast"); heat[tk]["venues"].add("climber")
    for r in (ranked.get("igniting") or [])[:12]:
        r = _d(r); tk = _tk(r); bump(tk, 14)
        if tk in heat: heat[tk]["why"].append("igniting (early)"); heat[tk]["venues"].add("igniting")
    for r in (ranked.get("most_bullish_stwt") or [])[:12]:
        r = _d(r); tk = _tk(r); bump(tk, 8, bull_pct=r.get("bull_pct"))
        if tk in heat: heat[tk]["venues"].add("stwt-bull")
    for i, r in enumerate(((stwt or {}).get("trending_equities") or [])[:20]):
        r = _d(r); tk = _tk(r); bump(tk, 12 - i*0.4, bull_pct=r.get("bull_pct"))
        if tk in heat: heat[tk]["venues"].add("stwt-trending")
    for r in ((buzz or {}).get("all_results") or [])[:20]:
        r = _d(r); tk = _tk(r); bump(tk, min(10, (r.get("velocity") or r.get("score") or 0)))
        if tk in heat: heat[tk]["venues"].add("buzz")

    # multi-venue confirmation bonus
    for tk, h in heat.items():
        h["venue_count"] = len(h["venues"]); h["venues"] = sorted(h["venues"])
        if h["venue_count"] >= 3: h["score"] += 12; h["why"].append("confirmed across %d venues" % h["venue_count"])
    out = sorted(heat.values(), key=lambda x: -x["score"])
    return out[:TOP_N]


def news_by_ticker(newswire, gdelt):
    nbt = {}
    items = (newswire or {}).get("scored_headlines") or (newswire or {}).get("items") or \
            (newswire or {}).get("headlines") or []
    for it in items:
        it = _d(it)
        tks = it.get("tickers") or ([it.get("fmp_symbol_hint")] if it.get("fmp_symbol_hint") else []) or \
              ([it.get("symbol")] if it.get("symbol") else [])
        sent = it.get("sentiment")
        if isinstance(sent, str):
            sent = {"positive": 1, "bullish": 1, "negative": -1, "bearish": -1}.get(sent.lower(), 0)
        rec = {"title": it.get("title") or it.get("summary") or it.get("headline"),
               "summary": it.get("summary"), "url": it.get("url"),
               "sentiment": sent if isinstance(sent, (int, float)) else 0,
               "source": it.get("origin") or it.get("source")}
        for tk in tks:
            if tk and isinstance(tk, str): nbt.setdefault(tk.upper(), []).append(rec)
    return nbt


def analyst_pass(market_ctx, hot, nbt):
    """ONE GLM call -> market read + per-name bull/bear/net. Graceful if LLM down."""
    try:
        from llm_router import complete
    except Exception:
        return {}
    lines = []
    for h in hot:
        nws = nbt.get(h["ticker"], [])
        good = [n["title"] for n in nws if (n.get("sentiment") or 0) > 0][:2]
        bad = [n["title"] for n in nws if (n.get("sentiment") or 0) < 0][:2]
        lines.append("%s | heat %.0f, %d venues, bull%% %s, vel %s | good_news: %s | bad_news: %s" % (
            h["ticker"], h["score"], h.get("venue_count", 0), h.get("bull_pct"), h.get("velocity"),
            " ; ".join(good) or "none surfaced", " ; ".join(bad) or "none surfaced"))
    sys = ("You are a sell-side desk analyst writing a punchy, candid morning retail-flow brief for a PM. "
           "Specific and concrete; name the catalyst; never hype. For each ticker give the bull case (why the crowd "
           "is on it), the bear case (the risk / what could go wrong), and a one-line NET read. Crowded/parabolic "
           "names should be flagged as crowded, not cheered. Return ONLY JSON:\n"
           '{"market_read":"2-3 sentences on overall retail risk appetite right now",'
           '"briefs":[{"ticker":"X","why_hot":"<12w>","bull":"<20w>","bear":"<20w>","net":"<10w, e.g. crowded long / early / fade>"}]}')
    prompt = "RETAIL SENTIMENT CONTEXT: %s\n\nHOT NAMES (heat, venues, sentiment, news):\n%s" % (
        market_ctx, "\n".join(lines))
    for _ in range(2):
        r = {}
        try:
            r = _parse_json(complete(prompt, tier="reason", max_tokens=3500, system=sys))
        except Exception:
            r = {}
        if r.get("briefs"):
            return r
    return {}


def render_email_html(out):
    rows = ""
    for h in out["hot_stocks"]:
        b = h.get("analyst") or {}
        net = (b.get("net") or "").upper()
        col = "#2e7d32" if "early" in net.lower() or "long" in net.lower() else "#b71c1c" if "fade" in net.lower() or "short" in net.lower() else "#555"
        rows += ("<tr><td style='padding:8px 6px;border-bottom:1px solid #eee'><b>%s</b><br>"
                 "<span style='color:#888;font-size:11px'>heat %.0f · %d venues · bull %s%%</span></td>"
                 "<td style='padding:8px 6px;border-bottom:1px solid #eee;font-size:12px'>%s"
                 "<br><span style='color:#2e7d32'>+ %s</span>"
                 "<br><span style='color:#b71c1c'>− %s</span>"
                 "<br><b style='color:%s'>NET: %s</b></td></tr>") % (
            h["ticker"], h["score"], h.get("venue_count", 0), h.get("bull_pct"),
            b.get("why_hot", h.get("why", [""])[0] if h.get("why") else ""), b.get("bull", "—"),
            b.get("bear", "—"), col, b.get("net", "—"))
    warn = ", ".join(w["ticker"] for w in out.get("warnings", [])[:8]) or "none"
    return ("""<div style="font-family:-apple-system,Segoe UI,Roboto,sans-serif;max-width:640px;margin:auto">
      <h2 style="margin:0">JustHodl · Retail Flow &amp; Sentiment Brief</h2>
      <div style="color:#888;font-size:12px">%s</div>
      <p style="font-size:13px;line-height:1.5"><b>Market read:</b> %s</p>
      <table style="width:100%%;border-collapse:collapse;font-size:13px">%s</table>
      <p style="font-size:12px;color:#b71c1c"><b>Crowded / fading (handle with care):</b> %s</p>
      <p style="font-size:11px;color:#aaa">Sources: Reddit/ApeWisdom + StockTwits + buzz + news-wire/GDELT. No X/Twitter (paid feed). Research, not investment advice.</p>
    </div>""") % (out["generated_at"][:16].replace("T", " ") + "Z", out.get("market_read", ""), rows, warn)


def send_email(out):
    import os
    to = os.getenv("SES_DIGEST_TO", SES_RECIPIENT)
    try:
        ses = boto3.client("ses", region_name="us-east-1")
        ses.send_email(Source=SES_SENDER, Destination={"ToAddresses": [to]},
                       Message={"Subject": {"Data": "Retail Flow Brief · %s · top: %s" % (
                           out["generated_at"][:10], ", ".join(h["ticker"] for h in out["hot_stocks"][:4]))},
                                "Body": {"Html": {"Data": render_email_html(out)}}})
        return to
    except Exception as e:
        print("[email] failed:", str(e)[:80]); return None


def lambda_handler(event, context):
    t0 = time.time()
    retail = _read("data/retail-sentiment.json") or {}
    stwt = _read("data/stocktwits.json") or {}
    buzz = _read("data/buzz-velocity.json") or {}
    newswire = _read("data/news-wire.json") or {}
    gdelt = _read("data/gdelt-buzz.json") or {}

    hot = build_hot_list(retail, stwt, buzz)
    nbt = news_by_ticker(newswire, gdelt)
    for h in hot:
        nws = nbt.get(h["ticker"], [])
        h["good_news"] = [{"title": n["title"], "url": n.get("url")} for n in nws if (n.get("sentiment") or 0) > 0][:3]
        h["bad_news"] = [{"title": n["title"], "url": n.get("url")} for n in nws if (n.get("sentiment") or 0) < 0][:3]

    rd = (retail or {}).get("market_regime_data") or {}
    market_ctx = "retail mentions %s vs prior (%s%% delta), regime %s" % (
        rd.get("total_mentions"), rd.get("delta_pct"), (retail or {}).get("regime"))
    ai = analyst_pass(market_ctx, hot, nbt)
    briefs = {b.get("ticker", "").upper(): b for b in (ai.get("briefs") or [])}
    for h in hot:
        h["analyst"] = briefs.get(h["ticker"])

    ranked = (retail or {}).get("ranked") or {}
    warnings = []
    for key, label in [("crowded_exhaustion", "crowded/exhausted"), ("fading", "fading"), ("peaking", "peaking")]:
        for r in (ranked.get(key) or [])[:5]:
            warnings.append({"ticker": _tk(r), "flag": label})

    out = {"engine": "hot-stocks-digest", "version": VERSION, "generated_at": datetime.now(timezone.utc).isoformat(),
           "duration_s": round(time.time() - t0, 1),
           "title": "Retail Flow & Sentiment Brief",
           "market_read": ai.get("market_read") or "Retail sentiment summary unavailable this run.",
           "market_context": market_ctx,
           "hot_stocks": hot, "warnings": warnings,
           "sources": ["Reddit/ApeWisdom", "StockTwits", "buzz-velocity", "news-wire", "GDELT"],
           "note": "Reads the retail crowd (Reddit + StockTwits + news). No X/Twitter (paid feed). Research, not advice.",
           "llm_narrative": bool(ai.get("briefs"))}
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache, max-age=0")
    emailed = send_email(out) if (event or {}).get("email", True) else None
    print("[hot-stocks-digest v%s] hot=%d narrative=%s emailed=%s top=%s" % (
        VERSION, len(hot), bool(ai.get("briefs")), emailed, [h["ticker"] for h in hot[:6]]))
    return {"statusCode": 200, "body": json.dumps({"hot": len(hot), "narrative": bool(ai.get("briefs")), "emailed": emailed})}
