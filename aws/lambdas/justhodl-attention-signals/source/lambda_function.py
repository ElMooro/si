"""
justhodl-attention-signals — THE PRE-PUMP ATTENTION LAYER
=========================================================
Stocks pump when accumulation + attention + narrative line up before the crowd
piles in. This engine fuses three free sources into one per-ticker read:

  • INSIDER ACCUMULATION (Finnhub insider-sentiment MSPR + insider-transactions) —
    insiders buying their own stock is the classic informed-money pre-move tell.
  • ANALYST UPGRADE MOMENTUM (Finnhub recommendation trends) — buy-lean and its
    month-over-month change; rising = the re-rating is starting on the sell-side.
  • RETAIL ATTENTION (Stocktwits trending + per-symbol bullish/bearish + volume) —
    a surge in retail chatter front-runs small-cap pumps.
  • NARRATIVE MOMENTUM (GDELT tone timelines, theme level) — which AI-infra themes
    are getting more (and more positive) coverage.

Focused on the names that matter (re-rating candidates + small/mid AI-infra +
whatever is trending), so it stays inside Finnhub's free 60/min and GDELT's 1/5s.

OUTPUT data/attention-signals.json   SCHEDULE daily 14:45 UTC. Real data, research only.
"""
import json
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

VERSION = "1.0.0"
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/attention-signals.json"
FINNHUB = "d8qlt5pr01qrf6e278d0d8qlt5pr01qrf6e278dg"
UA = {"User-Agent": "Mozilla/5.0 jh-attention"}
MAX_TICKERS = 42
THEMES = ["AI data center", "HBM memory", "AI power grid", "nuclear power data center",
          "liquid cooling data center", "Bitcoin miner AI datacenter", "GPU cloud",
          "AI capex", "optical transceiver AI", "semiconductor foundry"]
s3 = boto3.client("s3", region_name="us-east-1")


def _read(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def _get(url):
    try:
        r = urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=14)
        return r.getcode(), r.read().decode("utf-8", "ignore")
    except Exception:
        return 0, ""


def fh(path):
    c, b = _get(f"https://finnhub.io/api/v1/{path}{'&' if '?' in path else '?'}token={FINNHUB}")
    if c == 200 and b:
        try:
            return json.loads(b)
        except Exception:
            return None
    return None


def build_universe():
    syms, names = {}, {}
    rr = _read("data/ai-rerating-radar.json") or {}
    for r in ((rr.get("summary", {}) or {}).get("top_setups", []) or []):
        if r.get("symbol"):
            syms[r["symbol"]] = r.get("layer")
            names[r["symbol"]] = r.get("name")
    stk = _read("data/ai-infra-stack.json") or {}
    for layer in stk.get("stack", []):
        for n in layer.get("names", []):
            if n.get("symbol") and n.get("is_small_cap") and n["symbol"] not in syms:
                syms[n["symbol"]] = layer.get("layer")
                names[n["symbol"]] = n.get("name")
    return syms, names


def stocktwits_trending():
    c, b = _get("https://api.stocktwits.com/api/2/trending/symbols.json")
    out = []
    if c == 200 and b:
        try:
            out = [s.get("symbol") for s in json.loads(b).get("symbols", []) if s.get("symbol")]
        except Exception:
            pass
    return out


def stocktwits_sentiment(sym):
    c, b = _get(f"https://api.stocktwits.com/api/2/streams/symbol/{urllib.parse.quote(sym)}.json")
    if c != 200 or not b:
        return None
    try:
        msgs = json.loads(b).get("messages", [])
    except Exception:
        return None
    bull = bear = 0
    for m in msgs:
        s = ((m.get("entities", {}) or {}).get("sentiment") or {})
        v = (s or {}).get("basic")
        if v == "Bullish":
            bull += 1
        elif v == "Bearish":
            bear += 1
    tot = bull + bear
    return {"msgs": len(msgs), "bull": bull, "bear": bear,
            "bull_pct": round(bull / tot, 2) if tot else None}


def insider_mspr(sym):
    d = fh(f"stock/insider-sentiment?symbol={sym}&from={(datetime.now(timezone.utc)-timedelta(days=150)).date()}&to={datetime.now(timezone.utc).date()}")
    rows = (d or {}).get("data", []) if isinstance(d, dict) else []
    if not rows:
        return None, None
    rows = sorted(rows, key=lambda r: (r.get("year", 0), r.get("month", 0)))[-3:]
    mspr = [r.get("mspr") for r in rows if isinstance(r.get("mspr"), (int, float))]
    chg = sum(r.get("change", 0) for r in rows if isinstance(r.get("change"), (int, float)))
    return (round(sum(mspr) / len(mspr), 1) if mspr else None), chg


def analyst_trend(sym):
    d = fh(f"stock/recommendation?symbol={sym}")
    if not isinstance(d, list) or not d:
        return None, None
    d = sorted(d, key=lambda r: r.get("period", ""), reverse=True)
    def lean(r):
        tot = sum(r.get(k, 0) for k in ("strongBuy", "buy", "hold", "sell", "strongSell"))
        return ((r.get("strongBuy", 0) + r.get("buy", 0)) / tot) if tot else None
    cur = lean(d[0])
    prev = lean(d[1]) if len(d) > 1 else None
    mom = round(cur - prev, 3) if (cur is not None and prev is not None) else None
    return (round(cur, 2) if cur is not None else None), mom


def gdelt_theme(theme):
    c, b = _get(f"https://api.gdeltproject.org/api/v2/doc/doc?query={urllib.parse.quote('\"'+theme+'\"')}&mode=timelinetone&format=json&timespan=3months")
    if c != 200 or not b.strip().startswith("{"):
        return None
    try:
        series = json.loads(b).get("timeline", [])
        pts = series[0].get("data", []) if series else []
        vals = [p.get("value") for p in pts if isinstance(p.get("value"), (int, float))]
    except Exception:
        return None
    if len(vals) < 6:
        return None
    recent = sum(vals[-7:]) / len(vals[-7:])
    prior = sum(vals[-21:-7]) / len(vals[-21:-7]) if len(vals) >= 21 else sum(vals[:-7]) / max(1, len(vals[:-7]))
    return {"theme": theme, "tone_recent": round(recent, 2), "tone_prior": round(prior, 2),
            "tone_trend": round(recent - prior, 2)}


def lambda_handler(event, context):
    t0 = time.time()
    syms, names = build_universe()
    trending = stocktwits_trending()
    trend_set = set(trending)
    # include trending names that are in our universe at the front
    universe = list(syms.keys())[:MAX_TICKERS]

    # retail sentiment (threaded — Stocktwits has no hard key limit, stay polite)
    retail = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        fut = {ex.submit(stocktwits_sentiment, s): s for s in universe}
        for f in as_completed(fut):
            r = f.result()
            if r:
                retail[fut[f]] = r

    # Finnhub insider + analyst (SERIAL, ~1/s to respect free 60/min)
    insider, analyst = {}, {}
    for s in universe:
        m, chg = insider_mspr(s); time.sleep(1.05)
        insider[s] = {"mspr": m, "net_change": chg}
        a, mom = analyst_trend(s); time.sleep(1.05)
        analyst[s] = {"buy_pct": a, "upgrade_mom": mom}
        if time.time() - t0 > 230:
            break

    # GDELT theme narrative (SERIAL, 1 per 5.2s)
    theme_pulse = []
    for th in THEMES:
        if time.time() - t0 > 275:
            break
        g = gdelt_theme(th)
        if g:
            theme_pulse.append(g)
        time.sleep(5.2)
    theme_pulse.sort(key=lambda x: x["tone_trend"], reverse=True)

    # score each ticker
    rows = []
    for s in universe:
        ins = insider.get(s, {}); an = analyst.get(s, {}); rt = retail.get(s, {})
        score, why = 0.0, []
        mspr = ins.get("mspr")
        if mspr is not None:
            if mspr >= 30:
                score += 26; why.append(f"insiders buying (MSPR {mspr})")
            elif mspr > 5:
                score += 12; why.append(f"mild insider buying (MSPR {mspr})")
            elif mspr <= -30:
                score -= 12; why.append(f"insider selling (MSPR {mspr})")
        bp = an.get("buy_pct"); mom = an.get("upgrade_mom")
        if bp is not None and bp >= 0.7:
            score += 12; why.append(f"{int(bp*100)}% analyst buy-rated")
        if mom is not None and mom > 0.03:
            score += 16; why.append("analyst upgrades accelerating")
        elif mom is not None and mom < -0.03:
            score -= 8; why.append("analyst downgrades")
        bpct = rt.get("bull_pct"); msgs = rt.get("msgs") or 0
        if bpct is not None and bpct >= 0.65 and msgs >= 8:
            score += 14; why.append(f"retail {int(bpct*100)}% bullish ({msgs} msgs)")
        if s in trend_set:
            score += 18; why.append("trending on Stocktwits")
        rows.append({
            "symbol": s, "name": names.get(s), "layer": syms.get(s),
            "insider_mspr": mspr, "insider_net_change": ins.get("net_change"),
            "analyst_buy_pct": bp, "analyst_upgrade_mom": mom,
            "retail_bull_pct": bpct, "retail_msgs": msgs, "trending": s in trend_set,
            "attention_score": round(score, 1), "why": "; ".join(why),
        })
    rows.sort(key=lambda x: x["attention_score"], reverse=True)

    out = {
        "engine": "attention-signals", "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "thesis": "Fuse insider accumulation (Finnhub) + analyst upgrade momentum + retail attention "
                  "(Stocktwits) + theme narrative tone (GDELT) into a pre-pump attention read.",
        "n_tickers": len(rows), "n_with_insider": sum(1 for r in rows if r["insider_mspr"] is not None),
        "stocktwits_trending": trending[:25],
        "top_attention": rows[:25],
        "insider_buying": [r for r in rows if (r["insider_mspr"] or 0) >= 30][:15],
        "analyst_upgrading": [r for r in rows if (r["analyst_upgrade_mom"] or 0) > 0.03][:15],
        "theme_pulse": theme_pulse,
        "tickers": rows,
        "sources": ["Finnhub insider-sentiment + recommendation (free)", "Stocktwits (free)", "GDELT tone (free)"],
        "caveats": "Insider MSPR & analyst trends lag filings; retail attention can be noise or a pump-in-progress "
                   "(late, not early); GDELT tone is theme-level sentiment, not a price signal. Confirmation layer, "
                   "not a standalone trigger. Real data, research only — not investment advice.",
        "elapsed_s": round(time.time() - t0, 2),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=json.dumps(out).encode(), ContentType="application/json")
    print(f"[attention] tickers={len(rows)} insider={out['n_with_insider']} trending={len(trending)} "
          f"themes={len(theme_pulse)} {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "tickers": len(rows),
            "trending": len(trending), "themes": len(theme_pulse)})}
