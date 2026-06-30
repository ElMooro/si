"""
justhodl-search-attention — PER-COMPANY ATTENTION via Wikipedia pageview velocity
=================================================================================
The canonical "search attention" proxy. Da/Engelberg/Gao established Google search
volume as a direct measure of (mostly retail) investor attention that leads price
~2 weeks then mean-reverts. Google Trends has no real API and rate-limits hard, so
this engine uses the reliable, free Wikimedia REST pageviews API per company page —
which hedge-fund alt-data research explicitly endorses as a high-quality attention
signal whose unusual spikes often precede price moves.

Per ticker it computes:
  • views_recent   — mean daily pageviews, last 7d
  • views_baseline — mean daily pageviews, prior 21d
  • trend_pct      — (recent - baseline) / baseline * 100   (attention acceleration)
  • svi            — recent vs the page's own 60-day max, 0-100 (spike-vs-own-norm)

Ticker -> Wikipedia title resolved via the Wikimedia search API and cached in
data/search-attention-titlemap.json to avoid re-resolving daily.

OUTPUT  data/search-attention.json     SCHEDULE  daily 15:00 UTC (before confluence 15:10)
Real data only. Research, not investment advice.
"""
import json, os, time, datetime, urllib.parse, urllib.request

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/search-attention.json"
MAP_KEY = "data/search-attention-titlemap.json"
UA = {"User-Agent": "JustHodl/1.0 (research@justhodl.ai) attention-research"}
s3 = boto3.client("s3")


def _read(key, default=None):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return {} if default is None else default


def _get(url, timeout=15):
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8", "ignore"))
    except Exception:
        return None


def resolve_title(name, ticker):
    """Find the best en.wikipedia article title for a company."""
    q = name or ticker
    if not q:
        return None
    url = ("https://en.wikipedia.org/w/api.php?action=query&list=search&format=json"
           "&srlimit=1&srsearch=" + urllib.parse.quote(q + " (company OR stock OR Inc)"))
    j = _get(url)
    try:
        hits = j["query"]["search"]
        if hits:
            return hits[0]["title"]
    except Exception:
        pass
    # fallback: plain name
    url2 = ("https://en.wikipedia.org/w/api.php?action=query&list=search&format=json"
            "&srlimit=1&srsearch=" + urllib.parse.quote(q))
    j2 = _get(url2)
    try:
        return j2["query"]["search"][0]["title"]
    except Exception:
        return None


def pageviews(title, start, end):
    t = urllib.parse.quote(title.replace(" ", "_"), safe="")
    url = (f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
           f"en.wikipedia/all-access/all-agents/{t}/daily/{start}/{end}")
    j = _get(url)
    try:
        return [it["views"] for it in j["items"]]
    except Exception:
        return []


def lambda_handler(event=None, context=None):
    conf = _read("data/attention-confluence.json")
    tickers = conf.get("tickers", {}) if isinstance(conf, dict) else {}
    if not tickers:
        # fall back to attention-signals universe
        att = _read("data/attention-signals.json")
        tickers = {r.get("symbol"): {"name": r.get("name")} for r in att.get("tickers", []) if r.get("symbol")}

    title_map = _read(MAP_KEY, {})  # {TICKER: wiki_title or null}
    if not isinstance(title_map, dict):
        title_map = {}

    today = datetime.date.today()
    end = today.strftime("%Y%m%d")
    start = (today - datetime.timedelta(days=35)).strftime("%Y%m%d")

    out = {}
    resolved_new = 0
    fetched = 0
    for sym, meta in tickers.items():
        sym = (sym or "").upper()
        if not sym:
            continue
        name = (meta or {}).get("name") or sym
        # resolve + cache title
        if sym not in title_map:
            title_map[sym] = resolve_title(name, sym)
            resolved_new += 1
            time.sleep(0.12)  # polite
        title = title_map.get(sym)
        if not title:
            out[sym] = {"found": False, "wiki_title": None}
            continue
        views = pageviews(title, start, end)
        fetched += 1
        if fetched % 25 == 0:
            time.sleep(0.3)
        if len(views) < 14:
            out[sym] = {"found": True, "wiki_title": title, "views_recent": None,
                        "trend_pct": None, "svi": None, "n_days": len(views)}
            continue
        recent = views[-7:]
        baseline = views[-28:-7] if len(views) >= 28 else views[:-7]
        vr = sum(recent) / len(recent) if recent else 0.0
        vb = sum(baseline) / len(baseline) if baseline else 0.0
        trend = round((vr - vb) / vb * 100, 1) if vb > 0 else (None if vr == 0 else 100.0)
        pmax = max(views) if views else 0
        svi = round(vr / pmax * 100, 1) if pmax > 0 else 0.0
        out[sym] = {"found": True, "wiki_title": title,
                    "views_recent": round(vr, 1), "views_baseline": round(vb, 1),
                    "trend_pct": trend, "svi": svi, "n_days": len(views)}

    # persist title map (cache) + output
    s3.put_object(Bucket=S3_BUCKET, Key=MAP_KEY, Body=json.dumps(title_map).encode(),
                  ContentType="application/json")
    spikes = sorted([{"ticker": k, **v} for k, v in out.items()
                     if v.get("trend_pct") is not None],
                    key=lambda r: r["trend_pct"], reverse=True)[:30]
    result = {
        "engine": "search-attention",
        "version": "1.0.0",
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "thesis": ("Per-company Wikipedia pageview velocity as an investor-attention proxy "
                   "(Da/Engelberg/Gao style); recent 7d vs prior 21d baseline. Spikes lead "
                   "price ~2 weeks then mean-revert."),
        "n": len(out), "n_resolved_new": resolved_new, "n_with_data": sum(1 for v in out.values() if v.get("svi") is not None),
        "top_attention_spikes": spikes,
        "by_ticker": out,
        "source": "Wikimedia REST pageviews API (en.wikipedia, all-access, all-agents)",
        "caveats": "Attention is a short-term/leading signal that mean-reverts; research only.",
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=json.dumps(result).encode(),
                  ContentType="application/json", CacheControl="public, max-age=600")
    return {"ok": True, "n": len(out), "with_data": result["n_with_data"], "resolved_new": resolved_new}


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2)[:1200])
