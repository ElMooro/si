"""justhodl-finviz-news — Finviz ticker-tagged news + blogs → data/finviz-news.json
Builds a recent headline feed, a per-ticker index, and a 'news velocity' top-mentioned list
(useful for corroboration with retail/signals desks). 4x/trading day."""
import json, time
from collections import defaultdict, Counter
from datetime import datetime, timezone
import boto3
import finviz as FV

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")


def lambda_handler(event=None, context=None):
    try:
        news = FV.fetch_news(3)
    except Exception as e:
        news = []; print("news fail", str(e)[:60])
    time.sleep(3)
    try:
        blogs = FV.fetch_news(4)
    except Exception as e:
        blogs = []; print("blogs fail", str(e)[:60])

    by_ticker = defaultdict(list)
    for it in news:
        if it["ticker"]:
            by_ticker[it["ticker"]].append({"title": it["title"], "source": it["source"],
                                             "date": it["date"], "url": it["url"]})
    cnt = Counter({tk: len(v) for tk, v in by_ticker.items()})
    top = [{"ticker": tk, "n": n, "latest": by_ticker[tk][0]["title"]} for tk, n in cnt.most_common(40)]

    doc = {"generated_at": datetime.now(timezone.utc).isoformat(), "source": "finviz",
           "n_news": len(news), "n_blogs": len(blogs),
           "n_tickers": len(by_ticker),
           "top_tickers": top,
           "news": news[:120], "blogs": blogs[:60],
           "by_ticker": {tk: v[:5] for tk, v in by_ticker.items()}}
    s3.put_object(Bucket=BUCKET, Key="data/finviz-news.json",
                  Body=json.dumps(doc, separators=(",", ":"), default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=600")
    print("wrote finviz-news.json | news=%d blogs=%d tickers=%d" % (len(news), len(blogs), len(by_ticker)))
    return {"ok": True, "n_news": len(news), "n_blogs": len(blogs)}
