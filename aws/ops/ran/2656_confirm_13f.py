"""ops 2656 — light follow-up: does institutional_13f literally appear in today's top-25
(it's plausibly hidden inside a '+N more' pill), and sanity-check the 29/30 health count."""
import urllib.request, json, time
def get(u): return urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"M"}),timeout=25).read().decode()
j = json.loads(get(f"https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/data/master-ranker.json?cb={int(time.time())}"))
tt = j.get("top_tickers") or []
hits = [(t["ticker"], t["score"]) for t in tt if "institutional_13f" in (t.get("systems") or [])]
print("tickers with institutional_13f active in today's top-25:", hits or "none today")
fh = j.get("feed_health") or {}
loaded = sum(1 for v in fh.values() if v.get("loaded"))
print(f"feed_health: {loaded}/{len(fh)} loaded (page shows 29/30 — matches: {loaded==29 and len(fh)==30})")
print("stale_feeds_excluded:", j.get("stale_feeds_excluded"))
print("DONE 2656")
