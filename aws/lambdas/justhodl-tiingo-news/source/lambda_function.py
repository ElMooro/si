"""justhodl-tiingo-news v1.0 — the fleet gains eyes for news.
Per-ticker article velocity (24h/7d + burst vs trail) from Tiingo's
tagged news API across the fabric's top names. Feature source only:
no direction claims; the bus carries it, engines decide.
Output: data/tiingo-news.json"""
import json, os, time, urllib.request
from datetime import datetime, timezone, timedelta

import boto3

s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"
TOK = os.environ.get("TIINGO_API_KEY", "")


def rd(key):
    try:
        return json.loads(s3.get_object(Bucket=B, Key=key)
                          ["Body"].read())
    except Exception:
        return None


def fetch(batch):
    url = ("https://api.tiingo.com/tiingo/news?tickers=%s"
           "&limit=1000&sortBy=publishedDate&token=%s"
           % (",".join(t.lower() for t in batch), TOK))
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "justhodl/1.0"})
        return json.loads(urllib.request.urlopen(
            req, timeout=25).read().decode())
    except Exception as e:
        print("[tiingo] batch fail: %s" % str(e)[:80])
        return []


def lambda_handler(event=None, context=None):
    t0 = time.time()
    fab = rd("data/signal-fabric.json") or {}
    syms = [t["ticker"]
            for t in (fab.get("tickers") or [])[:120]]
    now = datetime.now(timezone.utc)
    c24 = now - timedelta(hours=24)
    c7 = now - timedelta(days=7)
    by = {s0: {"n_24h": 0, "n_7d": 0, "latest_title": None,
               "latest_at": None} for s0 in syms}
    for i in range(0, len(syms), 20):
        batch = syms[i:i + 20]
        arts = fetch(batch)
        for a in arts if isinstance(arts, list) else []:
            try:
                pd = datetime.fromisoformat(
                    str(a.get("publishedDate", ""))
                    .replace("Z", "+00:00"))
            except Exception:
                continue
            if pd < c7:
                continue
            for tk in (a.get("tickers") or []):
                tk = str(tk).upper()
                r0 = by.get(tk)
                if not r0:
                    continue
                r0["n_7d"] += 1
                if pd >= c24:
                    r0["n_24h"] += 1
                if not r0["latest_at"] \
                        or str(pd) > r0["latest_at"]:
                    r0["latest_at"] = str(pd)[:16]
                    r0["latest_title"] = str(
                        a.get("title", ""))[:110]
        time.sleep(0.4)
    vals = [r0["n_7d"] for r0 in by.values()]
    mean7 = (sum(vals) / len(vals)) if vals else 0
    for r0 in by.values():
        r0["burst"] = (round(r0["n_24h"] * 7.0
                             / max(1.0, r0["n_7d"]), 2)
                       if r0["n_7d"] else None)
        r0["rel_7d"] = (round(r0["n_7d"] / mean7, 2)
                        if mean7 else None)
    covered = sum(1 for r0 in by.values() if r0["n_7d"] > 0)
    out = {"engine": "justhodl-tiingo-news", "version": "1.0",
           "generated_at": now.isoformat(),
           "elapsed_s": round(time.time() - t0, 1),
           "source": "Tiingo tagged news API",
           "n_universe": len(syms), "n_covered": covered,
           "mean_7d": round(mean7, 1),
           "by_ticker": by}
    s3.put_object(Bucket=B, Key="data/tiingo-news.json",
                  Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="no-cache")
    print(json.dumps({"ok": True, "covered": covered,
                      "universe": len(syms),
                      "tok_len": len(TOK)}))
    return {"ok": True}
