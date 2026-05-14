#!/usr/bin/env python3
"""528 — Inspect StockTwits message-level sentiment field structure."""
import io, json, os, time as _time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/528_stocktwits_inspect.json"
lam = boto3.client("lambda", region_name="us-east-1")

PROBE_CODE = r"""
import json, urllib.request, time

def fetch(url):
    req = urllib.request.Request(url, headers={'User-Agent':'Mozilla/5.0'})
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read())

def lambda_handler(event, context):
    out = {}

    # Get NVDA stream and inspect message structure
    nvda = fetch("https://api.stocktwits.com/api/2/streams/symbol/NVDA.json")
    msgs = nvda.get("messages") or []
    out["nvda_n_messages"] = len(msgs)

    # Look at the sentiment fields in messages
    bull, bear, none_ = 0, 0, 0
    sample_msgs = []
    for m in msgs:
        ent = m.get("entities") or {}
        sent = ent.get("sentiment") or {}
        basic = sent.get("basic") or m.get("sentiment") or "none"
        if basic == "Bullish": bull += 1
        elif basic == "Bearish": bear += 1
        else: none_ += 1
        if len(sample_msgs) < 5:
            sample_msgs.append({
                "id": m.get("id"),
                "created": m.get("created_at"),
                "user_followers": (m.get("user") or {}).get("followers"),
                "body_snippet": (m.get("body") or "")[:200],
                "entities_sentiment": ent.get("sentiment"),
                "msg_sentiment": m.get("sentiment"),
                "symbols": [s.get("symbol") for s in (m.get("symbols") or [])[:5]],
                "likes": (m.get("likes") or {}).get("total"),
            })
    out["nvda_sentiment_counts"] = {"bullish": bull, "bearish": bear, "no_sentiment": none_}
    out["nvda_sample_messages"] = sample_msgs

    # Trending list with full structure
    tr = fetch("https://api.stocktwits.com/api/2/trending/symbols.json")
    syms = tr.get("symbols") or []
    out["trending_count"] = len(syms)
    out["trending_top_10"] = [
        {
            "symbol": s.get("symbol"),
            "title": s.get("title"),
            "trending_score": s.get("trending_score"),
            "watchlist_count": s.get("watchlist_count"),
            "sector": s.get("sector"),
            "industry": s.get("industry"),
        } for s in syms[:10]
    ]

    # Apewisdom paginated — get pages 1 & 2 to see top 100
    aw_p1 = fetch("https://apewisdom.io/api/v1.0/filter/all-stocks/page/1")
    aw_p2 = fetch("https://apewisdom.io/api/v1.0/filter/all-stocks/page/2")
    combined = (aw_p1.get("results") or []) + (aw_p2.get("results") or [])
    out["apewisdom_top_30"] = [
        {
            "rank": r.get("rank"),
            "ticker": r.get("ticker"),
            "name": r.get("name"),
            "mentions": r.get("mentions"),
            "mentions_24h_ago": r.get("mentions_24h_ago"),
            "rank_24h_ago": r.get("rank_24h_ago"),
            "upvotes": r.get("upvotes"),
            "sentiment_score": r.get("sentiment_score"),  # check if exists
            "sentiment": r.get("sentiment"),
        } for r in combined[:30]
    ]
    out["apewisdom_p1_total"] = aw_p1.get("count")
    out["apewisdom_pages"] = aw_p1.get("pages")

    return {'statusCode': 200, 'body': json.dumps(out, default=str)}
"""


def zip_str(s):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", s)
    return buf.getvalue()


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    NAME = "justhodl-tmp-stwt-inspect"
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler",
            Role="arn:aws:iam::857687956942:role/lambda-execution-role",
            MemorySize=256, Timeout=60, Code={"ZipFile": zip_str(PROBE_CODE)})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except lam.exceptions.ResourceConflictException:
        lam.update_function_code(FunctionName=NAME, ZipFile=zip_str(PROBE_CODE))
        lam.get_waiter("function_updated").wait(FunctionName=NAME)

    _time.sleep(2)
    try:
        r = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
        body = r["Payload"].read().decode("utf-8")
        try:
            p = json.loads(body)
            out["result"] = json.loads(p["body"]) if p.get("body") else p
        except: out["raw"] = body[:3000]
    except Exception as e:
        out["err"] = str(e)[:400]

    try: lam.delete_function(FunctionName=NAME)
    except: pass

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
