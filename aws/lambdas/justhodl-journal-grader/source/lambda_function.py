"""justhodl-journal-grader — grades the user's Decision Journal against reality.

Each journal entry is a locked decision: {id, ticker, direction (bullish/bearish/
watch), thesis, horizon_days, entry_price, created}. This Lambda fetches current
prices, grades any entry past its horizon (HIT/MISS/PENDING) by whether the move
went the predicted way, and computes the user's PERSONAL track record — overall
hit rate, by-direction, by-ticker, and a calibration read. Writes a summary the
Brain + the site can show. This makes the system the only one that scores YOUR
judgment and holds you to your own rules.

SCHEDULE: daily 14:00 UTC.
"""
import json, time
import urllib.request, urllib.parse
from datetime import datetime, timezone
import boto3

REGION = "us-east-1"; BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/journal-graded.json"
JOURNAL_URL = "https://justhodl-data-proxy.raafouis.workers.dev/journal"
POLYGON_KEY = "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
s3 = boto3.client("s3", region_name=REGION)


def http_json(url, timeout=12):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
        return json.loads(urllib.request.urlopen(req, timeout=timeout).read().decode())
    except Exception:
        return None


def price_now(ticker):
    d = http_json(f"https://financialmodelingprep.com/stable/quote?symbol={ticker}&apikey={FMP_KEY}")
    if isinstance(d, list) and d:
        return d[0].get("price")
    return None


def lambda_handler(event=None, context=None):
    t0 = time.time()
    jd = http_json(JOURNAL_URL + "?g=1")
    entries = (jd or {}).get("entries") or []
    if not entries:
        out = {"engine": "journal-grader", "generated_at": datetime.now(timezone.utc).isoformat(),
               "n_entries": 0, "track_record": {}, "graded": [], "note": "No journal entries yet."}
        s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out).encode(), ContentType="application/json")
        return {"statusCode": 200, "body": "no entries"}

    now_ms = time.time() * 1000
    graded = []
    price_cache = {}
    for e in entries:
        tk = (e.get("ticker") or "").upper()
        direction = (e.get("direction") or "watch").lower()
        entry_px = e.get("entry_price")
        horizon = e.get("horizon_days") or 30
        created = e.get("created") or now_ms
        age_days = (now_ms - created) / 86400000
        g = dict(e)
        if not tk or entry_px in (None, 0):
            g["grade"] = "UNGRADED"; g["note"] = "no ticker/entry price"
            graded.append(g); continue
        if age_days < horizon:
            g["grade"] = "PENDING"; g["days_left"] = round(horizon - age_days, 1)
            graded.append(g); continue
        if tk not in price_cache:
            price_cache[tk] = price_now(tk)
        cur = price_cache[tk]
        if cur is None:
            g["grade"] = "PENDING"; g["note"] = "price unavailable"
            graded.append(g); continue
        ret = (cur / entry_px - 1) * 100
        g["return_pct"] = round(ret, 1); g["current_price"] = cur
        # grade by predicted direction
        if direction == "bullish":
            g["grade"] = "HIT" if ret > 2 else "MISS" if ret < -2 else "FLAT"
        elif direction == "bearish":
            g["grade"] = "HIT" if ret < -2 else "MISS" if ret > 2 else "FLAT"
        else:  # watch — graded as "would it have worked if bullish"
            g["grade"] = "WATCH"; g["watch_move"] = round(ret, 1)
        graded.append(g)

    decided = [g for g in graded if g.get("grade") in ("HIT", "MISS", "FLAT")]
    hits = [g for g in decided if g["grade"] == "HIT"]
    misses = [g for g in decided if g["grade"] == "MISS"]
    n_dec = len(decided)
    hit_rate = round(100 * len(hits) / n_dec, 1) if n_dec else None
    avg_ret_when_right = round(sum(g["return_pct"] for g in hits) / len(hits), 1) if hits else None
    avg_ret_when_wrong = round(sum(g["return_pct"] for g in misses) / len(misses), 1) if misses else None

    # by direction
    by_dir = {}
    for d in ("bullish", "bearish"):
        sub = [g for g in decided if (g.get("direction") or "").lower() == d]
        sh = [g for g in sub if g["grade"] == "HIT"]
        if sub:
            by_dir[d] = {"n": len(sub), "hit_rate": round(100 * len(sh) / len(sub), 1)}

    track = {
        "n_decisions_graded": n_dec, "n_pending": len([g for g in graded if g.get("grade") == "PENDING"]),
        "hit_rate_pct": hit_rate, "n_hits": len(hits), "n_misses": len(misses),
        "avg_return_when_right_pct": avg_ret_when_right, "avg_return_when_wrong_pct": avg_ret_when_wrong,
        "by_direction": by_dir,
        "calibration": ("well-calibrated" if hit_rate and 45 <= hit_rate <= 70 else
                        "over-confident — review your misses" if hit_rate and hit_rate < 45 else
                        "strong — but check sample size" if hit_rate and hit_rate > 70 else "building sample"),
        "edge_summary": (f"Your decisions are right {hit_rate}% of the time"
                         + (f", winning +{avg_ret_when_right}% vs losing {avg_ret_when_wrong}% when wrong" if avg_ret_when_right is not None and avg_ret_when_wrong is not None else "")
                         + "." if hit_rate is not None else "Not enough graded decisions yet."),
    }

    out = {"engine": "journal-grader", "version": "1.0",
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "duration_s": round(time.time() - t0, 1),
           "n_entries": len(entries), "track_record": track,
           "graded": sorted(graded, key=lambda g: -(g.get("created") or 0))[:100]}
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=600")
    print(f"[journal-grader] {n_dec} graded, hit_rate={hit_rate}%")
    return {"statusCode": 200, "body": json.dumps(track)}
