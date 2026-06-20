"""
justhodl-benzinga-earnings — authoritative institutional earnings feed (Benzinga
via Massive/Polygon /benzinga/v1/earnings). One bulk paginated pull replaces
hundreds of per-ticker FMP calls and gives confirmed actuals + precomputed
surprise %, importance (1-5), and revenue surprise.

WHY: PEAD (post-earnings-announcement drift) is one of the most robust documented
anomalies. Its quality is bounded by the surprise data feeding it. The fleet's
16 earnings engines run on free Nasdaq-calendar / thin FMP data; this engine
publishes a single clean source they can consume.

OUTPUTS:
  data/benzinga-earnings.json          per-ticker recent surprises + streaks
  data/benzinga-earnings-calendar.json upcoming confirmed/projected reports (30d)

Schema per ticker in benzinga-earnings.json["tickers"][SYM]:
  recent: [{date, fiscal_period, fiscal_year, actual_eps, estimated_eps,
            eps_surprise_pct, actual_revenue, revenue_surprise_pct,
            importance, eps_method}]   (newest first, <=8)
  next_earnings_date, last_eps_surprise_pct, last_revenue_surprise_pct,
  beat_streak (consecutive recent EPS beats), avg_eps_surprise_4q,
  both_beat (latest EPS & revenue both beat), max_importance, days_since_report
"""
import os, json, time, datetime, urllib.request, urllib.error
import boto3

try:
    from massive import get_massive_key          # bundled shared helper
except Exception:
    def get_massive_key():
        try:
            return boto3.client("ssm","us-east-1").get_parameter(
                Name="/justhodl/massive-api-key", WithDecryption=True)["Parameter"]["Value"]
        except Exception:
            return os.environ.get("MASSIVE_API_KEY","")

S3 = boto3.client("s3","us-east-1")
BUCKET = os.environ.get("BUCKET","justhodl-dashboard-live")
BASE = "https://api.polygon.io/benzinga/v1/earnings"
KEY = get_massive_key()
REPORTED_LOOKBACK_DAYS = 120
UPCOMING_AHEAD_DAYS = 30
PAGE_LIMIT = 1000
MAX_PAGES = 60
TIMEOUT = 25

def _today():
    return datetime.date.today()

def _get(url):
    req = urllib.request.Request(url, headers={"User-Agent":"JustHodl-BZEarnings/1.0"})
    with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
        return json.loads(r.read().decode("utf-8","replace"))

def _paginate(first_url):
    """Yield result rows across paginated next_url responses."""
    url = first_url
    pages = 0
    while url and pages < MAX_PAGES:
        sep = "&" if "?" in url else "?"
        full = url if "apiKey=" in url else f"{url}{sep}apiKey={KEY}"
        try:
            data = _get(full)
        except urllib.error.HTTPError as e:
            if e.code == 429:                     # back off on rate limit
                time.sleep(2); continue
            break
        except Exception:
            break
        for row in (data.get("results") or []):
            yield row
        url = data.get("next_url")
        pages += 1
        if url:
            time.sleep(0.12)                      # gentle pacing

def fetch_reported():
    start = (_today() - datetime.timedelta(days=REPORTED_LOOKBACK_DAYS)).isoformat()
    end = _today().isoformat()
    url = f"{BASE}?date.gte={start}&date.lte={end}&limit={PAGE_LIMIT}"
    rows = []
    for r in _paginate(url):
        if r.get("actual_eps") is None:           # only truly reported rows
            continue
        rows.append(r)
    return rows

def fetch_upcoming():
    start = _today().isoformat()
    end = (_today() + datetime.timedelta(days=UPCOMING_AHEAD_DAYS)).isoformat()
    url = f"{BASE}?date.gte={start}&date.lte={end}&limit={PAGE_LIMIT}"
    out = []
    for r in _paginate(url):
        out.append({
            "ticker": r.get("ticker"), "company": r.get("company_name"),
            "date": r.get("date"), "time": r.get("time"),
            "fiscal_period": r.get("fiscal_period"), "fiscal_year": r.get("fiscal_year"),
            "estimated_eps": r.get("estimated_eps"), "estimated_revenue": r.get("estimated_revenue"),
            "importance": r.get("importance"), "date_status": r.get("date_status"),
        })
    out.sort(key=lambda x: (x["date"] or "9999", -(x["importance"] or 0)))
    return out

def _num(x):
    try: return float(x)
    except Exception: return None

def build_per_ticker(reported):
    by = {}
    for r in reported:
        t = r.get("ticker")
        if not t: continue
        by.setdefault(t, []).append(r)
    today = _today()
    tickers = {}
    for t, rows in by.items():
        rows.sort(key=lambda x: x.get("date") or "", reverse=True)  # newest first
        recent = []
        for r in rows[:8]:
            recent.append({
                "date": r.get("date"), "fiscal_period": r.get("fiscal_period"),
                "fiscal_year": r.get("fiscal_year"),
                "actual_eps": _num(r.get("actual_eps")), "estimated_eps": _num(r.get("estimated_eps")),
                "eps_surprise_pct": _num(r.get("eps_surprise_percent")),
                "actual_revenue": _num(r.get("actual_revenue")),
                "revenue_surprise_pct": _num(r.get("revenue_surprise_percent")),
                "importance": r.get("importance"), "eps_method": r.get("eps_method"),
            })
        if not recent: continue
        # beat streak: consecutive newest quarters with actual>estimated
        streak = 0
        for q in recent:
            if q["actual_eps"] is not None and q["estimated_eps"] is not None and q["actual_eps"] > q["estimated_eps"]:
                streak += 1
            else:
                break
        sp = [q["eps_surprise_pct"] for q in recent[:4] if q["eps_surprise_pct"] is not None]
        latest = recent[0]
        try:
            dsr = (today - datetime.date.fromisoformat(latest["date"])).days
        except Exception:
            dsr = None
        tickers[t] = {
            "recent": recent,
            "last_eps_surprise_pct": latest["eps_surprise_pct"],
            "last_revenue_surprise_pct": latest["revenue_surprise_pct"],
            "beat_streak": streak,
            "avg_eps_surprise_4q": round(sum(sp)/len(sp), 4) if sp else None,
            "both_beat": bool(latest["eps_surprise_pct"] and latest["eps_surprise_pct"] > 0
                              and latest["revenue_surprise_pct"] and latest["revenue_surprise_pct"] > 0),
            "max_importance": max([q["importance"] or 0 for q in recent]),
            "days_since_report": dsr,
        }
    return tickers

def lambda_handler(event, context):
    if not KEY:
        return {"statusCode": 500, "body": "no massive key"}
    reported = fetch_reported()
    tickers = build_per_ticker(reported)
    upcoming = fetch_upcoming()

    # PEAD focus board: reported within 30d, ranked by surprise, importance-gated
    drift = []
    for t, d in tickers.items():
        if d["days_since_report"] is None or d["days_since_report"] > 30:
            continue
        sp = d["last_eps_surprise_pct"]
        if sp is None or (d["max_importance"] or 0) < 3:
            continue
        drift.append({
            "ticker": t, "eps_surprise_pct": sp,
            "revenue_surprise_pct": d["last_revenue_surprise_pct"],
            "beat_streak": d["beat_streak"], "both_beat": d["both_beat"],
            "days_since_report": d["days_since_report"], "importance": d["max_importance"],
        })
    drift.sort(key=lambda x: -(x["eps_surprise_pct"] or 0))
    top_positive = [x for x in drift if (x["eps_surprise_pct"] or 0) > 0][:40]
    top_negative = sorted([x for x in drift if (x["eps_surprise_pct"] or 0) < 0],
                          key=lambda x: x["eps_surprise_pct"])[:20]

    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    main = {
        "generated_at": now, "source": "benzinga/v1/earnings (Massive)",
        "reported_lookback_days": REPORTED_LOOKBACK_DAYS,
        "n_reported_rows": len(reported), "n_tickers": len(tickers),
        "pead_top_positive": top_positive, "pead_top_negative": top_negative,
        "tickers": tickers,
    }
    cal = {"generated_at": now, "ahead_days": UPCOMING_AHEAD_DAYS,
           "n_upcoming": len(upcoming), "upcoming": upcoming}

    S3.put_object(Bucket=BUCKET, Key="data/benzinga-earnings.json",
                  Body=json.dumps(main).encode(), ContentType="application/json",
                  CacheControl="max-age=300")
    S3.put_object(Bucket=BUCKET, Key="data/benzinga-earnings-calendar.json",
                  Body=json.dumps(cal).encode(), ContentType="application/json",
                  CacheControl="max-age=300")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "n_reported_rows": len(reported), "n_tickers": len(tickers),
        "n_upcoming": len(upcoming), "pead_positive": len(top_positive),
        "pead_negative": len(top_negative),
        "sample_top": top_positive[0] if top_positive else None})}
