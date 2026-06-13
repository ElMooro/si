"""
justhodl-intraday-pulse v1.0 — the real-time layer (60-second cadence)
======================================================================
Two modes on one Lambda:
  ARM   (daily 13:25 UTC, event {"arm": true}) — builds the armed watchlist
        (underlooked top + HP serious + hottest S&P heat tiles + SPY/QQQ/IWM)
        with trigger levels from the rings: 60d high, prev close.
  PULSE (rate(1 minute), market hours 13:30–20:00 UTC Mon–Fri) — one Polygon
        snapshot call for the whole watchlist; detects: 60d-high breakouts
        (once per ticker per day), day moves <=-6% / >=+8%, SPY <=-1.8%.
        Events: -> alert-sentinel daily-report buffer (S3 state merge),
                -> data/intraday-pulse.json (live movers + today's events),
                -> CRITICAL events also fire Telegram immediately.
Cost: ~390 invocations/day at 128-512MB — free-tier dust. Websocket DO is
the future upgrade path if 60s granularity ever proves insufficient.
"""
import json, gzip, os, time, urllib.request, urllib.parse
from datetime import datetime, timezone
import boto3

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
POLY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
TG_TOKEN = os.environ.get("TELEGRAM_TOKEN", os.environ.get("TELEGRAM_BOT_TOKEN", ""))
TG_CHAT = os.environ.get("TELEGRAM_CHAT", "")
ARMED_KEY = "data/_intraday/armed.json"
STATE_KEY = "data/_intraday/state.json"
SENT_STATE = "data/_alerts/last.json"
OUT_KEY = "data/intraday-pulse.json"
VERSION = "1.0.1"


def jget(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl admin@justhodl.ai"})
    return json.loads(urllib.request.urlopen(req, timeout=timeout).read())


def s3j(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return default if default is not None else {}


def s3w(key, obj, cache="no-cache"):
    S3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(obj, default=str).encode(),
                  ContentType="application/json", CacheControl=cache)


def telegram(text):
    if not TG_TOKEN or not TG_CHAT:
        return False
    try:
        data = urllib.parse.urlencode({"chat_id": TG_CHAT, "text": text[:3900]}).encode()
        urllib.request.urlopen(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage", data=data, timeout=10)
        return True
    except Exception:
        return False


def arm():
    sv = s3j("data/stock-valuations.json")
    watch = set()
    for x in (sv.get("underlooked_top") or [])[:25]:
        watch.add(x["t"])
    for t in (sv.get("serious") or []):
        watch.add(t if isinstance(t, str) else t.get("t"))
    hm = (sv.get("heatmap") or {}).get("sp") or []
    for x in sorted(hm, key=lambda r: -r["a"])[:15]:
        watch.add(x["t"])
    watch |= {"SPY", "QQQ", "IWM"}
    watch = sorted(w for w in watch if w)[:48]
    up = json.loads(gzip.decompress(
        S3.get_object(Bucket=BUCKET, Key="data/_upside/state.json.gz")["Body"].read()))
    rings = up.get("rings") or {}
    armed = {}
    for t in watch:
        r = rings.get(t) or []
        if len(r) >= 60:
            armed[t] = {"hi60": round(max(r[-60:]), 4), "prev": round(r[-1], 4)}
        else:
            armed[t] = {"hi60": None, "prev": None}
    s3w(ARMED_KEY, {"date": datetime.now(timezone.utc).date().isoformat(),
                      "watch": watch, "levels": armed})
    s3w(STATE_KEY, {"date": datetime.now(timezone.utc).date().isoformat(),
                      "fired": {}, "events": []})
    print(f"[pulse] armed {len(watch)} tickers")
    return {"armed": len(watch)}


def push_to_sentinel_buffer(lines):
    try:
        st = s3j(SENT_STATE)
        buf = st.get("buffer") or []
        d = datetime.now(timezone.utc).date().isoformat()[5:]
        for ln in lines:
            buf.append({"d": d, "line": ln[:220]})
        st["buffer"] = buf[-400:]
        s3w(SENT_STATE, st)
        return True
    except Exception:
        return False


def pulse(force=False):
    now = datetime.now(timezone.utc)
    if not force:
        if now.weekday() > 4 or not (13 <= now.hour < 20) \
                or (now.hour == 13 and now.minute < 30):
            return {"skipped": "market closed"}
    armed = s3j(ARMED_KEY)
    levels = armed.get("levels") or {}
    watch = armed.get("watch") or []
    if not watch:
        return {"skipped": "not armed"}
    st = s3j(STATE_KEY, {"fired": {}, "events": []})
    if st.get("date") != now.date().isoformat():
        st = {"date": now.date().isoformat(), "fired": {}, "events": []}
    url = ("https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
            f"?tickers={','.join(watch)}&apiKey={POLY}")
    snap = jget(url)
    movers, events, critical = [], [], []
    for tk in snap.get("tickers") or []:
        t = tk.get("ticker")
        day = tk.get("day") or {}
        last = (tk.get("lastTrade") or {}).get("p") or day.get("c")
        chg = tk.get("todaysChangePerc")
        if last is None or chg is None:
            continue
        movers.append({"t": t, "p": round(float(last), 2), "chg": round(float(chg), 2)})
        lv = levels.get(t) or {}
        fired = st["fired"].setdefault(t, [])
        if lv.get("hi60") and float(last) > lv["hi60"] and "hi60" not in fired:
            fired.append("hi60")
            events.append(f"⚡ {t} broke its 60d high "
                           f"({lv['hi60']} → {round(float(last), 2)}, {chg:+.1f}% today)")
        if chg <= -6 and "crash" not in fired:
            fired.append("crash")
            ev = f"🔻 {t} down {chg:.1f}% intraday (now {round(float(last), 2)})"
            events.append(ev)
            critical.append(ev)
        if chg >= 8 and "spike" not in fired:
            fired.append("spike")
            events.append(f"🚀 {t} up {chg:+.1f}% intraday (now {round(float(last), 2)})")
        if t == "SPY" and chg <= -1.8 and "spy" not in fired:
            fired.append("spy")
            ev = f"🚨 SPY {chg:.1f}% — broad risk-off in progress"
            events.append(ev)
            critical.append(ev)
    if events:
        st["events"] = (st["events"] + [{"at": now.isoformat()[11:16], "line": e}
                                           for e in events])[-120:]
        push_to_sentinel_buffer(events)
        for ev in critical:
            telegram("⏱ INTRADAY CRITICAL\n" + ev)
    s3w(STATE_KEY, st)
    movers.sort(key=lambda m: -abs(m["chg"]))
    s3w(OUT_KEY, {"engine": "intraday-pulse", "version": VERSION,
                    "generated_at": now.isoformat(), "armed_n": len(watch),
                    "events_today": st["events"][-40:], "n_events_today": len(st["events"]),
                    "top_movers": movers[:20],
                    "methodology": ("60-second market-hours cadence over an armed "
                                     "watchlist (underlooked top + HP serious + hottest "
                                     "heat tiles + SPY/QQQ/IWM). Triggers: 60d-high "
                                     "breakout (once/day), <=-6% / >=+8% day moves, SPY "
                                     "<=-1.8%. Events feed the daily Telegram report "
                                     "buffer; critical events also send immediately. "
                                     "Research, not advice.")},
         cache="public, max-age=45")
    return {"events": len(events), "movers": len(movers)}


def lambda_handler(event=None, context=None):
    event = event or {}
    if event.get("arm"):
        return {"statusCode": 200, "body": json.dumps(arm())}
    return {"statusCode": 200, "body": json.dumps(pulse(force=bool(event.get("force"))))}
