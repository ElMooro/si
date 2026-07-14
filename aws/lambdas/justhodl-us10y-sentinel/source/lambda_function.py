"""justhodl-us10y-sentinel v1.0 — the 10-Year red-flag watchdog (ops 3286).

Khalid: "US 10Y spiking anywhere near 5% should be a MAJOR red flag for
risk and stocks." The fleet had curve SHAPE (justhodl-yield-curve),
regime CHANGE consensus (justhodl-bond-regime-detector) and 10Y as a
dollar canary — but no engine watching the ABSOLUTE LEVEL against the
5% danger line with velocity, equity-impact evidence and tripwires.
This engine is that watchdog. Real data only.

INPUTS
  - FRED DGS10 full daily history (1962→) + DFII10 (10y real, 2003→)
  - Yahoo v8 ^TNX latest quote (intraday 10Y, /10) — live leg on top
    of FRED's T+1 print
  - Yahoo v8 ^GSPC range=max daily closes — for the episode study and
    the 60d stock/yield correlation regime

COMPUTES
  - level (live), distance_to_5pct_bps, percentile since 1990
  - velocity d20/d60 in bps (rate SHOCKS kill stocks, grinds don't)
  - danger ladder BENIGN<4.00 ≤WATCH<4.25 ≤ELEVATED<4.50 ≤HIGH<4.75
    ≤RED<5.00 ≤CRITICAL, +1 tier bump when d60 ≥ +50bps (capped)
  - real 10y level (DFII10) — >2.25% historically compresses equity
    multiples; noted in reason string
  - EPISODE STUDY (data-driven, no hardcoded folklore): every first
    upward cross of 4.50 / 4.75 / 5.00 (first close ≥ thr after ≥250
    trading days below) since 1962 → SPX forward 1w/1m/3m returns,
    median + hit-rate per threshold
  - corr60: 60d correlation of SPX daily returns vs ΔDGS10 —
    corr ≤ -0.30 ⇒ "YIELDS_DRIVING_STOCKS" regime flag
  - 260d history array for the page sparkline

OUTPUT  data/us10y-sentinel.json      (schema 1.0)
SIGNAL  Telegram on tier CROSSES into/out of RED|CRITICAL only
        (state kept in own JSON — no daily spam)
CONSUMED BY  yield-curve.html sentinel strip (ops 3286),
        justhodl-master-allocator best_asset risk override (ops 3287).
Schedule: EventBridge Scheduler 5x/day (created by ops 3286).
"""
import json
import math
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/us10y-sentinel.json"
SCHEMA = "1.0"
S3 = boto3.client("s3", region_name=REGION)
SSM = boto3.client("ssm", region_name=REGION)

FRED = (os.environ.get("FRED_API_KEY") or os.environ.get("FRED_KEY")
        or "2f057499936072679d8843d7fce99989")
UA = {"User-Agent": "Mozilla/5.0 (jh-us10y-sentinel)"}

TIERS = [(5.00, "CRITICAL"), (4.75, "RED"), (4.50, "HIGH"),
         (4.25, "ELEVATED"), (4.00, "WATCH"), (-99, "BENIGN")]
TIER_ORDER = ["BENIGN", "WATCH", "ELEVATED", "HIGH", "RED", "CRITICAL"]


def _get(url, timeout=25, tries=3):
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8", "ignore"))
        except Exception as e:
            if i == tries - 1:
                print("GET fail %s: %s" % (url[:80], e))
            time.sleep(1.2 * (i + 1))
    return None


def fred_series(sid, start="1962-01-01"):
    url = ("https://api.stlouisfed.org/fred/series/observations?"
           + urllib.parse.urlencode({
               "series_id": sid, "api_key": FRED, "file_type": "json",
               "observation_start": start}))
    j = _get(url) or {}
    out = []
    for o in j.get("observations") or []:
        v = o.get("value")
        if v not in (None, ".", ""):
            try:
                out.append((o["date"], float(v)))
            except Exception:
                pass
    return out


def yahoo_daily(sym, rng="max"):
    url = ("https://query1.finance.yahoo.com/v8/finance/chart/"
           + urllib.parse.quote(sym)
           + "?range=%s&interval=1d" % rng)
    j = _get(url) or {}
    try:
        res = j["chart"]["result"][0]
        ts = res["timestamp"]
        cl = res["indicators"]["quote"][0]["close"]
        out = []
        for t, c in zip(ts, cl):
            if c is not None:
                out.append((datetime.fromtimestamp(
                    t, tz=timezone.utc).strftime("%Y-%m-%d"), float(c)))
        return out
    except Exception:
        return []


def tier_of(level):
    for thr, name in TIERS:
        if level >= thr:
            return name
    return "BENIGN"


def episode_study(dgs10, spx):
    """First upward crosses of each threshold after >=250 tds below,
    with SPX fwd 5/21/63-td returns. Pure data — no folklore."""
    px = {d: v for d, v in spx}
    dates = [d for d, _ in spx]
    idx = {d: i for i, d in enumerate(dates)}

    def fwd(d0, n):
        i = idx.get(d0)
        if i is None:
            # nearest next trading day
            later = [x for x in dates if x >= d0]
            if not later:
                return None
            i = idx[later[0]]
        if i + n >= len(dates):
            return None
        a, b = px[dates[i]], px[dates[i + n]]
        return round((b / a - 1) * 100, 2) if a else None

    out = {}
    ys = [v for _, v in dgs10]
    ds = [d for d, _ in dgs10]
    for thr in (4.50, 4.75, 5.00):
        eps = []
        for i in range(250, len(ys)):
            if ys[i] >= thr and max(ys[i - 250:i]) < thr:
                d0 = ds[i]
                eps.append({"date": d0, "y": round(ys[i], 2),
                            "spx_1w": fwd(d0, 5),
                            "spx_1m": fwd(d0, 21),
                            "spx_3m": fwd(d0, 63)})
        r3 = [e["spx_3m"] for e in eps if e["spx_3m"] is not None]
        r1 = [e["spx_1m"] for e in eps if e["spx_1m"] is not None]
        out["cross_%s" % ("%.2f" % thr)] = {
            "n": len(eps), "episodes": eps[-12:],
            "median_spx_1m": (round(sorted(r1)[len(r1) // 2], 2)
                              if r1 else None),
            "median_spx_3m": (round(sorted(r3)[len(r3) // 2], 2)
                              if r3 else None),
            "neg_3m_hit_rate_pct": (round(100 * sum(
                1 for x in r3 if x < 0) / len(r3), 1) if r3 else None)}
    return out


def corr(a, b):
    n = min(len(a), len(b))
    if n < 20:
        return None
    a, b = a[-n:], b[-n:]
    ma, mb = sum(a) / n, sum(b) / n
    cov = sum((x - ma) * (y - mb) for x, y in zip(a, b))
    va = math.sqrt(sum((x - ma) ** 2 for x in a))
    vb = math.sqrt(sum((y - mb) ** 2 for y in b))
    return round(cov / (va * vb), 3) if va and vb else None


def telegram(msg):
    try:
        tok = SSM.get_parameter(Name="/justhodl/telegram/bot_token",
                                WithDecryption=True)["Parameter"]["Value"]
        chat = SSM.get_parameter(Name="/justhodl/telegram/chat_id",
                                 WithDecryption=True)["Parameter"]["Value"]
        data = urllib.parse.urlencode(
            {"chat_id": chat, "text": msg,
             "parse_mode": "HTML"}).encode()
        urllib.request.urlopen(urllib.request.Request(
            "https://api.telegram.org/bot%s/sendMessage" % tok,
            data=data), timeout=15)
        return True
    except Exception as e:
        print("telegram fail: %s" % e)
        return False


def lambda_handler(event=None, context=None):
    t0 = time.time()
    prev = {}
    try:
        prev = json.loads(S3.get_object(
            Bucket=BUCKET, Key=OUT_KEY)["Body"].read())
    except Exception:
        pass

    dgs10 = fred_series("DGS10")
    if len(dgs10) < 1000:
        raise RuntimeError("DGS10 fetch too thin: %d" % len(dgs10))
    dfii = fred_series("DFII10", start="2003-01-01")
    spx = yahoo_daily("^GSPC", "max")

    # live intraday leg: ^TNX quote / 10 (CBOE 10y yield index)
    tnx = yahoo_daily("^TNX", "5d")
    live_lvl, live_src = None, "fred"
    if tnx:
        live_lvl = round(tnx[-1][1] / 10.0, 3)
        live_src = "yahoo_tnx_live"
    fred_lvl = round(dgs10[-1][1], 3)
    level = live_lvl if live_lvl and 0.2 < live_lvl < 20 else fred_lvl

    ys = [v for _, v in dgs10]
    since90 = [v for d, v in dgs10 if d >= "1990-01-01"]
    pct_rank = round(100 * sum(1 for v in since90 if v <= level)
                     / max(1, len(since90)), 1)
    d20 = round((level - ys[-21]) * 100, 1) if len(ys) > 21 else None
    d60 = round((level - ys[-61]) * 100, 1) if len(ys) > 61 else None

    tier = tier_of(level)
    bumped = False
    if d60 is not None and d60 >= 50 and tier != "CRITICAL":
        i = TIER_ORDER.index(tier)
        if i >= 1:  # only bump when already WATCH+
            tier = TIER_ORDER[min(i + 1, len(TIER_ORDER) - 1)]
            bumped = True

    real10 = round(dfii[-1][1], 2) if dfii else None
    dist_bps = round((5.00 - level) * 100, 1)

    # 60d corr of SPX returns vs Δ10y (FRED daily aligned by date)
    ymap = dict(dgs10)
    rets, dys = [], []
    spx_recent = spx[-130:]
    for i in range(1, len(spx_recent)):
        d1, p1 = spx_recent[i]
        d0, p0 = spx_recent[i - 1]
        if d1 in ymap and d0 in ymap and p0:
            rets.append(p1 / p0 - 1)
            dys.append(ymap[d1] - ymap[d0])
    c60 = corr(rets[-60:], dys[-60:])
    yields_driving = bool(c60 is not None and c60 <= -0.30)

    reason = ("10Y %.2f%% — %.0fbps from the 5%% line · pct-rank "
              "since 1990: %.0f · Δ60d %+0.0fbps%s · real 10y %s%%"
              % (level, dist_bps, pct_rank, (d60 or 0),
                 " (VELOCITY BUMP)" if bumped else "",
                 real10 if real10 is not None else "—"))
    if real10 is not None and real10 >= 2.25:
        reason += " (real yield in multiple-compression zone)"
    if yields_driving:
        reason += " · corr says YIELDS ARE DRIVING STOCKS right now"

    eps = episode_study(dgs10, spx) if spx else {}

    out = {
        "schema": SCHEMA, "engine": "justhodl-us10y-sentinel",
        "as_of": datetime.now(timezone.utc).isoformat(),
        "level": level, "level_source": live_src,
        "fred_close": fred_lvl, "fred_date": dgs10[-1][0],
        "distance_to_5pct_bps": dist_bps,
        "pct_rank_since_1990": pct_rank,
        "velocity": {"d20_bps": d20, "d60_bps": d60,
                     "velocity_bump": bumped},
        "real_10y": real10,
        "tier": tier, "tier_reason": reason,
        "prev_tier": prev.get("tier"),
        "corr60_spx_vs_dy": c60,
        "yields_driving_stocks": yields_driving,
        "episode_study": eps,
        "ladder": [{"thr": t, "name": n} for t, n in TIERS if t > 0],
        "history_260d": [{"d": d, "v": round(v, 3)}
                         for d, v in dgs10[-260:]],
        "duration_s": round(time.time() - t0, 1),
    }

    hot = {"RED", "CRITICAL"}
    pt = prev.get("tier")
    if pt and pt != tier and (tier in hot or pt in hot):
        arrow = "🔴⬆️" if TIER_ORDER.index(tier) > \
            TIER_ORDER.index(pt) else "🟢⬇️"
        med = ((eps.get("cross_4.75") or {}).get("median_spx_3m"))
        telegram("%s <b>US10Y SENTINEL: %s → %s</b>\n%s\n"
                 "History: median SPX 3m after first 4.75%% cross: %s%%"
                 % (arrow, pt, tier, reason,
                    med if med is not None else "n/a"))
        out["alert_sent"] = True

    S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out).encode(),
                  ContentType="application/json",
                  CacheControl="public, max-age=300")
    print("sentinel: %.2f%% tier=%s dist=%.0fbps eps=%s"
          % (level, tier, dist_bps,
             {k: v["n"] for k, v in eps.items()}))
    return {"statusCode": 200, "body": json.dumps(
        {"ok": True, "level": level, "tier": tier})}
