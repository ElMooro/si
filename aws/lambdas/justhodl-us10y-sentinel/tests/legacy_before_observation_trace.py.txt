"""Dated 10-year yield threshold monitor with validated daily SP500 price studies.
FRED DGS10/DFII10/SP500. Heuristic thresholds are review context, not trade calls.
SP500 coverage limits historical equity outcomes; dividends are excluded.
"""
import json
import math
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3
from managed_secret import managed_secret  # audit 2026-09-08 INST-06: no literal credentials

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/us10y-sentinel.json"
SCHEMA = "2.0"
S3 = boto3.client("s3", region_name=REGION)
SSM = boto3.client("ssm", region_name=REGION)

FRED = (os.environ.get("FRED_API_KEY") or os.environ.get("FRED_KEY")
        or managed_secret(('FRED_API_KEY', 'FRED_KEY'), ("/justhodl/fred/api-key",)))
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
                print("Provider request failed: %s" % type(e).__name__)
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


def clean_daily(rows, positive=False):
    clean={}
    for d,v in rows:
        try:
            datetime.strptime(d,'%Y-%m-%d')
            if math.isfinite(v) and (not positive or v>0):clean[d]=v
        except (ValueError,TypeError):pass
    return sorted(clean.items())


def episode_study(dgs10, spx):
    """Dated daily price-only studies; absent coverage cannot jump decades ahead."""
    from bisect import bisect_left
    from statistics import median
    yields=clean_daily(dgs10)
    prices=clean_daily(spx,positive=True)
    dates=[datetime.strptime(d,'%Y-%m-%d').date() for d,_ in prices]

    def fwd(d0,n):
        trigger=datetime.strptime(d0,'%Y-%m-%d').date()
        i=bisect_left(dates,trigger)
        if i>=len(dates) or (dates[i]-trigger).days>4:
            return None,'OUTSIDE_PRICE_COVERAGE'
        if i+n>=len(dates):return None,'HORIZON_NOT_COMPLETE'
        window=dates[i:i+n+1]
        if any((b-a).days>7 for a,b in zip(window,window[1:])):
            return None,'NON_DAILY_OR_GAPPED_PRICE_HISTORY'
        if (window[-1]-window[0]).days>n*2+7:
            return None,'HORIZON_DATE_MISMATCH'
        value=(prices[i+n][1]/prices[i][1]-1)*100
        if not math.isfinite(value) or abs(value)>100:
            return None,'QUARANTINED_IMPLAUSIBLE_SPX_RETURN'
        return round(value,2),'VALID'

    out={}
    ys=[v for _,v in yields]
    for thr in (4.50,4.75,5.00):
        eps=[]
        for i in range(250,len(yields)):
            if ys[i]<thr or max(ys[i-250:i])>=thr:continue
            window=[datetime.strptime(d,'%Y-%m-%d').date() for d,_ in yields[i-250:i+1]]
            if any((b-a).days>7 for a,b in zip(window,window[1:])):continue
            d0=yields[i][0]
            row={'date':d0,'y':round(ys[i],2),'return_basis':'SP500 price-only; dividends excluded','quality':{}}
            for label,n in (('1w',5),('1m',21),('3m',63)):
                value,status=fwd(d0,n)
                row['spx_'+label]=value;row['quality'][label]=status
            eps.append(row)
        r3=[e['spx_3m'] for e in eps if e['spx_3m'] is not None]
        r1=[e['spx_1m'] for e in eps if e['spx_1m'] is not None]
        out['cross_%.2f'%thr]={
            'n':len(eps),'episodes':eps[-12:],'n_valid_1m':len(r1),'n_valid_3m':len(r3),'minimum_summary_n':3,
            'median_spx_1m':round(median(r1),2) if len(r1)>=3 else None,
            'median_spx_3m':round(median(r3),2) if len(r3)>=3 else None,
            'neg_3m_hit_rate_pct':round(100*sum(x<0 for x in r3)/len(r3),1) if len(r3)>=3 else None,
            'status':'DESCRIPTIVE_SMALL_SAMPLE' if len(r3)>=3 else 'INSUFFICIENT_VALID_EPISODES',
            'source':'FRED SP500 daily close','return_basis':'price-only; excludes dividends',
            'price_start':prices[0][0] if prices else None,'price_end':prices[-1][0] if prices else None,
            'methodology_version':'daily-price-episodes.v2',
            'note':'250 prior daily yield observations below threshold; 5/21/63 subsequent daily equity closes. '
                   'Date gaps checked; no returns outside price coverage. Current-vintage descriptive study, not a forecast.'}
    return out


def observation_quality(d):
    try:age=(datetime.now(timezone.utc).date()-datetime.strptime(d,'%Y-%m-%d').date()).days
    except (ValueError,TypeError):age=None
    return {'status':'unavailable' if age is None else 'invalid' if age<0 else 'stale' if age>7 else 'fresh',
            'observation_date':d,'max_age_days':7,'age_days':age}


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

    dgs10 = clean_daily(fred_series("DGS10"))
    if not dgs10:
        stamp=datetime.now(timezone.utc).isoformat()
        out={'schema':SCHEMA,'engine':'justhodl-us10y-sentinel','as_of':stamp,'generated_at':stamp,
             'methodology_version':'daily-price-episodes.v2','quality':observation_quality(None),
             'level':None,'tier':'UNKNOWN','tier_reason':'FRED DGS10 unavailable.',
             'fred_close':None,'fred_date':None,'distance_to_5pct_bps':None,'pct_rank_since_1990':None,
             'velocity':{'d20_bps':None,'d60_bps':None,'velocity_bump':False},'real_10y':None,
             'corr60_spx_vs_dy':None,'yields_driving_stocks':None,'episode_study':{},'history_260d':[],
             'execution_eligible':False,'call':None}
        S3.put_object(Bucket=BUCKET,Key=OUT_KEY,Body=json.dumps(out).encode(),ContentType='application/json')
        return {'statusCode':200,'body':json.dumps({'ok':True,'quality':out['quality']})}
    dfii = fred_series("DFII10", start="2003-01-01")
    spx = clean_daily(fred_series("SP500", start="1962-01-01"), positive=True)

    # FRED constant-maturity yield is a daily observation, not an intraday quote.
    fred_lvl = round(dgs10[-1][1],3)
    live_src = "FRED_DGS10_daily"
    level = fred_lvl
    source_quality = observation_quality(dgs10[-1][0])
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

    dfii=clean_daily(dfii)
    real10 = round(dfii[-1][1],2) if dfii and observation_quality(dfii[-1][0])["status"]=="fresh" else None
    dist_bps = round((5.00 - level) * 100, 1)

    # 60d corr of SPX returns vs Δ10y (FRED daily aligned by date)
    ymap = dict(dgs10)
    rets, dys = [], []
    spx_recent = spx[-130:]
    for i in range(1, len(spx_recent)):
        d1, p1 = spx_recent[i]
        d0, p0 = spx_recent[i - 1]
        if d1 in ymap and d0 in ymap and p0 and (datetime.strptime(d1,"%Y-%m-%d")-datetime.strptime(d0,"%Y-%m-%d")).days<=7:
            rets.append(p1 / p0 - 1)
            dys.append(ymap[d1] - ymap[d0])
    c60 = corr(rets[-60:], dys[-60:])
    equity_quality=observation_quality(spx[-1][0] if spx else None)
    if equity_quality['status']!='fresh' or source_quality['status']!='fresh':
        c60=None
    negative_association = bool(c60 is not None and c60 <= -0.30)

    reason = ("10Y %.2f%% — %.0fbps from the 5%% line · pct-rank "
              "since 1990: %.0f · Δ60d %+0.0fbps%s · real 10y %s%%"
              % (level, dist_bps, pct_rank, (d60 or 0),
                 " (VELOCITY BUMP)" if bumped else "",
                 real10 if real10 is not None else "—"))
    if real10 is not None and real10 >= 2.25:
        reason += " (elevated real-yield review threshold, uncalibrated)"
    if negative_association:
        reason += " · negative equity-return/yield-change association; correlation does not establish causation"

    eps = episode_study(dgs10, spx) if spx else {}

    out = {
        "schema": SCHEMA, "engine": "justhodl-us10y-sentinel",
        "as_of": datetime.now(timezone.utc).isoformat(),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "quality": source_quality, "methodology_version": "daily-price-episodes.v2",
        "calibration_status": "HEURISTIC_REVIEW_ONLY", "execution_eligible": False, "call": None,
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
        "yields_driving_stocks": None,
        "negative_equity_yield_association": negative_association,
        "correlation_observations": min(60,len(rets)),
        "equity_quality": equity_quality,
        "real_10y_date": dfii[-1][0] if dfii else None,
        "episode_study": eps,
        "ladder": [{"thr": t, "name": n} for t, n in TIERS if t > 0],
        "history_260d": [{"d": d, "v": round(v, 3)}
                         for d, v in dgs10[-260:]],
        "duration_s": round(time.time() - t0, 1),
    }

    if source_quality['status'] != 'fresh':
        out.update(level=None, tier='UNKNOWN', distance_to_5pct_bps=None,
                   pct_rank_since_1990=None, tier_reason='Daily Treasury yield unavailable or stale.',
                   corr60_spx_vs_dy=None, negative_equity_yield_association=None,
                   velocity={'d20_bps':None,'d60_bps':None,'velocity_bump':False})
    hot = {"RED", "CRITICAL"}
    pt = prev.get("tier")
    if not (event or {}).get("suppress_alerts") and source_quality["status"]=="fresh" and pt in TIER_ORDER and pt != tier and (tier in hot or pt in hot):
        arrow = "🔴⬆️" if TIER_ORDER.index(tier) > \
            TIER_ORDER.index(pt) else "🟢⬇️"
        med = ((eps.get("cross_4.75") or {}).get("median_spx_3m"))
        telegram("%s <b>US10Y SENTINEL: %s → %s</b>\n%s\n"
                 "History: median SPX 3m after first 4.75%% cross: %s%%"
                 % (arrow, pt, tier, reason,
                    med if med is not None else "n/a"))
        out["alert_sent"] = True

    S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out,allow_nan=False).encode(),
                  ContentType="application/json",
                  CacheControl="public, max-age=300")
    print("sentinel: %.2f%% tier=%s dist=%.0fbps eps=%s"
          % (level, tier, dist_bps,
             {k: v["n"] for k, v in eps.items()}))
    return {"statusCode": 200, "body": json.dumps(
        {"ok": True, "level": level, "tier": tier})}


_orig_handler_4217 = lambda_handler


def lambda_handler(event=None, context=None):
    """bus_cross — bus enrichment wrapper (core math untouched)."""
    r = _orig_handler_4217(event, context)
    try:
        _bus = (json.loads(S3.get_object(Bucket=BUCKET, Key="data/indicator-bus.json")["Body"].read()) or {}).get("indicators") or {}
        _r = _bus.get("US10Y") or {}
        _blk = {"marker": "ops4217", "us10y_bus": _r.get("v"),
                "asof": _r.get("asof"), "src": _r.get("src"),
                "note": "independent cross-check leg (source diversity)"}
        _doc = json.loads(S3.get_object(Bucket=BUCKET, Key=OUT_KEY)["Body"].read())
        _doc["bus_cross"] = _blk
        S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                      Body=json.dumps(_doc).encode(),
                      ContentType="application/json")
        print("[bus_cross] wired: " + json.dumps(_blk)[:120])
    except Exception as _e:
        print("[bus_cross] EXC " + type(_e).__name__)
    return r
