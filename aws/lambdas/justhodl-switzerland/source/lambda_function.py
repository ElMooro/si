"""
justhodl-switzerland — Swiss safe-haven / crisis-radar data layer.

Switzerland is one of the cleanest early-warning tells for European/global
financial stress: the franc surges on flight-to-safety (2008, 2011, the 2015
floor break, 2020), the SMI/SPI lead risk sentiment, the KOF-style business
confidence leads the cycle, and industrial/manufacturing output confirms it.

Sources (all fresh, free):
  • Yahoo Finance — SMI (^SSMI), SPI (^SSHI), EUR/CHF, USD/CHF  (daily)
  • FRED (OECD/SECO) — business confidence, industrial & manufacturing
    production YoY, unemployment, + euro-area business & consumer confidence
    (so the ECB hub can surface EA business confidence too).

OUTPUT: data/switzerland.json    SCHEDULE: daily 06:30 UTC
Real data only — not investment advice.
"""
import json
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone

import boto3

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/switzerland.json"
FRED_KEY = "2f057499936072679d8843d7fce99989"

YAHOO = {  # id -> (symbol, label, kind)
    "smi": ("^SSMI", "Swiss Market Index (SMI)", "equity"),
    "spi": ("^SSHI", "Swiss Performance Index (SPI)", "equity"),
    "eurchf": ("EURCHF=X", "EUR/CHF — franc safe-haven gauge", "fx"),
    "usdchf": ("CHF=X", "USD/CHF — franc vs dollar", "fx"),
}
FRED = {  # id -> (series_id, label, unit)
    "ch_business_confidence": ("BSCICP02CHM460S", "Switzerland — business confidence (OECD)", "index"),
    "ch_ip_yoy": ("PRINTO01CHQ657S", "Switzerland — industrial production YoY (%)", "yoy"),
    "ch_mfg_yoy": ("PRMNTO01CHQ657S", "Switzerland — manufacturing production YoY (%)", "yoy"),
    "ch_unemployment": ("LMUNRRTTCHM156S", "Switzerland — unemployment rate (%, harmonised)", "pct"),
    "ea_business_confidence": ("BSCICP02EZM460S", "Euro Area — business confidence (OECD)", "index"),
    "ea_consumer_confidence": ("CSCICP02EZM460S", "Euro Area — consumer confidence (OECD)", "index"),
}


def _get(url, t=20, hdr=None):
    h = {"User-Agent": "Mozilla/5.0 (compatible; JustHodl-CH/1.0)"}
    h.update(hdr or {})
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=h), timeout=t) as r:
            return r.read().decode("utf-8", "ignore")
    except Exception as e:
        print("fetch fail %s: %s" % (url[:60], e))
        return None


def yahoo(sym, rng="10y"):
    body = _get("https://query1.finance.yahoo.com/v8/finance/chart/%s?range=%s&interval=1d"
                % (urllib.parse.quote(sym), rng))
    pts = []
    if body:
        try:
            r = json.loads(body)["chart"]["result"][0]
            ts = r["timestamp"]; cl = r["indicators"]["quote"][0]["close"]
            for t, c in zip(ts, cl):
                if c is not None:
                    pts.append([datetime.utcfromtimestamp(t).strftime("%Y-%m-%d"), round(float(c), 4)])
        except Exception as e:
            print("yahoo parse %s: %s" % (sym, e))
    return pts


def fred(series_id):
    body = _get("https://api.stlouisfed.org/fred/series/observations?series_id=%s&api_key=%s"
                "&file_type=json&observation_start=1990-01-01" % (series_id, FRED_KEY))
    pts = []
    if body:
        try:
            for o in json.loads(body).get("observations", []):
                v = o.get("value")
                if v not in (".", "", None):
                    pts.append([o["date"], round(float(v), 4)])
        except Exception as e:
            print("fred parse %s: %s" % (series_id, e))
    return pts


def stats(pts, freq_days):
    vals = [v for _, v in pts]
    if not vals:
        return {}
    latest = vals[-1]; n = len(vals); smin, smax = min(vals), max(vals)
    pctile = round(sum(1 for v in vals if v <= latest) / n * 100, 1)
    mean = sum(vals) / n
    sd = (sum((v - mean) ** 2 for v in vals) / n) ** 0.5
    z = round((latest - mean) / sd, 2) if sd else 0.0
    look = max(1, min(n - 1, freq_days))
    prior = vals[-look - 1] if n > look else vals[0]
    chg_pct = round((latest / prior - 1) * 100, 2) if prior else None
    return {"latest": latest, "min": round(smin, 4), "max": round(smax, 4),
            "pctile": pctile, "zscore": z, "chg_3m": chg_pct,
            "start_date": pts[0][0], "latest_date": pts[-1][0], "n_obs": n}


def weekly(pts):
    if len(pts) <= 1200:
        return pts
    seen, out = set(), []
    for d, v in pts:
        try:
            wk = "%s-W%02d" % (d[:4], datetime.strptime(d[:10], "%Y-%m-%d").isocalendar()[1])
        except Exception:
            wk = d[:7]
        if wk not in seen:
            seen.add(wk); out.append([d, v])
    if out and out[-1] != pts[-1]:
        out.append(pts[-1])
    return out


def lambda_handler(event, context):
    now = datetime.now(timezone.utc).isoformat()
    series = []
    for sid, (sym, label, kind) in YAHOO.items():
        pts = yahoo(sym); time.sleep(0.3)
        if not pts:
            continue
        st = stats(pts, 63)  # ~3m of trading days
        series.append({"id": sid, "label": label, "kind": kind, "source": "Yahoo Finance",
                       "freq": "daily", "points": weekly(pts), **st})
    for sid, (fsid, label, unit) in FRED.items():
        pts = fred(fsid); time.sleep(0.3)
        if not pts:
            continue
        st = stats(pts, 3 if unit in ("yoy",) else 6)
        series.append({"id": sid, "label": label, "kind": unit, "source": "FRED/OECD",
                       "freq": "monthly", "points": weekly(pts), **st})

    byid = {s["id"]: s for s in series}

    # ---- Swiss safe-haven / crisis signal -------------------------------
    # CHF strengthening (EUR/CHF & USD/CHF falling) = flight-to-safety;
    # SMI falling = risk-off; weak confidence / negative IP YoY = contraction.
    score = 0; drivers = []
    eurchf = byid.get("eurchf")
    if eurchf and eurchf.get("chg_3m") is not None:
        if eurchf["chg_3m"] <= -2:
            score += 30; drivers.append("CHF surging vs EUR (%.1f%% 3m) — flight-to-safety" % eurchf["chg_3m"])
        elif eurchf["chg_3m"] <= -0.5:
            score += 12; drivers.append("CHF firming vs EUR (%.1f%% 3m)" % eurchf["chg_3m"])
    smi = byid.get("smi")
    if smi and smi.get("chg_3m") is not None:
        if smi["chg_3m"] <= -8:
            score += 25; drivers.append("SMI down %.1f%% 3m — risk-off" % smi["chg_3m"])
        elif smi["chg_3m"] <= -3:
            score += 10; drivers.append("SMI soft (%.1f%% 3m)" % smi["chg_3m"])
    bc = byid.get("ch_business_confidence")
    if bc and bc.get("pctile") is not None and bc["pctile"] <= 25:
        score += 20; drivers.append("business confidence in bottom quartile (%.0f%%ile)" % bc["pctile"])
    ip = byid.get("ch_ip_yoy")
    if ip and ip.get("latest") is not None and ip["latest"] < 0:
        score += 15; drivers.append("industrial production contracting (%.1f%% YoY)" % ip["latest"])
    score = min(100, score)
    regime = ("CRISIS-WATCH" if score >= 70 else "ELEVATED" if score >= 45
              else "WATCH" if score >= 25 else "CALM")

    out = {
        "engine": "switzerland", "version": "1.0.0", "generated_at": now,
        "n_series": len(series),
        "crisis_signal": {"score_0_100": score, "regime": regime,
                          "drivers": drivers or ["No Swiss safe-haven / crisis signals firing."],
                          "note": "Composite of CHF safe-haven demand, SMI risk sentiment, "
                                  "business confidence and industrial output. Higher = more stress."},
        "provenance": "Yahoo Finance (SMI/SPI/CHF) + FRED/OECD (confidence, production, unemployment). "
                      "Includes euro-area business & consumer confidence for the ECB hub.",
        "series": series,
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, separators=(",", ":")).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    return {"statusCode": 200, "body": json.dumps({
        "n_series": len(series), "regime": regime, "score": score})}
