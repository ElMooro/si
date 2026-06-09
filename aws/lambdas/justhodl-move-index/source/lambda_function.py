"""justhodl-move-index — the REAL ICE BofA MOVE Index (bond-market VIX).

The audit's one genuine gap: bonds.html had a MOVE field reading FRED, but FRED
has no free MOVE series → always '—'. ICE owns MOVE and paywalls the raw feed,
BUT Yahoo Finance carries it as ^MOVE for free. This fetches ^MOVE daily, writes
data/move-index.json with the level + percentile + regime, so bonds.html and
bond-vol can show the real implied bond vol (not just the synthetic proxy).

OUTPUT: data/move-index.json · SCHEDULE: daily 13:20 UTC.
"""
import json, time, statistics
import urllib.request
from datetime import datetime, timezone
import boto3

REGION = "us-east-1"; BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/move-index.json"
# fetch ^MOVE via the data-proxy worker's yahoo route (server-side, no browser block)
YF_URL = "https://justhodl-data-proxy.raafouis.workers.dev/yf-ohlc?symbol=%5EMOVE&range=2y"
s3 = boto3.client("s3", region_name=REGION)


def fetch_move():
    try:
        req = urllib.request.Request(YF_URL, headers={"User-Agent": "JustHodl-MOVE/1.0", "Origin": "https://justhodl.ai"})
        d = json.loads(urllib.request.urlopen(req, timeout=20).read().decode())
        bars = d.get("bars") or []
        closes = [(b.get("time"), b.get("close")) for b in bars if b.get("close") is not None]
        return closes
    except Exception as e:
        print(f"[move] fetch err: {str(e)[:80]}")
        return []


def lambda_handler(event=None, context=None):
    t0 = time.time()
    closes = fetch_move()
    if not closes or len(closes) < 30:
        # fallback: try direct Yahoo if worker route was empty
        try:
            req = urllib.request.Request(
                "https://query1.finance.yahoo.com/v8/finance/chart/%5EMOVE?interval=1d&range=2y",
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36"})
            data = json.loads(urllib.request.urlopen(req, timeout=20).read().decode())
            res = data["chart"]["result"][0]
            ts = res["timestamp"]; q = res["indicators"]["quote"][0]["close"]
            closes = [(ts[i], q[i]) for i in range(len(ts)) if q[i] is not None]
        except Exception as e:
            print(f"[move] direct yahoo err: {str(e)[:80]}")

    if not closes:
        out = {"engine": "move-index", "generated_at": datetime.now(timezone.utc).isoformat(),
               "ok": False, "note": "MOVE feed unavailable (Yahoo ^MOVE)"}
        s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out).encode(), ContentType="application/json")
        return {"statusCode": 200, "body": "no data"}

    vals = [c[1] for c in closes]
    latest = vals[-1]
    prev = vals[-2] if len(vals) > 1 else None
    chg_1d = round(latest - prev, 2) if prev is not None else None
    # percentile over the available window
    below = sum(1 for v in vals if v <= latest)
    pctile = round(100 * below / len(vals), 1)
    # 1y stats
    yr = vals[-252:] if len(vals) >= 252 else vals
    avg_1y = round(statistics.mean(yr), 1)
    # 20d change
    chg_20d = round(latest - vals[-21], 2) if len(vals) > 21 else None
    # regime bands (standard MOVE interpretation)
    if latest >= 140: regime, color, interp = "CRISIS", "#ff5577", "Crisis-level bond vol — market pricing massive yield moves"
    elif latest >= 120: regime, color, interp = "ELEVATED", "#fb923c", "Elevated — bonds unstable, duration risk high"
    elif latest >= 90: regime, color, interp = "NORMAL", "#a8b3c7", "Normal range"
    else: regime, color, interp = "SUPPRESSED", "#26ffaf", "Suppressed — confidence in the Fed path, low vol"

    out = {
        "engine": "move-index", "version": "1.0", "source": "ICE BofA MOVE via Yahoo ^MOVE",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "ok": True, "duration_s": round(time.time() - t0, 1),
        "level": round(latest, 1), "change_1d": chg_1d, "change_20d": chg_20d,
        "percentile": pctile, "avg_1y": avg_1y,
        "regime": regime, "color": color, "interpretation": interp,
        "n_observations": len(vals),
        "history": [{"t": c[0], "v": round(c[1], 1)} for c in closes[-260:]],
        "latest_date": datetime.utcfromtimestamp(closes[-1][0]).date().isoformat() if closes[-1][0] else None,
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=600")
    print(f"[move] level={latest} regime={regime} pctile={pctile}")
    return {"statusCode": 200, "body": json.dumps({"level": latest, "regime": regime})}
