"""
justhodl-fleet-monitor — Full-Fleet Observability.

The platform already runs justhodl-health-monitor, but that engine checks a
hand-curated EXPECTATIONS list — it only sees components someone explicitly
added. With 270+ Lambdas and 200+ data outputs the system has outgrown a
manual checklist: every new engine is a blind spot until someone remembers
to register it.

This engine is the wide net. It AUTO-DISCOVERS — nothing has to be
registered — and watches all three failure surfaces of the platform:

  1. DATA  — sweeps every data/*.json output in S3. A broken engine stops
     producing, so a stale or shrunken or ok:false output is the truest
     health signal there is. New engines are covered automatically the
     moment they write their first file.

  2. COMPUTE — inventories the Lambda fleet (best-effort; degrades cleanly
     if the role lacks lambda:ListFunctions).

  3. DEPENDENCIES — actively probes the things the whole platform leans on
     and that fail silently: the Anthropic API (credit exhaustion silently
     degrades every AI feature) and each market-data provider key (FRED,
     FMP, Polygon, AlphaVantage, CoinMarketCap). One dead key takes down
     dozens of engines with no error of their own.

Anything red triggers a deduplicated Telegram alert. Output is written to
_health/fleet.json. This complements health-monitor — health-monitor is the
deep, tuned, curated net; this is the catch-all that nothing escapes.

OUTPUT: _health/fleet.json   SCHEDULE: every 3h
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "_health/fleet.json"
ALERT_STATE_KEY = "_health/fleet_last_alert.json"

# data-output staleness tiers (hours)
FRESH_H = 30
STALE_RED_H = 168          # >7d unfetched ⇒ almost certainly a dead engine
MIN_SIZE = 60              # bytes — smaller than this ⇒ effectively empty

# keys (provider keys are non-sensitive read keys; Anthropic key via env only)
KEYS = {
    "FRED": os.environ.get("FRED_API_KEY", "2f057499936072679d8843d7fce99989"),
    "FMP": os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"),
    "POLYGON": os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"),
    "ALPHAVANTAGE": os.environ.get("AV_KEY", "EOLGKSGAYZUXKPUL"),
    "CMC": os.environ.get("CMC_KEY", "17ba8e87-53f0-46f4-abe5-014d9cd99597"),
}
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
TG_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN",
                          "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs")
TG_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "8678089260")


def now():
    return datetime.now(timezone.utc)


def http(url, method="GET", headers=None, body=None, timeout=14):
    req = urllib.request.Request(url, method=method,
                                 data=body.encode() if body else None,
                                 headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", "ignore")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "ignore")[:400]
    except Exception as e:
        return None, str(e)[:200]


# ─────────────────────── 1. DATA OUTPUT SWEEP ───────────────────────
def sweep_data_outputs():
    """Every data/*.json output: age, size, and ok/error content flags."""
    keys = []
    token = None
    try:
        while True:
            kw = {"Bucket": BUCKET, "Prefix": "data/", "MaxKeys": 1000}
            if token:
                kw["ContinuationToken"] = token
            resp = s3.list_objects_v2(**kw)
            for o in resp.get("Contents", []):
                k = o["Key"]
                # depth-1 data/X.json only — skip snapshot subdirectories
                if k.endswith(".json") and k.count("/") == 1:
                    keys.append((k, o["LastModified"], o["Size"]))
            token = resp.get("NextContinuationToken")
            if not token:
                break
    except Exception as e:
        return {"available": False, "error": str(e)[:200]}

    t = now()
    red, yellow, degraded, static = [], [], [], []
    green = 0
    for key, lm, size in keys:
        age_h = (t - lm).total_seconds() / 3600.0
        name = key[len("data/"):-len(".json")]
        item = {"output": name, "age_hours": round(age_h, 1), "size": size}
        # config / static files are not engine outputs — they change only
        # when edited, so their age says nothing about system health
        if "config" in name.lower():
            static.append({"output": name, "age_hours": round(age_h, 1)})
            continue
        if size < MIN_SIZE:
            item["issue"] = "empty / truncated output"
            red.append(item)
            continue
        if age_h > STALE_RED_H:
            item["issue"] = f"stale {age_h/24:.1f}d — engine likely dead"
            red.append(item)
            continue
        # fresh-ish file: peek inside for a self-reported failure
        if age_h <= FRESH_H:
            try:
                body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
                d = json.loads(body)
                if isinstance(d, dict):
                    if d.get("ok") is False:
                        item["issue"] = "engine reports ok=false"
                        degraded.append(item)
                        continue
                    err = d.get("error")
                    if err:
                        item["issue"] = f"error field: {str(err)[:80]}"
                        degraded.append(item)
                        continue
            except Exception:
                pass
            green += 1
        else:
            item["issue"] = f"aging — {age_h:.0f}h since last write"
            yellow.append(item)

    red.sort(key=lambda x: -x["age_hours"])
    yellow.sort(key=lambda x: -x["age_hours"])
    return {"available": True, "total": len(keys), "green": green,
            "n_yellow": len(yellow), "n_red": len(red),
            "n_degraded": len(degraded), "n_static": len(static),
            "red": red[:40], "degraded": degraded[:40],
            "yellow": yellow[:25], "static": static[:30]}


# ─────────────────────── 2. COMPUTE INVENTORY ───────────────────────
def sweep_compute():
    """Best-effort Lambda inventory — degrades cleanly without ListFunctions."""
    try:
        lam = boto3.client("lambda")
        names, stale_code = [], []
        t = now()
        token = None
        while True:
            kw = {"MaxItems": 1000}
            if token:
                kw["Marker"] = token
            resp = lam.list_functions(**kw)
            for f in resp.get("Functions", []):
                names.append(f["FunctionName"])
            token = resp.get("NextMarker")
            if not token:
                break
        return {"available": True, "n_functions": len(names)}
    except Exception as e:
        return {"available": False, "error": str(e)[:160],
                "note": "lambda:ListFunctions not granted to role — "
                        "data-output sweep covers engine health instead"}


# ─────────────────────── 3. DEPENDENCY PROBES ───────────────────────
def probe_anthropic():
    if not ANTHROPIC_KEY:
        return {"name": "Anthropic API", "status": "unknown",
                "detail": "ANTHROPIC_API_KEY not set on this function"}
    body = json.dumps({"model": "claude-haiku-4-5-20251001", "max_tokens": 1,
                       "messages": [{"role": "user", "content": "ping"}]})
    code, text = http("https://api.anthropic.com/v1/messages", "POST",
                      {"x-api-key": ANTHROPIC_KEY,
                       "anthropic-version": "2023-06-01",
                       "content-type": "application/json"}, body)
    if code == 200:
        return {"name": "Anthropic API", "status": "green",
                "detail": "responding — credits OK"}
    low = (text or "").lower()
    if code in (400, 402) and ("credit" in low or "balance" in low or
                               "billing" in low):
        return {"name": "Anthropic API", "status": "red",
                "detail": "CREDIT EXHAUSTED — every AI feature degrades"}
    if code == 429:
        return {"name": "Anthropic API", "status": "yellow",
                "detail": "rate limited"}
    if code == 401:
        return {"name": "Anthropic API", "status": "red",
                "detail": "auth failed — bad/expired key"}
    return {"name": "Anthropic API", "status": "yellow",
            "detail": f"HTTP {code}: {str(text)[:80]}"}


def probe_provider(name, url, headers=None, av_check=False):
    code, text = http(url, headers=headers)
    if code == 200:
        if av_check and ("rate limit" in (text or "").lower() or
                         '"Information"' in (text or "")):
            return {"name": name, "status": "yellow",
                    "detail": "rate-limit notice in response"}
        return {"name": name, "status": "green", "detail": "key valid"}
    if code in (401, 403):
        return {"name": name, "status": "red",
                "detail": f"auth failed (HTTP {code}) — key dead/expired"}
    if code == 429:
        return {"name": name, "status": "yellow", "detail": "rate limited"}
    return {"name": name, "status": "yellow",
            "detail": f"HTTP {code}: {str(text)[:70]}"}


def probe_dependencies():
    probes = [probe_anthropic()]
    probes.append(probe_provider(
        "FRED", "https://api.stlouisfed.org/fred/series/observations"
        f"?series_id=DGS10&api_key={KEYS['FRED']}&file_type=json&limit=1"))
    probes.append(probe_provider(
        "FMP", f"https://financialmodelingprep.com/stable/quote"
        f"?symbol=AAPL&apikey={KEYS['FMP']}"))
    probes.append(probe_provider(
        "Polygon", "https://api.polygon.io/v3/reference/tickers"
        f"?limit=1&apiKey={KEYS['POLYGON']}"))
    probes.append(probe_provider(
        "AlphaVantage", "https://www.alphavantage.co/query"
        f"?function=GLOBAL_QUOTE&symbol=AAPL&apikey={KEYS['ALPHAVANTAGE']}",
        av_check=True))
    probes.append(probe_provider(
        "CoinMarketCap", "https://pro-api.coinmarketcap.com/v1/"
        "cryptocurrency/listings/latest?limit=1",
        headers={"X-CMC_PRO_API_KEY": KEYS["CMC"]}))
    return probes


# ─────────────────────────── ALERTING ───────────────────────────
def send_telegram(text):
    if not TG_TOKEN or not TG_CHAT:
        return False
    body = json.dumps({"chat_id": TG_CHAT, "text": text,
                       "parse_mode": "Markdown",
                       "disable_web_page_preview": True})
    code, _ = http(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                   "POST", {"content-type": "application/json"}, body)
    return code == 200


def lambda_handler(event, context):
    t0 = time.time()
    started = now()

    data = sweep_data_outputs()
    compute = sweep_compute()
    deps = probe_dependencies()

    dep_red = [p for p in deps if p["status"] == "red"]
    dep_yellow = [p for p in deps if p["status"] == "yellow"]
    data_red = data.get("n_red", 0) + data.get("n_degraded", 0)
    data_yellow = data.get("n_yellow", 0)

    if dep_red or data_red > 0 or not data.get("available"):
        system = "red"
    elif dep_yellow or data_yellow > 0:
        system = "yellow"
    else:
        system = "green"

    out = {
        "schema_version": "1.0",
        "engine": "fleet-monitor",
        "generated_at": started.isoformat(),
        "elapsed_s": round(time.time() - t0, 2),
        "system_status": system,
        "summary": {
            "data_outputs_total": data.get("total"),
            "data_outputs_fresh": data.get("green"),
            "data_outputs_aging": data_yellow,
            "data_outputs_stale_or_degraded": data_red,
            "data_outputs_static": data.get("n_static"),
            "lambda_count": compute.get("n_functions"),
            "dependencies_down": len(dep_red),
            "dependencies_degraded": len(dep_yellow),
        },
        "data_outputs": data,
        "compute": compute,
        "dependencies": deps,
        "note": ("Auto-discovering full-fleet observability — sweeps every "
                 "data output, inventories the Lambda fleet, and actively "
                 "probes the Anthropic API and every market-data key. New "
                 "engines are covered automatically. Complements the curated "
                 "health-monitor."),
    }
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, default=str).encode("utf-8"),
                  ContentType="application/json",
                  CacheControl="public, max-age=600")

    # ── deduplicated alerting ──
    fingerprint = json.dumps({
        "system": system,
        "dep_red": sorted(p["name"] for p in dep_red),
        "data_red": data.get("n_red", 0) + data.get("n_degraded", 0),
    }, sort_keys=True)
    try:
        prev = json.loads(s3.get_object(
            Bucket=BUCKET, Key=ALERT_STATE_KEY)["Body"].read()).get("fp")
    except Exception:
        prev = None

    alerted = False
    if system == "red" and fingerprint != prev:
        lines = ["🚨 *JustHodl Fleet Monitor — SYSTEM RED*"]
        for p in dep_red:
            lines.append(f"• *{p['name']}*: {p['detail']}")
        if data.get("n_red"):
            lines.append(f"• {data['n_red']} data output(s) stale/empty")
            for it in data.get("red", [])[:5]:
                lines.append(f"   – {it['output']}: {it.get('issue')}")
        if data.get("n_degraded"):
            lines.append(f"• {data['n_degraded']} engine(s) reporting "
                          f"degraded output")
            for it in data.get("degraded", [])[:5]:
                lines.append(f"   – {it['output']}: {it.get('issue')}")
        alerted = send_telegram("\n".join(lines))

    if fingerprint != prev:
        try:
            s3.put_object(Bucket=BUCKET, Key=ALERT_STATE_KEY,
                          Body=json.dumps({"fp": fingerprint,
                                           "at": started.isoformat()}).encode(),
                          ContentType="application/json")
        except Exception as e:
            print(f"[fleet-monitor] alert-state write failed: {e}")

    out["alert_sent"] = alerted
    print(f"[fleet-monitor] system={system} "
          f"data:{data.get('green')}ok/{data_yellow}aging/{data_red}bad "
          f"deps:{len(dep_red)}down/{len(dep_yellow)}deg "
          f"lambdas={compute.get('n_functions')} alerted={alerted} "
          f"{out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "system_status": system,
        "data_bad": data_red, "deps_down": len(dep_red),
        "alert_sent": alerted})}
