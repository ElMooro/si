"""justhodl-cryptoquant — ON-CHAIN VENDOR ADAPTER (the empty seat, filled).

Dedicated ingestion service for CryptoQuant Professional (the fleet way:
vendor-isolated auth/throttle/backfill, spec-driven endpoints so API drift is
a config fix, normalized feed for every downstream consumer).

  AUTH    SSM SecureString /justhodl/cryptoquant/token (Bearer). No token ->
          honest GATED feed (real-data rule: never fake).
  SPEC    data/config/cryptoquant-spec.json — per-metric {name, path, params,
          value_keys candidates}; resolved key self-heals back into spec.
  BACKFILL first armed run pulls ~5y daily per metric (paged), then daily tail.
  OUTPUT  data/cryptoquant-onchain.json:
            metrics: {name: {value, z365, wow, pctl_1y, as_of}}
            composite_onchain: mean risk-direction z (netflow+, whale+,
            mvrv+, sopr+, stablecoin_reserve- => risk), read.
          History: data/history/cryptoquant.json {name: {date: value}} 2000d.
  STATUS  PROVISIONAL — families graded by signal-scorecard excess-vs-BTC
          before effective_trust admits anything into crypto_risk_score.
  CRON    daily 21:05 UTC (ahead of crypto-exchange-flows 21:55).
"""
import json, time, urllib.request, urllib.error, statistics
from datetime import datetime, timezone, timedelta
import boto3

BUCKET, OUT = "justhodl-dashboard-live", "data/cryptoquant-onchain.json"
SPEC_KEY, HIST_KEY = "data/config/cryptoquant-spec.json", "data/history/cryptoquant.json"
BASE = "https://api.cryptoquant.com/v1"
s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")

DEFAULT_SPEC = {"base": BASE, "metrics": [
    {"name": "btc_exchange_netflow", "path": "/btc/exchange-flows/netflow",
     "params": {"exchange": "all_exchange", "window": "day"},
     "value_keys": ["netflow_total", "netflow", "value"], "risk_sign": +1},
    {"name": "btc_exchange_reserve", "path": "/btc/exchange-flows/reserve",
     "params": {"exchange": "all_exchange", "window": "day"},
     "value_keys": ["reserve", "value"], "risk_sign": +1},
    {"name": "eth_exchange_reserve", "path": "/eth/exchange-flows/reserve",
     "params": {"exchange": "all_exchange", "window": "day"},
     "value_keys": ["reserve", "value"], "risk_sign": +1},
    {"name": "btc_mpi", "path": "/btc/flow-indicator/mpi",
     "params": {"window": "day"}, "value_keys": ["mpi", "value"], "risk_sign": +1},
    {"name": "btc_whale_ratio", "path": "/btc/flow-indicator/exchange-whale-ratio",
     "params": {"exchange": "all_exchange", "window": "day"},
     "value_keys": ["exchange_whale_ratio", "whale_ratio", "value"], "risk_sign": +1},
    {"name": "btc_mvrv", "path": "/btc/market-indicator/mvrv",
     "params": {"window": "day"}, "value_keys": ["mvrv", "value"], "risk_sign": +1},
    {"name": "btc_sopr", "path": "/btc/market-indicator/sopr",
     "params": {"window": "day"}, "value_keys": ["sopr", "value"], "risk_sign": +1},
    {"name": "stablecoin_exchange_reserve", "path": "/stablecoin/exchange-flows/reserve",
     "params": {"exchange": "all_exchange", "window": "day"},
     "value_keys": ["reserve_usd", "reserve", "value"], "risk_sign": -1},
]}

def _j(k, d=None):
    try: return json.loads(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read())
    except Exception: return d

def _token():
    try:
        return ssm.get_parameter(Name="/justhodl/cryptoquant/token",
                                 WithDecryption=True)["Parameter"]["Value"].strip()
    except Exception:
        return None

def _get(url, tok, timeout=30):
    req = urllib.request.Request(url, headers={"Authorization": "Bearer " + tok,
                                               "User-Agent": "JustHodl/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as he:
        body = (he.read() or b"")[:180].decode("utf-8", "ignore")
        raise RuntimeError("HTTP %s %s :: %s" % (he.code, url.split("?")[0][-60:], body))

def _series(spec_m, tok, frm=None, limit=1000, from_format="plain"):
    q = dict(spec_m["params"]); q["limit"] = str(limit)
    if frm and from_format != "none":
        q["from"] = (frm + "T000000") if from_format == "T" else frm
    url = BASE + spec_m["path"] + "?" + "&".join("%s=%s" % kv for kv in q.items())
    doc = _get(url, tok)
    rows = ((doc or {}).get("result") or {}).get("data") or (doc or {}).get("data") or []
    out, vk_used = {}, None
    for r in rows:
        d = str(r.get("date") or r.get("datetime") or "")[:10]
        if not d: continue
        for vk in spec_m["value_keys"]:
            v = r.get(vk)
            if isinstance(v, (int, float)):
                out[d] = float(v); vk_used = vk; break
    return out, vk_used

def lambda_handler(event=None, context=None):
    event = event or {}
    spec = _j(SPEC_KEY) or {}
    if not spec.get("metrics"):
        spec = dict(DEFAULT_SPEC)
        s3.put_object(Bucket=BUCKET, Key=SPEC_KEY, Body=json.dumps(spec, indent=1).encode(),
                      ContentType="application/json")
    tok = _token()
    now = datetime.now(timezone.utc)
    if not tok:
        doc = {"engine": "justhodl-cryptoquant", "version": "1.0.0",
               "generated_at": now.isoformat(timespec="seconds"),
               "status": "GATED_PENDING_KEY",
               "note": "SSM /justhodl/cryptoquant/token absent — armed, will self-activate next run.",
               "armed_metrics": [m["name"] for m in spec["metrics"]]}
        s3.put_object(Bucket=BUCKET, Key=OUT, Body=json.dumps(doc, allow_nan=False).encode(),
                      ContentType="application/json", CacheControl="public, max-age=60")
        return {"ok": True, "status": "GATED_PENDING_KEY", "armed": len(spec["metrics"])}

    hist = _j(HIST_KEY, {}) or {}
    backfill = bool(event.get("backfill")) or not hist
    metrics, errors = {}, []
    for m in spec["metrics"]:
        name = m["name"]
        try:
            ser = dict(hist.get(name) or {})
            ffmt = spec.get("from_format", "plain")
            if backfill and len(ser) < 1200:
                frm = (now - timedelta(days=365 * 5 + 30)).strftime("%Y%m%d")
                for _page in range(3 if ffmt != "none" else 1):
                    got, vk = _series(m, tok, frm=frm, limit=1000, from_format=ffmt)
                    if not got: break
                    ser.update(got)
                    last = max(got)
                    if len(got) < 990: break
                    frm = (datetime.strptime(last, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y%m%d")
                    time.sleep(0.35)
            else:
                got, vk = _series(m, tok, limit=30)
                ser.update(got)
            if vk and m["value_keys"][0] != vk:
                m["resolved_key"] = vk
            ser = dict(sorted(ser.items())[-2000:])
            if len(ser) < 60: raise RuntimeError("series thin: %d" % len(ser))
            hist[name] = ser
            dates = sorted(ser); vals = [ser[d] for d in dates]
            w = vals[-365:]
            z = round((vals[-1] - statistics.mean(w)) / statistics.stdev(w), 2) \
                if len(w) >= 90 and statistics.stdev(w) > 0 else None
            y = vals[-365:]
            pctl = round(100 * sum(1 for x in y if x <= vals[-1]) / len(y)) if len(y) >= 90 else None
            metrics[name] = {"value": round(vals[-1], 4), "z365": z, "pctl_1y": pctl,
                             "wow": round(vals[-1] - vals[-8], 4) if len(vals) >= 8 else None,
                             "as_of": dates[-1], "risk_sign": m.get("risk_sign", 0)}
            time.sleep(1.15)
        except Exception as e:
            if "429" in str(e):
                print("[cq] %s -> 429, retrying in 16s" % name)
                time.sleep(16)
                try:
                    got, vk = _series(m, tok, limit=1000, from_format=spec.get("from_format", "none"))
                    ser = dict(sorted({**(hist.get(name) or {}), **got}.items())[-2000:])
                    if len(ser) >= 60:
                        hist[name] = ser
                        dates = sorted(ser); vals = [ser[d] for d in dates]
                        w = vals[-365:]
                        z = round((vals[-1] - statistics.mean(w)) / statistics.stdev(w), 2) if len(w) >= 90 and statistics.stdev(w) > 0 else None
                        pctl = round(100 * sum(1 for x in w if x <= vals[-1]) / len(w)) if len(w) >= 90 else None
                        metrics[name] = {"value": round(vals[-1], 4), "z365": z, "pctl_1y": pctl,
                                         "wow": round(vals[-1] - vals[-8], 4) if len(vals) >= 8 else None,
                                         "as_of": dates[-1], "risk_sign": m.get("risk_sign", 0)}
                        continue
                except Exception as e2:
                    e = e2
            errors.append({"metric": name, "err": str(e)[:110]})  # ops 2742
            print("[cq] %s -> %s" % (name, str(e)[:110]))
    min_hist = 900 if spec.get("from_format") == "none" else 1200
    assert len(metrics) >= 4, "too few metrics live: %s errors=%s" % (list(metrics), errors)
    s3.put_object(Bucket=BUCKET, Key=SPEC_KEY, Body=json.dumps(spec, indent=1).encode(),
                  ContentType="application/json")
    s3.put_object(Bucket=BUCKET, Key=HIST_KEY, Body=json.dumps(hist).encode(),
                  ContentType="application/json")
    rz = [m["z365"] * m["risk_sign"] for m in metrics.values()
          if m["z365"] is not None and m["risk_sign"]]
    comp = round(statistics.mean(rz), 2) if rz else None
    stale = max((now - datetime.strptime(m["as_of"], "%Y-%m-%d").replace(tzinfo=timezone.utc)).days
                for m in metrics.values())
    doc = {"engine": "justhodl-cryptoquant", "version": "1.0.0",
           "generated_at": now.isoformat(timespec="seconds"), "status": "LIVE",
           "grading": "PROVISIONAL — scorecard excess-vs-BTC gates admission to crypto_risk_score",
           "metrics": metrics, "composite_onchain_risk_z": comp,
           "read": ("On-chain risk composite %+0.2fz (netflow/whale/valuation up = distribution risk; "
                    "stablecoin exchange reserves down = less dry powder)" % comp) if comp is not None else None,
           "max_staleness_days": stale, "errors": errors or None,
           "source": "CryptoQuant Professional API, daily, entity-labeled; spec-driven adapter"}
    s3.put_object(Bucket=BUCKET, Key=OUT, Body=json.dumps(doc, separators=(",", ":"), allow_nan=False).encode(),
                  ContentType="application/json", CacheControl="public, max-age=60")
    return {"ok": True, "status": "LIVE", "n_metrics": len(metrics),
            "composite": comp, "staleness_d": stale, "errors": len(errors)}
