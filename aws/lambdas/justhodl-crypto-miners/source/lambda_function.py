"""justhodl-crypto-miners · v1.0 — Bitcoin miner economics: hash ribbons + Puell multiple.

Miner capitulation has historically marked major BTC bottoms. Two classic, *testable* signals,
a domain nothing else in the fleet touches:

  - HASH RIBBONS (Capriole / Charles Edwards): 30-day MA vs 60-day MA of network hash rate.
        30dMA < 60dMA  → miners capitulating (rigs going offline).
        30dMA crosses back ABOVE 60dMA → capitulation ending — historically a strong BUY.
  - PUELL MULTIPLE: daily miner revenue (USD) / its 365-day MA.
        < 0.5 = deep value (miners bleeding, historically a buy zone);
        > 4   = overheated (historically a distribution zone).
  - DIFFICULTY trend as a miner-commitment cross-check.

SOURCE: blockchain.com charts API (free, daily, ~3y). Both signals are POINT-IN-TIME
event-studied against forward BTC return (the MAs are trailing, so there is no look-ahead),
exactly like DVOL, and registered in the central FDR ledger for honest live grading.

NOTE: Puell here is revenue-based (issuance + fees) rather than issuance-only; at cycle lows —
where the buy signal matters — fees are negligible, so the two are equivalent there.
"""
import json
import time
import urllib.request
from datetime import date, datetime, timezone

import boto3

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/crypto-miners.json"
HIST_KEY = "data/crypto-miners-history.json"
BC = "https://api.blockchain.info/charts/"


def _get(url, timeout=40):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def chart(name, years=3):
    d = _get(BC + name + "?timespan=%dyears&format=json&sampled=false" % years)
    out = {}
    for p in d.get("values", []):
        iso = datetime.fromtimestamp(p["x"], tz=timezone.utc).date().isoformat()
        out[iso] = p["y"]  # last value per day wins
    return out


def trailing_ma(dmap, dates, w):
    """{date: trailing w-day mean} for dates with >= w history."""
    out = {}
    vals = [dmap[d] for d in dates]
    run = 0.0
    for i in range(len(dates)):
        run += vals[i]
        if i >= w:
            run -= vals[i - w]
        if i + 1 >= w:
            out[dates[i]] = run / w
    return out


def _mean(xs):
    xs = [x for x in xs if x is not None]
    return round(sum(xs) / len(xs), 1) if xs else None


def lambda_handler(event, context):
    t0 = time.time()
    out = {"generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "version": "1.0"}
    diag = []
    try:
        hr = chart("hash-rate")
        price = chart("market-price")
        rev = chart("miners-revenue")
        diff = chart("difficulty")
    except Exception as e:
        out["_err"] = "fetch:" + str(e)[:100]
        s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out).encode(), ContentType="application/json")
        return {"statusCode": 500, "body": out["_err"]}

    # forward BTC helper (clean target = market price)
    pdates = sorted(price)
    ppos = {d: i for i, d in enumerate(pdates)}

    def fwd(d, h):
        i = ppos.get(d)
        if i is None or i + h >= len(pdates):
            return None
        return (price[pdates[i + h]] / price[d] - 1) * 100

    # ── HASH RIBBONS ──
    hribbon = {}
    es_ribbon = {}
    hdates = sorted(hr)
    ma30 = trailing_ma(hr, hdates, 30)
    ma60 = trailing_ma(hr, hdates, 60)
    cdates = [d for d in hdates if d in ma30 and d in ma60]
    if cdates:
        def sign(d):
            return 1 if ma30[d] >= ma60[d] else -1
        cur = cdates[-1]
        in_cap = ma30[cur] < ma60[cur]
        # cross detection — a TRUE buy fires only when 30dMA crosses back above 60dMA
        # after a *sustained* capitulation (>=10d below). Naive "any cross-up" catches MA
        # whipsaw noise (the real hash-ribbon buy is rare — a few times per cycle).
        last_cross = None
        cross_dir = None
        crossups = []        # all up-crosses (noisy)
        crossups_true = []   # up-crosses ending a real capitulation
        cap_run = 0          # consecutive days with 30dMA < 60dMA
        for i in range(1, len(cdates)):
            if sign(cdates[i]) != sign(cdates[i - 1]):
                last_cross = cdates[i]
                cross_dir = "up" if sign(cdates[i]) > 0 else "down"
                if sign(cdates[i]) > 0:
                    crossups.append(cdates[i])
                    if cap_run >= 10:
                        crossups_true.append(cdates[i])
            cap_run = cap_run + 1 if sign(cdates[i]) < 0 else 0
        days_since = ((date.fromisoformat(cur) - date.fromisoformat(last_cross)).days
                      if last_cross else None)
        last_true_buy = crossups_true[-1] if crossups_true else None
        days_since_buy = ((date.fromisoformat(cur) - date.fromisoformat(last_true_buy)).days
                          if last_true_buy else None)
        buy_active = days_since_buy is not None and days_since_buy <= 60
        state = "CAPITULATION" if in_cap else ("RECOVERY/BUY" if buy_active else "HEALTHY")
        # current consecutive capitulation length
        cur_cap_run = 0
        for d in reversed(cdates):
            if sign(d) < 0:
                cur_cap_run += 1
            else:
                break
        hribbon = {
            "hash_rate_eh": round(hr[cur] / 1e6, 1),
            "ma30_eh": round(ma30[cur] / 1e6, 1), "ma60_eh": round(ma60[cur] / 1e6, 1),
            "state": state, "in_capitulation": in_cap,
            "days_in_capitulation": cur_cap_run if in_cap else 0,
            "last_cross": {"date": last_cross, "dir": cross_dir, "days_since": days_since},
            "last_true_buy": {"date": last_true_buy, "days_since": days_since_buy},
            "buy_active": buy_active,
            "n_true_buys_3y": len(crossups_true), "n_naive_crossups_3y": len(crossups),
        }
        # event study uses the capitulation-GATED true buys (the real signal)
        for h in (30, 90, 180):
            sig = _mean([fwd(d, h) for d in crossups_true])
            base = _mean([fwd(d, h) for d in pdates])
            es_ribbon["fwd%dd" % h] = {
                "buy_mean": sig, "baseline_mean": base,
                "edge_pp": round(sig - base, 1) if (sig is not None and base is not None) else None,
                "n_buys": len(crossups_true)}

    # ── PUELL MULTIPLE ──
    puell = {}
    es_puell = {}
    rdates = sorted(rev)
    ma365 = trailing_ma(rev, rdates, 365)
    if rdates and rdates[-1] in ma365 and ma365[rdates[-1]]:
        pc = rdates[-1]
        pv = rev[pc] / ma365[pc]
        zone = ("DEEP VALUE (buy)" if pv < 0.5 else "UNDERVALUED" if pv < 1.0
                else "NEUTRAL" if pv < 2.2 else "ELEVATED" if pv < 4.0 else "OVERHEATED (sell)")
        puell = {"value": round(pv, 2), "zone": zone,
                 "revenue_usd_m": round(rev[pc] / 1e6, 1), "ma365_usd_m": round(ma365[pc] / 1e6, 1)}
        pser = [(d, rev[d] / ma365[d]) for d in rdates if d in ma365 and ma365[d]]
        for h in (30, 90, 180):
            lo = _mean([fwd(d, h) for d, p in pser if p <= 0.6])    # deep value
            hi = _mean([fwd(d, h) for d, p in pser if p >= 2.5])    # overheated
            es_puell["fwd%dd" % h] = {
                "low_puell_mean": lo, "high_puell_mean": hi,
                "edge_low_minus_high_pp": round(lo - hi, 1) if (lo is not None and hi is not None) else None,
                "n_low": sum(1 for _, p in pser if p <= 0.6), "n_high": sum(1 for _, p in pser if p >= 2.5)}

    # ── DIFFICULTY trend ──
    difficulty = {}
    ddates = sorted(diff)
    if len(ddates) > 14 and diff[ddates[-15]]:
        difficulty = {"value": diff[ddates[-1]],
                      "chg_14d_pct": round((diff[ddates[-1]] / diff[ddates[-15]] - 1) * 100, 2)}

    # ── verdicts on the event studies ──
    def verdict_edge(e, strong=8, ok=3):
        if e is None:
            return "INSUFFICIENT"
        return ("CONFIRMED_STRONG" if e >= strong else "CONFIRMED" if e >= ok
                else "INVERTED" if e <= -ok else "INCONCLUSIVE")
    rib_e = (es_ribbon.get("fwd90d") or {}).get("edge_pp")
    pue_e = (es_puell.get("fwd90d") or {}).get("edge_low_minus_high_pp")
    es_ribbon["verdict"] = verdict_edge(rib_e)
    es_ribbon["standing"] = "DIAGNOSTIC"
    es_puell["verdict"] = verdict_edge(pue_e)
    es_puell["standing"] = "DIAGNOSTIC"

    # ── combined read ──
    bits = []
    if hribbon.get("state"):
        bits.append("hash ribbons: " + hribbon["state"])
    if puell.get("zone"):
        bits.append("Puell %.2f (%s)" % (puell["value"], puell["zone"]))
    read = " · ".join(bits) if bits else None

    out["hash_ribbons"] = hribbon
    out["puell"] = puell
    out["difficulty"] = difficulty
    out["event_study"] = {"hash_ribbon": es_ribbon, "puell": es_puell}
    out["interpretation"] = read

    # ── self-history ──
    try:
        try:
            hist = json.loads(s3.get_object(Bucket=BUCKET, Key=HIST_KEY)["Body"].read())
        except Exception:
            hist = {"series": []}
        ser = hist.get("series", [])
        today = datetime.now(timezone.utc).date().isoformat()
        snap = {"date": today, "hash_rate_eh": hribbon.get("hash_rate_eh"),
                "ribbon_state": hribbon.get("state"), "puell": puell.get("value"),
                "diff_chg_14d": difficulty.get("chg_14d_pct")}
        ser = [x for x in ser if x.get("date") != today] + [snap]
        ser = ser[-730:]
        hist["series"] = ser
        hist["updated_at"] = out["generated_at"]
        s3.put_object(Bucket=BUCKET, Key=HIST_KEY, Body=json.dumps(hist, default=str).encode(),
                      ContentType="application/json")
        out["history_n"] = len(ser)
    except Exception as e:
        diag.append("hist:" + str(e)[:60])

    out["duration_s"] = round(time.time() - t0, 1)
    out["sources"] = ["blockchain.com charts: hash-rate / miners-revenue / difficulty / market-price"]
    if diag:
        out["_diag"] = diag
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    return {"statusCode": 200, "body": json.dumps({"ribbon": hribbon.get("state"),
                                                    "puell": puell.get("value"),
                                                    "puell_zone": puell.get("zone")})}
