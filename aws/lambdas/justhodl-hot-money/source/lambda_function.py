"""justhodl-hot-money v1.0.0 -- daily foreign flows in liquid
markets, split into its own engine per Khalid directive 2026-08-17
("hot money and capital flow each into their own engine").
Marker: hot-money v1.1.0

Doctrine (Khalid's research doc): FDI into a copper mine and a
hedge fund buying government bonds are different animals -- macro
capital flows (BOP/TIC, slow) and HOT MONEY (daily exchange
foreign buy/sell, fast) must never be blended.  justhodl-
global-flows keeps the macro layer; THIS engine owns the daily
layer and the shared ledgers.

v1.0 countries:
  taiwan  TWSE rwd/en/fund/BFI82U (keyless): foreign net = sum of
          Difference over Items containing "Foreign"; paced
          backfill (TWSE throttles bursts -- 2.2s, 45 attempts per
          invoke, {"twse_backfill_days": N} event); ledger
          data/providers/twse/bfi82u-foreign.json (TAKEN OVER from
          global-flows v1.1 -- union-append, never overwrite).
  korea   DEFERRED: KRX daily investor data needs API keys
          (pending Khalid).
Metrics per country: latest, 5/20/60d sums, z_60d -- honest nulls
while ledgers accrue.  Daily 09:50 UTC (after TW close).
"""
import gzip
import json
import os
import time
from datetime import datetime, timezone, timedelta

import boto3
import urllib.request

VERSION = "1.1.0"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/hot-money.json"
TWSE_LEDGER = "data/providers/twse/bfi82u-foreign.json"
TPEX_LEDGER = "data/providers/tpex/insti-foreign.json"
TPEX_URL = ("https://www.tpex.org.tw/openapi/v1/"
            "tpex_3insti_summary")
TWSE_URL = ("https://www.twse.com.tw/rwd/en/fund/BFI82U"
            "?response=json")
BACKFILL_SLEEP = 2.2
BACKFILL_CAP = 45
MIN_Q = 24

DEFERRED = {"korea": {"status": "DEFERRED",
                      "why": "KRX daily investor data requires "
                             "API keys -- pending Khalid",
                      "specialty": "memory/electronics"}}

s3 = boto3.client("s3")


def _g(key):
    try:
        raw = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return json.loads(raw)
    except Exception:  # noqa: BLE001
        return None


def _put(key, obj):
    s3.put_object(Bucket=BUCKET, Key=key,
                  Body=json.dumps(obj, separators=(",", ":")).encode(),
                  ContentType="application/json")


def zlast(vals):
    if len(vals) < MIN_Q:
        return None
    hist, last = vals[:-1], vals[-1]
    mu = sum(hist) / len(hist)
    sd = (sum((v - mu) ** 2 for v in hist)
          / max(1, len(hist) - 1)) ** 0.5
    if sd <= 1e-12:
        return None
    return round(max(-4.0, min(4.0, (last - mu) / sd)), 2)


def twse_fetch(day=None):
    """(yyyymmdd, foreign_net_twd) or (None, reason).  Seam."""
    url = TWSE_URL + ("&dayDate=%s&type=day" % day if day else "")
    try:
        req = urllib.request.Request(
            url, headers={"User-Agent": "justhodl-hot-money"})
        with urllib.request.urlopen(req, timeout=45) as r:
            j = json.loads(r.read())
        if j.get("stat") != "OK" or not j.get("data"):
            return None, "stat=%s" % j.get("stat")
        net = 0.0
        found = False
        for row in j["data"]:
            if "foreign" in str(row[0]).lower():
                found = True
                net += float(str(row[3]).replace(",", ""))
        if not found:
            return None, "no foreign rows"
        return str(j.get("date")), net
    except Exception as e:  # noqa: BLE001
        return None, "fetch_error:%s" % str(e)[:60]


def tpex_fetch():
    """(yyyymmdd, foreign_net_twd) or (None, reason).  Binds the
    ops-4873 dump: row Investor == 外資及陸資合計, Net in NT$,
    Date in ROC calendar (115yy -> +1911).  Seam."""
    try:
        req = urllib.request.Request(
            TPEX_URL, headers={"User-Agent": "justhodl-hot-money",
                               "Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=45) as r:
            j = json.loads(r.read())
        row = None
        for x in j if isinstance(j, list) else []:
            inv = str(x.get("Investor", ""))
            if "外資及陸資合計" \
                    in inv:
                row = x
                break
        if row is None:
            return None, "foreign-total row absent"
        d = str(row.get("Date", ""))
        if len(d) != 7 or not d.isdigit():
            return None, "date shape %r" % d[:10]
        greg = "%04d%s" % (int(d[:3]) + 1911, d[3:])
        net = float(str(row.get("Net", "")).replace(",", ""))
        return greg, net
    except Exception as e:  # noqa: BLE001
        return None, "fetch_error:%s" % str(e)[:60]


def windows(nets):
    out = {}
    for w in (5, 20, 60):
        out["sum_%dd_bn" % w] = (round(sum(nets[-w:]) / 1e9, 2)
                                 if len(nets) >= w else None)
    return out


def taiwan(event):
    led = _g(TWSE_LEDGER) or {"source": "TWSE BFI82U foreign net "
                              "(TWD)", "rows": {}}
    n_before = len(led["rows"])
    attempts = 0
    n_backfill = 0
    try:
        bf = int((event or {}).get("twse_backfill_days") or 0)
    except (TypeError, ValueError):
        bf = 0
    if bf > 0:
        d0 = datetime.now(timezone.utc)
        for k in range(min(bf, 120)):
            if attempts >= BACKFILL_CAP:
                break
            day = (d0 - timedelta(days=k)).strftime("%Y%m%d")
            if day in led["rows"]:
                continue
            attempts += 1
            dd, net = twse_fetch(day)
            if dd is not None and dd == day:
                led["rows"][day] = net
                n_backfill += 1
            time.sleep(BACKFILL_SLEEP)
    dd, net = twse_fetch()
    fetch_why = None if dd is not None else net
    if dd is not None:
        led["rows"][dd] = net
    if len(led["rows"]) != n_before or n_backfill:
        _put(TWSE_LEDGER, led)
    days = sorted(led["rows"])
    nets = [led["rows"][d] for d in days]
    tw = {"status": "LIVE" if days else "MISSING",
          "unit": "TWD bn", "specialty": "semiconductors",
          "source": "TWSE BFI82U (keyless, daily)",
          "ledger_days": len(days)}
    if fetch_why:
        tw["today_fetch"] = fetch_why
    if days:
        tw["latest_day"] = days[-1]
        tw["latest_bn"] = round(nets[-1] / 1e9, 2)
        for w in (5, 20, 60):
            tw["sum_%dd_bn" % w] = (round(sum(nets[-w:]) / 1e9, 2)
                                    if len(nets) >= w else None)
        tw["why_partial"] = (None if len(nets) >= 60 else
                             "ledger accruing n=%d" % len(nets))
        zs = [n / 1e9 for n in nets[-61:]]
        tw["z_60d"] = zlast(zs) if len(zs) >= MIN_Q else None
    if bf > 0:
        tw["backfilled"] = n_backfill
        tw["backfill_attempts"] = attempts

    led2 = _g(TPEX_LEDGER) or {"source": "TPEx 3insti summary "
                               "foreign+mainland net (NT$)",
                               "rows": {}}
    n2_before = len(led2["rows"])
    d2, net2 = tpex_fetch()
    otc = {"status": "MISSING", "unit": "TWD bn",
           "source": "TPEx openapi 3insti_summary (keyless, "
                     "daily, OTC board)"}
    if d2 is not None:
        led2["rows"][d2] = net2
    else:
        otc["today_fetch"] = net2
    if len(led2["rows"]) != n2_before:
        _put(TPEX_LEDGER, led2)
    days2 = sorted(led2["rows"])
    if days2:
        nets2 = [led2["rows"][x] for x in days2]
        otc.update({"status": "LIVE",
                    "ledger_days": len(days2),
                    "latest_day": days2[-1],
                    "latest_bn": round(nets2[-1] / 1e9, 2)})
        otc.update(windows(nets2))
        otc["why_partial"] = (None if len(nets2) >= 60 else
                              "ledger accruing n=%d (no "
                              "historical endpoint -- honest "
                              "accrual)" % len(nets2))
        zs2 = [n / 1e9 for n in nets2[-61:]]
        otc["z_60d"] = zlast(zs2) if len(zs2) >= MIN_Q else None
    tw["otc"] = otc
    comb = {"status": "MISSING"}
    if tw.get("latest_day") and otc.get("latest_day"):
        if tw["latest_day"] == otc["latest_day"]:
            comb = {"status": "LIVE",
                    "latest_day": tw["latest_day"],
                    "latest_bn": round(tw["latest_bn"]
                                       + otc["latest_bn"], 2),
                    "note": "listed (TWSE) + OTC (TPEx), "
                            "same-day identity; window sums "
                            "stay per-board until ledgers "
                            "align in depth"}
        else:
            comb = {"status": "MISALIGNED",
                    "why": "listed %s vs otc %s"
                           % (tw["latest_day"],
                              otc["latest_day"])}
    tw["combined"] = comb
    return tw


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    doc = {"v": VERSION, "engine": "justhodl-hot-money",
           "as_of": now.date().isoformat(),
           "generated_at": now.isoformat(),
           "doctrine": "daily exchange foreign flow -- kept "
                       "strictly apart from macro BOP capital "
                       "(partial blends are lies)",
           "countries": {}, "deferred": dict(DEFERRED),
           "diag": {}}
    doc["countries"]["taiwan"] = taiwan(event or {})
    live = any(c.get("status") == "LIVE"
               for c in doc["countries"].values())
    doc["status"] = "LIVE" if live else "INSUFFICIENT_DATA"
    doc["diag"]["runtime_ms"] = int((time.time() - t0) * 1000)
    _put(OUT_KEY, doc)
    tw = doc["countries"]["taiwan"]
    return {"ok": live, "status": doc["status"],
            "tw_ledger_days": tw.get("ledger_days"),
            "tw_latest": tw.get("latest_bn")}
