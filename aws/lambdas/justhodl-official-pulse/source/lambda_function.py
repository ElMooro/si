"""justhodl-official-pulse v1.0.0 -- the WEEKLY heartbeat of
foreign official demand (probe ops 4863).
Marker: official-pulse v1.0.0

TIC tells us what reserve managers did six weeks ago; the Fed's
own H.4.1 tells us what they are doing THIS WEEK:
  * WLRRAFOIAL -- reverse repos with foreign official accounts
    (the "foreign repo pool"), weekly Wednesday level, PROVEN
    live 2026-08-12 = $357.4B (ops 4863).
  * Custody -- marketable Treasuries held at FRBNY for foreign
    officials.  FRED has churned this id (WMTSECL froze in 2012),
    so the engine RESOLVES the current series at runtime: test
    pinned candidates' last-obs currency, fall back to a FRED
    search, and if nothing is current publish custody as OMITTED
    with the why -- never a stale number presented as live.

Metrics per series: latest, 4w/13w/26w changes, z of the
13w-change vs 10y.  Then the DOLLAR LEG composite (STRESS-ONLY,
never a bullish signal) joins the monthly TIC anchor from
data/foreign-flows.json:
  leg1 official flows: holder_splits.lt_total.official z <= -1.0
  leg2 safe-haven rotation: signals.safe_haven z <= -1.5
  leg3 weekly custody drain: custody 13w-change z <= -1.5
status CALM(0) / WATCH(1) / STRESS(>=2 firing); unavailable legs
are named and excluded from the denominator, never counted as
calm.  Output data/official-pulse.json; banks under
data/providers/h41/.  Weekly Fri 09:00 UTC (H.4.1 lands Thu).
Consumed by justhodl-risk-gate as an advisory leg (ops 4864).
"""
import gzip
import json
import os
import time
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3

VERSION = "1.0.0"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
FRED_KEY = os.environ.get("FRED_KEY") or ""
OUT_KEY = "data/official-pulse.json"
FF_KEY = "data/foreign-flows.json"
BANK_PREFIX = "data/providers/h41/"
RRP_SID = "WLRRAFOIAL"
CUSTODY_CANDIDATES = ("WMTSEC", "WMTSECL")
CUSTODY_SEARCH = ("securities held in custody foreign official "
                  "marketable treasury")
CURRENT_DAYS = 30
Z_WIN = 520

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


def fred_json(path, **params):
    q = "&".join("%s=%s" % (k, urllib.request.quote(str(v)))
                 for k, v in params.items())
    url = ("https://api.stlouisfed.org/fred/%s?api_key=%s"
           "&file_type=json&%s" % (path, FRED_KEY, q))
    req = urllib.request.Request(
        url, headers={"User-Agent": "justhodl-official-pulse"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())


def fetch_obs(sid):
    """[(date, val_musd)] asc or (None, reason).  Seam."""
    try:
        j = fred_json("series/observations", series_id=sid,
                      observation_start="1989-01-01")
        rows = []
        for o in j.get("observations") or []:
            try:
                rows.append((o["date"], float(o["value"])))
            except (KeyError, TypeError, ValueError):
                continue
        if not rows:
            return None, "no_observations"
        return rows, None
    except Exception as e:  # noqa: BLE001
        return None, "fetch_error:%s" % str(e)[:60]


def search_weekly(text):
    """Candidate ids from FRED search, Weekly only.  Seam."""
    try:
        j = fred_json("series/search", search_text=text,
                      limit=15, order_by="popularity",
                      sort_order="desc")
        return [s["id"] for s in (j.get("seriess") or [])
                if str(s.get("frequency", "")).startswith("Week")]
    except Exception:  # noqa: BLE001
        return []


def is_current(rows, now):
    last = datetime.fromisoformat(rows[-1][0])
    return (now.replace(tzinfo=None) - last) <= timedelta(
        days=CURRENT_DAYS)


def resolve_custody(now):
    """(sid, rows) or (None, why).  Candidates first, search
    fallback, currency-tested -- never a stale bind."""
    tried = []
    for sid in list(CUSTODY_CANDIDATES) + [
            s for s in search_weekly(CUSTODY_SEARCH)
            if s not in CUSTODY_CANDIDATES][:6]:
        rows, err = fetch_obs(sid)
        if rows is None:
            tried.append("%s:%s" % (sid, err))
            continue
        if is_current(rows, now):
            return sid, rows
        tried.append("%s:stale(last %s)" % (sid, rows[-1][0]))
        time.sleep(0.4)
    return None, "no current series; tried " + "; ".join(
        tried)[:220]


def bank(sid, rows):
    key = BANK_PREFIX + sid + ".json"
    led = _g(key) or {"id": sid, "rows": {}}
    n0 = len(led["rows"])
    for d, v in rows:
        led["rows"][d] = v
    if len(led["rows"]) != n0:
        _put(key, led)
    return len(led["rows"])


def zlast(vals, win=Z_WIN):
    v = vals[-win:]
    if len(v) < 60:
        return None
    hist, last = v[:-1], v[-1]
    mu = sum(hist) / len(hist)
    sd = (sum((x - mu) ** 2 for x in hist)
          / max(1, len(hist) - 1)) ** 0.5
    if sd <= 1e-9:
        return None
    return round(max(-4.0, min(4.0, (last - mu) / sd)), 2)


def metrics(rows):
    vals = [v / 1000.0 for _, v in rows]        # musd -> $B
    out = {"latest_bn": round(vals[-1], 1),
           "latest_date": rows[-1][0],
           "n_obs": len(vals), "first": rows[0][0]}
    for w in (4, 13, 26):
        out["chg_%dw_bn" % w] = (round(vals[-1] - vals[-1 - w], 1)
                                 if len(vals) > w else None)
    chg13 = [vals[i] - vals[i - 13]
             for i in range(13, len(vals))]
    out["z_13wchg_10y"] = zlast(chg13) if chg13 else None
    return out


def dollar_leg(doc, ff):
    legs = {}
    hs = (((ff or {}).get("holder_splits") or {})
          .get("lt_total") or {})
    off_z = (hs.get("official") or {}).get("z_10y")
    legs["official_flows_monthly"] = {
        "z": off_z, "fires": (off_z is not None
                              and off_z <= -1.0),
        "rule": "TIC LT-total official flows z <= -1.0"}
    sh_z = (((ff or {}).get("signals") or {})
            .get("safe_haven") or {}).get("z_10y")
    legs["safe_haven_monthly"] = {
        "z": sh_z, "fires": (sh_z is not None and sh_z <= -1.5),
        "rule": "safe-haven rotation z <= -1.5"}
    cz = ((doc.get("custody") or {}).get("z_13wchg_10y")
          if (doc.get("custody") or {}).get("status") == "LIVE"
          else None)
    legs["custody_weekly"] = {
        "z": cz, "fires": (cz is not None and cz <= -1.5),
        "rule": "FRBNY custody 13w-change z <= -1.5",
        "available": cz is not None}
    avail = [k for k, v in legs.items()
             if v.get("z") is not None]
    firing = [k for k in avail if legs[k]["fires"]]
    status = ("STRESS" if len(firing) >= 2
              else "WATCH" if len(firing) == 1 else "CALM")
    return {"doctrine": "STRESS-ONLY: this leg can only warn, "
                        "never turn the gate bullish",
            "legs": legs, "available": len(avail),
            "legs_firing": len(firing), "firing": firing,
            "status": status}


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    doc = {"v": VERSION, "engine": "justhodl-official-pulse",
           "as_of": now.date().isoformat(),
           "generated_at": now.isoformat(),
           "doctrine": "weekly Fed-side pulse of foreign "
                       "official demand; TIC is the monthly "
                       "anchor, H.4.1 is the heartbeat",
           "diag": {}}
    if not FRED_KEY:
        doc["status"] = "INSUFFICIENT_DATA"
        doc["why"] = "FRED_KEY absent"
        _put(OUT_KEY, doc)
        return {"ok": False, "why": doc["why"]}
    rows, err = fetch_obs(RRP_SID)
    if rows is None:
        doc["status"] = "INSUFFICIENT_DATA"
        doc["why"] = "%s:%s" % (RRP_SID, err)
        _put(OUT_KEY, doc)
        return {"ok": False, "why": doc["why"]}
    doc["foreign_rrp"] = dict(metrics(rows), status="LIVE",
                              id=RRP_SID)
    doc["diag"]["bank_rrp_n"] = bank(RRP_SID, rows)
    c_sid, c_rows = resolve_custody(now)
    if c_sid:
        doc["custody"] = dict(metrics(c_rows), status="LIVE",
                              id=c_sid)
        doc["diag"]["bank_custody_n"] = bank(c_sid, c_rows)
    else:
        doc["custody"] = {"status": "OMITTED", "why": c_rows}
    ff = _g(FF_KEY)
    doc["ff_generated_at"] = (ff or {}).get("generated_at")
    doc["dollar_leg"] = dollar_leg(doc, ff)
    doc["status"] = "LIVE"
    doc["diag"]["runtime_ms"] = int((time.time() - t0) * 1000)
    _put(OUT_KEY, doc)
    return {"ok": True,
            "rrp_bn": doc["foreign_rrp"]["latest_bn"],
            "custody": doc["custody"]["status"],
            "dollar_leg": doc["dollar_leg"]["status"]}
