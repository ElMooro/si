"""justhodl-foreign-flows v1.0.0 -- US Foreign Portfolio Flows
(Treasury TIC via the 2026 CSLT dataset on FRED).
Marker: foreign-flows v1.0.0

Khalid doctrine: dollar view first. This engine adds the missing
organ -- where foreign money actually moves inside US markets --
publishing data/foreign-flows.json daily (21:30 UTC, after the 4pm ET
TIC release window) from six CSLT series:

  total   FORLTTOTALNET99996  net foreign purchases, all US LT secs
  treas   FORTREASNET69995    Treasuries (long + short term)
  equity  FORLTEQTYNET69995   US equities
  corp    FORLTCORPNET99996   US corporate bonds
  agency  FORLTAGCYNET99996   agency debt (FNMA/FHLMC/GNMA)
  tbills  FORSTTREASNET99996  short-term Treasuries

Derived signals (formulas exactly as specified in Khalid's research
doc, 2026-08-17):
  risk_appetite   = equity + corp + agency
  safe_haven      = treas - equity
  total_demand    = treas + agency + corp + equity
  official_private DEFERRED -- CSLT official/private split series ids
  not yet probed; never guessed (FRED-search probe op queued).

Discipline:
  * Series ids come from a post-cutoff research doc, so the birth op
    G0 live-verifies every id against FRED before this engine is
    trusted; at runtime any 404/short series is a NAMED exclusion.
    LIVE requires >=4 of 6; signals whose components are missing ship
    null with the reason, never synthesized.
  * FRED can retroactively window history (the ICE BofA lesson), so
    every series is banked under data/providers/tic-cslt/{sid}.json
    (Deny-Delete protected) with date-keyed union merge -- banked
    observations survive even if FRED later truncates.
  * Flows are TRANSACTIONS series -- never derived as holdings
    deltas (valuation-change trap); custodial-bias warning stamped in
    the output for country work later.
  * new_release flips true the day a newer month appears vs the
    prior doc; releases are appended to foreign-flows/releases.json
    as the alert substrate ("alert me on TIC release" -- Khalid).
"""
import gzip
import json
import os
import time
import urllib.request
from datetime import datetime, timezone

import boto3

VERSION = "1.0.0"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
FRED_KEY = os.environ.get("FRED_KEY") or ""
OUT_KEY = "data/foreign-flows.json"
BANK_FMT = "data/providers/tic-cslt/%s.json"
REL_KEY = "foreign-flows/releases.json"

SERIES = {
    "total": "FORLTTOTALNET99996",
    "treas": "FORTREASNET69995",
    "equity": "FORLTEQTYNET69995",
    "corp": "FORLTCORPNET99996",
    "agency": "FORLTAGCYNET99996",
    "tbills": "FORSTTREASNET99996",
}
SIGNALS = {
    "risk_appetite": ("equity", "corp", "agency"),
    "safe_haven": ("treas", "-equity"),
    "total_demand": ("treas", "agency", "corp", "equity"),
}
MIN_LIVE = 4
MIN_OBS = 60
Z_WINDOW = 120            # 10y of monthly obs

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


def fred_fetch(sid):
    """(units, [(date,val_musd)]) or (None, reason). Seam for the
    harness."""
    base = "https://api.stlouisfed.org/fred"
    try:
        req = urllib.request.Request(
            "%s/series?series_id=%s&api_key=%s&file_type=json"
            % (base, sid, FRED_KEY))
        with urllib.request.urlopen(req, timeout=30) as r:
            meta = json.loads(r.read())
        units = ((meta.get("seriess") or [{}])[0].get("units")
                 or "")
        req = urllib.request.Request(
            "%s/series/observations?series_id=%s&api_key=%s"
            "&file_type=json&observation_start=1985-01-01"
            % (base, sid, FRED_KEY))
        with urllib.request.urlopen(req, timeout=60) as r:
            obs = json.loads(r.read())
        rows = []
        for o in obs.get("observations") or []:
            try:
                rows.append((o["date"], float(o["value"])))
            except (KeyError, TypeError, ValueError):
                continue
        if not rows:
            return None, "no_observations"
        return units, rows
    except Exception as e:  # noqa: BLE001
        return None, "fetch_error:%s" % str(e)[:60]


def to_bn(v, units):
    u = (units or "").lower()
    if "billion" in u:
        return v
    return v / 1000.0        # CSLT ships in millions of dollars


def bank_merge(sid, rows):
    """Date-keyed union merge into the Deny-Delete provider bank;
    banked history survives FRED windowing. Returns (n_total,
    first_date, n_new)."""
    key = BANK_FMT % sid
    bank = _g(key) or {"id": sid, "source": "FRED CSLT",
                       "rows": {}}
    br = bank["rows"]
    n_new = 0
    for d, v in rows:
        if d not in br:
            n_new += 1
        br[d] = v
    if n_new:
        _put(key, bank)
    dates = sorted(br)
    return len(dates), dates[0] if dates else None, n_new


def zlast(vals):
    if len(vals) < 24:
        return None
    win = vals[-(Z_WINDOW + 1):]
    hist, last = win[:-1], win[-1]
    mu = sum(hist) / len(hist)
    sd = (sum((v - mu) ** 2 for v in hist)
          / max(1, len(hist) - 1)) ** 0.5
    if sd <= 1e-12:
        return None
    return round(max(-4.0, min(4.0, (last - mu) / sd)), 2)


def build():
    t0 = time.time()
    now = datetime.now(timezone.utc)
    doc = {"v": VERSION, "engine": "justhodl-foreign-flows",
           "as_of": now.date().isoformat(),
           "generated_at": now.isoformat(),
           "source": "US Treasury TIC via CSLT (FRED); monthly, "
                     "~1.5 month lag, 4pm ET releases",
           "flows_bn": {}, "signals": {}, "excluded": {},
           "warnings": [
               "flows are TIC transactions -- never compute as "
               "holdings deltas (valuation changes)",
               "country attribution carries custodial bias "
               "(BE/UK/LU/IE/KY are custody centers)"],
           "diag": {"bank": {}}}
    if not FRED_KEY:
        doc["status"] = "INSUFFICIENT_DATA"
        doc["why"] = "FRED_KEY absent -- refusing to publish"
        return doc
    comp = {}
    latest_month = None
    for name, sid in SERIES.items():
        units, rows = fred_fetch(sid)
        if units is None:
            doc["excluded"][name] = "%s:%s" % (sid, rows)
            continue
        if len(rows) < MIN_OBS:
            doc["excluded"][name] = "%s:too_short(n=%d)" % (sid,
                                                            len(rows))
            continue
        n_bank, first, n_new = bank_merge(sid, rows)
        doc["diag"]["bank"][sid] = {"n": n_bank, "first": first,
                                    "new": n_new}
        vals_bn = [to_bn(v, units) for _, v in rows]
        comp[name] = {"dates": [d for d, _ in rows],
                      "vals": vals_bn}
        d_last = rows[-1][0]
        latest_month = max(latest_month or d_last, d_last)
        doc["flows_bn"][name] = {
            "id": sid, "units_src": units,
            "latest": round(vals_bn[-1], 1),
            "latest_month": d_last,
            "sum_3m": round(sum(vals_bn[-3:]), 1),
            "sum_12m": round(sum(vals_bn[-12:]), 1),
            "z_10y": zlast(vals_bn),
            "n_obs": len(vals_bn), "first": rows[0][0]}
    if len(comp) < MIN_LIVE:
        doc["status"] = "INSUFFICIENT_DATA"
        doc["why"] = ("only %d/6 CSLT series resolved -- see "
                      "excluded; ids from the research doc must be "
                      "re-probed" % len(comp))
        return doc
    doc["status"] = "LIVE"
    doc["latest_month"] = latest_month

    for sig, parts in SIGNALS.items():
        vals = None
        missing = []
        for p in parts:
            neg = p.startswith("-")
            nm = p.lstrip("-")
            if nm not in comp:
                missing.append(nm)
                continue
            v = comp[nm]["vals"]
            if vals is None:
                vals = [0.0] * len(v)
            n = min(len(vals), len(v))
            vals = [(vals[len(vals) - n + i]
                     + (-v[len(v) - n + i] if neg
                        else v[len(v) - n + i]))
                    for i in range(n)]
        if missing or vals is None:
            doc["signals"][sig] = {"value": None,
                                   "why": "missing components: %s"
                                   % ",".join(missing)}
            continue
        doc["signals"][sig] = {
            "latest_bn": round(vals[-1], 1),
            "sum_3m_bn": round(sum(vals[-3:]), 1),
            "sum_12m_bn": round(sum(vals[-12:]), 1),
            "z_10y": zlast(vals),
            "formula": " + ".join(parts).replace("+ -", "- ")}
    doc["signals"]["official_private"] = {
        "value": None,
        "why": "DEFERRED -- official/private CSLT ids not probed "
               "yet; never guessed (FRED-search probe queued)"}

    prev = _g(OUT_KEY) or {}
    doc["new_release"] = bool(prev.get("latest_month")
                              and latest_month
                              and latest_month
                              > prev["latest_month"])
    if doc["new_release"]:
        doc["alert"] = ("TIC/CSLT new month %s ingested (prev %s): "
                        "total %+.1fB, treas %+.1fB, equity %+.1fB"
                        % (latest_month, prev.get("latest_month"),
                           doc["flows_bn"].get("total",
                                               {}).get("latest", 0),
                           doc["flows_bn"].get("treas",
                                               {}).get("latest", 0),
                           doc["flows_bn"].get("equity",
                                               {}).get("latest",
                                                       0)))
        rel = _g(REL_KEY) or {"rows": []}
        rel["rows"].append({"detected": doc["as_of"],
                            "month": latest_month,
                            "alert": doc["alert"]})
        rel["rows"] = rel["rows"][-240:]
        _put(REL_KEY, rel)
    doc["diag"]["runtime_ms"] = int((time.time() - t0) * 1000)
    return doc


def lambda_handler(event, context):
    doc = build()
    _put(OUT_KEY, doc)
    return {"ok": doc.get("status") == "LIVE", "v": VERSION,
            "status": doc.get("status"),
            "latest_month": doc.get("latest_month"),
            "new_release": doc.get("new_release"),
            "n_series": len(doc.get("flows_bn") or {})}
