"""justhodl-foreign-flows v1.1.0 -- US Foreign Portfolio Flows
(Treasury TIC via the 2026 CSLT dataset on FRED).
Marker: foreign-flows v1.1.1

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
  official_private = private - official (holder-suffix legend proven
  by ops 4827-4828 arithmetic identity: 99996=all, 99990=Foreign
  Official, 99991=private; May LT-Treasury reconciled gap 0.0).
  Every split family is RECONCILED AT RUNTIME (|all-(off+priv)| <=
  0.2B on the latest month) or excluded as UNRECONCILED -- never
  trusted on pattern alone. st_treas official ships as its own line
  (officials sold -61B of T-bills in May while private bought).
  Country block: LT-Treasury holdings for CN/JP/UK/BE/KY with the
  transactions + valuation-change decomposition (POS/NET/VALCHG) and
  the identity gap printed -- holdings deltas are never presented as
  flows.

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

VERSION = "1.1.1"
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
SPLITS = {   # (all, official-99990, private-99991) -- runtime-reconciled
    "lt_total": ("FORLTTOTALNET99996", "FORLTTOTALNET99990",
                 "FORLTTOTALNET99991"),
    "lt_treas": ("FORLTTREASNET99996", "FORLTTREASNET99990",
                 "FORLTTREASNET99991"),
    "lt_equity": ("FORLTEQTYNET99996", "FORLTEQTYNET99990",
                  "FORLTEQTYNET99991"),
    "lt_corp": ("FORLTCORPNET99996", "FORLTCORPNET99990",
                "FORLTCORPNET99991"),
    "lt_agency": ("FORLTAGCYNET99996", "FORLTAGCYNET99990",
                  "FORLTAGCYNET99991"),
    "st_treas": ("FORSTTREASNET99996", "FORSTTREASNET99990",
                 "FORSTTREASNET99991"),
}
RECON_TOL_BN = 0.2
COUNTRIES = {"china": "41408", "japan": "42609",
             "united_kingdom": "13005", "belgium": "10308",
             "cayman": "36137"}
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


def _fred_once(sid):
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



def fred_fetch(sid):
    """Retry shell: release-night FRED load flakes single series
    (burned 2026-08-17 21:30 -- total tile went null while 5/6
    siblings succeeded).  One retry after 2s; both failures ->
    honest exclusion as before."""
    units, rows = _fred_once(sid)
    if units is not None:
        return units, rows
    time.sleep(2.0)
    return _fred_once(sid)

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


def fetch_bn(sid):
    """(dates, vals_bn) or (None, reason)."""
    units, rows = fred_fetch(sid)
    if units is None:
        return None, "%s:%s" % (sid, rows)
    bank_merge(sid, rows)
    return [d for d, _ in rows], [to_bn(v, units) for _, v in rows]


def split_block(doc):
    """Holder-split families, reconciled at runtime; builds the
    official_private signal from lt_total."""
    fam_out = {}
    for fam, (sid_all, sid_off, sid_prv) in SPLITS.items():
        da, va = fetch_bn(sid_all)
        do_, vo = fetch_bn(sid_off)
        dp, vp = fetch_bn(sid_prv)
        if da is None or do_ is None or dp is None:
            fam_out[fam] = {"status": "MISSING",
                            "why": ";".join(x[1] for x in
                                            ((da, va), (do_, vo),
                                             (dp, vp))
                                            if x[0] is None)}
            continue
        if not (da[-1] == do_[-1] == dp[-1]):
            fam_out[fam] = {"status": "MISALIGNED",
                            "why": "latest months differ"}
            continue
        gap = abs(va[-1] - (vo[-1] + vp[-1]))
        if gap > RECON_TOL_BN:
            fam_out[fam] = {"status": "UNRECONCILED",
                            "why": "all-(off+priv) gap %.2fB"
                            % gap}
            continue
        fam_out[fam] = {
            "status": "OK", "month": da[-1],
            "recon_gap_bn": round(gap, 3),
            "official": {"latest": round(vo[-1], 1),
                         "sum_12m": round(sum(vo[-12:]), 1),
                         "z_10y": zlast(vo)},
            "private": {"latest": round(vp[-1], 1),
                        "sum_12m": round(sum(vp[-12:]), 1),
                        "z_10y": zlast(vp)},
            "_vo": vo, "_vp": vp}
    lt = fam_out.get("lt_total") or {}
    if lt.get("status") == "OK":
        vo, vp = lt.pop("_vo"), lt.pop("_vp")
        n = min(len(vo), len(vp))
        d = [vp[len(vp) - n + i] - vo[len(vo) - n + i]
             for i in range(n)]
        doc["signals"]["official_private"] = {
            "latest_bn": round(d[-1], 1),
            "sum_3m_bn": round(sum(d[-3:]), 1),
            "sum_12m_bn": round(sum(d[-12:]), 1),
            "z_10y": zlast(d),
            "formula": "private - official (LT total; suffixes "
                       "99991-99990, runtime-reconciled)"}
    else:
        doc["signals"]["official_private"] = {
            "value": None,
            "why": "lt_total split %s: %s"
            % (lt.get("status"), lt.get("why"))}
    for fam in fam_out.values():
        fam.pop("_vo", None)
        fam.pop("_vp", None)
    doc["holder_splits"] = fam_out


def country_block(doc):
    """LT-Treasury holdings + the tx/valchg decomposition; identity
    gap reported, never hidden (delta != flows doctrine)."""
    out = {}
    for name, code in COUNTRIES.items():
        dpos, vpos = fetch_bn("FORLTTREASPOS" + code)
        if dpos is None:
            out[name] = {"status": "MISSING", "why": vpos}
            continue
        row = {"status": "OK", "holdings_bn": round(vpos[-1], 1),
               "month": dpos[-1],
               "d12m_holdings_bn": (round(vpos[-1] - vpos[-13], 1)
                                    if len(vpos) >= 13 else None)}
        dn, vn = fetch_bn("FORLTTREASNET" + code)
        dv, vv = fetch_bn("FORLTTREASVALCHG" + code)
        if dn is not None:
            row["tx_12m_bn"] = round(sum(vn[-12:]), 1)
        if dv is not None:
            row["valchg_12m_bn"] = round(sum(vv[-12:]), 1)
        if (row.get("d12m_holdings_bn") is not None
                and "tx_12m_bn" in row and "valchg_12m_bn" in row):
            row["identity_gap_bn"] = round(
                row["d12m_holdings_bn"]
                - row["tx_12m_bn"] - row["valchg_12m_bn"], 1)
            row["note"] = ("dHoldings = tx + valchg + other "
                           "adjustments; gap = the 'other' term")
        out[name] = row
    doc["country_lt_treasury"] = out


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
    split_block(doc)
    country_block(doc)

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
