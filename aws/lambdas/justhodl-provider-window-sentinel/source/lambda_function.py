"""justhodl-provider-window-sentinel v1.0.0
Marker: provider-window-sentinel v1.0.0

The ICE BofA lesson made systematic: FRED can retroactively WINDOW
a series (drop old history) or silently REVISE values.  The
foreign-flows banks under data/providers/tic-cslt/ already survive
windowing via union-merge -- but survival without DETECTION means
nobody knows the bank has become the only source of truth, and
silent revisions never surface at all.

Weekly (Sun 09:00 UTC) this sentinel refetches every banked
tic-cslt series from FRED and diffs against the bank:
  * rows in bank but MISSING from the provider -> WINDOWED
    (named dates, capped list) -- the bank is now authoritative
    for those rows;
  * rows where provider value != banked value (>1e-6 rel) ->
    REVISED (informational; TIC revises routinely);
  * fetch failure -> UNVERIFIED, never OK.
Publishes data/provider-window-sentinel.json with per-series
verdicts + a top-level alert when any series is WINDOWED, and
appends WINDOWED events to provider-window-sentinel/events.json.
The sentinel never writes to the banks -- read-only vs providers
AND banks; the OUT doc + events log are its only writes.
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
BANK_PREFIX = "data/providers/tic-cslt/"
OUT_KEY = "data/provider-window-sentinel.json"
EVENTS_KEY = "provider-window-sentinel/events.json"
REL_TOL = 1e-6
CAP_LIST = 24

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


def list_banks():
    """All banked series keys under the tic-cslt prefix.  Seam."""
    out = []
    tok = None
    while True:
        kw = {"Bucket": BUCKET, "Prefix": BANK_PREFIX}
        if tok:
            kw["ContinuationToken"] = tok
        r = s3.list_objects_v2(**kw)
        out.extend(o["Key"] for o in r.get("Contents") or [])
        if not r.get("IsTruncated"):
            break
        tok = r.get("NextContinuationToken")
    return sorted(out)


def fred_obs(sid):
    """{date: value} from FRED or (None, reason).  Seam."""
    try:
        url = ("https://api.stlouisfed.org/fred/series/"
               "observations?series_id=%s&api_key=%s"
               "&file_type=json&observation_start=1900-01-01"
               % (sid, FRED_KEY))
        req = urllib.request.Request(
            url, headers={"User-Agent": "justhodl-sentinel"})
        with urllib.request.urlopen(req, timeout=60) as r:
            j = json.loads(r.read())
        out = {}
        for o in j.get("observations") or []:
            try:
                out[o["date"]] = float(o["value"])
            except (KeyError, TypeError, ValueError):
                continue
        if not out:
            return None, "no_observations"
        return out, None
    except Exception as e:  # noqa: BLE001
        return None, "fetch_error:%s" % str(e)[:60]


def diff_series(bank_rows, prov):
    missing = sorted(d for d in bank_rows if d not in prov)
    revised = []
    for d, bv in bank_rows.items():
        if d not in prov:
            continue
        pv = prov[d]
        denom = max(abs(bv), abs(pv), 1e-12)
        if abs(pv - bv) / denom > REL_TOL:
            revised.append({"date": d, "banked": bv,
                            "provider": pv})
    revised.sort(key=lambda r: r["date"])
    if missing:
        verdict = "WINDOWED"
    elif revised:
        verdict = "REVISED"
    else:
        verdict = "OK"
    return {"verdict": verdict,
            "bank_n": len(bank_rows), "provider_n": len(prov),
            "n_missing": len(missing),
            "missing_from_provider": missing[:CAP_LIST],
            "n_revised": len(revised),
            "revised": revised[:CAP_LIST]}


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    doc = {"v": VERSION,
           "engine": "justhodl-provider-window-sentinel",
           "as_of": now.date().isoformat(),
           "generated_at": now.isoformat(),
           "series": {}, "diag": {}}
    if not FRED_KEY:
        doc["status"] = "INSUFFICIENT_DATA"
        doc["why"] = "FRED_KEY absent"
        _put(OUT_KEY, doc)
        return {"ok": False, "why": doc["why"]}
    keys = list_banks()
    if not keys:
        doc["status"] = "INSUFFICIENT_DATA"
        doc["why"] = "no banks under %s" % BANK_PREFIX
        _put(OUT_KEY, doc)
        return {"ok": False, "why": doc["why"]}
    windowed = []
    n_ok = 0
    for key in keys:
        bank = _g(key) or {}
        sid = bank.get("id") or key.split("/")[-1].split(".")[0]
        rows = bank.get("rows") or {}
        if not rows:
            doc["series"][sid] = {"verdict": "UNVERIFIED",
                                  "why": "bank empty"}
            continue
        prov, err = fred_obs(sid)
        if prov is None:
            doc["series"][sid] = {"verdict": "UNVERIFIED",
                                  "why": err,
                                  "bank_n": len(rows)}
            continue
        res = diff_series(rows, prov)
        doc["series"][sid] = res
        if res["verdict"] == "WINDOWED":
            windowed.append(sid)
        if res["verdict"] == "OK":
            n_ok += 1
        time.sleep(0.25)
    doc["status"] = "LIVE"
    doc["summary"] = {"n_series": len(doc["series"]),
                      "n_ok": n_ok,
                      "windowed": windowed}
    if windowed:
        doc["alert"] = ("FRED WINDOWING DETECTED on %s -- the "
                        "Deny-Delete banks are now the only "
                        "source for the dropped rows"
                        % ",".join(windowed))
        ev = _g(EVENTS_KEY) or {"rows": []}
        ev["rows"].append({"detected": doc["as_of"],
                           "series": windowed,
                           "detail": {s: doc["series"][s]
                                      ["missing_from_provider"]
                                      for s in windowed}})
        ev["rows"] = ev["rows"][-200:]
        _put(EVENTS_KEY, ev)
    doc["diag"]["runtime_ms"] = int((time.time() - t0) * 1000)
    _put(OUT_KEY, doc)
    return {"ok": True, "status": "LIVE",
            "n_series": len(doc["series"]), "n_ok": n_ok,
            "windowed": windowed}
