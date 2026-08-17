"""justhodl-beaters-grader v1.0.0 -- Fusion 4.
Marker: beaters-grader v1.0.0

Closes the loop the beaters league cannot close itself: every
Saturday (15:00 UTC, after the league at 13:00) this engine BANKS
the week's listings snapshot, then GRADES every banked week that is
>=28 days old against realized forward returns from the league's own
weekly-closes ledger (beat = excess return vs SPY over 4w and 13w),
and publishes per-bucket win rates plus PROVISIONAL per-leg weight
tilts to data/beaters-learned-weights.json.

Honesty rules:
  * grades exist only for weeks with enough forward history --
    younger cohorts are ACCRUING, never guessed;
  * learned weights stay status=PROVISIONAL (consumption by the
    league is a separate, later op) until n_graded >= 100 rows AND
    every tilt is bounded to +/-20% of base;
  * the listings bank is append-only and idempotent (same Saturday
    re-run cannot duplicate a week);
  * SPY must exist in the ledger or grading refuses.
"""
import gzip
import json
import os
import time
from datetime import datetime, timezone, timedelta

import boto3

VERSION = "1.0.0"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
SRC_KEY = "data/spx-beaters.json"
LEDGER_KEY = "spx-beaters/weekly-closes.json"
BANK_KEY = "spx-beaters/listings-history.json"
OUT_KEY = "data/beaters-learned-weights.json"
BENCH = "SPY"
MIN_AGE_D = 28
H_WEEKS = (4, 13)
MIN_ROWS_ACTIVE = 100
TILT_CAP = 0.20
CAP_PER_BUCKET = 40

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


def bank_week(bank, src):
    """Append this snapshot's listings (slim rows) under its as_of
    date; idempotent."""
    wk = src.get("as_of") or src.get("generated_at", "")[:10]
    if not wk:
        return bank, None, "no as_of on source"
    if wk in bank["weeks"]:
        return bank, wk, "already banked"
    buckets = src.get("buckets")
    if not isinstance(buckets, dict):
        buckets = {k: v for k, v in src.items()
                   if isinstance(v, list) and v
                   and isinstance(v[0], dict) and "t" in v[0]
                   and "score" in v[0]}
    slim = {}
    for bname, rows in (buckets or {}).items():
        out = []
        for r in rows[:CAP_PER_BUCKET]:
            if not isinstance(r, dict) or "t" not in r:
                continue
            out.append({"t": r["t"],
                        "score": r.get("score"),
                        "legs": sorted((r.get("legs") or {}))
                        if isinstance(r.get("legs"), dict)
                        else list(r.get("legs") or [])})
        if out:
            slim[bname] = out
    if not slim:
        return bank, wk, "no listable buckets"
    bank["weeks"][wk] = {"buckets": slim,
                         "banked_at": datetime.now(
                             timezone.utc).isoformat()}
    return bank, wk, "banked %d buckets / %d rows" % (
        len(slim), sum(len(v) for v in slim.values()))


def fwd_ret(led, t, wk, weeks):
    """Forward return of t from the first ledger date >= wk over
    `weeks` steps; None if insufficient."""
    dates = led.get("dates") or []
    closes = (led.get("closes") or {}).get(t)
    if not closes:
        return None
    i0 = next((i for i, d in enumerate(dates) if d >= wk), None)
    if i0 is None or i0 + weeks >= len(dates):
        return None
    a = closes[i0] if i0 < len(closes) else None
    b = closes[i0 + weeks] if i0 + weeks < len(closes) else None
    if a in (None, 0) or b is None:
        return None
    return b / a - 1.0


def grade(bank, led, today):
    """Grade every banked, ungraded week old enough for each
    horizon."""
    if BENCH not in (led.get("closes") or {}):
        return None, "%s missing from ledger -- refusing" % BENCH
    graded = bank.setdefault("grades", {})
    n_new = 0
    for wk, snap in bank["weeks"].items():
        age_d = (datetime.fromisoformat(today)
                 - datetime.fromisoformat(wk)).days
        g = graded.setdefault(wk, {})
        for hw in H_WEEKS:
            hk = "%dw" % hw
            if hk in g or age_d < max(MIN_AGE_D, hw * 7):
                continue
            spy = fwd_ret(led, BENCH, wk, hw)
            if spy is None:
                continue
            rows = []
            for bname, lst in snap["buckets"].items():
                for r in lst:
                    rr = fwd_ret(led, r["t"], wk, hw)
                    if rr is None:
                        continue
                    rows.append({"t": r["t"], "bucket": bname,
                                 "legs": r.get("legs") or [],
                                 "excess": round(rr - spy, 5),
                                 "beat": rr > spy})
            if rows:
                g[hk] = {"spy_ret": round(spy, 5),
                         "rows": rows,
                         "graded_at": today}
                n_new += 1
    return n_new, None


def learn(bank):
    """Aggregate graded rows -> per-bucket win rates + per-leg
    tilts (PROVISIONAL until n >= MIN_ROWS_ACTIVE)."""
    agg_b, agg_l, n = {}, {}, 0
    for wk, g in (bank.get("grades") or {}).items():
        for hk, blk in g.items():
            for r in blk.get("rows") or []:
                n += 1
                b = agg_b.setdefault(
                    "%s_%s" % (r["bucket"], hk),
                    {"n": 0, "beat": 0, "sum_ex": 0.0})
                b["n"] += 1
                b["beat"] += 1 if r["beat"] else 0
                b["sum_ex"] += r["excess"]
                for leg in r.get("legs") or []:
                    q = agg_l.setdefault(leg, {"n": 0, "beat": 0})
                    q["n"] += 1
                    q["beat"] += 1 if r["beat"] else 0
    buckets = {k: {"n": v["n"],
                   "beat_rate": round(v["beat"] / v["n"], 4),
                   "mean_excess": round(v["sum_ex"] / v["n"], 5)}
               for k, v in agg_b.items() if v["n"]}
    base = (sum(v["beat"] for v in agg_l.values())
            / max(1, sum(v["n"] for v in agg_l.values()))) \
        if agg_l else None
    legs = {}
    for leg, v in agg_l.items():
        if not v["n"] or base is None:
            continue
        raw = v["beat"] / v["n"] - base
        legs[leg] = {"n": v["n"],
                     "beat_rate": round(v["beat"] / v["n"], 4),
                     "tilt": round(max(-TILT_CAP,
                                       min(TILT_CAP, raw)), 4)}
    status = ("ACTIVE-CANDIDATE" if n >= MIN_ROWS_ACTIVE
              else "PROVISIONAL")
    return {"n_graded_rows": n, "status": status,
            "base_beat_rate": (round(base, 4)
                               if base is not None else None),
            "buckets": buckets, "legs": legs,
            "note": "consumption by the league is a separate op; "
                    "tilts capped +/-%.0f%%" % (TILT_CAP * 100)}


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    today = now.date().isoformat()
    src = _g(SRC_KEY)
    led = _g(LEDGER_KEY)
    bank = _g(BANK_KEY) or {"weeks": {}, "grades": {}}
    doc = {"v": VERSION, "engine": "justhodl-beaters-grader",
           "as_of": today, "generated_at": now.isoformat(),
           "diag": {}}
    if not src or not led or not (led.get("dates")):
        doc["status"] = "INSUFFICIENT_DATA"
        doc["why"] = "beaters doc or closes ledger missing"
        _put(OUT_KEY, doc)
        return {"ok": False, "why": doc["why"]}
    bank, wk, msg = bank_week(bank, src)
    doc["diag"]["bank"] = {"week": wk, "msg": msg,
                           "n_weeks": len(bank["weeks"])}
    n_new, gerr = grade(bank, led, today)
    if gerr:
        doc["status"] = "INSUFFICIENT_DATA"
        doc["why"] = gerr
        _put(BANK_KEY, bank)
        _put(OUT_KEY, doc)
        return {"ok": False, "why": gerr}
    doc["diag"]["graded_new_blocks"] = n_new
    _put(BANK_KEY, bank)
    res = learn(bank)
    doc.update(res)
    doc["status"] = "LIVE"
    if res["n_graded_rows"] == 0:
        oldest = min(bank["weeks"]) if bank["weeks"] else None
        eta = None
        if oldest:
            eta = (datetime.fromisoformat(oldest)
                   + timedelta(days=MIN_AGE_D)).date().isoformat()
        doc["accruing"] = {"oldest_week": oldest,
                           "first_grade_eta": eta,
                           "why": "no cohort is %dd old yet"
                           % MIN_AGE_D}
    doc["diag"]["runtime_ms"] = int((time.time() - t0) * 1000)
    _put(OUT_KEY, doc)
    return {"ok": True, "status": "LIVE",
            "n_weeks_banked": len(bank["weeks"]),
            "n_graded_rows": res["n_graded_rows"],
            "weights_status": res["status"]}
