"""justhodl-base-rates v1.0.0 -- Fusion 1: the empirical base-rates
spine.  Marker: base-rates v1.0.0

Reads the spx-beaters full-market weekly-close ledger (READ-ONLY --
this engine never writes any spx-beaters/* key) and publishes
data/base-rates.json: beat-SPY odds + excess-return distributions by
momentum quintile x horizon (4/13/26w) x cap bucket x drawdown band,
plus a current_assignments map so any fleet engine (invest,
stock-buying, sp500 compare) can anchor a verdict in one join.

Math is convention-identical to spx-beaters v1.2+ base_rates():
compressed non-None close arrays, end-anchored offsets, 6m-return
formation, cross-sectional quintiles formed per formation date then
pooled across formations.  The local harness asserts identity against
a verbatim copy of that function on a synthetic ledger.  Cohorts are
in-sample; rolling 4/13w formations overlap week to week, so
n_formations ships alongside n_obs and consumers must not treat pooled
n as independent.  Quintile->odds mapping (qi increments while
current 6m ret > threshold) is byte-for-byte the league's
quintile_odds so odds chips can never disagree with the league's
published mom_quintile.  Runs Sat 14:30 UTC, after the beaters 13:00
run appends the ledger.
"""
import gzip
import json
import os
import time
from datetime import datetime, timezone

import boto3

VERSION = "1.0.0"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
LEDGER_KEY = "spx-beaters/weekly-closes.json"   # READ-ONLY here
UNIVERSE_KEY = "data/universe.json"
OUT_KEY = "data/base-rates.json"
HIST_KEY = "base-rates/history.json"

LOOKBACK = 26            # 6m-return formation window (weeks)
HORIZONS = (("4w", 4), ("13w", 13), ("26w", 26))
MAX_FORMS = {"4w": 23, "13w": 14, "26w": 13}
MIN_ALL = 200            # min cross-section per formation (v1.2 floor)
MIN_CELL = 40            # min n for bucket / dd-band cells
MIN_CB = 25              # min n for the comeback cohort (v1.2 floor)
DD_MIN_HIST = 40         # min closes before a dd band is assigned
HIST_KEEP = 520

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


def fnum(v):
    try:
        f = float(v)
        return f if f == f and abs(f) != float("inf") else None
    except (TypeError, ValueError):
        return None


def rows_of(doc, *keys):
    """Tolerant container reader; ops G0 verifies the live key."""
    if not isinstance(doc, dict):
        return []
    for k in keys:
        v = doc.get(k)
        if isinstance(v, list) and v:
            return v
    return []


def tick_of(r):
    for k in ("ticker", "symbol", "t", "sym"):
        v = r.get(k)
        if v:
            return str(v).upper()
    return None


def comp(led, t):
    """Compressed non-None weekly closes (ledger convention)."""
    return [v for v in (led["closes"].get(t) or []) if v]


def wilson_lb(k, n, z=1.96):
    """95% Wilson lower bound on a beat rate, in percent."""
    if not n:
        return None
    p = k / n
    z2 = z * z
    den = 1 + z2 / n
    ctr = p + z2 / (2 * n)
    rad = ((p * (1 - p) / n) + z2 / (4 * n * n)) ** 0.5
    return round(100 * (ctr - z * rad) / den, 1)


def bucket_of(r):
    """Cap-bucket normalization, byte-identical to the beaters scan."""
    cb = str(r.get("cap_bucket") or "").lower()
    if cb in ("nano",):
        cb = "micro"
    if cb == "mega":
        cb = "large"
    if cb not in ("large", "mid", "small", "micro"):
        mc = fnum(r.get("market_cap")) or 0
        cb = ("large" if mc >= 10e9 else "mid" if mc >= 2e9
              else "small" if mc >= 3e8 else "micro")
    return cb


def dd_band(dd):
    if dd is None:
        return None
    if dd >= -0.10:
        return "0_to_-10"
    if dd >= -0.20:
        return "-10_to_-20"
    if dd >= -0.35:
        return "-20_to_-35"
    return "below_-35"


def cohort_rows(led, tickers, spy, h, s):
    """One formation cohort: end-anchored, shifted back s weeks.
    Formation i = m-1-h-s needs a 26w lookback; outcome runs h weeks
    to m-1-s; excess vs SPY over the same end-anchored offsets.
    Returns rows of (t, form6, excess, dd_form, base4)."""
    ms = len(spy)
    si = ms - 1 - s
    fi = si - h
    if fi - LOOKBACK < 0 or not spy[si] or not spy[fi]:
        return []
    spy_out = spy[si] / spy[fi] - 1
    rows = []
    for t in tickers:
        arr = comp(led, t)
        m = len(arr)
        i = m - 1 - h - s
        if i - LOOKBACK < 0:
            continue
        form = arr[i] / arr[i - LOOKBACK] - 1
        out = arr[m - 1 - s] / arr[i] - 1
        peak = arr[0]
        for v in arr[:i + 1]:
            peak = max(peak, v)
        ddf = arr[i] / peak - 1 if peak else None
        base4 = arr[i] / arr[i - 4] - 1 if i >= 4 else None
        rows.append((t, form, out - spy_out, ddf, base4))
    return rows


def tag_quintiles(rows):
    """Cross-sectional quintile tags for one formation cohort.
    Returns (tagged rows with q appended, per-quintile segments)."""
    rows = sorted(rows, key=lambda r: r[1])
    n = len(rows)
    tagged, segs = [], []
    for q in range(5):
        seg = rows[int(n * q / 5):int(n * (q + 1) / 5)]
        segs.append(seg)
        for r in seg:
            tagged.append(r + (q + 1,))
    return tagged, segs


def cell_stats(ex_list, beat_key, med_key):
    ex = sorted(ex_list)
    n = len(ex)
    beat = sum(1 for x in ex if x > 0)
    return {"n": n,
            beat_key: round(100 * beat / n, 1),
            med_key: round(ex[n // 2] * 100, 1),
            "p25_excess_pp": round(ex[n // 4] * 100, 1),
            "p75_excess_pp": round(ex[(3 * n) // 4] * 100, 1),
            "wilson_lb95_pct": wilson_lb(beat, n)}


def build_horizon(led, tickers, spy, h_name, h, diag):
    pooled = []            # (t, form, excess, ddf, base4, q)
    n_forms = 0
    thresholds = None
    quints_s0 = None
    for s in range(MAX_FORMS[h_name]):
        rows = cohort_rows(led, tickers, spy, h, s)
        if len(rows) < MIN_ALL:
            continue
        tagged, segs = tag_quintiles(rows)
        pooled.extend(tagged)
        n_forms += 1
        if s == 0:
            quints_s0 = []
            for qn, seg in enumerate(segs):
                ex = sorted(x[2] for x in seg)
                beat = sum(1 for x in seg if x[2] > 0)
                quints_s0.append({
                    "q": qn + 1,
                    "form_6m_min_pct": round(seg[0][1] * 100, 1),
                    "form_6m_max_pct": round(seg[-1][1] * 100, 1),
                    "n": len(seg),
                    "beat_pct": round(100 * beat / len(seg), 1),
                    "median_excess_pp": round(ex[len(ex) // 2] * 100,
                                              1)})
            thresholds = [q["form_6m_max_pct"] / 100
                          for q in quints_s0[:-1]]
    diag["n_formations"][h_name] = n_forms
    if not pooled:
        return None, thresholds, quints_s0
    quints = []
    for qn in range(1, 6):
        ex = [r[2] for r in pooled if r[5] == qn]
        if not ex:
            continue
        st = cell_stats(ex, "beat_pct", "median_excess_pp")
        st["q"] = qn
        quints.append(st)
    by_bucket = {}
    for b in ("large", "mid", "small", "micro"):
        ex = [r[2] for r in pooled if BKT.get(r[0]) == b]
        if len(ex) >= MIN_CELL:
            by_bucket[b] = cell_stats(ex, "beat_pct",
                                      "median_excess_pp")
    bands = {}
    for r in pooled:
        band = dd_band(r[3])
        if band:
            bands.setdefault(band, []).append(r[2])
    dd_cells = {b: cell_stats(ex, "beat_pct", "median_excess_pp")
                for b, ex in bands.items() if len(ex) >= MIN_CELL}
    return {"n_obs": len(pooled), "n_formations": n_forms,
            "quintiles": quints, "by_bucket": by_bucket,
            "dd_bands": dd_cells}, thresholds, quints_s0


def comeback_26w(led, tickers, spy):
    """v1.2-parity comeback cohort: single s=0 26w formation,
    dd<=-30% at formation with a 4w base >= -3%."""
    rows = cohort_rows(led, tickers, spy, 26, 0)
    cb = [r for r in rows if r[3] is not None and r[3] <= -0.30
          and r[4] is not None and r[4] >= -0.03]
    if len(cb) < MIN_CB:
        return None
    ex = sorted(r[2] for r in cb)
    beat = sum(1 for r in cb if r[2] > 0)
    return {"n": len(cb),
            "beat_pct": round(100 * beat / len(cb), 1),
            "median_excess_pp": round(ex[len(ex) // 2] * 100, 1),
            "wilson_lb95_pct": wilson_lb(beat, len(cb)),
            "def": "dd<=-30% at 26w-ago formation, 4w base>=-3%"}


BKT = {}  # ticker -> bucket, filled in build()


def build():
    t0 = time.time()
    diag = {"feeds": {}, "n_formations": {}, "excluded": {}}
    led = _g(LEDGER_KEY)
    doc = {"v": VERSION, "engine": "justhodl-base-rates",
           "as_of": datetime.now(timezone.utc).date().isoformat(),
           "generated_at": datetime.now(timezone.utc).isoformat(),
           "source_ledger": LEDGER_KEY, "spy_ticker": "SPY",
           "diag": diag}
    if (not led or len(led.get("dates") or []) < LOOKBACK + 14
            or "SPY" not in (led.get("closes") or {})):
        doc["status"] = "INSUFFICIENT_DATA"
        doc["why"] = ("ledger missing, shallower than %dw, or SPY "
                      "absent -- honest empty, no synthetic odds"
                      % (LOOKBACK + 14))
        return doc
    doc["status"] = "LIVE"
    spy = comp(led, "SPY")
    tickers = [t for t in led["closes"] if t != "SPY"]
    diag["feeds"]["ledger_weeks"] = len(led["dates"])
    diag["feeds"]["ledger_first"] = led["dates"][0]
    diag["feeds"]["ledger_last"] = led["dates"][-1]
    diag["feeds"]["n_tickers"] = len(tickers)
    uni = _g(UNIVERSE_KEY) or {}
    stocks = rows_of(uni, "stocks", "rows", "universe")
    diag["feeds"]["universe"] = len(stocks)
    BKT.clear()
    for r in stocks:
        t = tick_of(r)
        if t:
            BKT[t] = bucket_of(r)
    doc["spy"] = {}
    for h_name, h in HORIZONS:
        if len(spy) > h:
            doc["spy"]["ret_%s_pct" % h_name] = round(
                (spy[-1] / spy[-1 - h] - 1) * 100, 1)
    cohorts = {}
    th26 = None
    for h_name, h in HORIZONS:
        cell, th, q0 = build_horizon(led, tickers, spy, h_name, h,
                                     diag)
        if cell:
            cohorts[h_name] = cell
        if h_name == "26w":
            th26 = th
            if q0:
                cohorts.setdefault("26w", {})["s0_quintiles"] = q0
    doc["cohorts"] = cohorts
    doc["comeback_26w"] = comeback_26w(led, tickers, spy)
    doc["quintile_thresholds_26w"] = th26
    assigns = {}
    n_short = 0
    for t in tickers:
        arr = comp(led, t)
        m = len(arr)
        if m < LOOKBACK + 1:
            n_short += 1
            continue
        r6c = arr[-1] / arr[-1 - LOOKBACK] - 1
        qi = None
        if th26:
            qi = 0
            for th in th26:          # byte-identical to quintile_odds
                if r6c > th:
                    qi += 1
            qi += 1
        band = None
        if m >= DD_MIN_HIST:
            hi = max(arr)
            band = dd_band(arr[-1] / hi - 1 if hi else None)
        assigns[t] = {"b": BKT.get(t, "other"), "q": qi,
                      "dd": band, "m6": round(r6c * 100, 1)}
    doc["current_assignments"] = assigns
    diag["excluded"]["short_history"] = n_short
    doc["caveats"] = [
        "in-sample odds from our own ledger; anchors, not gospel",
        "4w/13w formations overlap weekly -- pooled n is not "
        "independent; see n_formations",
        "compressed-close convention and end-anchored offsets match "
        "spx-beaters base_rates() exactly",
        "bucket membership is as-of-today universe applied to past "
        "formations (survivorship noted)"]
    hist = _g(HIST_KEY) or {"rows": []}
    q26 = (cohorts.get("26w") or {}).get("quintiles") or []
    row = {"as_of": doc["as_of"],
           "n_26w": (cohorts.get("26w") or {}).get("n_obs"),
           "q1_beat_26w": q26[0]["beat_pct"] if q26 else None,
           "q5_beat_26w": q26[-1]["beat_pct"] if len(q26) == 5
           else None,
           "spy_26w_pct": doc["spy"].get("ret_26w_pct")}
    if not hist["rows"] or hist["rows"][-1]["as_of"] != row["as_of"]:
        hist["rows"].append(row)
        hist["rows"] = hist["rows"][-HIST_KEEP:]
        _put(HIST_KEY, hist)
    diag["runtime_ms"] = int((time.time() - t0) * 1000)
    return doc


def lambda_handler(event, context):
    doc = build()
    _put(OUT_KEY, doc)
    c = doc.get("cohorts") or {}
    return {"ok": doc.get("status") == "LIVE", "v": VERSION,
            "status": doc.get("status"),
            "n_26w": (c.get("26w") or {}).get("n_obs"),
            "n_assignments": len(doc.get("current_assignments") or {}),
            "runtime_ms": doc["diag"].get("runtime_ms")}
