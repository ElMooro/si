"""FR2004 original-source settlement research.

The active handler publishes fails_store research or serves the current public
snapshot for HTTP requests. Former heuristics and the signal emitter are retained
below as unvalidated legacy source and are unreachable from the active handler.
"""
import json
import math
import urllib.request
from datetime import datetime, timezone

import boto3

from treasury import (GROSS_NOTE, _regime, annotate_treasury, normalized_treasury,
                      scope_quality, strict_json_dumps)

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/settlement-fails.json"
NY = "https://markets.newyorkfed.org/api/pd"
UA = {"User-Agent": "JustHodl Research raafouis@gmail.com"}

# class key -> (fails-to-deliver series, fails-to-receive series, label)
CLASSES = [
    ("ust_ex_tips", "PDFTD-USTET", "PDFTR-USTET", "U.S. Treasury (ex-TIPS)"),
    ("tips",        "PDFTD-UST",   "PDFTR-UST",   "TIPS"),
    ("corporate",   "PDFTD-CS",    "PDFTR-CS",    "Corporate securities"),
    ("agency_mbs",  "PDFTD-FGM",   "PDFTR-FGM",   "Agency MBS"),
    ("agency_debt", "PDFTD-FGEM",  "PDFTR-FGEM",  "Agency debt (ex-MBS)"),
    ("other_mbs",   "PDFTD-OM",    "PDFTR-OM",    "Other (non-agency) MBS"),
]


def _get(url, t=45):
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=t) as r:
            return json.loads(r.read().decode("utf-8", "ignore"))
    except Exception as e:
        print("fetch fail %s: %s" % (url[-40:], e))
        return None


def fetch(key, diagnostic=None):
    """Return [[asofdate, $bn]] ascending, dropping masked (*) values."""
    j = _get(NY + "/get/%s.json" % key)
    out = []
    if diagnostic is not None:
        diagnostic.update(status="unavailable" if j is None else "incomplete", observation_date=None)
    if j:
        for t in j.get("pd", {}).get("timeseries", []):
            day = t.get("asofdate")
            if not isinstance(day, str):
                continue
            v = t.get("value")
            status = "incomplete"
            if v not in ("*", "", None):
                try:
                    value = float(v) / 1000.0  # $m -> $bn
                    if not math.isfinite(value) or value < 0:
                        raise ValueError("invalid fails amount")
                    out.append([day, round(value, 2)])
                    status = "fresh"
                except (TypeError, ValueError, OverflowError):
                    status = "invalid"
            if diagnostic is not None and day >= (diagnostic["observation_date"] or ""):
                diagnostic.update(observation_date=day, status=status)
    out.sort(key=lambda x: x[0])
    return out


def combine(a, b):
    """Sum two date-keyed series (deliver + receive) on shared dates."""
    db = dict(b)
    return [[d, round(v + db[d], 2)] for d, v in a if d in db]


def sum_series(list_of_series):
    """All-asset totals require every class on the same date, never partial sums."""
    if not list_of_series:
        return []
    series = [dict(points) for points in list_of_series]
    common = set.intersection(*(set(points) for points in series))
    return [[day, round(sum(points[day] for points in series), 2)] for day in sorted(common)]


def stats(pts):
    vs = [p[1] for p in pts]
    if not vs:
        return {}
    n = len(vs); latest = vs[-1]
    mean = sum(vs) / n
    var = sum((x - mean) ** 2 for x in vs) / n
    sd = var ** 0.5
    z = round((latest - mean) / sd, 2) if sd else 0.0
    pctile = round(sum(1 for x in vs if x <= latest) / n * 100, 1)
    avg52 = round(sum(vs[-52:]) / min(52, n), 1)
    return {"latest": round(latest, 1), "mean": round(mean, 1), "max": round(max(vs), 1),
            "min": round(min(vs), 1), "z": z, "pctile": pctile, "avg_52w": avg52,
            "n_obs": n, "start": pts[0][0], "as_of": pts[-1][0],
            "spike": bool(z >= 2 or pctile >= 95)}




# ── ops 3307: closed-loop signal emission (graded by outcome-checker) ──
def _yprice(t):
    try:
        req = urllib.request.Request(
            "https://query1.finance.yahoo.com/v8/finance/chart/%s"
            "?range=1d&interval=1d" % t,
            headers={"User-Agent": "Mozilla/5.0 JustHodl"})
        with urllib.request.urlopen(req, timeout=20) as r:
            j = json.loads(r.read())
        return float(j["chart"]["result"][0]["meta"]["regularMarketPrice"])
    except Exception:
        return None


def _emit_signal(sid, stype, direction, ticker, benchmark, value,
                 windows, horizon, conf="0.55"):
    try:
        import boto3 as _b3
        from decimal import Decimal as _D
        from datetime import datetime as _dt, timezone as _tz
        px = _yprice(ticker)
        if px is None:
            print("[sig] no price for %s, skip %s" % (ticker, sid))
            return False
        now = _dt.now(_tz.utc)
        _b3.resource("dynamodb", "us-east-1").Table(
            "justhodl-signals").put_item(
            Item={"signal_id": sid, "signal_type": stype,
                  "predicted_direction": direction,
                  "signal_value": str(value),
                  "confidence": _D(conf),
                  "measure_against": "ticker_vs_benchmark",
                  "baseline_price": str(px), "benchmark": benchmark,
                  "check_windows": windows, "outcomes": {},
                  "accuracy_scores": {}, "status": "pending",
                  "logged_at": now.isoformat(),
                  "logged_epoch": int(now.timestamp()),
                  "horizon_days_primary": horizon,
                  "schema_version": "2"},
            ConditionExpression="attribute_not_exists(signal_id)")
        print("[sig] emitted %s" % sid)
        return True
    except Exception as e:
        if "ConditionalCheckFailed" in str(e):
            print("[sig] dedupe %s" % sid)
        else:
            print("[sig] emit failed %s: %s" % (sid, str(e)[:100]))
        return False


def _legacy_unvalidated_handler(event=None, context=None):
    validation_only = isinstance(event, dict) and event.get("mode") == "validate_only"
    classes = []
    ftd_all, ftr_all = [], []
    feed_status = {}
    for key, dk, rk, label in CLASSES:
        ftd_status, ftr_status = {}, {}
        ftd = fetch(dk, ftd_status); ftr = fetch(rk, ftr_status)
        feed_status["classes.%s.ftd" % key] = ftd_status
        feed_status["classes.%s.ftr" % key] = ftr_status
        ftd_all.append(ftd)
        ftr_all.append(ftr)
        if not ftd and not ftr:
            continue
        comb = combine(ftd, ftr)
        deep, seen = [], set()
        for d, v in comb:
            mk = d[:7]
            if mk not in seen:
                deep.append([d, v])
                seen.add(mk)
        classes.append({
            "key": key, "label": label,
            "deep": deep, "deep_start": (deep[0][0] if deep else None),
            "ftd": ftd[-720:], "ftr": ftr[-720:], "combined": comb[-720:],
            "ftd_latest": (ftd[-1][1] if ftd else None),
            "ftr_latest": (ftr[-1][1] if ftr else None),
            "stats": stats(comb),
        })

    now = datetime.now(timezone.utc).isoformat()
    for c in classes:
        c["quality"] = scope_quality(classes, (c["key"],), now, feed_status=feed_status)
        c["scope"] = c["key"]
        c["measurement_note"] = GROSS_NOTE
    # Additive normalized contract: ex-TIPS + TIPS on common dates only.
    treasury = annotate_treasury(normalized_treasury(classes), classes, now, feed_status=feed_status)

    # ops 3307: class spike -> graded signals (corporate & UST theses only)
    try:
        for c in classes:
            if validation_only or c["quality"]["status"] != "fresh" or treasury["quality"]["status"] != "fresh":
                continue
            st = c.get("stats") or {}
            if not st.get("spike"):
                continue
            aso = st.get("as_of", "")
            if c["key"] == "corporate":
                _emit_signal("fails-spike-corporate#LQD#%s" % aso,
                             "settlement_fails_spike", "DOWN", "LQD",
                             "SPY", st.get("latest"),
                             ["day_5", "day_21", "day_63"], 21)
            elif c["key"] == "ust_ex_tips":
                _emit_signal("fails-spike-ust#SPY#%s" % aso,
                             "settlement_fails_spike", "DOWN", "SPY",
                             "BIL", st.get("latest"),
                             ["day_5", "day_21", "day_63"], 21)
    except Exception as e:
        print("[fails] signal skip %s" % str(e)[:80])

    total_ftd = sum_series(ftd_all)
    total_ftr = sum_series(ftr_all)
    total_comb = combine(total_ftd, total_ftr)
    head = next((c for c in classes if c["key"] == "ust_ex_tips"), None)
    hs = head["stats"] if head else {}
    headline_quality = scope_quality(classes, ("ust_ex_tips",), now, feed_status=feed_status)
    totals_quality = scope_quality(classes, tuple(row[0] for row in CLASSES), now, feed_status=feed_status)
    headline_as_of = hs.get("as_of")
    headline_ftd = dict(head["ftd"]).get(headline_as_of) if head else None
    headline_ftr = dict(head["ftr"]).get(headline_as_of) if head else None

    # regime from the Treasury-fails percentile / z (the canonical plumbing tell)
    pct = hs.get("pctile"); z = hs.get("z")
    usable = headline_quality["status"] == "fresh" and treasury["quality"]["status"] == "fresh"
    regime, score = _regime(hs) if usable else ("UNKNOWN", None)

    drivers = []
    if usable:
        drivers.append("Treasury (ex-TIPS) settlement fails $%.0fbn combined (deliver $%.0fbn + receive $%.0fbn), %.0f%%ile / z %+.1f \u2014 %s"
                       % (hs["latest"], headline_ftd, headline_ftr,
                          pct, z, regime.lower()))
    else:
        drivers.append("Required Treasury data %s; no current ex-TIPS stress classification." % treasury["quality"]["status"])
    drivers.append(GROSS_NOTE)

    out = {
        "engine": "settlement-fails", "version": "1.1.0", "generated_at": now,
        "as_of": headline_as_of,
        "quality": treasury["quality"],
        "measurement_note": GROSS_NOTE,
        "signal": {"regime": regime, "score": score, "score_0_100": score, "drivers": drivers,
                   "scope": "ust_ex_tips", "quality": treasury["quality"]},
        "headline": {"label": "U.S. Treasury (ex-TIPS)",
                     "scope": "ust_ex_tips", "as_of": headline_as_of, "quality": headline_quality,
                     "measurement_note": GROSS_NOTE,
                     "field_units": {"ftd_bn": "usd_bn", "ftr_bn": "usd_bn", "combined_bn": "usd_bn",
                                     "max_bn": "usd_bn", "pctile": "pct", "z": "z_score"},
                     "ftd_bn": headline_ftd,
                     "ftr_bn": headline_ftr,
                     "combined_bn": (hs.get("latest") if head else None),
                     "z": z, "pctile": pct, "max_bn": hs.get("max"),
                     "combined": (head["combined"] if head else [])},
        "treasury": treasury,
        "classes": classes,
        "totals": {"ftd": total_ftd[-720:], "ftr": total_ftr[-720:], "combined": total_comb[-720:],
                   "scope": "all_asset", "label": "All-asset two-sided gross fails", "unit": "usd_bn",
                   "as_of": total_comb[-1][0] if total_comb else None, "quality": totals_quality,
                   "measurement_note": GROSS_NOTE},
        "source": "NY Fed Primary Dealer Statistics (FR 2004) \u2014 dealer financing settlement fails, weekly, $bn par",
    }
    if not validation_only:
        S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                      Body=strict_json_dumps(out, separators=(",", ":")).encode(),
                      ContentType="application/json", CacheControl="public, max-age=3600")
    return {"statusCode": 200, "body": strict_json_dumps({
        "validation_only": validation_only,
        "regime": regime, "score": score, "as_of": out["as_of"], "quality": out["quality"],
        "ust_combined_bn": hs.get("latest"), "ust_pctile": pct,
        "classes": len(classes), "total_combined_bn": (total_comb[-1][1] if total_comb else None)})}


def lambda_handler(event=None, context=None):
    """Publish original-response research; an anonymous HTTP read cannot collect or emit."""
    from fails_store import run, raw_reader
    from fails_research import CURRENT, CONTRACT
    try:
        request_context = event.get('requestContext', {}) if isinstance(event, dict) else {}
        if isinstance(request_context, dict) and request_context.get('http'):
            current = json.loads(raw_reader(S3, BUCKET)(CURRENT))
            if current.get('contract') != CONTRACT:
                raise ValueError('Original FR2004 publication is not available yet')
            return {'statusCode': 200, 'headers': {'Content-Type': 'application/json', 'Cache-Control': 'no-store'},
                    'body': strict_json_dumps(current, separators=(',', ':'))}
        result = run(S3, BUCKET)
        return {'statusCode': 200, 'body': strict_json_dumps(result)}
    except Exception as exc:
        print('[fails-research] publication unavailable: ' + type(exc).__name__)
        return {'statusCode': 503, 'headers': {'Cache-Control': 'no-store'},
                'body': strict_json_dumps({'status': 'unavailable', 'reason': 'Original FR2004 evidence could not be verified',
                                          'calls_eligible': False, 'sizing_eligible': False, 'portfolio_action': 'WAIT'})}
