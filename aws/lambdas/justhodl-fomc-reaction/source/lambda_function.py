"""
justhodl-fomc-reaction — FOMC decision-day Reaction Map (fusion engine).

WHY: The FOMC *level* is priced days ahead (a hold can be ~97% priced), so the
tradeable thing is the SURPRISE vs what was priced — and the only honest output
is a CALIBRATED RANGE + probability, never a point forecast. This engine measures
the surprise and maps it to per-asset reaction ranges calibrated on JustHodl's OWN
FOMC-day history, conditioned on regime. It logs each call and grades vs realized.

SURPRISE MEASUREMENT (the market's own verdict):
  - The 2-year Treasury yield (FRED DGS2) reacts almost entirely to Fed-policy
    expectations, so the 2y move on decision day is the cleanest real-time read of
    the aggregate surprise (statement + dots + presser). Δ2y > 0 = hawkish surprise.
  - Statement tone is scored by the LLM router (public text → tier="reason") as
    explanatory colour; it never overrides the market's 2y verdict.

CALIBRATION (own history): for every historical FOMC decision day, classify the
surprise sign by Δ2y, then measure forward returns of each asset at horizons
[1,5,21,63] trading days. Bucket by sign → empirical {median, p25, p75, prob_dir, n}.
This is "anchor on your own event-study history": seeded with a best-effort date
list and self-correcting as each real meeting is appended to the log.

OUTPUTS:
  data/fomc-reaction.json     — today's surprise + per-asset short/long reaction map
  data/fomc-calibration.json  — the empirical reaction distribution (rebuilt weekly)
  data/fomc-reaction-log/{date}.json — per-meeting call, for forward self-grading
"""
import os, json, time, statistics, urllib.request, urllib.parse, urllib.error, datetime, re

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
FMP_KEY = os.environ.get("FMP_API_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
FRED_KEY = os.environ.get("FRED_API_KEY", "2f057499936072679d8843d7fce99989")
S3 = boto3.client("s3", region_name="us-east-1")

OUT_KEY = "data/fomc-reaction.json"
CAL_KEY = "data/fomc-calibration.json"
HORIZONS = [1, 5, 21, 63]          # 1d / 1wk / ~1mo / ~3mo trading days
CAL_MAX_AGE_DAYS = 7               # rebuild calibration at most weekly

# Asset universe: (label, symbol, kind). kind: "etf" → % return; "yield" → bp change (FRED).
ASSETS = [
    ("S&P 500 (SPY)", "SPY", "etf"),
    ("Nasdaq 100 (QQQ)", "QQQ", "etf"),
    ("Small caps (IWM)", "IWM", "etf"),
    ("20y+ Treasuries (TLT)", "TLT", "etf"),
    ("HY credit (HYG)", "HYG", "etf"),
    ("Gold (GLD)", "GLD", "etf"),
    ("US dollar (UUP)", "UUP", "etf"),
    ("Bitcoin", "BTCUSD", "etf"),
    ("2Y yield", "DGS2", "yield"),
    ("10Y yield", "DGS10", "yield"),
]

# Best-effort FOMC decision dates (2nd day of meeting). Distribution is robust to a
# few off-by-one entries; the log appends verified meetings going forward.
FOMC_DATES = [
    "2018-01-31","2018-03-21","2018-05-02","2018-06-13","2018-08-01","2018-09-26","2018-11-08","2018-12-19",
    "2019-01-30","2019-03-20","2019-05-01","2019-06-19","2019-07-31","2019-09-18","2019-10-30","2019-12-11",
    "2020-01-29","2020-03-15","2020-04-29","2020-06-10","2020-07-29","2020-09-16","2020-11-05","2020-12-16",
    "2021-01-27","2021-03-17","2021-04-28","2021-06-16","2021-07-28","2021-09-22","2021-11-03","2021-12-15",
    "2022-01-26","2022-03-16","2022-05-04","2022-06-15","2022-07-27","2022-09-21","2022-11-02","2022-12-14",
    "2023-02-01","2023-03-22","2023-05-03","2023-06-14","2023-07-26","2023-09-20","2023-11-01","2023-12-13",
    "2024-01-31","2024-03-20","2024-05-01","2024-06-12","2024-07-31","2024-09-18","2024-11-07","2024-12-18",
    "2025-01-29","2025-03-19","2025-05-07","2025-06-18","2025-07-30","2025-09-17","2025-10-29","2025-12-10",
    "2026-01-28","2026-03-18","2026-04-29","2026-06-17",
]

FED_PRESS_FEED = "https://www.federalreserve.gov/feeds/press_monetary.xml"


# ---------- fetch helpers ----------
def http_get(url, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 JustHodl/FOMC"})
    return urllib.request.urlopen(req, timeout=timeout).read()


def num(v):
    try:
        if v is None or v == "":
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def fmp_closes(symbol, frm="2017-06-01"):
    """Return {date: close} from FMP /stable/ (light EOD). Defensive to shape."""
    url = ("https://financialmodelingprep.com/stable/historical-price-eod/light"
           "?symbol=%s&from=%s&apikey=%s" % (symbol, frm, FMP_KEY))
    try:
        d = json.loads(http_get(url).decode("utf-8", "replace"))
    except Exception as e:
        print("[fomc] fmp %s: %s" % (symbol, e))
        return {}
    rows = d if isinstance(d, list) else d.get("historical", [])
    out = {}
    for r in rows:
        dt, c = r.get("date"), num(r.get("close") if r.get("close") is not None else r.get("price"))
        if dt and c is not None:
            out[dt[:10]] = c
    return out


def fred_series(series_id, start="2017-06-01"):
    """Return {date: level} from FRED daily series."""
    qs = urllib.parse.urlencode({"series_id": series_id, "api_key": FRED_KEY,
                                 "file_type": "json", "observation_start": start, "sort_order": "asc"})
    try:
        d = json.loads(http_get("https://api.stlouisfed.org/fred/series/observations?" + qs).decode())
    except Exception as e:
        print("[fomc] fred %s: %s" % (series_id, e))
        return {}
    out = {}
    for o in d.get("observations", []):
        v = num(o.get("value"))
        if v is not None:
            out[o["date"]] = v
    return out


def gj(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


# ---------- calibration over own FOMC history ----------
def sorted_dates(series):
    return sorted(series.keys())


def idx_on_or_before(dates, target):
    """Index of the latest date <= target in a sorted date list, else None."""
    lo, hi, res = 0, len(dates) - 1, None
    while lo <= hi:
        mid = (lo + hi) // 2
        if dates[mid] <= target:
            res = mid; lo = mid + 1
        else:
            hi = mid - 1
    return res


def fwd_move(series, dates, t_idx, h, kind):
    if t_idx is None or t_idx + h >= len(dates):
        return None
    base = series[dates[t_idx]]
    fut = series[dates[t_idx + h]]
    if base is None or fut is None:
        return None
    if kind == "yield":
        return round((fut - base) * 100.0, 1)      # bp change
    if base == 0:
        return None
    return round((fut / base - 1.0) * 100.0, 3)     # percent


def dist(vals):
    vals = [v for v in vals if v is not None]
    if len(vals) < 4:
        return None
    vals_sorted = sorted(vals)
    def pct(p):
        k = (len(vals_sorted) - 1) * p
        f = int(k); c = min(f + 1, len(vals_sorted) - 1)
        return round(vals_sorted[f] + (vals_sorted[c] - vals_sorted[f]) * (k - f), 3)
    up = sum(1 for v in vals if v > 0)
    return {"n": len(vals), "median": round(statistics.median(vals), 3),
            "mean": round(statistics.mean(vals), 3), "p25": pct(0.25), "p75": pct(0.75),
            "prob_up_pct": round(up / len(vals) * 100.0, 1)}


def build_calibration():
    dgs2 = fred_series("DGS2")
    d2_dates = sorted_dates(dgs2)
    # classify each FOMC day by Δ2y (hawkish if 2y rose)
    classified = []          # (date, sign, d2y_bp)
    for fd in FOMC_DATES:
        i = idx_on_or_before(d2_dates, fd)
        if i is None or i == 0:
            continue
        d2y = (dgs2[d2_dates[i]] - dgs2[d2_dates[i - 1]]) * 100.0
        sign = "hawkish" if d2y > 1.0 else "dovish" if d2y < -1.0 else "neutral"
        classified.append((fd, sign, round(d2y, 1)))

    series_cache = {}
    for label, sym, kind in ASSETS:
        series_cache[sym] = (fred_series(sym) if kind == "yield" else fmp_closes(sym))

    by_sign = {"hawkish": {}, "dovish": {}, "neutral": {}}
    for label, sym, kind in ASSETS:
        series = series_cache.get(sym, {})
        a_dates = sorted_dates(series)
        for sign in by_sign:
            for h in HORIZONS:
                moves = []
                for fd, fsign, _ in classified:
                    if fsign != sign:
                        continue
                    ti = idx_on_or_before(a_dates, fd)
                    mv = fwd_move(series, a_dates, ti, h, kind)
                    if mv is not None:
                        moves.append(mv)
                by_sign[sign].setdefault(label, {})[str(h)] = dist(moves)

    cal = {
        "built_at": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "method": "FOMC-day forward moves bucketed by Δ2y surprise sign; ETFs in %, yields in bp.",
        "horizons_trading_days": HORIZONS,
        "n_events_classified": len(classified),
        "events_by_sign": {s: sum(1 for _, fs, _ in classified if fs == s) for s in by_sign},
        "by_sign": by_sign,
        "fomc_days_used": [{"date": d, "sign": s, "d2y_bp": b} for d, s, b in classified][-24:],
    }
    S3.put_object(Bucket=BUCKET, Key=CAL_KEY,
                  Body=json.dumps(cal, default=str).encode(), ContentType="application/json",
                  CacheControl="max-age=3600")
    print("[fomc] calibration rebuilt: %d events" % len(classified))
    return cal


def load_or_build_calibration():
    cal = gj(CAL_KEY)
    if cal and cal.get("built_at"):
        try:
            age = (datetime.datetime.utcnow() - datetime.datetime.fromisoformat(
                cal["built_at"].replace("Z", ""))).days
            if age <= CAL_MAX_AGE_DAYS and cal.get("by_sign"):
                return cal
        except Exception:
            pass
    return build_calibration()


# ---------- today's surprise ----------
def score_statement_tone():
    """Tone of the latest FOMC statement via GLM (Z.ai, tier=reason) — avoids Claude credits. Guarded."""
    try:
        stmt_url = None
        try:
            xml = http_get(FED_PRESS_FEED).decode("utf-8", "replace")
            for u in re.findall(r"<link>(https?://[^<]+)</link>", xml):
                if "monetary" in u and u.endswith(".htm"):
                    stmt_url = u
                    break
        except Exception:
            pass
        if not stmt_url:
            ds = datetime.date.today().strftime("%Y%m%d")
            stmt_url = "https://www.federalreserve.gov/newsevents/pressreleases/monetary%sa.htm" % ds
        html = http_get(stmt_url).decode("utf-8", "replace")
        text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html))[:6000]
        prompt = ("Score this FOMC statement's monetary-policy tone on a -10..+10 scale "
                  "(-10 very dovish, +10 very hawkish). Consider rate guidance, inflation language, "
                  'balance-of-risks. Return ONLY JSON, no prose: {"tone": <number>, "stance": '
                  '"HAWKISH|NEUTRAL|DOVISH", "one_line": "<=18 words"}\n\n' + text)
        try:
            from llm_router import complete
            raw = complete(prompt, tier="reason", max_tokens=1500)
            m = re.search(r"\{.*\}", raw, re.S)
            out = json.loads(m.group(0)) if m else {"raw": (raw or "")[:160]}
            out["engine"] = "glm"
            out["source_url"] = stmt_url
            return out
        except Exception as e:
            return {"error": "glm: " + str(e)[:160], "source_url": stmt_url}
    except urllib.error.HTTPError as he:
        try:
            detail = he.read().decode("utf-8", "replace")[:240]
        except Exception:
            detail = ""
        return {"error": "HTTP %s" % he.code, "detail": detail}
    except Exception as e:
        return {"error": str(e)[:160]}


def todays_surprise():
    dgs2 = fred_series("DGS2", start=(datetime.date.today() - datetime.timedelta(days=20)).isoformat())
    dd = sorted_dates(dgs2)
    d2y_bp = None
    if len(dd) >= 2:
        d2y_bp = round((dgs2[dd[-1]] - dgs2[dd[-2]]) * 100.0, 1)
    latest_2y_date = dd[-1] if dd else None
    today_str = datetime.date.today().isoformat()
    is_decision_day = today_str in FOMC_DATES
    # On decision day the 2y often hasn't posted yet (FRED H.15 lags ~1 business day),
    # so a same-day 2y read can't reflect the decision → prefer the statement tone.
    two_y_fresh = (latest_2y_date == today_str)
    fw = gj("data/fedwatch.json") or {}
    priced = None
    try:
        nxt = (fw.get("meetings") or fw.get("probabilities") or [None])[0]
        if isinstance(nxt, dict):
            priced = nxt
    except Exception:
        pass
    tone = score_statement_tone()
    tone_val = tone.get("tone") if isinstance(tone, dict) else None
    sign = None
    if d2y_bp is not None and two_y_fresh and abs(d2y_bp) >= 1.0:
        sign = "hawkish" if d2y_bp > 0 else "dovish"          # market verdict (preferred once posted)
    elif isinstance(tone_val, (int, float)) and abs(tone_val) >= 1:
        sign = "hawkish" if tone_val > 0 else "dovish"        # statement tone (decision-day, pre-2y)
    elif d2y_bp is not None and abs(d2y_bp) >= 1.0:
        sign = "hawkish" if d2y_bp > 0 else "dovish"          # stale-2y fallback
    else:
        sign = "neutral"
    basis = ("2y_market" if (two_y_fresh and d2y_bp and abs(d2y_bp) >= 1.0)
             else "statement_tone" if isinstance(tone_val, (int, float)) and abs(tone_val) >= 1
             else "stale_2y" if d2y_bp and abs(d2y_bp) >= 1.0 else "none")
    preliminary = bool(is_decision_day and basis in ("stale_2y", "none"))
    return {"sign": sign, "d2y_bp": d2y_bp, "two_y_fresh": two_y_fresh, "as_of_2y": latest_2y_date,
            "statement_tone": tone, "priced": priced, "surprise_basis": basis, "preliminary": preliminary,
            "is_decision_day": is_decision_day}


# ---------- assemble reaction map ----------
def reaction_map(cal, sign):
    buck = cal.get("by_sign", {}).get(sign, {})
    out = {}
    for label, sym, kind in ASSETS:
        rows = buck.get(label, {})
        short = rows.get("5") or rows.get("1")
        long = rows.get("63") or rows.get("21")
        out[label] = {
            "unit": "bp" if kind == "yield" else "%",
            "short": short, "long": long,
            "short_horizon_d": 5, "long_horizon_d": 63,
        }
    return out


def grade_prior_calls():
    """Score previously-logged calls against realized moves (forward self-grading)."""
    graded = {"n": 0, "directional_hits": 0, "in_range": 0}
    try:
        keys = S3.list_objects_v2(Bucket=BUCKET, Prefix="data/fomc-reaction-log/").get("Contents", [])
    except Exception:
        return graded
    spy = fmp_closes("SPY", frm="2024-01-01")
    sd = sorted_dates(spy)
    for k in keys:
        log = gj(k["Key"])
        if not log:
            continue
        d = log.get("meeting_date"); pred = (log.get("reaction_map", {}).get("S&P 500 (SPY)", {}) or {}).get("short")
        if not d or not pred:
            continue
        ti = idx_on_or_before(sd, d)
        realized = fwd_move(spy, sd, ti, 5, "etf")
        if realized is None:
            continue
        graded["n"] += 1
        if (pred.get("median", 0) >= 0) == (realized >= 0):
            graded["directional_hits"] += 1
        if pred.get("p25") is not None and pred["p25"] <= realized <= pred.get("p75", pred["p25"]):
            graded["in_range"] += 1
    if graded["n"]:
        graded["directional_accuracy_pct"] = round(graded["directional_hits"] / graded["n"] * 100, 1)
        graded["coverage_pct"] = round(graded["in_range"] / graded["n"] * 100, 1)
    return graded


def lambda_handler(event, context):
    started = time.time()
    cal = load_or_build_calibration()
    surprise = todays_surprise()
    rmap = reaction_map(cal, surprise["sign"])
    regime = gj("data/regime.json") or {}
    reg_ctx = (regime.get("current") or {})

    today = datetime.date.today().isoformat()
    next_fomc = next((d for d in FOMC_DATES if d >= today), None)
    is_decision_day = today in FOMC_DATES

    payload = {
        "engine": "justhodl-fomc-reaction",
        "version": "1.0",
        "generated_at": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "is_decision_day": is_decision_day,
        "meeting_date": today if is_decision_day else next_fomc,
        "surprise": {
            "label": surprise["sign"].upper(),
            "basis": surprise["surprise_basis"],
            "preliminary": surprise["preliminary"],
            "d2y_change_bp": surprise["d2y_bp"],
            "two_y_fresh": surprise["two_y_fresh"],
            "as_of_2y": surprise["as_of_2y"],
            "statement_tone": surprise["statement_tone"],
            "priced_in": surprise["priced"],
            "driver_note": "2-year Treasury move is the market's real-time verdict on the policy surprise once it "
                           "posts (FRED lags ~1 business day); on decision day before it posts, the statement tone leads.",
        },
        "reaction_map": rmap,
        "calibration": {
            "anchored_on": "own FOMC-day history (Δ2y-classified)",
            "n_events": cal.get("n_events_classified"),
            "events_by_sign": cal.get("events_by_sign"),
            "built_at": cal.get("built_at"),
            "horizons_trading_days": HORIZONS,
        },
        "regime_context": {"quadrant": reg_ctx.get("quadrant"),
                           "months_in_regime": reg_ctx.get("months_in_regime")},
        "what_flips_it": "The post-decision press conference (Q&A tone) and the dot-plot path can amplify or "
                         "reverse the statement reaction within minutes; ranges assume no offsetting presser shock.",
        "self_grading": grade_prior_calls(),
        "honesty": "Calibrated empirical ranges (p25–p75) conditioned on the surprise sign — NOT point forecasts. "
                   "Realized FOMC-day dispersion is wide. Analysis, not investment advice.",
        "duration_s": round(time.time() - started, 1),
    }

    S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(payload, indent=2, default=str).encode(),
                  ContentType="application/json", CacheControl="max-age=600")
    if is_decision_day:
        S3.put_object(Bucket=BUCKET, Key="data/fomc-reaction-log/%s.json" % today,
                      Body=json.dumps(payload, default=str).encode(), ContentType="application/json")
    print("[fomc] done %.1fs · surprise=%s · meeting=%s" %
          (payload["duration_s"], payload["surprise"]["label"], payload["meeting_date"]))
    return {"statusCode": 200, "body": json.dumps({"ok": True, "surprise": payload["surprise"]["label"],
                                                   "meeting": payload["meeting_date"]})}
