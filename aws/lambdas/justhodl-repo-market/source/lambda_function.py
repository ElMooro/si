"""
justhodl-repo-market -- the Repo Market Desk.

The dedicated overnight-funding engine: is the repo market CALM, FIRM,
ELEVATED, STRESSED or SEIZING -- and which pipe is the pressure in.

Why this engine exists: overnight repo is where black swans announce
themselves first. On 2019-09-17 SOFR printed 5.25 percent against an
admin rate near 2.10 -- and the 99th-percentile tail of the SOFR
distribution had been widening for DAYS before the median blew out.
March 2020, March 2023 and the 2024-25 quarter-end squeezes all showed
the same sequence: tail first, median second, everything else third.

Layers:

  1. RATES -- SOFR / TGCR / BGCR (secured) and EFFR / OBFR (unsecured)
     from the NY Fed markets API (the primary source, published ~8am ET),
     FRED fallback for SOFR/EFFR.

  2. DISTRIBUTION -- the crown jewel: the volume-weighted 1st/25th/75th/
     99th percentiles of SOFR. The tail (p99 minus the median) is THE
     early-warning metric; it is scored on level and on z vs its own
     1y history, and ranked against every day since April 2018.

  3. SPREADS -- SOFR-IORB (the modern reserve-scarcity gauge; stress is
     SOFR printing ABOVE the admin rate), SOFR-EFFR (secured trading
     over unsecured = collateral-side squeeze), SOFR minus the ON RRP
     award (floor softness), room to the SRF ceiling (top of the target
     range), TGCR-BGCR composition and OBFR-IORB. Pre-Jul-2021 history
     splices IOER into the admin-rate series so the z-scores and
     episode ranks span the Sep-2019 event.

  4. FACILITIES -- ON RRP balance (the buffer that absorbed the
     2022-24 drain; near zero it can no longer cushion reserve shocks),
     Standing Repo Facility take-up (anyone hitting the ceiling is
     news), discount-window primary credit and central-bank swap lines.

  5. RESERVES -- WRESBAL level and 4w/13w drain rate, TGA rebuilds and
     the Fed balance-sheet trend: the supply side of repo cash.

  6. CALENDAR -- month-end / quarter-end window flags plus the
     historically measured typical tail spike on those dates, computed
     from the fetched history itself, so a 12bp tail on Sep-30 is read
     differently from a 12bp tail on a random Tuesday.

  7. EPISODES -- the top historical stress days by tail since 2018
     (the engine rediscovers 2019-09-17 from raw data, nothing is
     hardcoded) and where today ranks against all of history.

Score: a weighted 0-100 composite over nine live subscores; missing
components renormalise over what is live. Regime flips into ELEVATED or
worse push a Telegram alert. Output: data/repo-market.json. All data is
NY Fed + FRED -- real, keyless-or-owned, auto-updating.
"""
import json
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3
try:
    import _fred_shim  # noqa: F401
except Exception:
    pass

SCHEMA = "1.0"
FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
NYFED = "https://markets.newyorkfed.org/api"
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/repo-market.json"
HIST_KEY = "data/repo-market-history.json"
HISTORY_START = "2018-04-02"   # first SOFR publication

s3 = boto3.client("s3")
SSM = boto3.client("ssm")

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")


def send_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("[tg] no creds")
        return
    try:
        body = json.dumps({"chat_id": TELEGRAM_CHAT_ID, "text": msg,
                           "parse_mode": "HTML",
                           "disable_web_page_preview": True}).encode()
        req = urllib.request.Request(
            "https://api.telegram.org/bot%s/sendMessage" % TELEGRAM_TOKEN,
            data=body, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10).read()
    except Exception as e:
        print("[tg] err: %s" % e)


# ---- fetch helpers ----------------------------------------------------------
def http_json(url, timeout=30, tries=3):
    req = urllib.request.Request(url, headers={
        "User-Agent": "justhodl-repo-market/1.0", "Accept": "application/json"})
    for attempt in range(tries):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8", "replace"))
        except Exception as e:
            if attempt == tries - 1:
                print("[http] %s -> %s" % (url[:80], e))
                return None
            time.sleep(1 + attempt)


def get_fred_key():
    for k in ("FRED_API_KEY", "FRED_KEY", "FRED_TOKEN"):
        if os.environ.get(k):
            return os.environ[k]
    try:
        return SSM.get_parameter(Name="/justhodl/fred/api-key",
                                 WithDecryption=True)["Parameter"]["Value"]
    except Exception:
        return "2f057499936072679d8843d7fce99989"


def fred_series(series_id, start, key):
    qs = urllib.parse.urlencode({"series_id": series_id, "api_key": key,
                                 "file_type": "json",
                                 "observation_start": start})
    d = http_json("%s?%s" % (FRED_BASE, qs)) or {}
    out = []
    for o in d.get("observations", []):
        v = o.get("value")
        if v in (None, ".", ""):
            continue
        try:
            out.append((o["date"], float(v)))
        except (TypeError, ValueError):
            continue
    out.sort(key=lambda x: x[0])
    return out


# ---- NY Fed markets API (defensive field parsing) --------------------------
def _row_num(row, *needles):
    """First numeric value whose (lowercased) key contains ALL needles."""
    for k, v in row.items():
        lk = k.lower()
        if all(n in lk for n in needles):
            try:
                return float(v)
            except (TypeError, ValueError):
                continue
    return None


def _row_date(row):
    for k in ("effectiveDate", "effective_date", "date"):
        if row.get(k):
            return str(row[k])[:10]
    return None


def nyfed_rates(kind, start):
    """kind in ('secured','unsecured'). Returns {TYPE: [rowdict,...]} sorted
    by date, rowdict = {date, rate, p1, p25, p75, p99, volume}. History is
    pulled in <=3y chunks so a server-side row cap on long ranges cannot
    silently truncate the 2018-2020 episodes out of the record."""
    end_dt = datetime.now(timezone.utc)
    rows = []
    cur = _dt(start)
    while cur < end_dt.replace(tzinfo=None):
        chunk_end = min(cur + timedelta(days=1095),
                        end_dt.replace(tzinfo=None))
        d = http_json("%s/rates/%s/all/search.json?startDate=%s&endDate=%s"
                      % (NYFED, kind, cur.strftime("%Y-%m-%d"),
                         chunk_end.strftime("%Y-%m-%d")))
        rows.extend((d or {}).get("refRates") or [])
        cur = chunk_end + timedelta(days=1)
    out = {}
    for r in rows:
        typ = str(r.get("type") or r.get("rateType") or "").upper()
        dt = _row_date(r)
        rate = _row_num(r, "percentrate") or _row_num(r, "rate")
        if not typ or not dt or rate is None:
            continue
        out.setdefault(typ, []).append({
            "date": dt, "rate": rate,
            "p1": _row_num(r, "ercentile1"),
            "p25": _row_num(r, "ercentile25"),
            "p75": _row_num(r, "ercentile75"),
            "p99": _row_num(r, "ercentile99"),
            "volume": _row_num(r, "olume"),
        })
    for typ in out:
        out[typ].sort(key=lambda x: x["date"])
    return out


def nyfed_srf_latest():
    d = http_json("%s/rp/srf/results/latest.json" % NYFED)
    ops = (d or {}).get("repo")
    if isinstance(ops, list) and ops:
        amt = None
        for k in ("totalAmtAccepted", "totalAmtSubmitted"):
            try:
                amt = float(ops[0].get(k))
                break
            except (TypeError, ValueError):
                continue
        return {"date": _row_date(ops[0]), "accepted_usd_bn":
                round((amt or 0.0) / 1e9, 2)}
    return None


# ---- series maths -----------------------------------------------------------
def _dt(s):
    return datetime.strptime(s[:10], "%Y-%m-%d")


def value_days_ago(series, days):
    if not series:
        return None
    target = _dt(series[-1][0]) - timedelta(days=days)
    best, bd = None, None
    for d, v in series:
        gap = abs((_dt(d) - target).days)
        if bd is None or gap < bd:
            bd, best = gap, v
    return best


def zscore_1y(series):
    vals = [v for _, v in series][-252:]
    if len(vals) < 40:
        return None
    m = sum(vals) / len(vals)
    var = sum((x - m) ** 2 for x in vals) / len(vals)
    if var <= 0:
        return 0.0
    return round((vals[-1] - m) / (var ** 0.5), 2)


def pctile_rank(value, values):
    vals = [v for v in values if v is not None]
    if value is None or len(vals) < 30:
        return None
    return round(100.0 * sum(1 for v in vals if v <= value) / len(vals), 1)


def is_period_end_window(date_str, months):
    """True when date_str sits within +/-2 calendar days of the last day
    of a month whose month number is in `months` (12,3,6,9 = quarter
    ends; all twelve = month ends). Covers the turn itself and its
    immediate aftermath, when window-dressing pressure prints."""
    d = _dt(date_str)
    ends = []
    first_this = d.replace(day=1)
    ends.append(first_this - timedelta(days=1))          # end of prev month
    nxt = (first_this + timedelta(days=32)).replace(day=1)
    ends.append(nxt - timedelta(days=1))                 # end of this month
    for e in ends:
        if e.month in months and abs((d - e).days) <= 2:
            return True
    return False


def clamp(v, lo=0.0, hi=100.0):
    return max(lo, min(hi, v))


def ramp(value, zero_at, full_at):
    """Linear 0..100 subscore between two calibration points (works for
    rising or falling stress directions)."""
    if value is None:
        return None
    if full_at == zero_at:
        return None
    t = (value - zero_at) / float(full_at - zero_at)
    return clamp(t * 100.0)


# ---- the composite ----------------------------------------------------------
REGIMES = [(80, "SEIZING"), (60, "STRESSED"), (40, "ELEVATED"),
           (20, "FIRM"), (-1, "CALM")]


def regime_of(score):
    for th, name in REGIMES:
        if score >= th:
            return name
    return "CALM"


def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    key = get_fred_key()
    start_1y = (now - timedelta(days=430)).strftime("%Y-%m-%d")

    # ---- 1. NY Fed rates (full history for episode work) ----
    sec = nyfed_rates("secured", HISTORY_START)
    uns = nyfed_rates("unsecured", start_1y)
    sofr_rows = sec.get("SOFR") or []
    tgcr_rows = sec.get("TGCR") or []
    bgcr_rows = sec.get("BGCR") or []
    effr_rows = uns.get("EFFR") or []
    obfr_rows = uns.get("OBFR") or []
    src_primary = bool(sofr_rows)

    # FRED fallbacks / admin-rate stack
    sofr_fred = fred_series("SOFR", HISTORY_START, key)
    effr_fred = fred_series("EFFR", start_1y, key)
    iorb = fred_series("IORB", "2021-07-29", key)
    ioer = fred_series("IOER", HISTORY_START, key)      # pre-Jul-2021
    admin = sorted({d: v for d, v in (ioer + iorb)}.items())
    rrp = fred_series("RRPONTSYD", HISTORY_START, key)
    rrp_award = fred_series("RRPONTSYAWARD", start_1y, key)
    srf_hist = fred_series("RPONTSYD", HISTORY_START, key)
    wresbal = fred_series("WRESBAL", HISTORY_START, key)
    tga = fred_series("WTREGEN", start_1y, key)
    walcl = fred_series("WALCL", start_1y, key)
    dwin = fred_series("WLCFLPCL", start_1y, key)
    swpt = fred_series("SWPT", start_1y, key)
    ceil = fred_series("DFEDTARU", start_1y, key)

    if not sofr_rows and sofr_fred:
        sofr_rows = [{"date": d, "rate": v, "p1": None, "p25": None,
                      "p75": None, "p99": None, "volume": None}
                     for d, v in sofr_fred]
    if not sofr_rows:
        print("[fatal] no SOFR from NY Fed or FRED")
        return {"statusCode": 500,
                "body": json.dumps({"ok": False, "err": "no SOFR"})}
    if not effr_rows and effr_fred:
        effr_rows = [{"date": d, "rate": v} for d, v in effr_fred]

    sofr_s = [(r["date"], r["rate"]) for r in sofr_rows]
    last = sofr_rows[-1]
    as_of = last["date"]

    # ---- 2. distribution + tail ----
    tail_hist = [(r["date"], (r["p99"] - r["rate"]) * 100.0)
                 for r in sofr_rows
                 if r.get("p99") is not None and r.get("rate") is not None]
    tail_bps = tail_hist[-1][1] if tail_hist else None
    tail_z = zscore_1y(tail_hist) if tail_hist else None
    tail_rank = pctile_rank(tail_bps, [v for _, v in tail_hist])
    distribution = {
        "available": bool(tail_hist),
        "as_of": as_of,
        "sofr": last["rate"], "p1": last.get("p1"), "p25": last.get("p25"),
        "p75": last.get("p75"), "p99": last.get("p99"),
        "tail_bps": round(tail_bps, 1) if tail_bps is not None else None,
        "tail_z_1y": tail_z,
        "tail_pctile_since_2018": tail_rank,
        "volume_usd_bn": last.get("volume"),
        "series_tail_1y": [[d, round(v, 1)] for d, v in tail_hist[-260:]],
        "note": ("p99 minus the volume-weighted median. The marginal "
                 "borrower pays the tail; it widens days before the "
                 "median moves -- the Sep-2019 signature."),
    }

    # SOFR volume z
    vol_hist = [(r["date"], r["volume"]) for r in sofr_rows
                if r.get("volume") is not None]
    vol_z = zscore_1y(vol_hist) if vol_hist else None

    # ---- 3. spreads ----
    admin_by_d = dict(admin)

    def spread_vs(series_rows, other_by_date, scale=100.0):
        outp = []
        for r in series_rows:
            o = other_by_date.get(r["date"])
            if o is not None and r.get("rate") is not None:
                outp.append((r["date"], (r["rate"] - o) * scale))
        return outp

    # forward-fill admin rate onto SOFR dates (IORB changes step-wise)
    ff_admin, lastv = {}, None
    a_i, a_list = 0, admin
    for d, _v in sofr_s:
        while a_i < len(a_list) and a_list[a_i][0] <= d:
            lastv = a_list[a_i][1]
            a_i += 1
        if lastv is not None:
            ff_admin[d] = lastv
    sofr_iorb_hist = spread_vs(sofr_rows, ff_admin)
    si_bps = sofr_iorb_hist[-1][1] if sofr_iorb_hist else None
    si_rank = pctile_rank(si_bps, [v for _, v in sofr_iorb_hist])

    effr_by_d = {r["date"]: r["rate"] for r in effr_rows}
    sofr_effr_hist = spread_vs(sofr_rows, effr_by_d)
    se_bps = sofr_effr_hist[-1][1] if sofr_effr_hist else None

    tgcr_by_d = {r["date"]: r["rate"] for r in tgcr_rows}
    bg_tg = None
    if bgcr_rows and tgcr_rows:
        b = bgcr_rows[-1]
        t = tgcr_by_d.get(b["date"])
        if t is not None:
            bg_tg = round((b["rate"] - t) * 100.0, 1)

    floor_bps = None
    if rrp_award and sofr_s:
        floor_bps = round((sofr_s[-1][1] - rrp_award[-1][1]) * 100.0, 1)
    ceiling_room_bps = None
    if ceil and sofr_s:
        ceiling_room_bps = round((ceil[-1][1] - sofr_s[-1][1]) * 100.0, 1)
    obfr_iorb_bps = None
    if obfr_rows and ff_admin:
        oa = ff_admin.get(obfr_rows[-1]["date"])
        if oa is not None:
            obfr_iorb_bps = round((obfr_rows[-1]["rate"] - oa) * 100.0, 1)

    spreads = {
        "sofr_iorb": {
            "bps": round(si_bps, 1) if si_bps is not None else None,
            "z_1y": zscore_1y(sofr_iorb_hist),
            "pctile_since_2018": si_rank,
            "series_1y": [[d, round(v, 1)]
                          for d, v in sofr_iorb_hist[-260:]],
            "note": ("SOFR minus the admin rate (IORB; IOER spliced "
                     "pre-Jul-2021). Stress is SOFR printing ABOVE it -- "
                     "Sep-2019 peaked around +300bps. Persistently "
                     "negative = abundant reserves.")},
        "sofr_effr": {
            "bps": round(se_bps, 1) if se_bps is not None else None,
            "z_1y": zscore_1y(sofr_effr_hist),
            "note": "Secured above unsecured = collateral-side squeeze."},
        "sofr_floor": {"bps": floor_bps,
                       "note": "SOFR minus the ON RRP award rate."},
        "ceiling_room": {"bps": ceiling_room_bps,
                         "note": ("Top of the target range (the SRF rate) "
                                  "minus SOFR. Zero or negative = the "
                                  "ceiling is being tested.")},
        "bgcr_tgcr": {"bps": bg_tg,
                      "note": "GCF/blind-brokered pressure vs tri-party."},
        "obfr_iorb": {"bps": obfr_iorb_bps,
                      "note": "Unsecured-side parallel of SOFR-IORB."},
    }

    # ---- 4. facilities ----
    srf_now = nyfed_srf_latest()
    srf_fred_bn = (srf_hist[-1][1] / 1000.0) if srf_hist else None
    srf_bn = (srf_now or {}).get("accepted_usd_bn")
    if srf_bn is None:
        srf_bn = round(srf_fred_bn, 2) if srf_fred_bn is not None else None
    rrp_bn = round(rrp[-1][1], 1) if rrp else None
    dwin_bn = round(dwin[-1][1] / 1000.0, 1) if dwin else None
    swpt_bn = round(swpt[-1][1], 1) if swpt else None
    facilities = {
        "rrp_usd_bn": rrp_bn,
        "rrp_series_1y": [[d, round(v, 1)] for d, v in rrp[-260:]],
        "rrp_note": ("The buffer. Near zero the RRP can no longer "
                     "cushion reserve drains -- shocks pass straight "
                     "into repo."),
        "srf_usd_bn": srf_bn, "srf_as_of": (srf_now or {}).get("date"),
        "srf_note": "Anyone paying the ceiling rate is news.",
        "discount_window_usd_bn": dwin_bn,
        "swap_lines_usd_bn": swpt_bn,
    }

    # ---- 5. reserves ----
    res_now = wresbal[-1][1] if wresbal else None
    res_4w = value_days_ago(wresbal, 28) if wresbal else None
    res_13w = value_days_ago(wresbal, 91) if wresbal else None
    reserves = {
        "wresbal_usd_bn": round(res_now, 0) if res_now else None,
        "chg_4w_pct": (round((res_now / res_4w - 1) * 100, 2)
                       if res_now and res_4w else None),
        "chg_13w_pct": (round((res_now / res_13w - 1) * 100, 2)
                        if res_now and res_13w else None),
        "tga_usd_bn": round(tga[-1][1], 1) if tga else None,
        "walcl_13w_chg_usd_bn": (round((walcl[-1][1] -
                                        (value_days_ago(walcl, 91) or
                                         walcl[-1][1])) / 1000.0, 1)
                                 if walcl else None),
    }

    # ---- 6. calendar context (measured, not asserted) ----
    qe_win = is_period_end_window(as_of, {3, 6, 9, 12})
    me_win = is_period_end_window(as_of, set(range(1, 13)))
    qe_tails = [v for d, v in tail_hist
                if is_period_end_window(d, {3, 6, 9, 12})]
    normal_tails = [v for d, v in tail_hist
                    if not is_period_end_window(d, set(range(1, 13)))]

    def med(vs):
        vs = sorted(vs)
        return round(vs[len(vs) // 2], 1) if vs else None
    calendar = {
        "quarter_end_window": qe_win, "month_end_window": me_win,
        "median_tail_quarter_end_bps": med(qe_tails),
        "median_tail_normal_bps": med(normal_tails),
        "note": ("Dealer balance-sheet window dressing makes period-ends "
                 "structurally tighter; the same tail reading means less "
                 "inside a window than outside one."),
    }

    # ---- 7. episodes (rediscovered from raw history) ----
    si_by_d = dict(sofr_iorb_hist)
    ranked = sorted(tail_hist, key=lambda x: -x[1])[:5] if tail_hist else []
    episodes = {
        "top_tail_days": [{"date": d, "tail_bps": round(v, 1),
                           "sofr_iorb_bps": (round(si_by_d.get(d), 1)
                                             if si_by_d.get(d) is not None
                                             else None)}
                          for d, v in ranked],
        "today_tail_rank_pctile": tail_rank,
        "today_sofr_iorb_rank_pctile": si_rank,
    }

    # ---- 8. the composite ----
    comps = []

    def comp(cid, label, sub, weight, value_txt, detail):
        if sub is None:
            return
        comps.append({"id": cid, "label": label,
                      "subscore": round(sub, 1), "weight": weight,
                      "reading": value_txt, "detail": detail})

    comp("tail", "SOFR p99 tail", ramp(tail_bps, 4, 60), 0.22,
         ("%.1f bps" % tail_bps) if tail_bps is not None else "n/a",
         "The marginal borrower's rate over the median. Widens first.")
    if (tail_z is not None and comps and comps[-1]["id"] == "tail"):
        # z-kicker folded into the tail subscore rather than double-counted
        comps[-1]["subscore"] = round(clamp(
            comps[-1]["subscore"] + max(0.0, (tail_z - 1.5)) * 8.0), 1)
    comp("sofr_iorb", "SOFR - IORB", ramp(si_bps, -3, 25), 0.22,
         ("%+.1f bps" % si_bps) if si_bps is not None else "n/a",
         "Above the admin rate = cash scarce. Sep-2019 hit ~+300.")
    comp("ceiling", "Room to SRF ceiling",
         (100.0 - ramp(ceiling_room_bps, 0, 15)
          if ceiling_room_bps is not None else None), 0.10,
         ("%.1f bps" % ceiling_room_bps)
         if ceiling_room_bps is not None else "n/a",
         "SOFR at the top of the target range = the backstop is live.")
    comp("srf", "SRF take-up", ramp(srf_bn, 0.2, 25), 0.12,
         ("$%.1fB" % srf_bn) if srf_bn is not None else "n/a",
         "Standing Repo Facility usage -- rare, therefore loud.")
    comp("rrp_buffer", "RRP buffer depletion",
         (100.0 - ramp(rrp_bn, 5, 250) if rrp_bn is not None else None),
         0.08, ("$%.0fB" % rrp_bn) if rrp_bn is not None else "n/a",
         "The shock absorber between QT and repo.")
    comp("reserves", "Reserve drain (13w)",
         (ramp(-(reserves["chg_13w_pct"] or 0), 0, 6)
          if reserves["chg_13w_pct"] is not None else None), 0.10,
         ("%+.1f%%" % reserves["chg_13w_pct"])
         if reserves["chg_13w_pct"] is not None else "n/a",
         "Falling reserves thin the cash side of repo.")
    comp("volume", "SOFR volume surge",
         ramp(vol_z, 1.0, 4.0) if vol_z is not None else None, 0.06,
         ("z=%.1f" % vol_z) if vol_z is not None else "n/a",
         "Squeezes print on record volume.")
    comp("discount", "Discount window", ramp(dwin_bn, 2, 50), 0.05,
         ("$%.1fB" % dwin_bn) if dwin_bn is not None else "n/a",
         "Primary credit -- bank-level funding distress.")
    comp("sofr_effr", "SOFR - EFFR", ramp(se_bps, 0, 20), 0.05,
         ("%+.1f bps" % se_bps) if se_bps is not None else "n/a",
         "Secured over unsecured = collateral squeeze.")

    tw = sum(c["weight"] for c in comps)
    score = (round(sum(c["weight"] * c["subscore"] for c in comps) / tw, 1)
             if tw else None)
    regime = regime_of(score) if score is not None else "UNKNOWN"

    prev = {}
    try:
        prev = json.loads(s3.get_object(Bucket=S3_BUCKET,
                                        Key=OUT_KEY)["Body"].read())
    except Exception:
        pass
    prev_regime = prev.get("regime")

    top = sorted(comps, key=lambda c: -(c["subscore"] * c["weight"]))[:3]
    headline = ("Repo stress %s/100 -- %s. Tail %s, SOFR-IORB %s, RRP "
                "buffer %s. Hottest: %s."
                % (score, regime,
                   ("%.1fbps" % tail_bps) if tail_bps is not None else "n/a",
                   ("%+.1fbps" % si_bps) if si_bps is not None else "n/a",
                   ("$%.0fB" % rrp_bn) if rrp_bn is not None else "n/a",
                   ", ".join(c["label"] for c in top)))
    if qe_win:
        headline += " Inside a quarter-end window."

    out = {
        "schema_version": SCHEMA,
        "engine": "justhodl-repo-market",
        "generated_at": now.isoformat(),
        "build_seconds": round(time.time() - t0, 1),
        "as_of": as_of,
        "source_primary": "NY Fed markets API" if src_primary
                          else "FRED (NY Fed unavailable)",
        "repo_stress_score": score,
        "regime": regime,
        "prev_regime": prev_regime,
        "headline": headline,
        "components": comps,
        "components_live": len(comps),
        "rates": {
            "sofr": {"value": last["rate"], "as_of": as_of,
                     "series_1y": [[d, v] for d, v in sofr_s[-260:]]},
            "tgcr": ({"value": tgcr_rows[-1]["rate"],
                      "as_of": tgcr_rows[-1]["date"]} if tgcr_rows else None),
            "bgcr": ({"value": bgcr_rows[-1]["rate"],
                      "as_of": bgcr_rows[-1]["date"]} if bgcr_rows else None),
            "effr": ({"value": effr_rows[-1]["rate"],
                      "as_of": effr_rows[-1]["date"]} if effr_rows else None),
            "obfr": ({"value": obfr_rows[-1]["rate"],
                      "as_of": obfr_rows[-1]["date"]} if obfr_rows else None),
        },
        "distribution": distribution,
        "spreads": spreads,
        "facilities": facilities,
        "reserves": reserves,
        "calendar": calendar,
        "episodes": episodes,
        "how_to_read": (
            "The score is a weighted blend of nine live repo-plumbing "
            "gauges, 0 (glassy) to 100 (seizing). The two heaviest are "
            "the SOFR p99 tail -- the rate the marginal borrower pays, "
            "which widens days before the median -- and SOFR minus IORB, "
            "which flips positive when cash is scarce. Facilities and "
            "reserves describe how much buffer is left; the calendar "
            "block says how much of today's pressure is just the "
            "quarter-end turn. Episodes rank today against every day "
            "since SOFR began in April 2018."),
        "disclaimer": ("A probabilistic funding-stress radar built from "
                       "official NY Fed and FRED data. It shifts odds; "
                       "it is not investment advice."),
    }

    try:
        s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                      Body=json.dumps(out, allow_nan=False),
                      ContentType="application/json",
                      CacheControl="max-age=300")
    except Exception as e:
        print("[s3] %s" % e)
        return {"statusCode": 500, "body": json.dumps({"ok": False})}

    # daily history ledger (append, capped)
    try:
        hist = []
        try:
            hist = json.loads(s3.get_object(
                Bucket=S3_BUCKET, Key=HIST_KEY)["Body"].read())
        except Exception:
            pass
        day = as_of
        hist = [h for h in hist if h.get("date") != day][-900:]
        hist.append({"date": day, "score": score, "regime": regime,
                     "tail_bps": distribution.get("tail_bps"),
                     "sofr_iorb_bps": spreads["sofr_iorb"]["bps"],
                     "rrp_usd_bn": rrp_bn, "srf_usd_bn": srf_bn})
        s3.put_object(Bucket=S3_BUCKET, Key=HIST_KEY,
                      Body=json.dumps(hist), ContentType="application/json")
    except Exception as e:
        print("[history] %s" % e)

    order = ["CALM", "FIRM", "ELEVATED", "STRESSED", "SEIZING"]
    if (prev_regime in order and regime in order and
            order.index(regime) > order.index(prev_regime) and
            order.index(regime) >= 2):
        send_telegram("\u26a0\ufe0f <b>Repo Market</b> regime %s \u2192 "
                      "<b>%s</b> (score %s). %s"
                      % (prev_regime, regime, score, headline))

    print(json.dumps({"score": score, "regime": regime,
                      "components": len(comps), "tail_bps":
                      distribution.get("tail_bps"),
                      "src": out["source_primary"]}))
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "score": score, "regime": regime,
        "components": len(comps)})}
