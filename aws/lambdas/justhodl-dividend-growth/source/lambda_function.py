"""
justhodl-dividend-growth - the Dividend Compounder screen.

A rising dividend is one of the hardest signals a company can send. Once a
board raises the payout it has publicly committed cash it must keep finding,
quarter after quarter - so a long, unbroken record of dividend GROWTH is a
costly, credible statement that management believes the cash flow is durable.
The total-return math compounds: a reinvested, growing dividend has driven a
large share of the equity market's real return for a century.

But the naive dividend screen is a trap, so this engine gates hard:

  - HIGHEST YIELD IS NOT THE GOAL. A 9% headline yield is usually the market
    pricing a cut that has not been announced yet. We rank dividend GROWTH,
    not dividend level, and we prefer a moderate, well-covered yield.
  - A DIVIDEND THE CASH FLOW CANNOT FUND gets cut. We require free cash flow
    to largely cover the payout (or, where FCF is not clean, a sane earnings
    payout ratio).
  - A COMPANY THAT HAS CUT BEFORE will cut again. Any sustained year-over-year
    drop in the annual dividend disqualifies a name from the compounder tiers.
  - SPECIAL DIVIDENDS distort a raw annual series (a one-off special spikes a
    year, then "falls" the next). We normalise one-year spikes out before
    measuring the streak and the CAGR.
  - A DISTRESSED BALANCE SHEET cannot sustain a payout. We screen out names
    in Altman distress / weak Piotroski.

The universe is every dividend payer in the stock-screener; per name we pull
the full per-payment dividend history from FMP, aggregate it by calendar
year, normalise specials, and measure the 3y / 5y dividend CAGR, the
consecutive growth streak, and cash coverage.

Tiers: ARISTOCRAT (10+ year streak, real growth), GROWER (5+), EMERGING (3+).
High-yield names that fail the gates are quarantined in a YIELD-TRAP list.

OUTPUT data/dividend-growth.json     SCHEDULE daily 14:20 UTC
Real data only. Research, not advice.
"""
import json
import os
import time
import urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3")
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/dividend-growth.json"
SCHEMA = "dividend-growth-1.0"

FMP_KEY = os.environ.get("FMP_KEY") or "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
FMP_BASE = "https://financialmodelingprep.com/stable"

UNIVERSE_CAP = 700      # top-N dividend payers by market cap
MIN_YIELD = 0.5         # below this it is a token dividend, not an income case
MAX_YIELD = 9.0         # above this almost always a trap candidate
MIN_YEARS = 4           # complete years needed for a full CAGR/streak score
WORKERS = 12


def num(v):
    try:
        f = float(v)
        return f if f == f else None
    except (TypeError, ValueError):
        return None


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def median(xs):
    xs = sorted(x for x in xs if x is not None)
    if not xs:
        return None
    n = len(xs)
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2.0


# ---------------------------------------------------------------- FMP fetch
def fetch_dividends(symbol):
    url = "%s/dividends?symbol=%s&apikey=%s" % (FMP_BASE, symbol, FMP_KEY)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read().decode("utf-8", "ignore"))
        if isinstance(data, list):
            return symbol, data
    except Exception:
        pass
    return symbol, None


# ----------------------------------------------------------- dividend math
def annual_dividends(records):
    """Sum the (split-adjusted) dividend paid in each calendar year."""
    by_year = defaultdict(float)
    for rec in records:
        if not isinstance(rec, dict):
            continue
        d = rec.get("date") or rec.get("paymentDate") or ""
        if len(d) < 4:
            continue
        try:
            yr = int(d[:4])
        except ValueError:
            continue
        adj = num(rec.get("adjDividend"))
        if adj is None:
            adj = num(rec.get("dividend"))
        if adj is None or adj <= 0:
            continue
        by_year[yr] += adj
    return dict(by_year)


def normalise_specials(series):
    """series = [(year, total)] ascending. Damp one-year special-dividend
    spikes down to the neighbouring baseline so the streak / CAGR reflect the
    REGULAR dividend, not a one-off."""
    vals = [v for _, v in series]
    out = list(vals)
    for i in range(len(vals)):
        nb = []
        if i > 0:
            nb.append(vals[i - 1])
        if i < len(vals) - 1:
            nb.append(vals[i + 1])
        if nb:
            ref = sum(nb) / len(nb)
            if ref > 0 and vals[i] > ref * 1.4:
                out[i] = ref
    return [(series[i][0], out[i]) for i in range(len(series))]


def analyse_history(records):
    """Return dividend-growth metrics from the per-payment history, or None."""
    ann = annual_dividends(records)
    if not ann:
        return None
    cur_year = datetime.now(timezone.utc).year
    complete = sorted(y for y in ann if y < cur_year)
    if len(complete) < 3:
        return None
    raw = [(y, ann[y]) for y in complete]
    series = normalise_specials(raw)        # [(year, value)] ascending

    def cagr(n):
        if len(series) < n + 1:
            return None
        start = series[-1 - n][1]
        end = series[-1][1]
        if start <= 0 or end <= 0:
            return None
        return ((end / start) ** (1.0 / n) - 1.0) * 100.0

    cagr5 = cagr(5)
    cagr3 = cagr(3)

    # consecutive growth streak (most-recent years, small noise tolerated)
    streak = 0
    for i in range(len(series) - 1, 0, -1):
        prev = series[i - 1][1]
        cur = series[i][1]
        if prev <= 0:
            break
        if cur >= prev * 0.995:
            streak += 1
        else:
            break

    # sustained cut: a real drop, not a special rolling off
    had_cut = False
    for i in range(1, len(series)):
        prev = series[i - 1][1]
        cur = series[i][1]
        if prev > 0 and cur < prev * 0.95:
            if i < 2 or cur < series[i - 2][1] * 0.95:
                had_cut = True
                break

    hist = [{"year": y, "dividend": round(v, 4)} for y, v in series[-8:]]
    return {
        "cagr5": cagr5,
        "cagr3": cagr3,
        "streak": streak,
        "had_cut": had_cut,
        "n_years": len(series),
        "history": hist,
        "latest_annual": series[-1][1],
    }


# --------------------------------------------------------------- scoring
def score_name(row, m):
    """row = screener record, m = analyse_history() result -> entry dict."""
    sym = (row.get("symbol") or "").upper()
    sector = row.get("sector") or ""
    dy = num(row.get("dividendYield"))
    if dy is None or dy <= 0:
        return None
    fcfy = num(row.get("fcfYieldCalc"))
    pe = num(row.get("peRatio"))
    pio = num(row.get("piotroski"))
    az = num(row.get("altmanZ"))
    de = num(row.get("debtToEquity"))

    cagr5 = m["cagr5"]
    cagr3 = m["cagr3"]
    streak = m["streak"]
    had_cut = m["had_cut"]
    # growth rate used for tiers: prefer 5y, fall back to 3y
    grate = cagr5 if cagr5 is not None else cagr3

    # ---- coverage / sustainability ----
    fcf_cov = (fcfy / dy) if (fcfy is not None and dy > 0) else None
    earn_payout = (dy * pe) if (pe is not None and pe > 0) else None
    is_re = sector in ("Real Estate", "Financial Services", "Financials")

    if fcf_cov is not None and fcf_cov > 0:
        sustainable = fcf_cov >= 0.70
    elif earn_payout is not None and not is_re:
        sustainable = earn_payout < 80.0
    else:
        # REIT / lender with no clean FCF read - cannot gate, allow but flag
        sustainable = True

    distress = (az is not None and az < 1.8) or (pio is not None and pio < 3)

    # ---- yield-trap quarantine ----
    trap = False
    if dy >= 6.0:
        if had_cut or distress or (fcf_cov is not None and fcf_cov < 0.6) \
           or (grate is not None and grate < 0):
            trap = True
    if (grate is not None and grate < -2) and dy >= 4.0:
        trap = True

    entry = {
        "symbol": sym,
        "name": row.get("name") or "",
        "sector": sector,
        "industry": row.get("industry") or "",
        "price": num(row.get("price")),
        "market_cap": num(row.get("marketCap")),
        "dividend_yield_pct": round(dy, 2),
        "div_cagr_5y_pct": round(cagr5, 1) if cagr5 is not None else None,
        "div_cagr_3y_pct": round(cagr3, 1) if cagr3 is not None else None,
        "growth_streak_years": streak,
        "had_cut": had_cut,
        "years_history": m["n_years"],
        "fcf_coverage": round(fcf_cov, 2) if fcf_cov is not None else None,
        "earnings_payout_pct": round(earn_payout, 1) if earn_payout is not None else None,
        "fcf_yield_pct": round(fcfy, 2) if fcfy is not None else None,
        "piotroski": int(pio) if pio is not None else None,
        "altman_z": round(az, 2) if az is not None else None,
        "debt_to_equity": round(de, 2) if de is not None else None,
        "annual_history": m["history"],
    }

    if trap:
        entry["tier"] = "YIELD TRAP"
        bad = []
        if had_cut:
            bad.append("has cut the dividend before")
        if distress:
            bad.append("balance-sheet distress")
        if fcf_cov is not None and fcf_cov < 0.6:
            bad.append("free cash flow does not cover the payout")
        if grate is not None and grate < 0:
            bad.append("the dividend is shrinking, not growing")
        entry["why"] = ("%.1f%% headline yield, but %s - this is the kind of "
                        "high yield that precedes a cut, not an income "
                        "compounder." % (dy, "; ".join(bad) or "weak fundamentals"))
        entry["risk_flags"] = ["High yield with failed sustainability gates - "
                               "treat the dividend as at risk."]
        entry["compounder_score"] = 0.0
        return entry, "trap"

    # ---- a compounder must actually be GROWING and not have cut ----
    if had_cut or grate is None or grate <= 0 or streak < 3 or distress \
       or not sustainable:
        return None

    # ---- score components ----
    growth_c = clamp((grate) / 16.0, 0, 1)            # 0..16%+ dividend CAGR
    streak_c = clamp(streak / 15.0, 0, 1)             # 0..15+ year streak
    if fcf_cov is not None:
        sustain_c = clamp(fcf_cov / 1.6, 0, 1)
    elif earn_payout is not None:
        sustain_c = clamp((85.0 - earn_payout) / 60.0, 0, 1)
    else:
        sustain_c = 0.55
    q_p = clamp((pio if pio is not None else 5) / 9.0, 0, 1)
    q_z = clamp((az if az is not None else 3.0) / 6.0, 0, 1)
    quality_c = 0.6 * q_p + 0.4 * q_z
    yield_c = clamp(1.0 - abs(dy - 3.5) / 4.0, 0, 1)  # peak reward near 3.5%

    score = 100.0 * (0.34 * growth_c + 0.24 * streak_c +
                     0.20 * sustain_c + 0.13 * quality_c + 0.09 * yield_c)
    score = round(clamp(score, 0, 100), 1)

    # ---- tier ----
    if streak >= 10 and grate >= 5.0:
        tier = "ARISTOCRAT"
    elif streak >= 5 and grate >= 4.0:
        tier = "GROWER"
    else:
        tier = "EMERGING"

    cov_txt = ("free cash flow covers it %.2fx" % fcf_cov) if fcf_cov is not None \
        else (("a %.0f%% earnings payout" % earn_payout)
              if earn_payout is not None else "a self-funded payout")
    entry["tier"] = tier
    entry["compounder_score"] = score
    entry["why"] = (
        "%.1f%% yield growing at a %.1f%%/yr clip, raised %d years running "
        "with no cut; %s. A %s-grade dividend compounder - the income rises "
        "while you hold it." % (dy, grate, streak, cov_txt, tier.lower()))

    flags = []
    if fcf_cov is None and earn_payout is None:
        flags.append("Payout coverage could not be measured cleanly "
                      "(financial / property vehicle) - confirm FFO / AFFO.")
    elif earn_payout is not None and earn_payout > 70 and fcf_cov is None:
        flags.append("Earnings payout ratio is high (%.0f%%) - little room "
                     "for error if earnings dip." % earn_payout)
    if de is not None and de > 2.0:
        flags.append("Elevated leverage (D/E %.1f) - debt service competes "
                     "with the dividend." % de)
    if dy > 6.0:
        flags.append("Yield above 6%% - higher than the typical compounder; "
                     "watch the coverage trend.")
    if flags:
        entry["risk_flags"] = flags
    return entry, "ok"


# --------------------------------------------------------------- handler
def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)

    try:
        sc = json.loads(s3.get_object(
            Bucket=S3_BUCKET, Key="screener/data.json")["Body"].read())
    except Exception as e:
        return {"statusCode": 500, "body": "screener read failed: %s" % e}

    rows = sc.get("stocks")
    if not isinstance(rows, list):
        bs = sc.get("by_symbol") or {}
        rows = list(bs.values()) if isinstance(bs, dict) else []

    # universe: dividend payers in a sane yield band, biggest first
    payers = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        sym = (r.get("symbol") or "").upper()
        dy = num(r.get("dividendYield"))
        mc = num(r.get("marketCap"))
        if not sym or dy is None or mc is None:
            continue
        if dy < MIN_YIELD or dy > MAX_YIELD:
            continue
        payers.append((mc, sym, r))
    payers.sort(key=lambda x: x[0], reverse=True)
    payers = payers[:UNIVERSE_CAP]
    n_universe = len(payers)

    by_symbol = {sym: r for _, sym, r in payers}
    symbols = list(by_symbol.keys())

    # pull dividend history concurrently
    hist = {}
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for sym, data in ex.map(fetch_dividends, symbols):
            if data:
                hist[sym] = data

    compounders = []
    yield_traps = []
    n_with_hist = 0
    for sym in symbols:
        recs = hist.get(sym)
        if not recs:
            continue
        m = analyse_history(recs)
        if not m:
            continue
        n_with_hist += 1
        res = score_name(by_symbol[sym], m)
        if not res:
            continue
        entry, kind = res
        if kind == "trap":
            yield_traps.append(entry)
        else:
            compounders.append(entry)

    compounders.sort(key=lambda c: c["compounder_score"], reverse=True)
    yield_traps.sort(key=lambda c: c.get("dividend_yield_pct") or 0,
                     reverse=True)

    n_arist = sum(1 for c in compounders if c["tier"] == "ARISTOCRAT")
    n_grow = sum(1 for c in compounders if c["tier"] == "GROWER")
    n_emerg = sum(1 for c in compounders if c["tier"] == "EMERGING")

    med_y = median([c["dividend_yield_pct"] for c in compounders])
    med_g = median([c["div_cagr_5y_pct"] for c in compounders
                    if c["div_cagr_5y_pct"] is not None])

    headline = ("%d dividend compounders pass the screen - %d aristocrats "
                "(10+ year growth streaks), %d growers, %d emerging. "
                "%d high-yield names quarantined as traps."
                % (len(compounders), n_arist, n_grow, n_emerg,
                   len(yield_traps)))
    how_to_read = (
        "A growing dividend is a costly, credible signal: the board has "
        "publicly committed cash it must keep paying. This screen ranks "
        "dividend GROWTH, not dividend level - real 3y/5y dividend CAGR, a "
        "multi-year streak of raises with no cut, and a payout free cash flow "
        "can actually fund. The naive 'highest yield' screen buys yield "
        "traps; here the highest, weakest yields are quarantined separately. "
        "Score blends the dividend growth rate, streak length, cash coverage, "
        "balance-sheet quality and a moderate-yield preference.")

    out = {
        "ok": True,
        "schema_version": SCHEMA,
        "generated_at": now.strftime("%Y-%m-%d %H:%M UTC"),
        "elapsed_s": round(time.time() - t0, 1),
        "source": "stock-screener universe + FMP /stable/dividends history",
        "headline": headline,
        "how_to_read": how_to_read,
        "n_evaluated": n_universe,
        "n_with_history": n_with_hist,
        "n_compounders": len(compounders),
        "summary": {
            "n_aristocrats": n_arist,
            "n_growers": n_grow,
            "n_emerging": n_emerg,
            "n_yield_traps": len(yield_traps),
            "median_yield_pct": round(med_y, 2) if med_y is not None else None,
            "median_cagr5_pct": round(med_g, 1) if med_g is not None else None,
        },
        "compounders": compounders,
        "yield_traps": yield_traps,
    }

    body = json.dumps(out, separators=(",", ":")).encode("utf-8")
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=body,
                  ContentType="application/json", CacheControl="max-age=300")

    return {"statusCode": 200, "body": json.dumps({
        "ok": True,
        "n_compounders": len(compounders),
        "aristocrats": n_arist,
        "yield_traps": len(yield_traps),
        "elapsed_s": out["elapsed_s"],
    })}
