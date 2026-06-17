"""
justhodl-sovereign-fiscal — the foreign-demand & fiscal-dominance layer.

Three things the platform was blind to:
  • TIC  — foreign holdings of US Treasuries by country (who is buying / dumping;
           the de-dollarization tell).  Source: ticdata.treasury.gov mfhhis01.txt
  • MTS  — Monthly Treasury Statement: federal deficit / receipts / outlays
           (the issuance-pressure driver behind the auction supply).
  • Debt service — average interest rate on the debt + total debt outstanding
           (the interest-expense / fiscal-sustainability trajectory).

OUTPUT: data/sovereign-fiscal.json     SCHEDULE: daily 07:10 UTC
Real official data only — not investment advice.
"""
import json
import time
import calendar
import urllib.request
from datetime import datetime, timezone

import boto3

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/sovereign-fiscal.json"
FD = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
UA = {"User-Agent": "JustHodl Research raafouis@gmail.com", "Accept": "application/json, text/plain, */*"}
FRED_KEY = "2f057499936072679d8843d7fce99989"
MON = {"Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04", "May": "05", "Jun": "06",
       "Jul": "07", "Aug": "08", "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12"}
FEATURED = ["Grand Total", "Japan", "China, Mainland", "United Kingdom", "Belgium",
            "Luxembourg", "Switzerland", "Cayman Islands", "Canada", "Ireland",
            "Taiwan", "India", "France", "Brazil", "Hong Kong"]


def _get(url, t=45):
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=t) as r:
            return r.read().decode("utf-8", "ignore")
    except Exception as e:
        print("fetch fail %s: %s" % (url[:70], e))
        return None


def _stats(pts):
    vals = [p[1] for p in pts]
    if not vals:
        return {}
    latest = vals[-1]; n = len(vals)
    pctile = round(sum(1 for v in vals if v <= latest) / n * 100, 1)
    return {"latest": round(latest, 2), "min": round(min(vals), 2), "max": round(max(vals), 2),
            "pctile": pctile, "start_date": pts[0][0], "latest_date": pts[-1][0], "n_obs": n}


def _chg(pts, k):
    """change vs k observations ago (absolute + %)."""
    if len(pts) <= k:
        return None, None
    a, b = pts[-1][1], pts[-1 - k][1]
    return round(a - b, 1), (round((a / b - 1) * 100, 1) if b else None)


# ---------------------------------------------------------------- TIC
def fetch_tic():
    """Single-pass parse of the tab-separated MFH history (blocks newest-first,
    blank line between the dashes row and the first country in some blocks)."""
    body = _get("https://ticdata.treasury.gov/Publish/mfhhis01.txt")
    if not body:
        return {}
    lines = body.split("\n")
    series = {}
    dates = []
    for k, ln in enumerate(lines):
        cells = [c.strip() for c in ln.split("\t")]
        months = [c for c in cells if c in MON]
        if len(months) >= 6:                                  # month header → set the date window
            years = []
            for look in range(k + 1, min(k + 3, len(lines))):  # year row is within the next 1-2 lines
                cand = [c.strip() for c in lines[look].split("\t") if c.strip().isdigit() and len(c.strip()) == 4]
                if len(cand) >= 6:
                    years = cand
                    break
            dates = [f"{years[x]}-{MON[months[x]]}" for x in range(min(len(months), len(years)))] if years else []
            continue
        if dates and len([c for c in cells if c.isdigit() and len(c) == 4]) >= 6:
            continue                                           # skip the year row itself
        name = cells[0].strip().strip('"') if cells else ""
        if name and len(name) > 1 and dates and not name.startswith("-"):
            for d, v in zip(dates, cells[1:1 + len(dates)]):
                try:
                    series.setdefault(name, {})[d] = float(v.replace(",", ""))
                except Exception:
                    pass
    out = {}
    for c, dd in series.items():
        pts = sorted(([d, v] for d, v in dd.items()), key=lambda x: x[0])
        if len(pts) >= 3:
            out[c] = pts
    return out


# ---------------------------------------------------------------- fiscaldata
def fiscaldata(path, params, pages=8, size=1000):
    rows = []
    for pg in range(1, pages + 1):
        url = "%s%s?%s&page[size]=%d&page[number]=%d" % (FD, path, params, size, pg)
        body = _get(url)
        if not body:
            break
        try:
            j = json.loads(body)
        except Exception:
            break
        data = j.get("data", [])
        rows.extend(data)
        if len(data) < size:
            break
        time.sleep(0.2)
    return rows


def _fred(sid, start="2004-10-01"):
    body = _get("https://api.stlouisfed.org/fred/series/observations?series_id=%s&api_key=%s"
                "&file_type=json&observation_start=%s" % (sid, FRED_KEY, start))
    pts = []
    if body:
        try:
            for o in json.loads(body).get("observations", []):
                v = o.get("value")
                if v not in (".", "", None):
                    pts.append([o["date"][:7], float(v)])
        except Exception as e:
            print("fred %s: %s" % (sid, e))
    return pts


def fetch_mts():
    """Monthly deficit/receipts/outlays from FRED MTS series (clean, unambiguous).
    MTSDS is signed (negative = deficit); store deficit as positive $bn."""
    dfc = [[d, round(-v / 1000.0, 1)] for d, v in _fred("MTSDS133FMS")]   # +ve = deficit ($bn)
    rcpt = [[d, round(v / 1000.0, 1)] for d, v in _fred("MTSR133FMS")]
    outl = [[d, round(v / 1000.0, 1)] for d, v in _fred("MTSO133FMS")]
    return dfc, rcpt, outl


def fetch_avg_interest():
    rows = fiscaldata("/v2/accounting/od/avg_interest_rates",
                      "sort=-record_date&fields=record_date,security_type_desc,security_desc,avg_interest_rate_amt",
                      pages=12, size=1000)
    want = {"Total Marketable": {}, "Total Interest-bearing Debt": {}, "Treasury Notes": {}, "Treasury Bonds": {}}
    for r in rows:
        desc = (r.get("security_desc") or "").strip()
        if desc in want:
            try:
                want[desc][r["record_date"][:7]] = round(float(r["avg_interest_rate_amt"]), 3)
            except Exception:
                pass
    return {k: sorted(([d, v] for d, v in vv.items()), key=lambda x: x[0]) for k, vv in want.items() if vv}


def fetch_debt():
    rows = fiscaldata("/v2/accounting/od/debt_to_penny",
                      "sort=-record_date&fields=record_date,tot_pub_debt_out_amt", pages=12, size=1000)
    by_month = {}
    for r in rows:
        try:
            by_month[r["record_date"][:7]] = round(float(r["tot_pub_debt_out_amt"]) / 1e12, 3)  # $tn, last in month wins
        except Exception:
            pass
    pts = sorted(([d, v] for d, v in by_month.items()), key=lambda x: x[0])
    latest = rows[0] if rows else {}
    return pts, (round(float(latest.get("tot_pub_debt_out_amt", 0)) / 1e12, 3) if latest else None), latest.get("record_date")


def lambda_handler(event=None, context=None):
    now = datetime.now(timezone.utc).isoformat()

    # ---- TIC foreign demand ----
    tic = fetch_tic()
    holders = []
    total = tic.get("Grand Total") or tic.get("Grand Total ") or []
    for c in FEATURED:
        pts = tic.get(c)
        if not pts:
            continue
        d3, p3 = _chg(pts, 3); d12, p12 = _chg(pts, 12)
        holders.append({"country": c, "points": pts[-180:], **_stats(pts),
                        "chg_3m_bn": d3, "chg_3m_pct": p3, "chg_12m_bn": d12, "chg_12m_pct": p12})
    # all-country latest snapshot (for ranking) — exclude aggregate / memo rows
    skip_kw = ("total", "official", "t-bond", "t-bill", "treasury bill", "all other", "of which", "grand")
    latest_all = []
    for c, pts in tic.items():
        if any(k in c.lower() for k in skip_kw) or len(pts) < 1:
            continue
        d12, p12 = _chg(pts, 12)
        latest_all.append({"country": c, "holdings_bn": pts[-1][1], "chg_12m_bn": d12, "as_of": pts[-1][0]})
    latest_all.sort(key=lambda x: x["holdings_bn"], reverse=True)

    # ---- MTS fiscal ----
    dfc, rcpt, outl = fetch_mts()
    # trailing-12-month deficit (the real fiscal-trajectory metric)
    ttm = []
    if len(dfc) >= 12:
        for i in range(11, len(dfc)):
            window = [dfc[j][1] for j in range(i - 11, i + 1)]
            ttm.append([dfc[i][0], round(sum(window), 1)])

    # ---- debt service ----
    avg_int = fetch_avg_interest()
    debt_pts, debt_latest, debt_date = fetch_debt()
    # implied annual interest expense = avg rate on total interest-bearing debt x total debt
    interest_expense = None
    tib = avg_int.get("Total Interest-bearing Debt") or []
    if tib and debt_latest:
        interest_expense = round(tib[-1][1] / 100 * debt_latest * 1000, 0)  # $bn/yr

    # ---- reads / signals ----
    drivers = []
    if total:
        d12, p12 = _chg(total, 12)
        if d12 is not None:
            drivers.append("Total foreign UST holdings %s $%.0fbn YoY (%.1f%%) to $%.0fbn"
                           % ("up" if d12 >= 0 else "down", abs(d12), p12 or 0, total[-1][1]))
    china = tic.get("China, Mainland")
    if china:
        d12, p12 = _chg(china, 12)
        if d12 is not None:
            drivers.append("China %s $%.0fbn YoY to $%.0fbn (%s)"
                           % ("added" if d12 >= 0 else "cut", abs(d12), china[-1][1],
                              "de-dollarization watch" if d12 < 0 else "buying"))
    if ttm:
        drivers.append("Trailing-12m federal deficit $%.0fbn" % ttm[-1][1])
    if interest_expense:
        drivers.append("Implied annual interest on the debt ~$%.0fbn (avg rate %.2f%%)"
                       % (interest_expense, tib[-1][1]))

    out = {
        "engine": "sovereign-fiscal", "version": "1.0.0", "generated_at": now,
        "reads": drivers,
        "tic": {"holders": holders, "total": (total[-180:] if total else []),
                "ranking": latest_all[:25], "as_of": (total[-1][0] if total else None),
                "source": "U.S. Treasury TIC — Major Foreign Holders (ticdata.treasury.gov)"},
        "fiscal": {"deficit_monthly": dfc[-180:], "receipts_monthly": rcpt[-180:],
                   "outlays_monthly": outl[-180:], "deficit_ttm": ttm[-180:],
                   "as_of": (dfc[-1][0] if dfc else None),
                   "source": "U.S. Treasury Monthly Treasury Statement (fiscaldata mts_table_1)"},
        "debt_service": {"avg_interest": {k: v[-360:] for k, v in avg_int.items()},
                         "total_debt_tn": debt_pts[-240:], "total_debt_latest_tn": debt_latest,
                         "debt_as_of": debt_date, "implied_annual_interest_bn": interest_expense,
                         "source": "U.S. Treasury fiscaldata — avg interest rates + debt to the penny"},
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, separators=(",", ":")).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    return {"statusCode": 200, "body": json.dumps({
        "tic_holders": len(holders), "tic_as_of": out["tic"]["as_of"],
        "deficit_months": len(dfc), "ttm_deficit_bn": (ttm[-1][1] if ttm else None),
        "debt_tn": debt_latest, "implied_interest_bn": interest_expense})}
