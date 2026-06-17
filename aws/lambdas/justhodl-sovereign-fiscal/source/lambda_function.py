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
    body = _get("https://ticdata.treasury.gov/Publish/mfhhis01.txt")
    if not body:
        return {}
    lines = body.split("\n")
    series = {}
    i = 0
    while i < len(lines):
        cells = [c.strip() for c in lines[i].split("\t")]
        months = [c for c in cells if c in MON]
        if len(months) >= 6 and i + 1 < len(lines):
            yrs = [c.strip() for c in lines[i + 1].split("\t")]
            years = [c for c in yrs if c.isdigit() and len(c) == 4]
            dates = [f"{y}-{MON[m]}" for m, y in zip(months, years)] if len(years) == len(months) else []
            i += 2
            while i < len(lines):
                row = lines[i]
                rc = [c.strip() for c in row.split("\t")]
                if not row.strip() or len([c for c in rc if c in MON]) >= 6:
                    break
                name = rc[0]
                if name and len(name) > 1 and dates:
                    for d, v in zip(dates, rc[1:1 + len(dates)]):
                        try:
                            series.setdefault(name, {})[d] = float(v.replace(",", ""))
                        except Exception:
                            pass
                i += 1
        else:
            i += 1
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


def fetch_mts():
    """Monthly deficit/surplus, receipts, outlays from mts_table_1 (month rows)."""
    rows = fiscaldata("/v1/accounting/mts/mts_table_1",
                      "sort=-record_date&fields=record_date,classification_desc,"
                      "current_month_gross_rcpt_amt,current_month_gross_outly_amt,current_month_dfct_sur_amt",
                      pages=10, size=1000)
    dfc, rcpt, outl = {}, {}, {}
    for r in rows:
        rd = r.get("record_date") or ""
        if len(rd) < 7:
            continue
        mon_name = calendar.month_name[int(rd[5:7])]   # report month full name
        if (r.get("classification_desc") or "").strip() != mon_name:
            continue
        ym = rd[:7]
        for fld, store in (("current_month_dfct_sur_amt", dfc),
                           ("current_month_gross_rcpt_amt", rcpt),
                           ("current_month_gross_outly_amt", outl)):
            v = r.get(fld)
            try:
                if v not in (None, "null", ""):
                    store[ym] = round(float(v) / 1e9, 1)   # $bn
            except Exception:
                pass
    def srt(d):
        return sorted(([k, v] for k, v in d.items()), key=lambda x: x[0])
    return srt(dfc), srt(rcpt), srt(outl)


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
    # all-country latest snapshot (for ranking) — exclude aggregate rows
    skip = {"Grand Total", "Of which: For. Official", "All Other", "Of which: Foreign Official"}
    latest_all = []
    for c, pts in tic.items():
        if c in skip or len(pts) < 1:
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
