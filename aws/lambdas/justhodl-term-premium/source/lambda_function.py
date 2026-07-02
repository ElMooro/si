"""justhodl-term-premium — REAL Adrian-Crump-Moench term premia (NY Fed).

Replaces the fleet's regression proxy with the official ACM daily dataset
(1961->). The 10y term premium is the compensation investors demand for
duration risk — the cleanest lens on whether long-end selloffs are Fed-path
expectations or premium (supply/fiscal/vol) repricing. Bond-vigilante episodes
(1994, Oct-2023) are TP shocks, not expectations shocks.

Source: newyorkfed.org medialibrary ACMTermPremium.xls (BIFF .xls) — parsed
in-Lambda via vendored pure-python xlrd, so the engine self-updates daily with
no runner dependency. Full series archived to data/history/acm-term-premium.json;
graceful fallback to the archive if the fetch/parse fails (source_degraded).

Outputs data/term-premium.json:
  latest{date,tp10,tp5,tp2,y10,rn10}, deltas_bps{d5,d21,d63},
  z_10y, pctile_full, regime, momentum, decomposition, curve,
  history_chart (weekly, ~15y, jh-enhance shape), stats, source.
Consumers: yield-curve engine (real value into term_premium_proxy_bps ->
cycle-clock inherits), signal-board "Term Premium (ACM)", term-premium.html.
"""
import json, io as _io, urllib.request
from datetime import datetime, timezone, timedelta
import boto3
import xlrd   # vendored 2.0.1 (pure python)

BUCKET = "justhodl-dashboard-live"
OUT, HIST = "data/term-premium.json", "data/history/acm-term-premium.json"
ACM_URLS = [
    "https://www.newyorkfed.org/medialibrary/media/research/data_indicators/ACMTermPremium.xls",
    "https://www.newyorkfed.org/medialibrary/media/research/data_indicators/ACM_TermPremium.xls",
]
s3 = boto3.client("s3", region_name="us-east-1")


def _fetch(url, timeout=45):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 jh-terminal/1"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def _parse_acm(blob):
    bk = xlrd.open_workbook(file_contents=blob)
    sh = max((bk.sheet_by_index(i) for i in range(bk.nsheets)), key=lambda s_: s_.nrows)
    hdr_row, cols = None, {}
    for r in range(min(12, sh.nrows)):
        vals = [str(sh.cell_value(r, c)).strip().upper() for c in range(sh.ncols)]
        if "ACMTP10" in vals:
            hdr_row = r
            for c, v in enumerate(vals):
                cols[v] = c
            break
    if hdr_row is None:
        raise ValueError("ACMTP10 header not found")
    want = {"tp10": "ACMTP10", "tp5": "ACMTP05", "tp2": "ACMTP02",
            "y10": "ACMY10", "rn10": "ACMRNY10"}
    dcol = cols.get("DATE", 0)
    rows = []
    for r in range(hdr_row + 1, sh.nrows):
        cell = sh.cell(r, dcol)
        d = None
        if cell.ctype == xlrd.XL_CELL_DATE:
            d = datetime(*xlrd.xldate_as_tuple(cell.value, bk.datemode)[:3]).date()
        else:
            t = str(cell.value).strip()
            for fmt in ("%d-%b-%Y", "%m/%d/%Y", "%Y-%m-%d", "%d-%b-%y"):
                try:
                    d = datetime.strptime(t, fmt).date(); break
                except ValueError:
                    pass
        if not d:
            continue
        row = {"date": d.isoformat()}
        ok = True
        for k, h in want.items():
            c = cols.get(h)
            try:
                row[k] = round(float(sh.cell_value(r, c)), 4)
            except (TypeError, ValueError, IndexError):
                ok = False
        if ok and -6 < row["tp10"] < 8:
            rows.append(row)
    rows.sort(key=lambda x: x["date"])
    return rows


def _s3json(key, default=None):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return default


def lambda_handler(event=None, context=None):
    rows, source = [], "live"
    for u in ACM_URLS:
        try:
            rows = _parse_acm(_fetch(u))
            if len(rows) > 10000:
                print(f"[acm] parsed {len(rows)} rows from {u.rsplit('/',1)[-1]}")
                break
        except Exception as e:
            print(f"[acm] {u.rsplit('/',1)[-1]}: {str(e)[:90]}")
            rows = []
    if len(rows) > 10000:
        s3.put_object(Bucket=BUCKET, Key=HIST,
                      Body=json.dumps(rows, separators=(",", ":")).encode(),
                      ContentType="application/json")
    else:
        rows = _s3json(HIST, []) or []
        source = "archive_fallback"
        print(f"[acm] using archived series: {len(rows)} rows")
    if len(rows) < 2600:
        raise RuntimeError("ACM series unavailable live and in archive")

    L = rows[-1]
    tp = [r["tp10"] for r in rows]

    def _bps(back):
        return round((L["tp10"] - rows[-1 - back]["tp10"]) * 100, 1) if len(rows) > back else None

    win = tp[-2520:]
    mu = sum(win) / len(win)
    sd = (sum((x - mu) ** 2 for x in win) / len(win)) ** 0.5
    z10 = round((L["tp10"] - mu) / sd, 2) if sd else None
    pct = round(100 * sum(1 for x in tp if x <= L["tp10"]) / len(tp), 1)
    d21 = _bps(21)
    lvl = ("SUPPRESSED" if L["tp10"] < -0.5 else "NEGATIVE" if L["tp10"] < 0
           else "LOW_NORMAL" if L["tp10"] < 0.75 else "ELEVATED" if L["tp10"] < 1.5 else "HIGH")
    mom = ("SPIKING" if (d21 or 0) >= 45 else "RISING" if (d21 or 0) >= 15
           else "COLLAPSING" if (d21 or 0) <= -45 else "FALLING" if (d21 or 0) <= -15 else "STABLE")

    # weekly downsample, last ~15y, jh-enhance shape
    chart, lastwk = [], None
    for r in rows[-3900:]:
        _y, _w, _ = datetime.strptime(r["date"], "%Y-%m-%d").isocalendar()
        wk = "%d-%02d" % (_y, _w)
        if wk != lastwk:
            chart.append({"date": r["date"], "value": r["tp10"]})
            lastwk = wk
    exp10 = round(L["y10"] - L["tp10"], 3)

    doc = {"engine": "justhodl-term-premium", "version": "1.0.0",
           "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
           "source": source, "series_n": len(rows), "first_date": rows[0]["date"],
           "latest": L,
           "deltas_bps": {"d5": _bps(5), "d21": d21, "d63": _bps(63)},
           "z_10y": z10, "pctile_full_history": pct,
           "regime": {"level": lvl, "momentum": mom},
           "decomposition": {"acm_fitted_10y_pct": L["y10"], "risk_neutral_10y_pct": L["rn10"],
                             "term_premium_10y_pct": L["tp10"],
                             "expectations_component_pct": exp10,
                             "identity_check_pct": round(L["y10"] - (L["rn10"] + L["tp10"]), 4),
                             "read": ("10y yield = %.2f%% expectations + %.2f%% term premium"
                                      % (L["rn10"], L["tp10"]))},
           "curve": {"tp2": L["tp2"], "tp5": L["tp5"], "tp10": L["tp10"],
                     "tp10_minus_tp2_bps": round((L["tp10"] - L["tp2"]) * 100, 1)},
           "history_chart": chart,
           "method": ("Official NY Fed Adrian-Crump-Moench daily term premia, parsed in-Lambda "
                      "from ACMTermPremium.xls (vendored xlrd). z vs trailing 10y; percentile vs "
                      "full history since %s. Bond-vigilante episodes are TP shocks." % rows[0]["date"][:4])}
    s3.put_object(Bucket=BUCKET, Key=OUT, Body=json.dumps(doc, separators=(",", ":")).encode(),
                  ContentType="application/json", CacheControl="public, max-age=1800")
    print("[acm] %s tp10=%.2f%% Δ21d=%sbps z10y=%s p=%s %s/%s src=%s"
          % (L["date"], L["tp10"], d21, z10, pct, lvl, mom, source))
    return {"ok": True, "date": L["date"], "tp10": L["tp10"], "n": len(rows), "source": source}
