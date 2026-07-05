"""justhodl-taiwan-moea — Taiwan MOEA forward-momentum leads.

Taiwan is the global tech supply chain's chokepoint, and its Ministry of Economic
Affairs (MOEA) publishes two of the most forward-looking hard-data series on the
planet — FREE, machine-readable CSV, no key, under Taiwan's Government Open Data
License:

  1. EXPORT ORDERS (外銷訂單) — booked orders, which lead actual shipments (and
     therefore global tech demand) by ~1-3 months. The grand-total series (region
     code 00) runs monthly back to 1984, in US$ millions.
  2. ELECTRONIC-COMPONENTS PRODUCTION (電子零組件業 生產價值) — Taiwan's semiconductor
     production value, monthly back to 1982. Taiwan is THE global chip hub, so this
     is arguably the best real semiconductor-cycle bellwether that exists. The same
     file carries inventory value (存貨價值) — a scarcity/glut tell.

Neither is on FRED (Taiwan is not an OECD member), so this is genuine white-space:
the forward tech-demand + chip-cycle leads the platform could not otherwise see.

OUTPUT  data/taiwan-moea.json      SCHEDULE  daily 03:30 UTC (MOEA updates monthly)
Real MOEA open data — not investment advice.
"""
import json
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/taiwan-moea.json"
BASE = "https://service.moea.gov.tw/EE520/opendata/"

# dataset filenames on the MOEA open-data endpoint
F_EXPORT_ORDERS = "經濟部統計處_外銷訂單_按地區分.csv"
F_IP_ELECTRONICS = "經濟部統計處_工業生產_電子零組件業.csv"


def fetch_csv(fname, timeout=45):
    """Fetch a MOEA open-data CSV (percent-encode the CJK path)."""
    url = BASE + fname
    p = urllib.parse.urlsplit(url)
    url2 = urllib.parse.urlunsplit((p.scheme, p.netloc, urllib.parse.quote(p.path), p.query, p.fragment))
    req = urllib.request.Request(url2, headers={"User-Agent": "Mozilla/5.0 (justhodl-taiwan)", "Accept": "*/*"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
    for enc in ("utf-8-sig", "utf-8", "big5", "cp950"):
        try:
            return raw.decode(enc)
        except Exception:
            pass
    return raw.decode("utf-8", "ignore")


def _rows(txt):
    out = []
    for ln in txt.splitlines():
        # simple CSV — fields have no embedded commas in these files
        parts = ln.split(",")
        out.append([c.strip() for c in parts])
    return out


def roc_to_iso(period):
    """ROC date 'YYYMM' (Minguo year = Gregorian - 1911) -> 'YYYY-MM'."""
    period = str(period).strip()
    if len(period) < 4 or not period.isdigit():
        return None
    year = int(period[:-2]) + 1911
    month = int(period[-2:])
    if not (1 <= month <= 12):
        return None
    return "%04d-%02d" % (year, month)


def series_yoy(pairs):
    """pairs = [(iso, value)] -> dict of latest value + YoY + 3m-avg YoY + z + history."""
    pairs = sorted((p for p in pairs if p[0] and p[1] is not None))
    if len(pairs) < 14:
        return {"error": "insufficient history", "n": len(pairs)}
    by = dict(pairs)
    isos = [p[0] for p in pairs]
    latest_iso, latest_v = pairs[-1]

    def yoy_at(i):
        cur = pairs[i][1]
        y, m = pairs[i][0].split("-")
        prior_iso = "%04d-%s" % (int(y) - 1, m)
        pv = by.get(prior_iso)
        return (cur / pv - 1.0) * 100 if pv else None

    yoy = yoy_at(len(pairs) - 1)
    # 3-month-average YoY (smooths the volatile monthly print)
    yoys3 = [yoy_at(i) for i in range(len(pairs) - 3, len(pairs))]
    yoys3 = [x for x in yoys3 if x is not None]
    yoy_3mma = round(sum(yoys3) / len(yoys3), 2) if yoys3 else None
    # z-score of the YoY over the trailing ~5y
    hist_yoy = [yoy_at(i) for i in range(max(0, len(pairs) - 60), len(pairs))]
    hist_yoy = [x for x in hist_yoy if x is not None]
    z = None
    if len(hist_yoy) >= 12 and yoy is not None:
        mu = sum(hist_yoy) / len(hist_yoy)
        sd = (sum((x - mu) ** 2 for x in hist_yoy) / (len(hist_yoy) - 1)) ** 0.5
        z = round((yoy - mu) / sd, 2) if sd > 0 else None
    return {
        "latest_period": latest_iso, "latest_value": round(latest_v, 2),
        "yoy_pct": round(yoy, 2) if yoy is not None else None,
        "yoy_3mma_pct": yoy_3mma, "yoy_z_5y": z,
        "history": [{"p": p, "v": round(v, 2)} for p, v in pairs[-60:]],
        "n": len(pairs),
    }


def build_export_orders():
    rows = _rows(fetch_csv(F_EXPORT_ORDERS))
    # header: 統計項目,地區代碼,地區別,資料期(民國年),統計值(金額),計量單位
    pairs, unit = [], None
    for r in rows[1:]:
        if len(r) < 6:
            continue
        item, region_code, period, value = r[0], r[1], r[3], r[4]
        if region_code != "00" or "美元" not in item:   # region 00 = grand total, USD leg
            continue
        iso = roc_to_iso(period)
        try:
            v = float(value)
        except Exception:
            continue
        if iso:
            pairs.append((iso, v)); unit = r[5]
    d = series_yoy(pairs)
    d["unit"] = unit or "US$ million"
    d["series"] = "Taiwan export orders — grand total (US$)"
    if d.get("yoy_3mma_pct") is not None:
        y = d["yoy_3mma_pct"]
        d["read"] = ("EXPANDING — global tech demand firm" if y > 3
                     else "CONTRACTING — global tech-demand slowdown (leads shipments ~1-3mo)" if y < -3
                     else "FLAT")
    return d


def build_semiconductor():
    rows = _rows(fetch_csv(F_IP_ELECTRONICS))
    # header: 統計項目,行業別,資料期(民國年),統計值(金額),計量單位
    prod, inv = [], []
    for r in rows[1:]:
        if len(r) < 5:
            continue
        item, period, value = r[0], r[2], r[3]
        iso = roc_to_iso(period)
        try:
            v = float(value)
        except Exception:
            continue
        if not iso:
            continue
        if item == "生產價值":
            prod.append((iso, v))
        elif item == "存貨價值":
            inv.append((iso, v))
    out = {"production": series_yoy(prod)}
    out["production"]["series"] = "Taiwan electronic-components production (semiconductor bellwether, value NT$k)"
    if inv:
        out["inventory"] = series_yoy(inv)
        out["inventory"]["series"] = "Taiwan electronic-components inventory (scarcity/glut tell)"
    p = out["production"]
    if p.get("yoy_3mma_pct") is not None:
        y = p["yoy_3mma_pct"]
        p["read"] = ("UP-LEG — chip cycle expanding" if y > 5
                     else "DOWN-LEG — chip cycle rolling over (leads global tech/capex)" if y < -3
                     else "FLAT")
    return out


def lambda_handler(event=None, context=None):
    now = datetime.now(timezone.utc)
    out = {"engine": "justhodl-taiwan-moea", "generated_at": now.isoformat(),
           "source": "Taiwan MOEA Dept. of Statistics open data (service.moea.gov.tw) — free, Gov Open Data License",
           "note": "Export orders lead shipments ~1-3mo; electronic-components production is the global chip-cycle bellwether. Real MOEA data — not advice."}
    errs = []
    try:
        out["export_orders"] = build_export_orders()
    except Exception as e:
        errs.append("export_orders: " + str(e)[:120]); out["export_orders"] = {"error": str(e)[:120]}
    try:
        out["semiconductor"] = build_semiconductor()
    except Exception as e:
        errs.append("semiconductor: " + str(e)[:120]); out["semiconductor"] = {"error": str(e)[:120]}
    # headline early-warning read
    eo = (out.get("export_orders") or {}).get("yoy_3mma_pct")
    sc = ((out.get("semiconductor") or {}).get("production") or {}).get("yoy_3mma_pct")
    if eo is not None or sc is not None:
        out["headline"] = ("Taiwan export orders %s%% YoY (3mma), semiconductor production %s%% YoY — %s" % (
            "n/a" if eo is None else eo, "n/a" if sc is None else sc,
            "the global tech supply chain's forward pulse."))
    out["_errors"] = errs or None
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, ensure_ascii=False, default=str).encode("utf-8"),
                  ContentType="application/json; charset=utf-8", CacheControl="max-age=3600")
    return {"ok": True, "export_orders_yoy": eo, "semiconductor_yoy": sc, "errors": errs}
