"""justhodl-apac-leadlag v1.0 — does Asian foreign flow lead the US, and by how much?

Backfills ~90 days of daily Asian foreign-flow series (Taiwan semis via TWSE T86
date-loop, Korea memory via Naver per-stock trend, Hong Kong Southbound via
Eastmoney history), pulls matched US forward returns (FMP), and computes, for each
Asia->US pair, the correlation between the Asian flow on day T and the US ETF/stock
forward return over horizons {1,3,5,10} trading days. Reports the best horizon,
Pearson r, sample size, and a significance flag (t-test). This turns the radar's
same-day divergence read into a calibrated predictive signal.

Feeds: data/apac-leadlag.json + data/apac-leadlag-series.json (aligned history).
Real data only. Significance honestly gated by sample size.
"""
import os, json, math, re, time, urllib.request, urllib.parse
from datetime import datetime, timezone, timedelta
import boto3

BUCKET = "justhodl-dashboard-live"
OUT, SERIES = "data/apac-leadlag.json", "data/apac-leadlag-series.json"
s3 = boto3.client("s3", region_name="us-east-1")
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/121.0 Safari/537.36")

TW_SEMI = ["2330", "2454", "2303", "3711", "2344", "2408", "2337", "3034", "2379", "3529", "6488"]
KR_MEMORY = ["005930", "000660"]
PAIRS = [
    {"name": "Taiwan semis → SOXX", "asia": "tw_semi", "us": "SOXX", "desc": "TSMC/MediaTek foreign flow vs US semi ETF"},
    {"name": "Taiwan semis → SMH", "asia": "tw_semi", "us": "SMH", "desc": "TW foundry/fabless foreign flow vs US semis"},
    {"name": "Taiwan semis → NVDA", "asia": "tw_semi", "us": "NVDA", "desc": "TW supply-chain foreign flow vs Nvidia"},
    {"name": "Korea memory → SMH", "asia": "kr_memory", "us": "SMH", "desc": "Samsung/SK Hynix foreign flow vs US semis"},
    {"name": "Korea memory → MU", "asia": "kr_memory", "us": "MU", "desc": "KR memory foreign flow vs Micron"},
    {"name": "HK Southbound → KWEB", "asia": "hk_south", "us": "KWEB", "desc": "Mainland→HK flow vs US China internet ETF"},
    {"name": "HK Southbound → FXI", "asia": "hk_south", "us": "FXI", "desc": "Southbound vs US China large-cap ETF"},
]
HORIZONS = [1, 3, 5, 10]


def _put(key, obj):
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(obj, separators=(",", ":"), allow_nan=False).encode(),
                  ContentType="application/json", CacheControl="public, max-age=300")


def _num(x):
    try:
        return float(re.sub(r"[,\s]", "", str(x)))
    except Exception:
        return None


def _get_json(url, headers=None, timeout=25):
    h = {"User-Agent": UA, "Accept": "application/json, text/plain, */*"}
    if headers:
        h.update(headers)
    with urllib.request.urlopen(urllib.request.Request(url, headers=h), timeout=timeout) as r:
        return json.loads(r.read())


def _http_bytes(url, headers=None, timeout=20):
    h = {"User-Agent": UA, "Accept": "*/*"}
    if headers:
        h.update(headers)
    with urllib.request.urlopen(urllib.request.Request(url, headers=h), timeout=timeout) as r:
        return r.read()


# ---------- Asian flow backfills ----------
def tw_semi_history(cal_days=95):
    """Daily Taiwan foreign net (semis) via TWSE T86 date loop (rwd JSON)."""
    out = {}
    taipei = datetime.now(timezone.utc) + timedelta(hours=8)
    for back in range(1, cal_days):
        d = taipei - timedelta(days=back)
        if d.weekday() >= 5:
            continue
        dt = d.strftime("%Y%m%d")
        got = False
        for base in ("https://www.twse.com.tw/rwd/zh/fund/", "https://www.twse.com.tw/fund/"):
            try:
                doc = _get_json(base + "T86?" + urllib.parse.urlencode({"date": dt, "selectType": "ALL", "response": "json"}),
                                headers={"Referer": "https://www.twse.com.tw/"}, timeout=20)
            except Exception:
                continue
            if str(doc.get("stat", "")).upper() != "OK":
                continue
            fields, data = doc.get("fields"), doc.get("data")
            if not (fields and data):
                for t in doc.get("tables") or []:
                    if t.get("fields") and t.get("data"):
                        fields, data = t["fields"], t["data"]; break
            if not (fields and data):
                continue
            ci = next((i for i, f in enumerate(fields) if "證券代號" in str(f) or "代號" in str(f)), 0)
            fi = next((i for i, f in enumerate(fields) if "外" in str(f) and "買賣超" in str(f) and "自營" not in str(f)), None)
            if fi is None:
                fi = next((i for i, f in enumerate(fields) if "外" in str(f) and "買賣超" in str(f)), None)
            if fi is None:
                continue
            tot = 0.0
            for r in data:
                code = str(r[ci]).strip() if ci < len(r) else ""
                if code in TW_SEMI and fi < len(r):
                    v = _num(r[fi])
                    if v is not None:
                        tot += v
            out["%s-%s-%s" % (dt[:4], dt[4:6], dt[6:])] = round(tot)
            got = True
            break
        if got:
            time.sleep(0.25)
    return out


def _kr_stock_foreign_hist(code, pages=6):
    """Deep per-stock foreign net history via Naver desktop frgn.naver (EUC-KR,
    ~20 trading days/page). Foreign net = 2nd signed-integer cell (기관 then 외국인).
    Dedupe by date. Returns {date: foreign_net}."""
    out = {}
    for page in range(1, pages + 1):
        try:
            html = _http_bytes("https://finance.naver.com/item/frgn.naver?code=%s&page=%d" % (code, page),
                               headers={"Referer": "https://finance.naver.com/item/frgn.naver?code=%s" % code}).decode("euc-kr", "ignore")
        except Exception:
            break
        added = 0
        for rh in re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.S):
            dm = re.search(r"(\d{4})\.(\d{2})\.(\d{2})", rh)
            if not dm:
                continue
            signed = re.findall(r">\s*([+-][\d,]+)\s*<", rh)
            if len(signed) >= 2:
                fn = _num(signed[1])
                if fn is not None:
                    key = "%s-%s-%s" % (dm.group(1), dm.group(2), dm.group(3))
                    if key not in out:
                        out[key] = fn
                        added += 1
        if added == 0:
            break
        time.sleep(0.2)
    return out


def kr_memory_history():
    """Korea memory foreign net (Samsung + SK Hynix), deep history via desktop
    frgn.naver; falls back to mobile trend (pageSize=30) if desktop is blocked."""
    agg = {}
    for code in KR_MEMORY:
        h = _kr_stock_foreign_hist(code)
        if len(h) < 15:  # desktop blocked/thin -> mobile fallback (~30d)
            try:
                doc = _get_json("https://m.stock.naver.com/api/stock/%s/trend?pageSize=30" % code,
                                headers={"Referer": "https://m.stock.naver.com/"}, timeout=15)
                rows = doc if isinstance(doc, list) else (doc.get("result") or [])
                for r in rows:
                    bz = str(r.get("bizdate", ""))
                    if len(bz) == 8:
                        k = "%s-%s-%s" % (bz[:4], bz[4:6], bz[6:])
                        v = _num(r.get("foreignerPureBuyQuant"))
                        if v is not None and k not in h:
                            h[k] = v
            except Exception:
                pass
        for d, v in h.items():
            agg[d] = agg.get(d, 0) + v
    return {k: round(v) for k, v in agg.items()}


def hk_southbound_history(rows=400):
    """Daily HK Southbound net (Eastmoney code 006 = total 港股通)."""
    out = {}
    try:
        params = {"reportName": "RPT_MUTUAL_DEAL_HISTORY", "columns": "ALL", "source": "WEB",
                  "sortColumns": "TRADE_DATE", "sortTypes": "-1", "pageSize": str(rows), "pageNumber": "1"}
        doc = _get_json("https://datacenter-web.eastmoney.com/api/data/v1/get?" + urllib.parse.urlencode(params),
                        headers={"Referer": "https://data.eastmoney.com/"}, timeout=20)
        for r in (doc.get("result") or {}).get("data") or []:
            if str(r.get("MUTUAL_TYPE")) == "006":
                net = _num(r.get("NET_DEAL_AMT"))
                if net is not None:
                    out[str(r.get("TRADE_DATE"))[:10]] = round(net, 2)
    except Exception:
        pass
    return out


# ---------- US forward returns ----------
def us_prices(symbols, key, start):
    """Daily closes via FMP stable historical-price-eod. {symbol: {date: close}}."""
    out = {}
    for sym in symbols:
        for url in ("https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=%s&from=%s&apikey=%s" % (sym, start, key),
                    "https://financialmodelingprep.com/stable/historical-price-eod/light?symbol=%s&from=%s&apikey=%s" % (sym, start, key)):
            try:
                doc = _get_json(url, timeout=25)
                rows = doc if isinstance(doc, list) else (doc.get("historical") or [])
                px = {}
                for r in rows:
                    dt = str(r.get("date"))[:10]
                    c = _num(r.get("close") or r.get("price") or r.get("adjClose"))
                    if dt and c is not None:
                        px[dt] = c
                if px:
                    out[sym] = px
                    break
            except Exception:
                continue
        time.sleep(0.2)
    return out


def pearson(xs, ys):
    n = len(xs)
    if n < 8:
        return None, n
    mx, my = sum(xs) / n, sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    syy = sum((y - my) ** 2 for y in ys)
    sxy = sum((xs[i] - mx) * (ys[i] - my) for i in range(n))
    if sxx <= 0 or syy <= 0:
        return None, n
    return sxy / math.sqrt(sxx * syy), n


def fwd_return_corr(flow_by_date, close_by_date, horizon):
    """Correlate Asian flow on day T with US forward return over `horizon` sessions."""
    dates = sorted(set(flow_by_date) & set(close_by_date))
    idx = {d: i for i, d in enumerate(sorted(close_by_date))}
    ordered = sorted(close_by_date)
    xs, ys = [], []
    for d in dates:
        i = idx.get(d)
        if i is None or i + horizon >= len(ordered):
            continue
        c0, c1 = close_by_date[ordered[i]], close_by_date[ordered[i + horizon]]
        if c0 and c0 > 0:
            xs.append(flow_by_date[d])
            ys.append((c1 / c0 - 1.0) * 100.0)
    r, n = pearson(xs, ys)
    if r is None:
        return None
    t = r * math.sqrt(max(n - 2, 1) / max(1e-9, 1 - r * r))
    return {"horizon": horizon, "r": round(r, 3), "n": n, "t": round(t, 2), "significant": abs(t) >= 2.0}


def lambda_handler(event=None, context=None):
    now = datetime.now(timezone.utc)
    key = os.environ.get("FMP_KEY") or os.environ.get("FMP_API_KEY")
    start = (now - timedelta(days=140)).strftime("%Y-%m-%d")
    # backfill Asian flows
    asia = {"tw_semi": tw_semi_history(), "kr_memory": kr_memory_history(), "hk_south": hk_southbound_history()}
    us_syms = sorted({p["us"] for p in PAIRS})
    us = us_prices(us_syms, key, start) if key else {}

    results = []
    for p in PAIRS:
        flow = asia.get(p["asia"]) or {}
        close = us.get(p["us"]) or {}
        overlap = len(set(flow) & set(close))
        by_h = [fwd_return_corr(flow, close, h) for h in HORIZONS]
        by_h = [x for x in by_h if x]
        best = max(by_h, key=lambda x: abs(x["r"]), default=None) if by_h else None
        results.append({"name": p["name"], "desc": p["desc"], "asia_series": p["asia"], "us": p["us"],
                        "flow_days": len(flow), "overlap_days": overlap,
                        "best": best, "by_horizon": by_h})

    proven = [r for r in results if r.get("best") and r["best"]["significant"]]
    doc = {"engine": "justhodl-apac-leadlag", "version": "1.0.0",
           "generated_at": now.isoformat(timespec="seconds"),
           "window_days": 140, "horizons": HORIZONS,
           "series_days": {k: len(v) for k, v in asia.items()},
           "us_symbols_loaded": list(us),
           "pairs": results,
           "proven_leads": [{"name": r["name"], "lead_days": r["best"]["horizon"], "r": r["best"]["r"], "n": r["best"]["n"]} for r in proven],
           "status": "LIVE" if any(r["overlap_days"] >= 15 for r in results) else "WARMING",
           "read": None}
    # human read
    if proven:
        follow = [r for r in proven if r["best"]["r"] > 0]
        contra = [r for r in proven if r["best"]["r"] < 0]
        parts = []
        if follow:
            f = max(follow, key=lambda r: r["best"]["r"]); bf = f["best"]
            parts.append("FOLLOW-THROUGH — %s (r=%+.2f, %dd lead, n=%d): Asia buying precedes US gains" % (f["name"], bf["r"], bf["horizon"], bf["n"]))
        if contra:
            c = min(contra, key=lambda r: r["best"]["r"]); bc = c["best"]
            parts.append("CONTRARIAN — %s (r=%+.2f, %dd, n=%d): Asia buying precedes US weakness" % (c["name"], bc["r"], bc["horizon"], bc["n"]))
        doc["read"] = " · ".join(parts) + (". %d significant lead(s) across %d pairs; regime-dependent." % (len(proven), len(results)))
    else:
        mx = max((r for r in results if r.get("best")), key=lambda r: abs(r["best"]["r"]), default=None)
        doc["read"] = ("No statistically significant lead-lag yet (need more history). Strongest so far: %s r=%.2f n=%d." % (
            mx["name"], mx["best"]["r"], mx["best"]["n"]) if mx else "Backfill warming — insufficient overlap.")
    _put(SERIES, {"generated_at": doc["generated_at"], "asia": asia, "us_close": us})
    _put(OUT, doc)
    return {"ok": True, "status": doc["status"], "series_days": doc["series_days"],
            "us_loaded": len(us), "proven": len(proven),
            "read": doc["read"]}
