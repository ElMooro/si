"""justhodl-apac-flows v1.0 — ASIA→US CROSS-MARKET FOREIGN-FLOW RADAR.

The thesis (Khalid): Taiwan & Korea foreign/retail investors often pile into a
stock or sector days-to-weeks before the US equivalent moves. Track the leader
so the US laggard becomes an opportunity.

REAL OFFICIAL FREE SOURCES (probe-not-guess; language-agnostic key detection):
  - TAIWAN — TWSE OpenAPI (openapi.twse.com.tw, no auth):
      T86    = daily three-institutional-investors net buy/sell PER STOCK
               (foreign net is the headline; ranked top buy/sell + sector rollup)
      BFI82U = market-wide institutional buy/sell AMOUNTS (foreign net NT$)
  - KOREA  — KRX data portal (data.krx.co.kr getJsonData, no auth): best-effort
      market investor net (individual/foreign/institution). Graceful on failure.
  - JAPAN  — JPX weekly investor-type flows: v1.1 (weekly Excel parse).
  - US catch-up — reads existing fleet feed data/etf-flows/daily.json to show
      whether the US bridge ETFs have moved yet (rigorous lead-lag = v1.1 once
      apac history accrues).

Feeds: data/apac-flows.json (latest) + data/history/apac-flows.json (400d).
No key faked; every source's success/failure is recorded in `sources`.
"""
import os, gzip, zlib, json, re, time, urllib.request, urllib.error, urllib.parse
from datetime import datetime, timezone, timedelta
import boto3

BUCKET = "justhodl-dashboard-live"
OUT, HIST = "data/apac-flows.json", "data/history/apac-flows.json"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/121.0 Safari/537.36",
      "Accept": "application/json, text/plain, */*", "Referer": "https://www.twse.com.tw/"}
TWSE_BASES = ("https://www.twse.com.tw/rwd/zh/fund/", "https://www.twse.com.tw/fund/")
KRX = "http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
s3 = boto3.client("s3", region_name="us-east-1")

# Taiwan bellwethers + sector map (the names foreign money moves that lead US demand)
TW_NAMES = {
    "2330": "TSMC", "2454": "MediaTek", "2303": "UMC", "3711": "ASE Technology",
    "2344": "Winbond (memory)", "2408": "Nanya Tech (memory)", "2337": "Macronix (memory)",
    "6770": "Powerchip (memory)", "3034": "Novatek", "2379": "Realtek", "3529": "eMemory",
    "3661": "Alchip", "6488": "GlobalWafers", "5347": "Vanguard Intl Semi", "3006": "E Ink",
    "3443": "GUC", "2449": "King Yuan", "3105": "Win Semiconductors",
    "2317": "Hon Hai (Foxconn)", "2382": "Quanta", "3231": "Wistron", "2357": "Asus",
    "2324": "Compal", "4938": "Pegatron", "2308": "Delta Electronics", "2301": "Lite-On",
    "3481": "Innolux", "2409": "AU Optronics", "2474": "Catcher", "2377": "Micro-Star",
    "2881": "Fubon Financial", "2882": "Cathay Financial", "2891": "CTBC Financial",
    "2886": "Mega Financial", "2884": "E.SUN Financial", "2892": "First Financial",
    "5880": "Taiwan Cooperative", "2887": "Taishin Financial", "2890": "SinoPac Financial",
    "2412": "Chunghwa Telecom", "3045": "Taiwan Mobile", "4904": "FarEasTone",
    "2603": "Evergreen Marine", "2609": "Yang Ming", "2615": "Wan Hai Lines",
    "1301": "Formosa Plastics", "1303": "Nan Ya Plastics", "6505": "Formosa Petrochemical",
    "1326": "Formosa Chemicals", "1101": "Taiwan Cement", "2327": "Yageo", "3008": "LargAn"}
TW_SECTOR = {c: "Semiconductors" for c in
             ["2330", "2454", "2303", "3711", "2344", "2408", "2337", "6770", "3034", "2379",
              "3529", "3661", "6488", "5347", "3443", "2449", "3105", "2327", "3008"]}
TW_SECTOR.update({c: "Electronics/Hardware" for c in
                  ["2317", "2382", "3231", "2357", "2324", "4938", "2308", "2301", "3481",
                   "2409", "2474", "2377", "3006"]})
TW_SECTOR.update({c: "Financials" for c in
                  ["2881", "2882", "2891", "2886", "2884", "2892", "5880", "2887", "2890"]})
TW_SECTOR.update({c: "Telecom" for c in ["2412", "3045", "4904"]})
TW_SECTOR.update({c: "Shipping" for c in ["2603", "2609", "2615"]})
TW_SECTOR.update({c: "Materials" for c in ["1301", "1303", "6505", "1326", "1101"]})


# Asia -> US catch-up bridges
BRIDGES = [
    {"name": "Semiconductors (TW → US)", "sector": "Semiconductors",
     "tw_codes": ["2330", "2454", "2303", "3711"], "kr_codes": ["000660", "005930"],
     "us_etf": ["SOXX", "SMH"], "us_names": ["NVDA", "AMD", "AVGO", "TSM", "MU"]},
    {"name": "AI Hardware / Foxconn build-out (TW → US)", "sector": "Electronics/Hardware",
     "tw_codes": ["2317", "2382", "3231"], "kr_codes": [],
     "us_etf": ["SMH", "XLK"], "us_names": ["NVDA", "DELL", "SMCI", "AVGO"]},
    {"name": "Memory / EV Battery (KR → US)", "sector": "Memory/Battery",
     "tw_codes": [], "kr_codes": ["000660", "373220", "006400"],
     "us_etf": ["LIT", "SMH"], "us_names": ["MU", "ALB", "TSLA"]},
    {"name": "Southbound → US-listed China (HK → US)", "sector": "China Internet",
     "tw_codes": [], "kr_codes": [], "hk": True,
     "us_etf": ["KWEB", "FXI"], "us_names": ["BABA", "JD", "PDD"]},
]


def _put(key, obj):
    s3.put_object(Bucket=BUCKET, Key=key,
                  Body=json.dumps(obj, separators=(",", ":"), allow_nan=False).encode(),
                  ContentType="application/json", CacheControl="public, max-age=120")


def _j(key, d=None):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return d


def _num(x):
    try:
        return float(re.sub(r"[,\s]", "", str(x)))
    except Exception:
        return None


def _get_json(url, headers=None, timeout=30):
    req = urllib.request.Request(url, headers=headers or {"User-Agent": "JustHodl/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def _detect_key(row, want_substrings, must_numeric=False, avoid=()):
    """Find the dict key whose NAME contains any wanted substring (CJK or English),
    optionally requiring a numeric value, avoiding keys matching `avoid`."""
    for k in row:
        kl = str(k).lower()
        if any(a in kl for a in avoid):
            continue
        if any(sub.lower() in kl for sub in want_substrings):
            if not must_numeric or _num(row[k]) is not None:
                return k
    return None


def _twse(path, date_key, extra):
    """TWSE legacy rwd JSON: walks back to the last trading day, tries both hosts.
    Returns (fields, data_rows, date_used)."""
    from datetime import timedelta
    taipei = datetime.now(timezone.utc) + timedelta(hours=8)
    for back in range(0, 9):
        dt = (taipei - timedelta(days=back)).strftime("%Y%m%d")
        for base in TWSE_BASES:
            q = {date_key: dt, "response": "json"}; q.update(extra)
            url = base + path + "?" + urllib.parse.urlencode(q)
            try:
                doc = _get_json(url, headers=UA, timeout=30)
            except Exception:
                continue
            if str(doc.get("stat", "")).upper() != "OK":
                continue
            fields, data = doc.get("fields"), doc.get("data")
            if not (fields and data):
                for t in doc.get("tables") or []:
                    if t.get("fields") and t.get("data"):
                        fields, data = t["fields"], t["data"]; break
            if fields and data:
                return fields, data, dt
    return None, None, None


def _idx(fields, *subs, avoid=()):
    for i, f in enumerate(fields):
        fs = str(f)
        if any(a in fs for a in avoid):
            continue
        if all(any(x in fs for x in ([sub] if isinstance(sub, str) else sub)) for sub in subs):
            return i
    return None


def taiwan_t86():
    """Per-stock foreign net buy/sell shares — TWSE T86 (legacy rwd JSON)."""
    fields, data, dt = _twse("T86", "date", {"selectType": "ALL"})
    if not fields:
        raise RuntimeError("T86 unreachable/no-data on all hosts+dates")
    ci = _idx(fields, ["證券代號", "代號", "Code"])
    ni = _idx(fields, ["證券名稱", "名稱", "Name"])
    fi = _idx(fields, "外", "買賣超", avoid=("自營",)) or _idx(fields, "外", "買賣超") or _idx(fields, "Foreign")
    ti = _idx(fields, "投信", "買賣超")
    if ci is None or fi is None:
        raise RuntimeError("T86 col detect failed fields=%s" % fields[:6])
    out = []
    for r in data:
        try:
            code = str(r[ci]).strip()
        except Exception:
            continue
        fnet = _num(r[fi]) if fi < len(r) else None
        if not re.match(r"^\d{4}$", code) or fnet is None:
            continue
        out.append({"code": code, "name": TW_NAMES.get(code) or (str(r[ni]).strip() if ni is not None and ni < len(r) else ""),
                    "foreign_net_shares": fnet,
                    "trust_net_shares": (_num(r[ti]) if ti is not None and ti < len(r) else None),
                    "sector": TW_SECTOR.get(code)})
    out.sort(key=lambda x: x["foreign_net_shares"], reverse=True)
    sec = {}
    for r in out:
        if r["sector"]:
            sec[r["sector"]] = sec.get(r["sector"], 0) + r["foreign_net_shares"]
    return {"as_of": "%s-%s-%s" % (dt[:4], dt[4:6], dt[6:]), "foreign_col_detected": str(fields[fi]),
            "n_stocks": len(out), "top_buy": out[:15], "top_sell": list(reversed(out[-15:])),
            "sector_flows_shares": {k: round(v) for k, v in sorted(sec.items(), key=lambda kv: kv[1], reverse=True)},
            "tracked": {c: next((r["foreign_net_shares"] for r in out if r["code"] == c), None)
                        for c in ("2330", "2454", "2317")}}


def taiwan_bfi():
    """Market-wide foreign net dollar (NT$) — TWSE BFI82U (legacy rwd JSON)."""
    fields, data, dt = _twse("BFI82U", "dayDate", {"type": "day"})
    if not fields:
        return {"foreign_net_twd": None}
    neti = _idx(fields, ["買賣差額", "買賣超", "差額", "Difference"])
    buyi = _idx(fields, ["買進", "Buy"], avoid=("賣", "Sell"))
    selli = _idx(fields, ["賣出", "Sell"])
    for r in data:
        label = str(r[0]) if r else ""
        if "外" in label or "foreign" in label.lower():
            nv = _num(r[neti]) if (neti is not None and neti < len(r)) else (
                (_num(r[buyi]) or 0) - (_num(r[selli]) or 0)
                if buyi is not None and selli is not None else None)
            if nv is not None:
                return {"foreign_net_twd": round(nv), "foreign_net_twd_bn": round(nv / 1e9, 2),
                        "foreign_row": label}
    return {"foreign_net_twd": None}


KR_NAMES = {"005930": "Samsung Electronics", "000660": "SK Hynix", "373220": "LG Energy Solution",
            "006400": "Samsung SDI", "051910": "LG Chem", "005380": "Hyundai Motor", "000270": "Kia",
            "035420": "NAVER", "035720": "Kakao", "005490": "POSCO Holdings", "012330": "Hyundai Mobis",
            "066570": "LG Electronics", "207940": "Samsung Biologics", "105560": "KB Financial",
            "005490b": "", "000100": "Yuhan", "096770": "SK Innovation"}
KR_SECTOR = {"005930": "Memory/Semis", "000660": "Memory/Semis",
             "373220": "Battery", "006400": "Battery", "051910": "Battery", "096770": "Battery",
             "005380": "Autos", "000270": "Autos", "012330": "Autos",
             "035420": "Internet", "035720": "Internet",
             "005490": "Materials", "066570": "Electronics", "207940": "Biotech", "105560": "Financials"}
NAVER_TREND = "https://m.stock.naver.com/api/stock/%s/trend"


def korea_naver():
    """Per-stock foreign/institution/individual net + foreign hold ratio via Naver
    mobile API (KRX data endpoint blocks AWS IPs; Naver is reachable)."""
    out = []
    for code in [c for c in KR_NAMES if KR_NAMES[c]]:
        try:
            doc = _get_json(NAVER_TREND % code, headers={"User-Agent": UA["User-Agent"],
                            "Referer": "https://m.stock.naver.com/"}, timeout=15)
            rows = doc if isinstance(doc, list) else (doc.get("result") or doc.get("trends") or [])
            if not rows:
                continue
            latest = max(rows, key=lambda r: str(r.get("bizdate", "")))
            out.append({"code": code, "name": KR_NAMES[code], "sector": KR_SECTOR.get(code),
                        "foreign_net_shares": _num(latest.get("foreignerPureBuyQuant")),
                        "inst_net_shares": _num(latest.get("organPureBuyQuant")),
                        "indiv_net_shares": _num(latest.get("individualPureBuyQuant")),
                        "foreign_hold_pct": _num(latest.get("foreignerHoldRatio")),
                        "as_of": str(latest.get("bizdate"))})
            time.sleep(0.25)
        except Exception:
            continue
    if not out:
        return {"status": "PENDING", "note": "Naver unreachable this run"}
    out.sort(key=lambda x: (x["foreign_net_shares"] if x["foreign_net_shares"] is not None else -9e18), reverse=True)
    sec = {}
    for r in out:
        if r["sector"] and r["foreign_net_shares"] is not None:
            sec[r["sector"]] = sec.get(r["sector"], 0) + r["foreign_net_shares"]
    tot = sum(r["foreign_net_shares"] for r in out if r["foreign_net_shares"] is not None)
    bz = out[0]["as_of"]
    return {"status": "LIVE", "source": "Naver Finance", "as_of": "%s-%s-%s" % (bz[:4], bz[4:6], bz[6:]) if len(bz) == 8 else bz,
            "n_names": len(out), "foreign_net_total_shares": round(tot),
            "top_buy": out[:8], "top_sell": list(reversed(out[-6:])),
            "sector_flows_shares": {k: round(v) for k, v in sorted(sec.items(), key=lambda kv: kv[1], reverse=True)},
            "tracked": {c: next((r["foreign_net_shares"] for r in out if r["code"] == c), None)
                        for c in ("005930", "000660", "373220")}}


def us_side():
    """Read existing fleet ETF-flow feed to show the US bridge state."""
    feed = _j("data/etf-flows/daily.json", {}) or {}
    mets = feed.get("metrics") or feed.get("data") or []
    flow = {}
    for m in mets if isinstance(mets, list) else []:
        t = (m.get("ticker") or m.get("symbol") or "").upper()
        if t:
            flow[t] = m.get("flow_5d_usd") or m.get("flow_5d") or m.get("net_flow")
    return flow


def _http_bytes(url, headers=None, timeout=30):
    h = {"User-Agent": UA["User-Agent"], "Accept": "*/*", "Accept-Language": "en,ko,ja,zh"}
    if headers: h.update(headers)
    req = urllib.request.Request(url, headers=h)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def _http_text(url, headers=None, timeout=30):
    """Robust text fetch: full browser headers, gzip/deflate decompression by
    Content-Encoding and magic bytes, multi-charset decode."""
    h = {"User-Agent": UA["User-Agent"],
         "Accept": "text/javascript, application/json, text/html, */*",
         "Accept-Encoding": "gzip, deflate", "Accept-Language": "en-US,en;q=0.9",
         "Connection": "keep-alive"}
    if headers: h.update(headers)
    req = urllib.request.Request(url, headers=h)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
        enc = (r.headers.get("Content-Encoding") or "").lower()
    if "gzip" in enc or raw[:2] == b"\x1f\x8b":
        try: raw = gzip.decompress(raw)
        except Exception: pass
    elif "deflate" in enc:
        try: raw = zlib.decompress(raw)
        except Exception:
            try: raw = zlib.decompress(raw, -15)
            except Exception: pass
    for cdc in ("utf-8", "utf-16", "latin-1"):
        try:
            t = raw.decode(cdc)
            if t.count(chr(0)) < len(t) // 4:
                return t
        except Exception:
            continue
    return raw.decode("utf-8", "ignore")


def korea_market():
    """True market-wide Korea foreign/institution/individual net via Naver index
    trend (KOSPI + KOSDAQ). Values are Naver net (KRW, ~100M-won units)."""
    out = {}
    for mkt in ("KOSPI", "KOSDAQ"):
        try:
            doc = _get_json("https://m.stock.naver.com/api/index/%s/trend" % mkt,
                            headers={"User-Agent": UA["User-Agent"], "Referer": "https://m.stock.naver.com/"}, timeout=15)
            if isinstance(doc, dict) and ("foreignValue" in doc or "bizdate" in doc):
                rows = [doc]
            elif isinstance(doc, list):
                rows = doc
            else:
                rows = doc.get("result") or doc.get("trends") or doc.get("datas") or []
            rows = [r for r in rows if isinstance(r, dict) and r.get("foreignValue") is not None]
            if rows:
                latest = max(rows, key=lambda r: str(r.get("bizdate", "")))
                out[mkt] = {"foreign_value": _num(latest.get("foreignValue")),
                            "institution_value": _num(latest.get("institutionalValue")),
                            "individual_value": _num(latest.get("personalValue")),
                            "as_of": str(latest.get("bizdate"))}
        except Exception:
            pass
    if any("foreign_value" in v for v in out.values()):
        tot = sum(v["foreign_value"] for v in out.values() if v.get("foreign_value") is not None)
        out["total_foreign_value"] = round(tot)
        out["unit"] = "KRW net (Naver index units)"
    return out


def hongkong_southbound():
    """Stock Connect SOUTHBOUND net (mainland -> HK, the HK foreign-flow analog)
    from HKEX daily-stat JS (SSE + SZSE Southbound)."""
    last_err = None
    for back in range(0, 6):
        ymd = (datetime.now(timezone.utc) + timedelta(hours=8) - timedelta(days=back)).strftime("%Y%m%d")
        try:
            txt = _http_text("https://www.hkex.com.hk/eng/csm/DailyStat/data_tab_daily_%se.js" % ymd,
                             headers={"Referer": "https://www.hkex.com.hk/Mutual-Market/Stock-Connect/Statistics/Historical-Daily?sc_lang=en"})
        except Exception as e:
            last_err = "fetch %s: %s" % (ymd, str(e)[:60]); continue
        try:
            i, jdx = txt.find("["), txt.rfind("]")
            data = json.loads(txt[i:jdx + 1])
        except Exception as e:
            last_err = "parse %s: %s len=%d head=%r" % (ymd, str(e)[:40], len(txt), txt[:50]); continue
        sb, asof = {}, None
        for blk in data:
            mkt = str(blk.get("market", ""))
            asof = asof or blk.get("date")
            if "Southbound" not in mkt:
                continue
            for cont in blk.get("content", []):
                tbl = cont.get("table") or {}
                schema = tbl.get("schema") or []
                headers = schema[0] if (schema and isinstance(schema[0], list)) else schema
                bi = next((k for k, h in enumerate(headers) if "Buy Turnover" in str(h)), None)
                si = next((k for k, h in enumerate(headers) if "Sell Turnover" in str(h)), None)
                rows = tbl.get("data") or []
                if bi is not None and si is not None and rows:
                    row = rows[0] if isinstance(rows[0], list) else rows
                    buy, sell = _num(row[bi]) if bi < len(row) else None, _num(row[si]) if si < len(row) else None
                    if buy is not None and sell is not None:
                        sb[mkt] = {"buy": buy, "sell": sell, "net": round(buy - sell)}
        if sb:
            tot = sum(v["net"] for v in sb.values())
            return {"status": "LIVE", "source": "HKEX Stock Connect", "as_of": asof,
                    "markets": sb, "southbound_net_total": round(tot), "unit": "HKD turnover"}
        if not last_err:
            last_err = "no Southbound rows; markets=%s" % ([str(blk.get("market")) for blk in data][:6])
    return {"status": "PENDING", "note": last_err or "no data"}


def japan_jpx():
    """JPX weekly Trading-by-Type-of-Investors (TSE Prime). Foreign net = latest
    week's Balance for Foreigners (JPY thousands)."""
    try:
        listing = _http_bytes("https://www.jpx.co.jp/english/markets/statistics-equities/investor-type/index.html").decode("utf-8", "ignore")
        vals = re.findall(r"[\w./_-]*stock_val[\w./_-]*\.xls", listing, re.I)
        if not vals:
            return {"status": "PENDING", "note": "no stock_val xls link"}
        url = vals[0]
        if url.startswith("/"): url = "https://www.jpx.co.jp" + url
        xls = _http_bytes(url, timeout=45)
        import xlrd
        book = xlrd.open_workbook(file_contents=xls)
        by_mkt = {}
        for si in range(min(book.nsheets, 3)):
            sh = book.sheet_by_index(si)
            bal_cols, weeks = [], []
            for i in range(min(sh.nrows, 14)):
                rv = [str(sh.cell_value(i, j)) for j in range(sh.ncols)]
                if not bal_cols and any(("Balance" in v or "差引き" in v) for v in rv):
                    bal_cols = [j for j, v in enumerate(rv) if ("Balance" in v or "差引き" in v)]
                if not weeks and any(("/" in v and ("~" in v or "～" in v)) for v in rv):
                    weeks = [v for v in rv if "/" in v]
            def net_for(label):
                for i in range(sh.nrows):
                    c0 = str(sh.cell_value(i, 0)).strip()
                    if c0 == label:
                        vb = [_num(sh.cell_value(i, c)) for c in bal_cols if c < sh.ncols]
                        vb = [v for v in vb if v is not None]
                        return vb[-1] if vb else None
                return None
            by_mkt[sh.name] = {"foreign_net": net_for("Foreigners"),
                               "individual_net": net_for("Individuals"),
                               "institution_net": net_for("Institutions")}
            if si == 0 and weeks:
                latest_week = weeks[-1]
        prime = by_mkt.get("TSE Prime") or next(iter(by_mkt.values()), {})
        return {"status": "LIVE", "source": "JPX weekly", "file": url.split("/")[-1],
                "unit": "JPY thousands", "week": (latest_week if "latest_week" in dir() else None),
                "foreign_net": prime.get("foreign_net"), "individual_net": prime.get("individual_net"),
                "institution_net": prime.get("institution_net"), "by_market": by_mkt}
    except Exception as e:
        return {"status": "PENDING", "err": str(e)[:140]}


def us_returns(symbols):
    """Real US recent returns via FMP. Tries multi-period price-change (stable +
    v3), falls back to stable/quote 1-day change. Returns {sym:{d5,m1,m3,d1}}."""
    key = os.environ.get("FMP_KEY") or os.environ.get("FMP_API_KEY")
    if not key or not symbols:
        return {}
    out = {}
    for sym in sorted(set(symbols)):
        row = None
        for url in ("https://financialmodelingprep.com/stable/stock-price-change?symbol=%s&apikey=%s" % (sym, key),
                    "https://financialmodelingprep.com/api/v3/stock-price-change/%s?apikey=%s" % (sym, key)):
            try:
                doc = _get_json(url, timeout=15)
                row = doc[0] if isinstance(doc, list) and doc else (doc if isinstance(doc, dict) and doc else None)
                if row and (row.get("5D") is not None or row.get("1M") is not None):
                    break
                row = None
            except Exception:
                row = None
        if row:
            out[sym] = {"d5": row.get("5D"), "m1": row.get("1M"), "m3": row.get("3M")}
            continue
        try:
            q = _get_json("https://financialmodelingprep.com/stable/quote?symbol=%s&apikey=%s" % (sym, key), timeout=15)
            r = q[0] if isinstance(q, list) and q else (q if isinstance(q, dict) and q else None)
            if r:
                out[sym] = {"d5": None, "m1": None, "m3": None,
                            "d1": r.get("changePercentage") if r.get("changePercentage") is not None else r.get("changesPercentage")}
        except Exception:
            pass
    return out


def eastmoney_southbound():
    """Southbound (南向: mainland -> HK) net via Eastmoney (datacenter-accessible,
    unlike Akamai-fronted HKEX). Southbound legs = sh2hk + sz2hk (港股通沪/深).
    Prefer daily-history endpoint (authoritative last trading day); fall back to
    the real-time snapshot during market hours."""
    dump = {}
    # A) daily history — reliable last-trading-day Southbound net (MUTUAL_TYPE 003/004)
    try:
        params = {"reportName": "RPT_MUTUAL_DEAL_HISTORY", "columns": "ALL", "source": "WEB",
                  "sortColumns": "TRADE_DATE", "sortTypes": "-1", "pageSize": "10", "pageNumber": "1",
                  "filter": chr(40) + "MUTUAL_TYPE in (" + chr(34) + "003" + chr(34) + "," + chr(34) + "004" + chr(34) + ")" + chr(41)}
        url = "https://datacenter-web.eastmoney.com/api/data/v1/get?" + urllib.parse.urlencode(params)
        doc2 = _get_json(url, headers={"User-Agent": UA["User-Agent"], "Referer": "https://data.eastmoney.com/"}, timeout=15)
        res = (doc2.get("result") or {}).get("data") or []
        if res:
            dump["daily_keys"] = list(res[0])[:22]
            dump["daily_first"] = {k: res[0].get(k) for k in list(res[0])[:22]}
            netk = "NET_DEAL_AMT" if "NET_DEAL_AMT" in res[0] else next((k for k in res[0] if k.upper() in ("FUND_INFLOW", "NET_BUY_AMT")), None)
            datek, typek = "TRADE_DATE", "MUTUAL_TYPE"
            tmap = {"003": "港股通(沪) SH→HK", "004": "港股通(深) SZ→HK"}
            valid = [r for r in res if _num(r.get(netk)) is not None]
            if valid:
                latest_date = max(str(r.get(datek)) for r in valid)
                legs = {tmap.get(str(r.get(typek)), str(r.get(typek))): _num(r.get(netk))
                        for r in valid if str(r.get(datek)) == latest_date}
                tot = sum(v for v in legs.values() if v is not None)
                if legs:
                    return {"status": "LIVE", "source": "Eastmoney Southbound (daily, settled)",
                            "as_of": latest_date[:10], "southbound_net_total": round(tot),
                            "markets": legs, "net_field": netk, "unit": "CNY (Eastmoney NET_DEAL_AMT)", "_dump": dump}
    except Exception as e:
        dump["daily_err"] = str(e)[:90]
    # B) real-time snapshot (Southbound = sh2hk + sz2hk)
    try:
        doc = _get_json("https://push2.eastmoney.com/api/qt/kamt/get?"
                        "fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57,f58"
                        "&ut=b2884a393a59ad64002292a3e90d46a5",
                        headers={"User-Agent": UA["User-Agent"], "Referer": "https://data.eastmoney.com/"}, timeout=15)
        data = doc.get("data") or {}
        dump["kamt_keys"] = list(data)
        def _leg(k):
            leg = data.get(k) or {}
            return _num(leg.get("dayNetAmtIn")), (leg.get("date2") or leg.get("date"))
        shv, dt = _leg("sh2hk"); szv, _ = _leg("sz2hk")
        nets = [x for x in (shv, szv) if x is not None]
        if nets:
            return {"status": "LIVE", "source": "Eastmoney HSGT (realtime)", "as_of": dt,
                    "southbound_net_total": round(sum(nets)),
                    "markets": {"港股通(沪) SH→HK": shv, "港股通(深) SZ→HK": szv},
                    "unit": "CNY (Eastmoney, 万元)", "_dump": dump}
    except Exception as e:
        dump["kamt_err"] = str(e)[:90]
    return {"status": "PENDING", "note": "eastmoney parse pending", "_dump": dump}


def lambda_handler(event=None, context=None):
    now = datetime.now(timezone.utc)
    doc = {"engine": "justhodl-apac-flows", "version": "1.0.0",
           "generated_at": now.isoformat(timespec="seconds"), "status": "LIVE",
           "thesis": "Taiwan/Korea foreign-flow leadership as an early read on US sectors",
           "sources": {}}
    # Taiwan
    try:
        t86 = taiwan_t86()
        try:
            t86.update(taiwan_bfi())
        except Exception as e:
            t86["bfi_err"] = str(e)[:80]
        t86["as_of"] = now.strftime("%Y-%m-%d")
        doc["taiwan"] = t86
        doc["sources"]["twse_t86"] = True
        doc["sources"]["twse_bfi"] = "foreign_net_twd" in t86 and t86.get("foreign_net_twd") is not None
    except Exception as e:
        doc["taiwan"] = {"status": "ERROR", "err": str(e)[:140]}
        doc["sources"]["twse_t86"] = False
    # Korea per-stock (Naver — KRX blocks AWS) + TRUE market-wide via Naver index
    kr = korea_naver()
    try:
        kr["market_wide"] = korea_market()
    except Exception as e:
        kr["market_wide"] = {"err": str(e)[:80]}
    doc["korea"] = kr
    doc["sources"]["korea_naver"] = kr.get("status") == "LIVE"
    doc["sources"]["korea_marketwide"] = (kr.get("market_wide") or {}).get("total_foreign_value") is not None
    # Japan (JPX weekly Trading-by-Type-of-Investors)
    doc["japan"] = japan_jpx()
    doc["sources"]["japan_jpx"] = doc["japan"].get("status") == "LIVE"
    # Hong Kong (Stock Connect Southbound). HKEX is Akamai-blocked from AWS -> Eastmoney.
    hkd = hongkong_southbound()
    if hkd.get("status") != "LIVE":
        hkd = eastmoney_southbound()
    doc["hongkong"] = hkd
    doc["sources"]["hk_southbound"] = hkd.get("status") == "LIVE"
    # US catch-up bridges — attach REAL US returns (FMP) + divergence verdict
    usf = us_side()
    all_us = sorted({s for b in BRIDGES for s in (b["us_etf"] + b["us_names"])})
    usr = us_returns(all_us)
    bridges = []
    for b in BRIDGES:
        tw_sig = None
        if b["tw_codes"] and doc.get("taiwan", {}).get("top_buy") is not None:
            m = {r["code"]: r["foreign_net_shares"]
                 for r in doc["taiwan"]["top_buy"] + doc["taiwan"]["top_sell"]}
            vals = [m.get(c) for c in b["tw_codes"] if m.get(c) is not None]
            tw_sig = round(sum(vals)) if vals else None
        def _r(sym):
            r = usr.get(sym, {})
            return r.get("d5") if r.get("d5") is not None else r.get("d1")
        kr_sig = None
        if b["kr_codes"] and doc.get("korea", {}).get("status") == "LIVE":
            krm = {r["code"]: r["foreign_net_shares"]
                   for r in (doc["korea"].get("top_buy", []) + doc["korea"].get("top_sell", []))}
            kv = [krm.get(c) for c in b["kr_codes"] if krm.get(c) is not None]
            kr_sig = round(sum(kv)) if kv else None
        hk_sig = None
        if b.get("hk") and doc.get("hongkong", {}).get("status") == "LIVE":
            hk_sig = doc["hongkong"].get("southbound_net_total")
        etf_ret = {e: _r(e) for e in b["us_etf"] if e in usr}
        name_ret = {n: _r(n) for n in b["us_names"] if n in usr}
        us5 = [v for v in list(etf_ret.values()) + list(name_ret.values()) if isinstance(v, (int, float))]
        us_avg5 = round(sum(us5) / len(us5), 2) if us5 else None
        # verdict: Asia buying (tw_sig>0) while US flat/down = US hasn't caught up = opportunity
        asia_vals = [v for v in (tw_sig, kr_sig, hk_sig) if v is not None]
        asia_net = sum(asia_vals) if asia_vals else None
        verdict = "insufficient data"
        if asia_net is not None and us_avg5 is not None:
            if asia_net > 0 and us_avg5 < 1.5:
                verdict = "ASIA LEADING — US lagging (potential setup)"
            elif asia_net > 0 and us_avg5 >= 1.5:
                verdict = "confirmed — both bid"
            elif asia_net < 0 and us_avg5 > 1.5:
                verdict = "US extended, Asia foreign selling — caution"
            else:
                verdict = "both soft"
        bridges.append({"name": b["name"], "sector": b["sector"],
                        "tw_foreign_net_shares": tw_sig, "kr_foreign_net_shares": kr_sig, "hk_southbound_net": hk_sig, "kr_codes": b["kr_codes"],
                        "us_etf": b["us_etf"], "us_names": b["us_names"],
                        "us_etf_ret5d": etf_ret or None, "us_name_ret5d": name_ret or None,
                        "us_avg_ret5d": us_avg5, "us_etf_5d_flow_usd": {e: usf.get(e) for e in b["us_etf"] if e in usf} or None,
                        "verdict": verdict})
    doc["bridges"] = bridges
    doc["us_returns_live"] = bool(usr)
    doc["us_feed_available"] = bool(usf)
    doc["us_feed_available"] = bool(usf)

    # history
    hist = _j(HIST, {"days": {}}) or {"days": {}}
    day = now.strftime("%Y-%m-%d")
    hist["days"][day] = {"tw_foreign_net_twd": doc.get("taiwan", {}).get("foreign_net_twd"),
                         "tw_sector_flows": doc.get("taiwan", {}).get("sector_flows_shares"),
                         "tw_tracked": doc.get("taiwan", {}).get("tracked"),
                         "kr_status": doc["korea"].get("status")}
    hist["days"] = dict(sorted(hist["days"].items())[-400:])
    _put(HIST, hist)

    tw = doc.get("taiwan", {})
    if tw.get("top_buy"):
        top = tw["top_buy"][0]
        doc["read"] = ("Taiwan foreign investors' biggest net buy today: %s (%s); market foreign net %s. "
                       "Leading sector by foreign flow: %s." % (
                           top.get("name") or top["code"], top.get("sector") or "—",
                           ("NT$%.1fB" % tw["foreign_net_twd_bn"]) if tw.get("foreign_net_twd_bn") is not None else "n/a",
                           next(iter(tw.get("sector_flows_shares", {})), "—")))
    doc["history_days"] = len(hist["days"])
    _put(OUT, doc)
    return {"ok": True, "status": "LIVE", "sources": doc["sources"],
            "tw_stocks": tw.get("n_stocks"), "tw_foreign_col": tw.get("foreign_col_detected"),
            "tw_top_buy": (tw.get("top_buy") or [{}])[0].get("name"),
            "kr": doc["korea"].get("status"), "bridges": len(bridges)}
