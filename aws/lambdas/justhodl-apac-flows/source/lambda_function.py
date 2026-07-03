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
import json, re, time, urllib.request, urllib.error, urllib.parse
from datetime import datetime, timezone
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
TW_NAMES = {"2330": "TSMC", "2454": "MediaTek", "2317": "Hon Hai (Foxconn)", "2303": "UMC",
            "3711": "ASE Technology", "2308": "Delta Electronics", "2382": "Quanta",
            "2357": "Asus", "3034": "Novatek", "2379": "Realtek", "3231": "Wistron",
            "2891": "CTBC Financial", "2412": "Chunghwa Telecom", "2881": "Fubon Financial",
            "2603": "Evergreen Marine", "6505": "Formosa Petrochemical", "1301": "Formosa Plastics",
            "2409": "AU Optronics", "3008": "LargAn", "2327": "Yageo"}
TW_SECTOR = {"2330": "Semiconductors", "2454": "Semiconductors", "2303": "Semiconductors",
             "3711": "Semiconductors", "3034": "Semiconductors", "2379": "Semiconductors",
             "3008": "Semiconductors", "2327": "Semiconductors",
             "2317": "Electronics/Hardware", "2308": "Electronics/Hardware", "2382": "Electronics/Hardware",
             "2357": "Electronics/Hardware", "3231": "Electronics/Hardware", "2409": "Electronics/Hardware",
             "2891": "Financials", "2881": "Financials",
             "2412": "Telecom", "2603": "Shipping", "6505": "Materials", "1301": "Materials"}

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


def korea_krx():
    """Best-effort KRX market investor net. Never throws."""
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "http://data.krx.co.kr/",
               "Content-Type": "application/x-www-form-urlencoded"}
    for bld in ("dbms/MDC/STAT/standard/MDCSTAT02203", "dbms/MDC/STAT/standard/MDCSTAT02201"):
        try:
            body = urllib.parse.urlencode({"bld": bld, "mktId": "STK", "strtDd": today, "endDd": today,
                                           "trdVolVal": "2", "askBid": "3", "share": "1", "money": "1"}).encode()
            req = urllib.request.Request(KRX, data=body, headers=headers)
            with urllib.request.urlopen(req, timeout=25) as r:
                doc = json.loads(r.read())
            block = doc.get("output") or doc.get("OutBlock_1") or doc.get("block1") or []
            if block:
                return {"status": "LIVE", "bld": bld, "rows": len(block), "raw_keys": list(block[0])[:6]}
        except Exception:
            continue
    return {"status": "PENDING", "note": "KRX endpoint/bld to be resolved in v1.1 dedicated probe"}


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
    # Korea (best-effort)
    kr = korea_krx()
    doc["korea"] = kr
    doc["sources"]["krx"] = kr.get("status") == "LIVE"
    # Japan placeholder
    doc["japan"] = {"status": "PENDING_v1_1", "note": "JPX weekly investor-type flows (Thursday Excel)"}
    # US catch-up bridges
    usf = us_side()
    bridges = []
    for b in BRIDGES:
        tw_sig = None
        if b["tw_codes"] and doc.get("taiwan", {}).get("top_buy") is not None:
            m = {r["code"]: r["foreign_net_shares"] for r in doc["taiwan"]["top_buy"] + doc["taiwan"]["top_sell"]}
            vals = [m.get(c) for c in b["tw_codes"] if m.get(c) is not None]
            tw_sig = round(sum(vals)) if vals else None
        us_flow = {e: usf.get(e) for e in b["us_etf"] if e in usf}
        bridges.append({"name": b["name"], "sector": b["sector"],
                        "tw_foreign_net_shares": tw_sig,
                        "kr_codes": b["kr_codes"], "us_etf": b["us_etf"], "us_names": b["us_names"],
                        "us_etf_5d_flow_usd": us_flow or None,
                        "note": ("Asia foreign buying — check if US ETF flow has followed"
                                 if (tw_sig or 0) > 0 else "watch")})
    doc["bridges"] = bridges
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
