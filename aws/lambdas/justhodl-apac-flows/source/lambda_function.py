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
TWSE = "https://openapi.twse.com.tw/v1"
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


def taiwan_t86():
    """Per-stock foreign net buy/sell (shares). Language-agnostic key detection."""
    rows = _get_json(TWSE + "/exchangeReport/T86")
    if not isinstance(rows, list) or not rows:
        raise RuntimeError("T86 empty")
    sample = rows[0]
    code_key = _detect_key(sample, ["code", "證券代號", "代號"]) or \
        next((k for k in sample if re.match(r"^\d{4}$", str(sample[k]).strip())), None)
    name_key = _detect_key(sample, ["name", "名稱"])
    # foreign net: prefer the foreign-investors net-buy/sell column, excluding dealer sub-columns
    fk = _detect_key(sample, ["外陸資買賣超", "外資買賣超", "foreigninvestors", "foreign investors"],
                     must_numeric=True, avoid=("dealer", "自營"))
    if not fk:
        fk = _detect_key(sample, ["外", "foreign"], must_numeric=True, avoid=("dealer", "自營"))
    trust_key = _detect_key(sample, ["投信", "trust"], must_numeric=True)
    if not (code_key and fk):
        raise RuntimeError("T86 key detect failed keys=%s" % list(sample)[:8])
    out = []
    for r in rows:
        code = str(r.get(code_key, "")).strip()
        fnet = _num(r.get(fk))
        if not re.match(r"^\d{4}$", code) or fnet is None:
            continue
        out.append({"code": code, "name": TW_NAMES.get(code) or str(r.get(name_key, "")).strip(),
                    "foreign_net_shares": fnet,
                    "trust_net_shares": _num(r.get(trust_key)) if trust_key else None,
                    "sector": TW_SECTOR.get(code)})
    out.sort(key=lambda x: x["foreign_net_shares"], reverse=True)
    sec = {}
    for r in out:
        if r["sector"]:
            sec[r["sector"]] = sec.get(r["sector"], 0) + r["foreign_net_shares"]
    return {"foreign_col_detected": fk, "n_stocks": len(out),
            "top_buy": out[:15], "top_sell": list(reversed(out[-15:])),
            "sector_flows_shares": {k: round(v) for k, v in sorted(sec.items(), key=lambda kv: kv[1], reverse=True)},
            "tracked": {c: next((r["foreign_net_shares"] for r in out if r["code"] == c), None)
                        for c in ("2330", "2454", "2317", "000660") if c in TW_NAMES}}


def taiwan_bfi():
    """Market-wide foreign net dollar (NT$) from BFI82U."""
    rows = _get_json(TWSE + "/exchangeReport/BFI82U")
    if not isinstance(rows, list) or not rows:
        raise RuntimeError("BFI82U empty")
    for r in rows:
        label = " ".join(str(v) for v in r.values() if isinstance(v, str))
        if "外" in label or "foreign" in label.lower():
            buy = _detect_key(r, ["買進", "buy"], must_numeric=True, avoid=("賣", "sell"))
            sell = _detect_key(r, ["賣出", "sell"], must_numeric=True)
            net = _detect_key(r, ["買賣超", "diff", "net"], must_numeric=True)
            nv = _num(r.get(net)) if net else (
                (_num(r.get(buy)) or 0) - (_num(r.get(sell)) or 0) if buy and sell else None)
            if nv is not None:
                return {"foreign_net_twd": round(nv), "foreign_net_twd_bn": round(nv / 1e9, 2)}
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
