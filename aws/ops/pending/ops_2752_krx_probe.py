"""ops 2752 rev2 — KRX PROBE with session cookie + ko_KR locale + control bld.

rev1 got HTTP 400 on every getJsonData call (KRX's own error page returned on
OTP => we reach KRX; it rejects request shape). Fixes: (1) establish a session
via cookie jar by GETting the MDC loader page first; (2) locale=ko_KR (en is
rejected); (3) Referer=data.krx.co.kr root; (4) test a KNOWN-simple control bld
(stock list MDCSTAT01901) to separate 'format wrong' from 'AWS blocked'; then
(5) probe investor blds with minimal pykrx-style params and extract foreign/
individual/institution net. Report: aws/ops/reports/2752_krx_probe.json.
"""
import os, io, json, time, urllib.request, urllib.parse
from http.cookiejar import CookieJar
from datetime import datetime, timezone, timedelta

R = {"ops": 2752, "ts": datetime.now(timezone.utc).isoformat(), "trials": []}
ROOT = "http://data.krx.co.kr"
LOADER = ROOT + "/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020201"
GETJSON = ROOT + "/comm/bldAttendant/getJsonData.cmd"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/121.0 Safari/537.36")
cj = CookieJar()
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))

def last_trading_day():
    kst = datetime.now(timezone.utc) + timedelta(hours=9)
    d = kst - timedelta(days=1) if kst.hour < 8 else kst
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.strftime("%Y%m%d")
DATE = last_trading_day()
print("KST last trading day:", DATE)

# 1) establish session cookie
try:
    req = urllib.request.Request(LOADER, headers={"User-Agent": UA})
    with opener.open(req, timeout=25) as r:
        r.read(2000)
    R["session_cookies"] = [c.name for c in cj]
    print("session cookies:", R["session_cookies"])
except Exception as e:
    R["session_err"] = str(e)[:120]
    print("session err:", str(e)[:100])

def getjson(params, timeout=25):
    body = urllib.parse.urlencode(params).encode()
    req = urllib.request.Request(GETJSON, data=body, headers={
        "User-Agent": UA, "Referer": ROOT + "/", "Origin": ROOT,
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/x-www-form-urlencoded"})
    with opener.open(req, timeout=timeout) as r:
        return r.status, r.read()

def block_of(doc):
    for k in ("output", "OutBlock_1", "block1", "out", "list"):
        if isinstance(doc.get(k), list) and doc[k]:
            return k, doc[k]
    for k, v in doc.items():
        if isinstance(v, list) and v and isinstance(v[0], dict):
            return k, v
    return None, []

# 2) CONTROL: known-simple stock-list bld — confirms request shape works at all
print("\n== CONTROL: stock list MDCSTAT01901 ==")
ctrl_ok = False
try:
    st, raw = getjson({"bld": "dbms/MDC/STAT/standard/MDCSTAT01901", "locale": "ko_KR",
                       "mktId": "STK", "share": "1", "csvxls_isNo": "false"})
    try:
        doc = json.loads(raw); bk, rows = block_of(doc)
        ctrl_ok = len(rows) > 0
        R["control"] = {"http": st, "block": bk, "rows": len(rows),
                        "row_keys": list(rows[0])[:10] if rows else None}
        print("  control http=%s block=%s rows=%s" % (st, bk, len(rows)))
    except Exception:
        R["control"] = {"http": st, "non_json": raw[:80].decode("utf-8", "ignore")}
        print("  control non-json:", raw[:80].decode("utf-8", "ignore"))
except Exception as e:
    R["control"] = {"err": str(e)[:120]}
    print("  control ERR:", str(e)[:100])

# 3) investor blds with minimal pykrx-style params
print("\n== INVESTOR PROBES ==")
INV = [
    ("MDCSTAT02201", {"inqTpCd": "2", "trdVolVal": "2", "askBid": "3", "strtDd": DATE, "endDd": DATE, "mktId": "STK", "detailView": "1"}),
    ("MDCSTAT02202", {"inqTpCd": "2", "trdVolVal": "2", "askBid": "3", "strtDd": DATE, "endDd": DATE, "mktId": "STK", "detailView": "1"}),
    ("MDCSTAT02203", {"inqTpCd": "2", "trdVolVal": "2", "askBid": "3", "trdDd": DATE, "mktId": "STK", "detailView": "1"}),
    ("MDCSTAT02401", {"inqTpCd": "1", "trdVolVal": "2", "askBid": "1", "trdDd": DATE, "mktId": "STK", "invstTpCd": "9000"}),
]
winner = None
for bld, extra in INV:
    p = {"bld": "dbms/MDC/STAT/standard/" + bld, "locale": "ko_KR",
         "money": "1", "csvxls_isNo": "false"}
    p.update(extra)
    t = {"bld": bld}
    try:
        st, raw = getjson(p)
        t["http"] = st
        doc = json.loads(raw); bk, rows = block_of(doc)
        t["block"] = bk; t["rows"] = len(rows)
        if rows:
            t["row_keys"] = list(rows[0])[:16]
            t["sample"] = {k: rows[0][k] for k in list(rows[0])[:16]}
            if winner is None and len(rows) <= 60:
                winner = {"bld": bld, "block": bk, "rows": rows, "row_keys": list(rows[0])}
        print("  %-13s http=%s block=%s rows=%s keys=%s" % (bld, st, bk, len(rows), t.get("row_keys", [])[:6]))
        R["trials"].append(t)
    except Exception as e:
        t["err"] = str(e)[:110]; R["trials"].append(t)
        print("  %-13s ERR %s" % (bld, str(e)[:80]))
    time.sleep(1.0)

if winner:
    rows = winner["rows"]; keys = winner["row_keys"]
    name_key = next((k for k in keys if any(s in k.upper() for s in ("INVST", "TP_NM", "NM", "INVESTOR"))), keys[0])
    net_key = next((k for k in keys if "NETBID" in k.upper() or ("NET" in k.upper() and "VAL" in k.upper())), None)
    if not net_key:
        net_key = next((k for k in keys if "NET" in k.upper()), keys[-1])
    def num(x):
        try: return float(str(x).replace(",", ""))
        except Exception: return None
    ext = {}
    for r in rows:
        nm = str(r.get(name_key, ""))
        tag = ("foreign" if "외국인" in nm else "individual" if "개인" in nm
               else "institution" if "기관" in nm else None)
        if tag:
            ext[tag] = num(r.get(net_key))
    R["winner"] = {"bld": winner["bld"], "block": winner["block"], "name_key": name_key,
                   "net_key": net_key, "row_keys": keys, "names": [str(r.get(name_key)) for r in rows][:20],
                   "extracted_net_krw": ext}
    print("\nWINNER:", winner["bld"], "| name_key=%s net_key=%s" % (name_key, net_key))
    print("investor rows:", [str(r.get(name_key)) for r in rows][:12])
    print("extracted (foreign/individual/institution KRW):", json.dumps(ext, ensure_ascii=False))
    R["verdict"] = "KRX_JSON_WORKS:" + winner["bld"]
else:
    R["verdict"] = "control_ok=%s but no investor winner" % ctrl_ok if ctrl_ok else "KRX rejects/blocks from AWS"
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2752_krx_probe.json", "w") as f:
    json.dump(R, f, indent=1, ensure_ascii=False, default=str)
print("\nVERDICT:", R["verdict"])
print("OPS 2752 rev2 COMPLETE")
