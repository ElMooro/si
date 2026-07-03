"""ops 2752 — KRX INVESTOR-FLOW PROBE (Khalid: run KRX probe).

Read-only reconnaissance. KRX data portal exposes investor trading data via
POST data.krx.co.kr/comm/bldAttendant/getJsonData.cmd with a `bld` report code.
Codes are undocumented, so probe a matrix of candidate blds x param sets with
proper browser headers, record HTTP status + block name + row count + the exact
field keys of the first row. Where a market investor-type table is found, extract
today's KOSPI foreign / individual / institution net so the adapter can be wired
next. Also tries the OTP->CSV fallback if JSON is blocked from AWS.
Report: aws/ops/reports/2752_krx_probe.json.
"""
import os, io, json, time, urllib.request, urllib.parse
from datetime import datetime, timezone, timedelta

R = {"ops": 2752, "ts": datetime.now(timezone.utc).isoformat(), "trials": []}
GETJSON = "data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
OTPGEN = "data.krx.co.kr/comm/fileDn/GenerateOTP.cmd"
OTPDL = "data.krx.co.kr/comm/fileDn/download.cmd"
HDRS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/121.0 Safari/537.36",
        "Referer": "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020103",
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}

def last_trading_day():
    kst = datetime.now(timezone.utc) + timedelta(hours=9)
    d = kst
    # step back to a weekday; markets closed Sat/Sun. Also back off one day if very early.
    if kst.hour < 8:
        d = kst - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.strftime("%Y%m%d")

DATE = last_trading_day()
print("probe date (KST last trading day):", DATE)

def post(host_path, params, scheme="https", timeout=25):
    url = scheme + "://" + host_path
    body = urllib.parse.urlencode(params).encode()
    req = urllib.request.Request(url, data=body, headers=HDRS)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
    return r.status, raw

# candidate report codes (KRX MDC investor-trading family) + rich superset params
BLDS = [
    ("MDCSTAT02201", "market investor - period total"),
    ("MDCSTAT02202", "market investor - daily trend"),
    ("MDCSTAT02203", "market investor - by ticker daily"),
    ("MDCSTAT02301", "ticker investor - period total"),
    ("MDCSTAT02302", "ticker investor - daily trend"),
    ("MDCSTAT02401", "investor net-buy ranking"),
]
def sup(bld, askbid):
    return {"bld": "dbms/MDC/STAT/standard/" + bld, "locale": "en",
            "mktId": "STK", "trdDd": DATE, "strtDd": DATE, "endDd": DATE,
            "share": "1", "money": "1", "csvxls_isNo": "false",
            "trdVolVal": "2", "askBid": str(askbid), "inqTpCd": "1", "detailView": "1"}

BLOCK_KEYS = ("output", "OutBlock_1", "block1", "out", "list")
def block_of(doc):
    for k in BLOCK_KEYS:
        if isinstance(doc.get(k), list) and doc[k]:
            return k, doc[k]
    # any list value
    for k, v in doc.items():
        if isinstance(v, list) and v and isinstance(v[0], dict):
            return k, v
    return None, []

winner = None
for bld, desc in BLDS:
    for askbid in (1, 3):
        for scheme in ("https", "http"):
            t = {"bld": bld, "desc": desc, "askBid": askbid, "scheme": scheme}
            try:
                st, raw = post(GETJSON, sup(bld, askbid), scheme=scheme)
                t["http"] = st
                try:
                    doc = json.loads(raw)
                except Exception:
                    t["parse"] = "non-json len=%d head=%s" % (len(raw), raw[:60].decode("utf-8", "ignore"))
                    R["trials"].append(t); break
                bk, rows = block_of(doc)
                t["block"] = bk; t["rows"] = len(rows)
                t["top_keys"] = list(doc)[:8]
                if rows:
                    t["row_keys"] = list(rows[0])[:14]
                    t["sample_row"] = {k: rows[0][k] for k in list(rows[0])[:14]}
                    if winner is None:
                        winner = {"bld": bld, "askBid": askbid, "scheme": scheme, "block": bk,
                                  "row_keys": list(rows[0]), "rows": rows}
                R["trials"].append(t)
                print("  %-14s ab=%d %-5s http=%s block=%s rows=%s keys=%s" % (
                    bld, askbid, scheme, t.get("http"), bk, t.get("rows"), t.get("row_keys", [])[:5]))
                break  # scheme loop: https worked (or returned), no need http
            except Exception as e:
                t["err"] = str(e)[:100]
                R["trials"].append(t)
                print("  %-14s ab=%d %-5s ERR %s" % (bld, askbid, scheme, str(e)[:70]))
        time.sleep(1.0)

# If we found a table, try to extract foreign / individual / institution net
if winner:
    print("\nWINNER:", winner["bld"], "ab=%d" % winner["askBid"], winner["scheme"], "block=%s" % winner["block"])
    print("row_keys:", winner["row_keys"])
    rows = winner["rows"]
    # find an investor-type name column + a net-value column
    name_key = next((k for k in winner["row_keys"] if any(s in k.upper() for s in ("INVST", "TP_NM", "NM", "INVESTOR"))), None)
    net_key = next((k for k in winner["row_keys"] if any(s in k.upper() for s in ("NETBID", "NET", "순매수"))), None)
    def num(x):
        try: return float(str(x).replace(",", ""))
        except Exception: return None
    extract = {}
    for r in rows:
        nm = str(r.get(name_key, "")) if name_key else ""
        low = nm.lower()
        tag = ("foreign" if ("외국인" in nm or "foreign" in low) else
               "individual" if ("개인" in nm or "individual" in low or "retail" in low) else
               "institution" if ("기관" in nm or "institution" in low) else None)
        if tag and net_key:
            extract[tag] = num(r.get(net_key))
    R["winner"] = {"bld": winner["bld"], "askBid": winner["askBid"], "scheme": winner["scheme"],
                   "block": winner["block"], "name_key": name_key, "net_key": net_key,
                   "row_keys": winner["row_keys"], "extracted_net": extract,
                   "all_rows_names": [str(r.get(name_key)) for r in rows][:20] if name_key else None}
    print("extracted net (foreign/individual/institution):", json.dumps(extract, ensure_ascii=False))
else:
    print("\nNO JSON WINNER — trying OTP->CSV fallback on MDCSTAT02201")
    try:
        otp_params = dict(sup("MDCSTAT02201", 1)); otp_params["name"] = "fileDown"; otp_params["url"] = "dbms/MDC/STAT/standard/MDCSTAT02201"
        st, raw = post(OTPGEN, otp_params, scheme="https")
        code = raw.decode("utf-8", "ignore")[:400]
        R["otp_probe"] = {"http": st, "otp_head": code[:120]}
        print("  OTP gen http=%s head=%s" % (st, code[:100]))
        if st == 200 and len(code) > 10 and "<" not in code[:20]:
            st2, raw2 = post(OTPDL, {"code": code}, scheme="https")
            R["otp_probe"]["download_http"] = st2
            R["otp_probe"]["csv_head"] = raw2[:200].decode("utf-8", "ignore")
            print("  OTP download http=%s head=%s" % (st2, raw2[:120].decode("utf-8", "ignore")))
    except Exception as e:
        R["otp_probe"] = {"err": str(e)[:140]}
        print("  OTP fallback err:", str(e)[:100])

R["verdict"] = "KRX_JSON_WORKS: %s" % winner["bld"] if winner else "KRX_JSON_BLOCKED_see_otp_probe"
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2752_krx_probe.json", "w") as f:
    json.dump(R, f, indent=1, ensure_ascii=False, default=str)
print("\nVERDICT:", R["verdict"])
print("OPS 2752 COMPLETE — KRX reconnaissance done")
