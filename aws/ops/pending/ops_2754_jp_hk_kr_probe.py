"""ops 2754 — JAPAN / HONG KONG / KOREA-MARKETWIDE source recon.

Pure read-only probe (always completes). Discovers what's reachable from AWS +
exact schema before building:
  KOREA market-wide: Naver index endpoints (KOSPI/KOSDAQ) + legacy deal-trend HTML.
  JAPAN: JPX investor-type listing page -> extract .xls/.xlsx links (weekly
         'Trading by Type of Investors'); confirm one downloads.
  HONG KONG: Stock Connect SOUTHBOUND (mainland->HK = the HK foreign-flow analog)
             via HKEX endpoints + AAStocks fallback.
Report: aws/ops/reports/2754_jp_hk_kr_probe.json.
"""
import os, io, re, json, urllib.request
from datetime import datetime, timezone

R = {"ops": 2754, "ts": datetime.now(timezone.utc).isoformat(), "korea": [], "japan": [], "hongkong": []}
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/121.0 Safari/537.36")

def fetch(url, headers=None, timeout=25):
    h = {"User-Agent": UA, "Accept": "*/*", "Accept-Language": "en,ko,ja,zh"}
    if headers: h.update(headers)
    req = urllib.request.Request(url, headers=h)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.headers.get("Content-Type", ""), r.read()

def characterize(url, kind, headers=None):
    t = {"url": url}
    try:
        st, ct, raw = fetch(url, headers=headers)
        t["http"] = st; t["ctype"] = ct[:40]; t["len"] = len(raw)
        txt = raw.decode("utf-8", "ignore")
        t["invest_terms"] = [w for w in ("외국인", "기관", "개인", "投資部門", "海外", "investor",
                             "foreign", "Southbound", "港股通", "南向", "北向", "frgn", "netvalue")
                             if w in txt]
        if kind == "json":
            try:
                doc = json.loads(raw)
                t["json"] = True
                if isinstance(doc, dict):
                    t["keys"] = list(doc)[:14]
                    for probe_k in ("result", "datas", "list", "output"):
                        v = doc.get(probe_k)
                        if isinstance(v, list) and v and isinstance(v[0], dict):
                            t["nested_%s_keys" % probe_k] = list(v[0])[:14]
                elif isinstance(doc, list) and doc:
                    t["list_first_keys"] = list(doc[0])[:14] if isinstance(doc[0], dict) else str(doc[0])[:80]
            except Exception:
                t["json"] = False; t["head"] = txt[:120]
        elif kind == "links":
            hrefs = re.findall(r'href=["\']([^"\']+\.xlsx?)["\']', txt, re.I)
            t["xls_links"] = list(dict.fromkeys(hrefs))[:12]
            t["head"] = txt[:80]
        else:
            t["head"] = re.sub(r"\s+", " ", txt[:200])
    except Exception as e:
        t["err"] = str(e)[:110]
    return t

print("== KOREA market-wide ==")
for url, kind in [
    ("https://m.stock.naver.com/api/index/KOSPI/integration", "json"),
    ("https://m.stock.naver.com/api/index/KOSDAQ/integration", "json"),
    ("https://m.stock.naver.com/api/index/KOSPI/trend", "json"),
    ("https://api.stock.naver.com/index/KOSPI/basic", "json"),
    ("https://finance.naver.com/sise/sise_deal_trend.naver", "html"),
    ("https://finance.naver.com/sise/investorDealTrendDay.naver", "html"),
]:
    t = characterize(url, kind, headers={"Referer": "https://m.stock.naver.com/"})
    R["korea"].append(t)
    print("  %-58s http=%s len=%s terms=%s keys=%s" % (
        url[-58:], t.get("http"), t.get("len"), t.get("invest_terms"),
        (t.get("keys") or t.get("list_first_keys") or t.get("nested_result_keys") or "")))

print("\n== JAPAN JPX ==")
jpx_pages = [
    "https://www.jpx.co.jp/english/markets/statistics-equities/investor-type/index.html",
    "https://www.jpx.co.jp/markets/statistics-equities/investor-type/index.html",
]
xls_found = []
for url in jpx_pages:
    t = characterize(url, "links")
    R["japan"].append(t)
    links = t.get("xls_links") or []
    xls_found += links
    print("  %-58s http=%s xls_links=%d" % (url[-58:], t.get("http"), len(links)))
    for L in links[:6]:
        print("      ", L)
# try downloading first plausible investor-type xls
if xls_found:
    cand = xls_found[0]
    if cand.startswith("/"):
        cand = "https://www.jpx.co.jp" + cand
    try:
        st, ct, raw = fetch(cand, timeout=30)
        R["japan_download"] = {"url": cand, "http": st, "ctype": ct[:40], "len": len(raw),
                               "is_xls": raw[:4] in (b"\xd0\xcf\x11\xe0", b"PK\x03\x04"),
                               "magic": raw[:4].hex()}
        print("  DL %-40s http=%s len=%s magic=%s" % (cand[-40:], st, len(raw), raw[:4].hex()))
    except Exception as e:
        R["japan_download"] = {"url": cand, "err": str(e)[:110]}
        print("  DL err:", str(e)[:90])

print("\n== HONG KONG Southbound ==")
ymd = datetime.now(timezone.utc).strftime("%Y%m%d")
for url, kind in [
    ("https://www.hkex.com.hk/eng/csm/DailyStat/data_tab_daily_%se.js" % ymd, "text"),
    ("https://www1.hkex.com.hk/hkexwidget/data/getmutualmarketdata?lang=eng&token=&type=SB&qid=0&callback=jQuery", "text"),
    ("https://www.hkex.com.hk/Mutual-Market/Stock-Connect/Statistics/Historical-Daily?sc_lang=en", "html"),
    ("http://www.aastocks.com/en/stocks/market/dtsc/dtsc.aspx", "html"),
    ("http://www.aastocks.com/en/stocks/market/ahstockcompare/ah-price-compare", "html"),
    ("https://www.hkex.com.hk/-/media/HKEX-Market/Mutual-Market/Stock-Connect/Statistics/SB_Turnover/2026/SB_Turnover_202607.json", "json"),
]:
    t = characterize(url, kind)
    R["hongkong"].append(t)
    print("  %-56s http=%s len=%s terms=%s" % (url[-56:], t.get("http"), t.get("len"), t.get("invest_terms")))

# verdicts
def any_ok(sec, need_terms):
    for t in R[sec]:
        if t.get("http") == 200 and (t.get("invest_terms") or t.get("keys") or t.get("nested_result_keys") or t.get("xls_links")):
            return t.get("url")
    return None
R["verdict"] = {
    "korea_marketwide": any_ok("korea", True),
    "japan": (R.get("japan_download", {}).get("url") if R.get("japan_download", {}).get("is_xls") else None) or any_ok("japan", True),
    "hongkong": any_ok("hongkong", True)}
print("\nVERDICT:", json.dumps(R["verdict"], ensure_ascii=False))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2754_jp_hk_kr_probe.json", "w") as f:
    json.dump(R, f, indent=1, ensure_ascii=False, default=str)
print("OPS 2754 COMPLETE — JP/HK/KR recon done")
