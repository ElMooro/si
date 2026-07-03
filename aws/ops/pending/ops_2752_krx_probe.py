"""ops 2752 rev3 — KOREA source probe: KRX blocked from AWS -> try Naver Finance.

Finding so far (recorded): KRX getJsonData rejects AWS-origin POSTs with HTTP 400
even on the control bld with a valid JSESSIONID => datacenter-IP filtering on the
data endpoint. This rev probes Naver Finance (the standard alternate for Korean
个人/外国人/机关 investor net) across mobile-API + legacy endpoints for market-wide
KOSPI flow and per-stock (Samsung 005930, SK Hynix 000660). Pure recon: always
completes, records which endpoints return usable investor data + their shape.
Report: aws/ops/reports/2752_krx_probe.json.
"""
import os, io, json, urllib.request
from datetime import datetime, timezone

R = {"ops": 2752, "rev": 3, "ts": datetime.now(timezone.utc).isoformat(),
     "krx_finding": "getJsonData HTTP 400 from AWS even on control bld w/ valid session => datacenter-IP block on data endpoint",
     "naver": []}
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/121.0 Safari/537.36")

def fetch(url, headers=None, timeout=20):
    h = {"User-Agent": UA, "Accept": "application/json, text/plain, */*"}
    if headers: h.update(headers)
    req = urllib.request.Request(url, headers=h)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read()

# Naver candidate endpoints (mobile API JSON + legacy HTML)
CAND = [
    ("m-api stock trend 005930", "https://m.stock.naver.com/api/stock/005930/trend", "json"),
    ("m-api stock investor 005930", "https://m.stock.naver.com/api/stock/005930/investor", "json"),
    ("m-api stock integration 005930", "https://m.stock.naver.com/api/stock/005930/integration", "json"),
    ("api.stock foreignInstitution 005930", "https://api.stock.naver.com/stock/005930/foreignInstitution", "json"),
    ("m-api index KOSPI trend", "https://m.stock.naver.com/api/index/KOSPI/trend", "json"),
    ("m-api index KOSPI integration", "https://m.stock.naver.com/api/index/KOSPI/integration", "json"),
    ("legacy frgn html 005930", "https://finance.naver.com/item/frgn.naver?code=005930", "html"),
    ("legacy investor trend day", "https://finance.naver.com/sise/investorDealTrendDay.naver", "html"),
]
hit = None
for name, url, kind in CAND:
    t = {"name": name, "url": url}
    try:
        st, raw = fetch(url, headers={"Referer": "https://m.stock.naver.com/"})
        t["http"] = st; t["len"] = len(raw)
        txt = raw.decode("utf-8", "ignore")
        has_inv = any(s in txt for s in ("외국인", "기관", "individual", "foreign", "frgn", "institution", "투자자"))
        t["has_investor_terms"] = has_inv
        if kind == "json":
            try:
                doc = json.loads(raw)
                if isinstance(doc, dict):
                    t["json_keys"] = list(doc)[:12]
                elif isinstance(doc, list) and doc:
                    t["json_list_first_keys"] = list(doc[0])[:12] if isinstance(doc[0], dict) else str(doc[0])[:60]
                t["is_json"] = True
                if has_inv and hit is None:
                    hit = {"name": name, "url": url, "keys": t.get("json_keys") or t.get("json_list_first_keys")}
            except Exception:
                t["is_json"] = False
                t["head"] = txt[:120]
        else:
            t["head"] = txt[:140].replace("\n", " ")
            if has_inv and hit is None:
                hit = {"name": name, "url": url, "kind": "html"}
        print("  %-34s http=%s len=%s inv_terms=%s json=%s" % (name, t.get("http"), t.get("len"), has_inv, t.get("is_json")))
        R["naver"].append(t)
    except Exception as e:
        t["err"] = str(e)[:110]; R["naver"].append(t)
        print("  %-34s ERR %s" % (name, str(e)[:80]))

R["naver_usable"] = hit
R["verdict"] = ("NAVER_USABLE: " + hit["name"]) if hit else "NAVER_ALSO_UNAVAILABLE_from_AWS"
print("\nVERDICT:", R["verdict"])
if hit:
    print("usable endpoint:", json.dumps(hit, ensure_ascii=False))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2752_krx_probe.json", "w") as f:
    json.dump(R, f, indent=1, ensure_ascii=False, default=str)
print("OPS 2752 rev3 COMPLETE — Korea source recon done")
