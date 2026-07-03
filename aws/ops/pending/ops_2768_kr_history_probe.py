"""ops 2768 — Korea deep-history source recon (memory foreign flow ~90d).
Naver mobile trend caps ~10 rows; KRX blocks AWS. Probe: (A) mobile trend with
pagination params; (B) desktop finance.naver.com frgn.naver paginated HTML
(EUC-KR) for row count + raw sample to build parser. Read-only.
Report: 2768_kr_history_probe.json.
"""
import os, re, json, urllib.request
from datetime import datetime, timezone
R = {"ops": 2768, "ts": datetime.now(timezone.utc).isoformat(), "mobile": [], "desktop": []}
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/121.0 Safari/537.36")

def fetch(url, timeout=20, ref="https://finance.naver.com/"):
    req = urllib.request.Request(url, headers={"User-Agent": UA, "Referer": ref, "Accept": "*/*"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()

print("== A) mobile trend pagination ==")
for url in [
    "https://m.stock.naver.com/api/stock/005930/trend",
    "https://m.stock.naver.com/api/stock/005930/trend?pageSize=90",
    "https://m.stock.naver.com/api/stock/005930/trend?page=2&pageSize=30",
    "https://m.stock.naver.com/api/stock/005930/trend?count=90",
]:
    t = {"url": url}
    try:
        raw = fetch(url, ref="https://m.stock.naver.com/")
        doc = json.loads(raw)
        rows = doc if isinstance(doc, list) else (doc.get("result") or doc.get("trends") or [])
        t["rows"] = len(rows)
        if rows:
            t["first_date"] = rows[0].get("bizdate")
            t["last_date"] = rows[-1].get("bizdate")
        print("  rows=%s span=%s..%s  %s" % (t.get("rows"), t.get("first_date"), t.get("last_date"), url[-40:]))
    except Exception as e:
        t["err"] = str(e)[:80]; print("  ERR %s  %s" % (str(e)[:50], url[-40:]))
    R["mobile"].append(t)

print("\n== B) desktop frgn.naver paginated HTML (EUC-KR) ==")
for page in (1, 4):
    t = {"page": page}
    try:
        raw = fetch("https://finance.naver.com/item/frgn.naver?code=005930&page=%d" % page)
        html = raw.decode("euc-kr", "ignore")
        # count data rows: dates like 2026.07.03
        dates = re.findall(r"\d{4}\.\d{2}\.\d{2}", html)
        t["date_count"] = len(dates)
        t["dates_sample"] = dates[:3] + (["...", dates[-1]] if len(dates) > 3 else [])
        # grab the first data <tr> that contains a date, show its raw cells
        rows_html = re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.S)
        sample_rows = []
        for rh in rows_html:
            if re.search(r"\d{4}\.\d{2}\.\d{2}", rh):
                # extract text of each td/span with numbers + classes
                cells = re.findall(r'<(?:td|span)[^>]*class="([^"]*)"[^>]*>\s*([^<]+?)\s*</(?:td|span)>', rh)
                sample_rows.append([(c[:14], v.strip()[:14]) for c, v in cells if v.strip()])
                if len(sample_rows) >= 2:
                    break
        t["sample_rows"] = sample_rows
        print("  page %d: dates=%d sample dates=%s" % (page, t["date_count"], t["dates_sample"]))
        for sr in sample_rows:
            print("     row:", sr)
    except Exception as e:
        t["err"] = str(e)[:100]; print("  page %d ERR %s" % (page, str(e)[:70]))
    R["desktop"].append(t)

R["verdict"] = {
    "mobile_max_rows": max((m.get("rows", 0) for m in R["mobile"]), default=0),
    "desktop_rows_per_page": R["desktop"][0].get("date_count") if R["desktop"] else None,
}
print("\nVERDICT:", json.dumps(R["verdict"]))
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/2768_kr_history_probe.json", "w"), indent=1, ensure_ascii=False, default=str)
print("OPS 2768 COMPLETE — Korea deep-history recon done")
