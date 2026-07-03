"""ops 2755 — structure dump for HK .js + JPX .xls (build parsers from truth).

HK: fetch HKEX data_tab_daily_{date}e.js, show head + context around 'Southbound'.
JP: pip-install xlrd on the runner, download latest JPX stock_val .xls, dump
    sheet name/dims + first ~36 rows so we can locate Foreigners row + Balance col.
KR: dump KOSPI/trend JSON to confirm market-wide field layout.
Read-only recon. Report: aws/ops/reports/2755_struct_dump.json.
"""
import os, io, re, json, subprocess, urllib.request
from datetime import datetime, timezone, timedelta

R = {"ops": 2755, "ts": datetime.now(timezone.utc).isoformat()}
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/121.0 Safari/537.36")
def http(url, timeout=30, headers=None):
    h = {"User-Agent": UA, "Accept": "*/*"}
    if headers: h.update(headers)
    with urllib.request.urlopen(urllib.request.Request(url, headers=h), timeout=timeout) as r:
        return r.read()

print("== KOREA KOSPI/trend layout ==")
try:
    raw = http("https://m.stock.naver.com/api/index/KOSPI/trend", headers={"Referer": "https://m.stock.naver.com/"})
    doc = json.loads(raw)
    R["korea_trend"] = doc if not isinstance(doc, list) else {"list_len": len(doc), "first": doc[0], "last": doc[-1]}
    print(" ", json.dumps(R["korea_trend"], ensure_ascii=False)[:400])
except Exception as e:
    R["korea_trend"] = {"err": str(e)[:120]}; print("  err", str(e)[:100])

print("\n== HONG KONG data_tab_daily .js ==")
hk = None
for back in range(0, 6):
    ymd = (datetime.now(timezone.utc) - timedelta(days=back)).strftime("%Y%m%d")
    try:
        raw = http("https://www.hkex.com.hk/eng/csm/DailyStat/data_tab_daily_%se.js" % ymd)
        txt = raw.decode("utf-8", "ignore")
        if "outhbound" in txt or "{" in txt:
            hk = {"date": ymd, "len": len(txt), "head": txt[:900]}
            ctx = []
            for m in re.finditer(r"[Ss]outhbound", txt):
                ctx.append(txt[max(0, m.start() - 40):m.start() + 220])
            hk["southbound_context"] = ctx[:3]
            break
    except Exception as e:
        continue
R["hongkong_js"] = hk or {"err": "no js reachable"}
if hk:
    print("  date", hk["date"], "len", hk["len"])
    print("  HEAD:", hk["head"][:500])
    for c in hk.get("southbound_context", []):
        print("  SB-CTX:", re.sub(r"\s+", " ", c)[:220])

print("\n== JAPAN JPX xls dump ==")
try:
    subprocess.run(["pip", "install", "-q", "xlrd==2.0.1"], check=False)
    import xlrd
    listing = http("https://www.jpx.co.jp/english/markets/statistics-equities/investor-type/index.html").decode("utf-8", "ignore")
    vals = re.findall(r'href=["\']([^"\']*stock_val[^"\']*\.xls)["\']', listing, re.I)
    url = vals[0]
    if url.startswith("/"): url = "https://www.jpx.co.jp" + url
    R["jpx_file"] = url
    xls = http(url, timeout=40)
    book = xlrd.open_workbook(file_contents=xls)
    sh = book.sheet_by_index(0)
    R["jpx_sheet"] = {"name": sh.name, "nrows": sh.nrows, "ncols": sh.ncols, "url": url}
    print("  file:", url.split("/")[-1], "| sheet:", sh.name, "%dx%d" % (sh.nrows, sh.ncols))
    rows = []
    for i in range(min(sh.nrows, 40)):
        vals_r = [str(sh.cell_value(i, j))[:16] for j in range(min(sh.ncols, 12))]
        rows.append(vals_r)
        joined = " | ".join(v for v in vals_r if v.strip())
        if joined.strip():
            print("  r%02d: %s" % (i, joined[:150]))
    R["jpx_rows"] = rows
except Exception as e:
    R["jpx_sheet"] = {"err": str(e)[:150]}
    print("  JPX err:", str(e)[:130])

os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2755_struct_dump.json", "w") as f:
    json.dump(R, f, indent=1, ensure_ascii=False, default=str)
print("\nOPS 2755 COMPLETE — structures captured")
