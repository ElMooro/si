"""ops 2847 — inspect the actual MOEA CSVs (export orders + semiconductor production)."""
import os, json, urllib.parse, urllib.request
from datetime import datetime, timezone
R={"ops":2847,"ts":datetime.now(timezone.utc).isoformat()}
def _get(url,t=60):
    p=urllib.parse.urlsplit(url)
    url2=urllib.parse.urlunsplit((p.scheme,p.netloc,urllib.parse.quote(p.path),p.query,p.fragment))
    req=urllib.request.Request(url2,headers={"User-Agent":"Mozilla/5.0","Accept":"*/*"})
    with urllib.request.urlopen(req,timeout=t) as r:
        raw=r.read()
    for enc in ("utf-8-sig","utf-8","big5","cp950"):
        try: return raw.decode(enc)
        except Exception: pass
    return raw.decode("utf-8","ignore")
TARGETS={
 "export_orders_by_region":"https://service.moea.gov.tw/EE520/opendata/經濟部統計處_外銷訂單_按地區分.csv",
 "ip_electronic_components":"https://service.moea.gov.tw/EE520/opendata/經濟部統計處_工業生產_電子零組件業.csv",
 "ip_total":"https://service.moea.gov.tw/EE520/opendata/d.csv",
 "mfg_sales_index":"https://service.moea.gov.tw/EE520/opendata/經濟部統計處_製造業銷售量指數.csv",
}
for k,u in TARGETS.items():
    try:
        txt=_get(u); lines=txt.splitlines()
        R[k]={"url_ok":True,"n_lines":len(lines),
              "header":lines[0][:260] if lines else None,
              "row2":lines[1][:260] if len(lines)>1 else None,
              "row3":lines[2][:260] if len(lines)>2 else None,
              "last1":lines[-1][:260] if lines else None,
              "last2":lines[-2][:260] if len(lines)>1 else None}
    except Exception as e:
        R[k]={"err":str(e)[:120]}
print(json.dumps(R,ensure_ascii=False,indent=1)[:3800])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2847_taiwan_inspect.json","w"),ensure_ascii=False,indent=1)
print("OPS 2847 COMPLETE")
