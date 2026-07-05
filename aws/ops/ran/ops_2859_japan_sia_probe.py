"""ops 2859 — probe Japan JMTBA machine-tool orders + SIA semiconductor billings for keyless machine-readable data."""
import os, json, re, urllib.request
from datetime import datetime, timezone
R={"ops":2859,"ts":datetime.now(timezone.utc).isoformat()}
def _get(url,t=30):
    req=urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0","Accept":"*/*"})
    with urllib.request.urlopen(req,timeout=t) as r: return r.read().decode("utf-8","ignore")
# JMTBA data pages
for u in ["https://www.jmtba.or.jp/machine/data","https://www.jmtba.or.jp/english/machine/data",
          "https://www.jmtba.or.jp/machine/statistics"]:
    try:
        h=_get(u)
        xls=re.findall(r'href="([^"]+\.(?:xls|xlsx|csv|pdf))"',h,re.I)
        R.setdefault("jmtba",{})[u]={"len":len(h),"files":xls[:8],"has_juchu":"受注" in h or "order" in h.lower()}
    except Exception as e: R.setdefault("jmtba",{})[u]=str(e)[:80]
# SIA global semiconductor sales
for u in ["https://www.semiconductors.org/global-semiconductor-sales-data/",
          "https://www.semiconductors.org/category/news/latest-news/"]:
    try:
        h=_get(u)
        # look for $ figures + YoY mentions + data files
        files=re.findall(r'href="([^"]+\.(?:xls|xlsx|csv))"',h,re.I)
        R.setdefault("sia",{})[u]={"len":len(h),"files":files[:6],"billion_mentions":len(re.findall(r'\$[0-9]+\.[0-9]+ billion',h))}
    except Exception as e: R.setdefault("sia",{})[u]=str(e)[:80]
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:2600])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2859_japan_sia_probe.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2859 COMPLETE")
