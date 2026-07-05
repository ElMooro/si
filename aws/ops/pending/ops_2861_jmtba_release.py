"""ops 2861 — fetch JMTBA statistics index + latest release to extract order data format."""
import os, json, re, urllib.request
from datetime import datetime, timezone
R={"ops":2861,"ts":datetime.now(timezone.utc).isoformat()}
def _get(url,t=35):
    req=urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0","Accept":"*/*"})
    with urllib.request.urlopen(req,timeout=t) as r: return r.read().decode("utf-8","ignore")
idx=_get("https://www.jmtba.or.jp/statistics/")
# find all statistics release links + their numbers
rel=sorted(set(re.findall(r'/statistics/(statistics-\d+)/',idx)))
R["release_count"]=len(rel); R["latest_releases"]=rel[-6:]
# fetch the highest-numbered release
if rel:
    nums=sorted(int(x.split('-')[1]) for x in rel)
    latest="statistics-%d"%nums[-1]
    R["fetched"]=latest
    try:
        h=_get("https://www.jmtba.or.jp/statistics/%s/"%latest)
        R["files"]=re.findall(r'href="([^"]+\.(?:xls|xlsx|csv|pdf))"',h,re.I)[:8]
        R["title"]=(re.search(r'<title>([^<]+)</title>',h) or [None,""])[1][:80]
        R["yen_nums"]=re.findall(r'([0-9][0-9,\.]+)\s*(?:億円|百万円)',h)[:12]
        R["order_context"]=[m[:70] for m in re.findall(r'.{0,20}(?:受注総額|内需|外需|前年同月比).{0,30}',h)][:8]
        R["date_context"]=re.findall(r'20\d\d年\d+月',h)[:4]
    except Exception as e: R["release_err"]=str(e)[:100]
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:2600])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2861_jmtba_release.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2861 COMPLETE")
