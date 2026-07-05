"""ops 2860 — dig into JMTBA statistics page to find machine-readable machine-tool order data."""
import os, json, re, urllib.request
from datetime import datetime, timezone
R={"ops":2860,"ts":datetime.now(timezone.utc).isoformat()}
def _get(url,t=35):
    req=urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0","Accept":"*/*"})
    with urllib.request.urlopen(req,timeout=t) as r: return r.read().decode("utf-8","ignore")
h=_get("https://www.jmtba.or.jp/machine/statistics")
# all links on the page
links=re.findall(r'href="([^"]+)"',h)
R["all_data_links"]=[l for l in links if re.search(r'\.(xls|xlsx|csv|pdf)$',l,re.I) or 'statistic' in l.lower() or '受注' in l or 'juchu' in l.lower() or 'order' in l.lower()][:20]
R["link_sample"]=[l for l in links if l.startswith('/') or l.startswith('http')][:25]
# look for month + order value patterns in the HTML tables (億円 = 100M yen)
R["juchu_context"]=[m[:80] for m in re.findall(r'.{0,30}(?:総額|内需|外需|受注額).{0,40}',h)][:8]
# any inline table numbers
R["nums_near_yen"]=re.findall(r'([0-9,]+)\s*億円',h)[:10]
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:2600])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2860_jmtba_dig.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2860 COMPLETE")
