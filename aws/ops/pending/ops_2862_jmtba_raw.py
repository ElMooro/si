"""ops 2862 — extract the actual JMTBA order numbers + any Excel link from /machine/statistics."""
import os, json, re, urllib.request
from datetime import datetime, timezone
R={"ops":2862,"ts":datetime.now(timezone.utc).isoformat()}
def _get(url,t=35):
    req=urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0","Accept":"*/*"})
    with urllib.request.urlopen(req,timeout=t) as r: return r.read().decode("utf-8","ignore")
h=_get("https://www.jmtba.or.jp/machine/statistics")
# strip tags, get text, find the order section
text=re.sub(r'<[^>]+>',' ',h); text=re.sub(r'\s+',' ',text)
# locate 受注 (orders) region
i=text.find("受注")
R["text_around_juchu"]=text[max(0,i-40):i+400] if i>=0 else "受注 not found"
# any excel/pdf anywhere (incl wp-content/uploads)
R["data_files"]=sorted(set(re.findall(r'https?://[^"\' ]+\.(?:xlsx?|csv|pdf)',h,re.I)))[:10]
# wp-content upload links (JMTBA posts attach xls there)
R["uploads"]=sorted(set(re.findall(r'https?://[^"\' ]*wp-content/uploads[^"\' ]+',h)))[:10]
# numbers with 億円 or 百万円 in the text
R["yen_values"]=re.findall(r'([0-9][0-9,\.]+)\s*(億円|百万円|million)',text)[:12]
R["ym"]=re.findall(r'20\d\d年\s*\d+月',text)[:5]
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:2600])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2862_jmtba_raw.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2862 COMPLETE")
