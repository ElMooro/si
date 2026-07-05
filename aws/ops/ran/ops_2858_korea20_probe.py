"""ops 2858 — find a keyless machine-readable source for Korea 20-day exports."""
import os, json, re, urllib.request
from datetime import datetime, timezone
R={"ops":2858,"ts":datetime.now(timezone.utc).isoformat()}
def _get(url,t=35,decode=True):
    req=urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0","Accept":"*/*"})
    with urllib.request.urlopen(req,timeout=t) as r: b=r.read()
    return b.decode("utf-8","ignore") if decode else b
# 1) data.go.kr KCS port trade API WITHOUT key (does it hard-fail?)
try:
    t=_get("http://apis.data.go.kr/1220000/nitemtrade/getNitemtradeList?strYear=2026&strStartMonth=05&strEndMonth=05")
    R["datago_nokey"]=t[:200]
except Exception as e: R["datago_nokey"]=str(e)[:120]
# 2) KOSIS openapi sample (needs key too?)
try:
    t=_get("https://kosis.kr/openapi/statisticsData.do?method=getList&format=json")
    R["kosis_nokey"]=t[:200]
except Exception as e: R["kosis_nokey"]=str(e)[:120]
# 3) KCS customs.go.kr — is there a trade-stats JSON/CSV endpoint?
for u in ["https://unipass.customs.go.kr/ets/","https://tradedata.go.kr/cts/index.do"]:
    try:
        t=_get(u); R.setdefault("kcs_pages",{})[u]="ok len=%d"%len(t)
    except Exception as e: R.setdefault("kcs_pages",{})[u]=str(e)[:80]
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:2200])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2858_korea20_probe.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2858 COMPLETE")
