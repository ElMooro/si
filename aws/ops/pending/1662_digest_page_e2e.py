import json, urllib.request
try:
    h=json.loads(urllib.request.urlopen(urllib.request.Request("https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/data/digest-trends-history.json",headers={"User-Agent":"x"}),timeout=20).read())
    ser=h.get("series",[])
    print("FEED digest-trends-history:",len(ser),"days; latest:",ser[-1]["date"] if ser else "—","fields:",list(ser[-1].keys()) if ser else [])
except Exception as e: print("FEED err",str(e)[:80])
p=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/digest-trends.html",headers={"User-Agent":"Mozilla/5.0"}),timeout=20).read().decode("utf-8","ignore")
checks={"trends section":'id="trends-section"' in p,"renderTrends fn":"function renderTrends" in p,
 "tiles container":'id="trend-tiles"' in p,"sparkline fn":"function _tspark" in p,
 "history fetch":"digest-trends-history.json" in p,"hook wired":"renderTrends(history);" in p,"css":".trend-tile" in p}
print("PAGE live:")
for k,v in checks.items(): print(f"  {'OK' if v else 'MISS'}  {k}")
