import json, urllib.request
d=json.loads(urllib.request.urlopen(urllib.request.Request("https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/data/opportunities-research.json",headers={"User-Agent":"x"}),timeout=20).read())
bt=d.get("by_ticker",{})
print("FEED: tickers",len(bt),"| theses",sum(1 for v in bt.values() if v.get("thesis")),"| scorecards",sum(1 for v in bt.values() if v.get("score_bull") is not None),"| financials",sum(1 for v in bt.values() if v.get("financials")))
p=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/opportunities.html",headers={"User-Agent":"Mozilla/5.0"}),timeout=20).read().decode("utf-8","ignore")
checks={"research feed wired":"opportunities-research.json" in p,"deepDrawer fn":"function deepDrawer" in p,
 "drawer call in card":"${deepDrawer(r.ticker)}" in p,"details.deep css":"details.deep" in p,
 "scorecard css":".dscore" in p,"RESEARCH global":"RESEARCH={}" in p or "RESEARCH ={}" in p or "BYSYM={}, RESEARCH" in p}
print("PAGE live:")
for k,v in checks.items(): print(f"  {'OK' if v else 'MISS'}  {k}")
