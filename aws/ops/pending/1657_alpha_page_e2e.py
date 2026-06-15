import json, urllib.request
d=json.loads(urllib.request.urlopen(urllib.request.Request("https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/data/alpha-scoreboard-research.json",headers={"User-Agent":"x"}),timeout=20).read())
bt=d.get("by_ticker",{})
print("FEED: tickers",len(bt),"| theses",sum(1 for v in bt.values() if v.get("thesis")),"| scorecards",sum(1 for v in bt.values() if v.get("score_bull") is not None),"| financials",sum(1 for v in bt.values() if v.get("financials")))
print("  track_record:",json.dumps(d.get("track_record")),"| concentration:",json.dumps(d.get("concentration")))
p=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/alpha-scoreboard.html",headers={"User-Agent":"Mozilla/5.0"}),timeout=20).read().decode("utf-8","ignore")
checks={"research feed wired":"alpha-scoreboard-research.json" in p,"drawer fn":"function drawer(sym)" in p,"renderTrust":"renderTrust" in p,
 "row clickable":"tr.stk" in p,"scorecard css":"scorecard" in p,"explainer":"How to read this" in p,"trustbar":'id="trustbar"' in p}
print("PAGE live:")
for k,v in checks.items(): print(f"  {'OK' if v else 'MISS'}  {k}")
