import json, urllib.request
d=json.loads(urllib.request.urlopen(urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/data/bottleneck-boom-research.json",headers={"User-Agent":"x"}),timeout=20).read())
print("FEED:")
print("  #1 track_record:",json.dumps(d.get("track_record")))
print("  #2 concentration:",json.dumps(d.get("concentration")))
print("  #3 changes:",json.dumps(d.get("changes")))
print("  #4 pressure_pctiles:",json.dumps(d.get("pressure_pctiles")))
p=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/bottleneck-boom.html",headers={"User-Agent":"Mozilla/5.0"}),timeout=20).read().decode("utf-8","ignore")
checks={"track-record bar":"Engine track record vs SPY" in p,"concentration banner":"Concentration check" in p,
 "what-changed":"What changed since" in p,"percentile (cards)":"vs 10y history" in p,
 "percentile (drawer)":"th pctile" in p,"renderTrust":"renderTrust" in p}
print("\nPAGE live:")
for k,v in checks.items(): print(f"  {'OK' if v else 'MISS'}  {k}")
