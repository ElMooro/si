import json, urllib.request
from datetime import datetime, timezone
def g(url):
    req=urllib.request.Request(url,headers={"User-Agent":"justhodl/1.0"})
    with urllib.request.urlopen(req,timeout=40) as r: return json.loads(r.read())
B="https://api.blockchain.info/charts/"
for chart in ["hash-rate","difficulty","miners-revenue","market-price","total-bitcoins"]:
    try:
        d=g(B+chart+"?timespan=3years&format=json&sampled=false")
        v=d.get("values",[])
        if not v: print(chart,"-> EMPTY"); continue
        first=datetime.fromtimestamp(v[0]["x"],tz=timezone.utc).date().isoformat()
        last=datetime.fromtimestamp(v[-1]["x"],tz=timezone.utc).date().isoformat()
        # cadence: median gap in days between first few points
        gaps=[(v[i+1]["x"]-v[i]["x"])/86400 for i in range(min(5,len(v)-1))]
        print(f"{chart:16s} unit={d.get('unit'):10s} n={len(v):5d} {first}->{last} gap~{round(sum(gaps)/len(gaps),1)}d  last_y={v[-1]['y']}")
    except Exception as e:
        print(chart,"ERR",str(e)[:80])
print("DONE 2386")
