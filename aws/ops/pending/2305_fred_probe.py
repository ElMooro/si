import urllib.request, urllib.parse, json
KEY="dded5f4ccba98e2bce4f3b6e6b6f9d3a"  # placeholder; replaced below
import re
src=open("aws/lambdas/justhodl-bottleneck-boom/source/lambda_function.py").read()
m=re.search(r'FRED_KEY\s*=\s*["\']([^"\']+)["\']', src)
if m: KEY=m.group(1)
print("using FRED key suffix:", KEY[-4:])
cands={
 "MACHINERY_333":["A33SUO","A33SNO","A33SVS"],
 "ELECTRICAL_335":["A35SUO","A35SNO","A35SVS"],
 "TRANSPORT_336":["A36SUO","A36SNO","A36SVS"],
 "DEFENSE_CAPGOODS":["ADXTUO","ADXTNO","ADXTVS"],
 "DEFENSE_ALT":["ADXDUO","ADXDNO","ADXDVS"],
}
def probe(sid):
    u="https://api.stlouisfed.org/fred/series/observations?"+urllib.parse.urlencode({"series_id":sid,"api_key":KEY,"file_type":"json","sort_order":"desc","limit":2})
    try:
        j=json.load(urllib.request.urlopen(u,timeout=15))
        obs=j.get("observations") or []
        if obs: return f"OK last={obs[0]['date']} val={obs[0]['value']}"
        return "EMPTY"
    except Exception as e:
        return "FAIL "+str(e)[:50]
for g,ids in cands.items():
    print(f"\n{g}:")
    for sid in ids:
        print(f"  {sid}: {probe(sid)}")
print("DONE 2305")
