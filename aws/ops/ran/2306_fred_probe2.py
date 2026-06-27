import urllib.request, urllib.parse, json
KEY="2f057499936072679d8843d7fce99989"   # correct FRED key (env fallback in the engine)
print("FRED key suffix:", KEY[-4:])
cands={
 "MACHINERY_333":["A33SUO","A33SNO","A33SVS"],
 "ELECTRICAL_335":["A35SUO","A35SNO","A35SVS"],
 "TRANSPORT_336":["A36SUO","A36SNO","A36SVS"],
 "DEFENSE_CAPGOODS":["ADXTUO","ADXTNO","ADXTVS"],
 "DEFENSE_ALT":["ADXDUO","ADXDNO","ADXDVS"],
 "FABMETAL_332":["A32SUO","A32SNO","A32SVS"],
}
def probe(sid):
    u="https://api.stlouisfed.org/fred/series/observations?"+urllib.parse.urlencode({"series_id":sid,"api_key":KEY,"file_type":"json","sort_order":"desc","limit":2})
    try:
        j=json.load(urllib.request.urlopen(u,timeout=15))
        obs=j.get("observations") or []
        if obs: return f"OK last={obs[0]['date']} val={obs[0]['value']}"
        return "EMPTY"
    except Exception as e:
        return "FAIL "+str(e)[:60]
for g,ids in cands.items():
    print(f"\n{g}:")
    for sid in ids:
        print(f"  {sid}: {probe(sid)}")
print("DONE 2306")
