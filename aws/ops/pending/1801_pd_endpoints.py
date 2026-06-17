import urllib.request, json
UA={"User-Agent":"JustHodl Research raafouis@gmail.com"}
def get(url,t=60):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers=UA),timeout=t) as r: return r.read().decode("utf-8","ignore")
    except Exception as e: return "ERR:"+str(getattr(e,'code',e))
NY="https://markets.newyorkfed.org/api/pd"
# 1) raw list item field names
lst=json.loads(get(NY+"/list/timeseries.json"))["pd"]["timeseries"]
print("list item keys:",list(lst[0].keys()))
print("sample item:",json.dumps(lst[0]))
# build label map from whatever text field exists
tf=[k for k in lst[0].keys() if k!='keyid'][0]
fails=[x for x in lst if 'fail' in json.dumps(x).lower()]
print("fail series count:",len(fails),"| text field guess:",tf)
# 2) find aggregate (no-tenor) treasury/corp/agency fails keyids
print("\n-- ALL distinct fail keyids --")
fk=sorted(x['keyid'] for x in fails)
print(" ", " ".join(fk))
# 3) try endpoints for latest-all
for ep in ["/latest.json","/get/all/latest.json","/get/all/lastTwoWeeks.json"]:
    r=get(NY+ep); ok = (not r.startswith("ERR")) if isinstance(r,str) else False
    print(f"\nendpoint {ep}: {'OK len='+str(len(r)) if ok else r[:40]}")
    if ok:
        try:
            jj=json.loads(r); rows=jj["pd"]["timeseries"]; print("  rows:",len(rows),"asof:",rows[0].get("asofdate"),"sample:",json.dumps(rows[0])[:120])
        except Exception as e: print("  parse:",e)
# 4) probe a few candidate live fails series individually (latest non-*)
print("\n-- latest non-* value per candidate fails key --")
for k in fk:
    b=get(NY+f"/get/{k}.json")
    try:
        ts=json.loads(b)["pd"]["timeseries"]
        live=[(t['asofdate'],t['value']) for t in ts if t.get('value') not in ('*','',None)]
        print(f"  {k:24} live_obs={len(live):4}  latest={live[-1] if live else 'ALL MASKED'}")
    except: print(f"  {k:24} ERR")
