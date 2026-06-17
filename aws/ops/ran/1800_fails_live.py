import urllib.request, json, collections
UA={"User-Agent":"JustHodl Research raafouis@gmail.com"}
def get(url,t=60):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers=UA),timeout=t) as r: return r.read().decode("utf-8","ignore")
    except Exception as e: return None
NY="https://markets.newyorkfed.org/api/pd"
# label lookup
lst=json.loads(get(NY+"/list/timeseries.json"))["pd"]["timeseries"]
lab={x["keyid"]:x.get("label","") for x in lst}
failkeys=set(k for k,v in lab.items() if 'fail' in (v+k).lower())
print("fail series in list:",len(failkeys))
# latest release: all series + values in one shot
L=json.loads(get(NY+"/latest.json"))
rows=L["pd"]["timeseries"]
print("latest release rows:",len(rows),"| asof sample:",rows[0].get("asofdate"))
live=[]; masked=0
for r in rows:
    k=r.get("keyid")
    if k in failkeys:
        v=r.get("value")
        if v in ("*","",None): masked+=1
        else:
            try: live.append((k,float(v),lab.get(k,"")))
            except: pass
print(f"fails: {len(live)} live, {masked} masked")
live.sort(key=lambda x:-x[1])
print("\n=== fails series WITH REAL values at latest release (top 30 by size) ===")
for k,v,l in live[:30]:
    side="DELIVER" if (k.endswith("-TD") or k.endswith("-FDT")) else "RECEIVE" if (k.endswith("-TR") or k.endswith("-FRT")) else "?"
    print(f"  {k:26} {side:8} {v:14,.0f}  {l[:46]}")
print("\ntotal live FTD sum:", f"{sum(v for k,v,l in live if k.endswith('-TD') or k.endswith('-FDT')):,.0f}")
print("total live FTR sum:", f"{sum(v for k,v,l in live if k.endswith('-TR') or k.endswith('-FRT')):,.0f}")
