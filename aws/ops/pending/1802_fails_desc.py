import urllib.request, json
UA={"User-Agent":"JustHodl Research raafouis@gmail.com"}
def get(url,t=60):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers=UA),timeout=t) as r: return r.read().decode("utf-8","ignore")
    except Exception as e: return None
NY="https://markets.newyorkfed.org/api/pd"
lst=json.loads(get(NY+"/list/timeseries.json"))["pd"]["timeseries"]
desc={x["keyid"]:x.get("description","") for x in lst}
for k in ["PDFTD-USTET","PDFTD-UST","PDFTD-CS","PDFTD-FGM","PDFTD-FGEM","PDFTD-OM","PDFTD-USTETC","PDFTR-USTET","PDFTR-CS","PDFTR-FGM"]:
    print(f"  {k:14} {desc.get(k,'?')[:95]}")
# history span of the headline series
for k in ["PDFTD-USTET","PDFTR-USTET"]:
    ts=json.loads(get(NY+f"/get/{k}.json"))["pd"]["timeseries"]
    live=[(t['asofdate'],float(t['value'])) for t in ts if t.get('value') not in ('*','',None)]
    vals=[v for _,v in live]
    print(f"\n{k}: n={len(live)} {live[0][0]}..{live[-1][0]} | latest={live[-1][1]:,.0f} min={min(vals):,.0f} max={max(vals):,.0f}")
    # show the biggest spikes (top 5 weeks)
    top=sorted(live,key=lambda x:-x[1])[:5]
    print("  biggest weeks:", [(d,f'{v:,.0f}') for d,v in top])
