import urllib.request, json
UA={"User-Agent":"JustHodl Research raafouis@gmail.com","Accept":"*/*"}
def get(url,t=45):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers=UA),timeout=t) as r: return r.read().decode("utf-8","ignore")
    except Exception as e: return None
MON={"Jan":"01","Feb":"02","Mar":"03","Apr":"04","May":"05","Jun":"06","Jul":"07","Aug":"08","Sep":"09","Oct":"10","Nov":"11","Dec":"12"}
body=get("https://ticdata.treasury.gov/Publish/mfhhis01.txt")
lines=body.split("\n")
# find FIRST header, parse just block 1 with prints
for i,ln in enumerate(lines):
    cells=[c.strip() for c in ln.split("\t")]
    months=[c for c in cells if c in MON]
    if len(months)>=6:
        print("HEADER at line",i,"months=",months)
        yrs=[c.strip() for c in lines[i+1].split("\t")]; years=[c for c in yrs if c.isdigit() and len(c)==4]
        print("year row line",i+1,"years=",years[:14])
        dates=[f"{years[k]}-{MON[months[k]]}" for k in range(min(len(months),len(years)))]
        print("dates=",dates)
        # read next ~5 data rows
        for j in range(i+2,i+8):
            rc=[c.strip().strip('"') for c in lines[j].split("\t")]
            print("  row:",rc[0][:20],"vals=",rc[1:4])
        break
print("\n=== FRED MTS clean series check ===")
F="2f057499936072679d8843d7fce99989"
for nm,sid in {"deficit MTSDS":"MTSDS133FMS","receipts MTSR":"MTSR133FMS","outlays MTSO":"MTSO133FMS"}.items():
    b=get(f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}&api_key={F}&file_type=json&sort_order=desc&limit=2")
    try:
        o=json.loads(b)["observations"]; print(f"  {nm:14} {sid:12} -> {o[0]['value']} ({o[0]['date']}), {o[1]['value']} ({o[1]['date']})")
    except: print(f"  {nm:14} {sid:12} -> FAIL")
