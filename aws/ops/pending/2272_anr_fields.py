import json, urllib.request
FMP="wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
def fmp(c): 
    u=f"https://financialmodelingprep.com/stable/{c}{'&' if '?' in c else '?'}apikey={FMP}"
    try: return json.loads(urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"jh"}),timeout=25).read())
    except Exception as e: return {"_err":str(e)[:60]}
for c in ["grades-consensus?symbol=LDOS","price-target-summary?symbol=LDOS"]:
    r=fmp(c); print(c.split('?')[0],"->",json.dumps(r[0] if isinstance(r,list) and r else r)[:300])
print("\ngrades (recent actions) LDOS:")
r=fmp("grades?symbol=LDOS&limit=6")
for x in (r[:6] if isinstance(r,list) else []): print("  ",json.dumps(x)[:160])
print("\ngrades-historical (ratings trend) LDOS:")
r=fmp("grades-historical?symbol=LDOS&limit=8")
for x in (r[:6] if isinstance(r,list) else []): print("  ",json.dumps(x)[:200])
print("DONE 2272")
