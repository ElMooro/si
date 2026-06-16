import urllib.request
def fetch(url,t=55):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"JustHodl raafouis@gmail.com"}),timeout=t) as r:
            return r.read().decode("utf-8","ignore")
    except Exception as e: return f"ERR {getattr(e,'code',type(e).__name__)}"
def rows(body):
    out=[]
    if body.startswith("ERR"): return out,body
    lines=body.splitlines();
    if len(lines)<2: return out,"EMPTY"
    h={x:i for i,x in enumerate(lines[0].split(","))}
    ki=h.get("KEY",0); ti=h.get("TITLE"); tpi=h.get("TIME_PERIOD"); ovi=h.get("OBS_VALUE")
    for ln in lines[1:]:
        c=ln.split(",")
        if len(c)<=ki: continue
        out.append((c[ki], c[ti][:60] if ti is not None and len(c)>ti else "", c[tpi] if tpi and len(c)>tpi else "", c[ovi] if ovi and len(c)>ovi else ""))
    return out,"OK"

print("=== DOLLAR SHORTAGE — Eurosystem FX claims (titles) ===")
for nm,fk in {
 "EA-res FX W A030000 Z06":"ILM/W.U2.C.A030000.U2.Z06",
 "nonEA-res FX W A020000 Z06":"ILM/W.U2.C.A020000.U2.Z06",
 "nonEA-res FX W A020000 U4":"ILM/W.U2.C.A020000.U4.Z06",
}.items():
    r,st=rows(fetch("https://data-api.ecb.europa.eu/service/data/"+fk+"?format=csvdata&lastNObservations=1"))
    print(f"  {nm:26} {fk:30} -> {r[0] if r else st}")
# list ALL ILM weekly items (counterpart U2 currency Z06 = foreign currency) to find both claims lines
print("\n--- ILM weekly foreign-currency items (wildcard) ---")
r,st=rows(fetch("https://data-api.ecb.europa.eu/service/data/ILM/W.U2.C..U2.Z06?format=csvdata&lastNObservations=1"))
print("status",st,"count",len(r))
for k,t,tp,v in r[:20]: print(f"  {k:34} {t} | {tp} {v}")

print("\n=== UNEMPLOYMENT universe (LFSI) ===")
r,st=rows(fetch("https://data-api.ecb.europa.eu/service/data/LFSI/M.I9.S.UNEHRT?format=csvdata&lastNObservations=1"))
print("EA breakdowns status",st,"count",len(r))
for k,t,tp,v in r[:14]: print(f"  {k:40} {t} | {tp} {v}")

print("\n=== INDUSTRIAL/MANUFACTURING universe (STS PROD, EA) ===")
r,st=rows(fetch("https://data-api.ecb.europa.eu/service/data/STS/M.I9.Y.PROD?format=csvdata&lastNObservations=1"))
print("STS PROD status",st,"count",len(r))
for k,t,tp,v in r[:25]: print(f"  {k:38} {t} | {tp} {v}")
