import urllib.request
def title(flow_key,t=40):
    # SDMX-CSV with labels: use 'csvdata' + detail; TITLE columns appear for many flows
    url="https://data-api.ecb.europa.eu/service/data/"+flow_key+"?format=csvdata&lastNObservations=1"
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"JustHodl raafouis@gmail.com","Accept":"text/csv"}),timeout=t) as r:
            body=r.read().decode("utf-8","ignore"); lines=body.splitlines()
            hdr=lines[0].split(","); row=lines[1].split(",") if len(lines)>1 else []
            d=dict(zip(hdr,row))
            # title-ish fields
            tit=d.get("TITLE") or d.get("TITLE_COMPL") or ""
            return tit[:90], d.get("UNIT") or d.get("UNIT_MEASURE") or "", d.get("TIME_PERIOD"), d.get("OBS_VALUE")
    except Exception as e: return f"ERR {getattr(e,'code',type(e).__name__)}","","",""
for nm,fk in {
 "unemployment":"LFSI/M.I9.S.UNEHRT.TOTAL0.15_74.T",
 "indprod NS0010":"STS/M.I9.Y.PROD.NS0010.4.000",
 "indprod NS0020":"STS/M.I9.Y.PROD.NS0020.4.000",
 "indprod NS0021":"STS/M.I9.Y.PROD.NS0021.4.000",
 "USD ops A050100":"ILM/W.U2.C.A050100.U2.EUR",
 "EURUSD":"EXR/D.USD.EUR.SP00.A",
 "ESTR":"EST/B.EU000A2X2A25.WT",
}.items():
    tt=title(fk); print(f"  {nm:18} | title='{tt[0]}' unit='{tt[1]}' latest={tt[2]} val={tt[3]}")
# try to find more USD/funding series in ILM around A0501xx
print("\n--- ILM USD-ops neighborhood probe ---")
for code in ["A050100","A050200","A050000"]:
    fk=f"ILM/W.U2.C.{code}.U2.EUR"; tt=title(fk); print(f"  {code}: title='{tt[0]}' latest={tt[2]} val={tt[3]}")
