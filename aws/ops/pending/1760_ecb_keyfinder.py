import urllib.request
def probe(flow_key,t=40):
    url="https://data-api.ecb.europa.eu/service/data/"+flow_key+"?format=csvdata&lastNObservations=2"
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"JustHodl raafouis@gmail.com"}),timeout=t) as r:
            body=r.read().decode("utf-8","ignore"); lines=body.splitlines()
            if len(lines)<2: return "EMPTY"
            h={x:i for i,x in enumerate(lines[0].split(","))}
            last=lines[-1].split(",")
            tp=last[h.get("TIME_PERIOD",5)] if "TIME_PERIOD" in h else "?"
            ov=last[h.get("OBS_VALUE",6)] if "OBS_VALUE" in h else "?"
            return f"OK n={len(lines)-1} latest={tp} val={ov}"
    except Exception as e: return f"{getattr(e,'code',type(e).__name__)}"

cands={
 # --- UNEMPLOYMENT (euro area, monthly rate) ---
 "UNEMP STS I8":"STS/M.I8.S.UNEH.RTT000.4.000",
 "UNEMP STS I9":"STS/M.I9.S.UNEH.RTT000.4.000",
 "UNEMP LFSI I9":"LFSI/M.I9.S.UNEHRT.TOTAL0.15_74.T",
 "UNEMP LFSI I8":"LFSI/M.I8.S.UNEHRT.TOTAL0.15_74.T",
 # --- INDUSTRIAL PRODUCTION (euro area, monthly index) ---
 "INDPROD STS I8":"STS/M.I8.Y.PROD.NS0020.4.000",
 "INDPROD STS I9":"STS/M.I9.Y.PROD.NS0020.4.000",
 "INDPROD STS total":"STS/M.I9.Y.PROD.NS0010.4.000",
 # --- MANUFACTURING / industrial confidence (BCS / ESI components) ---
 "IND CONF BCS":"BCS/M.I9.TOT.COF.BS.ZS",
 "MANUF PROD STS":"STS/M.I9.Y.PROD.NS0021.4.000",
 # --- DOLLAR SHORTAGE / FX swap / USD ops ---
 "EURUSD ref":"EXR/D.USD.EUR.SP00.A",
 "USD ops ILM":"ILM/W.U2.C.A050100.U2.EUR",
 "USD ops ILM2":"ILM/W.U2.C.LT00.Z5.EUR",
 "FM eonia":"FM/D.U2.EUR.4F.MM.EONIA.HSTA",
 "ESTR":"EST/B.EU000A2X2A25.WT",
 "FX swap EURUSD":"FM/D.U2.USD.DS.EURUSD.HSTA",
}
for nm,fk in cands.items():
    print(f"  {nm:22} {fk:42} -> {probe(fk)}")
# also list a few dataflows to confirm names exist
print("\n--- dataflow existence check ---")
for fl in ["STS","LFSI","BCS","ILM","EST","EXR","FM"]:
    try:
        u="https://data-api.ecb.europa.eu/service/dataflow/ECB/"+fl
        with urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"JustHodl raafouis@gmail.com"}),timeout=25) as r:
            print(f"  {fl}: dataflow exists (http {r.status})")
    except Exception as e: print(f"  {fl}: {getattr(e,'code',type(e).__name__)}")
