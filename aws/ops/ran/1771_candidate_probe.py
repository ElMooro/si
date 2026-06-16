import urllib.request
def one(fk,t=30):
    url="https://data-api.ecb.europa.eu/service/data/"+fk+"?format=csvdata&lastNObservations=1"
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"JustHodl raafouis@gmail.com"}),timeout=t) as r:
            lines=r.read().decode("utf-8","ignore").splitlines()
            if len(lines)<2: return "EMPTY","",""
            h={x:i for i,x in enumerate(lines[0].split(","))}; c=lines[1].split(",")
            return (c[h.get("TITLE",99)][:50] if "TITLE" in h and len(c)>h["TITLE"] else ""), (c[h.get("TIME_PERIOD",5)] if len(c)>h.get("TIME_PERIOD",5) else ""), (c[h.get("OBS_VALUE",6)] if len(c)>h.get("OBS_VALUE",6) else "")
    except Exception as e: return f"ERR{getattr(e,'code',type(e).__name__)}","",""
C={
 # INFLATION breakdown + market expectations
 "HICP core (xEF)":"ICP/M.U2.N.XEF000.4.ANR",
 "HICP services":"ICP/M.U2.N.SERV00.4.ANR",
 "HICP energy":"ICP/M.U2.N.NRGY00.4.ANR",
 "5y5y infl swap":"FM/B.U2.EUR.RT.MM.EURIRS5X5Y.HSTA",
 "5y5y infl ILS":"FM/M.U2.EUR.RT.IL.EUR5Y5Y_R.HSTA",
 # PRODUCER PRICES (CPI lead)
 "PPI industry":"STS/M.I9.N.PRIN.NS0020.3.000",
 "PPI industry Y":"STS/M.I9.Y.PRIN.NS0020.3.000",
 # MONEY (M1 narrow = cycle lead)
 "M1 growth":"BSI/M.U2.Y.V.M10.X.I.U2.2300.Z01.A",
 "M1 idx":"BSI/M.U2.Y.V.M10.X.1.U2.2300.Z01.A",
 # GDP
 "GDP YoY":"MNA/Q.Y.I9.W2.S1.S1.B.B1GQ._Z._Z._Z.EUR.LR.GY",
 # SURVEYS
 "Consumer confidence":"RTD/M.S0.S.Y_BCS_CSMCI.LEVEL",
 "Capacity util":"RTD/Q.S0.S.Y_BCS_CAPUT.LEVEL",
 # EXTERNAL
 "Current account":"BP6/M.N.I9.W1.S1.S1.T.B.CA._Z._Z._Z.EUR._T._X.N",
 "REER (broad)":"EXR/M.E5.EUR.ERC0.A",
 "EUR/CNY":"EXR/D.CNY.EUR.SP00.A",
 "EUR/JPY":"EXR/D.JPY.EUR.SP00.A",
 # BANK LENDING SURVEY + bank rates
 "Bank rate NFC (MIR)":"MIR/M.U2.B.A2A.A.R.A.2240.EUR.N",
 # APP/PEPP holdings
 "APP holdings":"ILM/W.U2.C.A050200.U2.EUR",
}
for nm,fk in C.items():
    tt=one(fk); print(f"  {nm:22} {fk:46} -> {tt[2]} ({tt[1]}) {tt[0]}")
