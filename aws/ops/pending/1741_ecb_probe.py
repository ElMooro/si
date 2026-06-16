import urllib.request, urllib.error
BASE="https://data-api.ecb.europa.eu/service/data/"
def test(flow_key):
    url=BASE+flow_key+"?format=csvdata&lastNObservations=2"
    try:
        req=urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            body=r.read().decode("utf-8","ignore"); rows=body.count("\n")
            return f"200 rows~{rows}"
    except urllib.error.HTTPError as e: return f"{e.code}"
    except Exception as e: return f"ERR {str(e)[:40]}"

print("=== CISS sub-indices — OLD convention (ecb-history, 404-ing) vs NEW (ecb-auto-updater) ===")
old=["CISS/D.U2.Z0Z.4F.EC.SS_CIN.IDX","CISS/D.U2.Z0Z.4F.EC.SS_FI.CON","CISS/D.U2.Z0Z.4F.EC.SS_BO.CON",
     "CISS/D.U2.Z0Z.4F.EC.SS_FX.CON","CISS/D.U2.Z0Z.4F.EC.SS_EQ.CON","CISS/D.U2.Z0Z.4F.EC.SS_MM.CON"]
new=["CISS/D.U2.Z0Z.4F.EC.SOV_EW.IDX","CISS/D.U2.Z0Z.4F.EC.FII_CI.IDX","CISS/D.U2.Z0Z.4F.EC.BON_CI.IDX",
     "CISS/D.U2.Z0Z.4F.EC.FX_CI.IDX","CISS/D.U2.Z0Z.4F.EC.EQU_CI.IDX","CISS/D.U2.Z0Z.4F.EC.MMS_CI.IDX"]
labels=["composite/CIN","financial-interm","bond","fx","equity","money-mkt"]
for lab,o,n in zip(labels,old,new):
    print(f"  {lab:16} OLD {o.split('.EC.')[1]:14} -> {test(o):14} | NEW {n.split('.EC.')[1]:12} -> {test(n)}")

print("\n=== main CISS composite candidates ===")
for k in ["CISS/D.U2.Z0Z.4F.EC.SS_CIN.IDX","CISS/D.U2.Z0Z.4F.EC.SOV_GDPW.IDX","CISS/D.U2.Z0Z.4F.EC.SS_CI.IDX"]:
    print(f"  {k.split('/')[1]:30} -> {test(k)}")

print("\n=== ILM series (ecb-derived / ecb-history) ===")
for k in ["ILM/W.U2.C.A030000.U2.Z06","ILM/W.U2.C.L060000.U4.EUR","ILM/W.U2.C.A050000.U2.EUR",
          "ILM/W.U2.C.L010000.U2.EUR","ILM/W.U2.C.L080000.U4.EUR","ILM/W.U2.C.A050100.U2.EUR",
          "ILM/W.U2.C.A050200.U2.EUR","ILM/W.U2.C.A050500.U2.EUR"]:
    print(f"  {k.split('/')[1]:24} -> {test(k)}")

print("\n=== ecb-auto-updater: the SovCISS section + host (DNS-failing) ===")
import re
src=open("aws/lambdas/ecb-auto-updater/source/lambda_function.py").read()
for ln in src.splitlines():
    if any(x in ln for x in ["SovCISS","Sov","http","host","BASE","url=","wsrest","def fetch","service/data"]):
        print("   "+ln.strip()[:110])
