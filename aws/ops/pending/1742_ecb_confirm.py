import urllib.request, urllib.error
def test(base, flow_key, params="?format=csvdata&lastNObservations=2"):
    url=base+flow_key+params
    try:
        req=urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            return f"200 rows~{r.read().decode('utf-8','ignore').count(chr(10))}"
    except urllib.error.HTTPError as e: return f"{e.code}"
    except Exception as e: return f"ERR {str(e)[:34]}"
NEW="https://data-api.ecb.europa.eu/service/data/"
print("=== correct CISS bond/equity segment codes ===")
for lab,cands in [("bond",["SS_BM.CON","SS_BM.IDX","SS_BD.CON"]),("equity",["SS_EM.CON","SS_EM.IDX","SS_SM.CON","SS_EQ.IDX"])]:
    for c in cands:
        print(f"  {lab:7} CISS/D.U2.Z0Z.4F.EC.{c:10} -> {test(NEW,'CISS/D.U2.Z0Z.4F.EC.'+c)}")
print("\n=== SovCISS on the NEW host (old host sdw-wsrest is dead) ===")
for k in ["CISS/M.U2.Z0Z.4F.EC.SOVCISS_CI.IDX","CISS/D.U2.Z0Z.4F.EC.SOVCISS_CI.IDX","CISS/M.U2.Z0Z.4F.EC.SS_SOV.IDX"]:
    print(f"  {k.split('/')[1]:34} -> {test(NEW,k)}")
print("\n=== ILM banknotes (L010000) + FX-liab (L080000) variants ===")
for k in ["ILM/W.U2.C.L010000.U2.EUR","ILM/W.U2.C.L020000.U2.EUR","ILM/W.U2.C.L010000.U4.EUR",
          "ILM/W.U2.C.L080000.U4.EUR","ILM/W.U2.C.L070000.U4.EUR","ILM/W.U2.C.A080000.U2.Z06"]:
    print(f"  {k.split('/')[1]:24} -> {test(NEW,k)}")
print("\n=== confirm old host truly dead ===")
print("  sdw-wsrest.ecb.europa.eu ->", test("https://sdw-wsrest.ecb.europa.eu/service/data/","CISS/D.U2.Z0Z.4F.EC.SS_CIN.IDX"))
