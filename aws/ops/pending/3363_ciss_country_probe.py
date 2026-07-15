import json, urllib.request, boto3
from ops_report import report
def probe(url):
    try:
        req=urllib.request.Request(url,headers={"User-Agent":"jh/1.0","Accept":"text/csv, */*"})
        with urllib.request.urlopen(req,timeout=20) as r:
            body=r.read().decode()[:200]; return f"HTTP {r.status}: {body[:120]}"
    except Exception as e: return f"{type(e).__name__}: {str(e)[:100]}"
with report("3363_ciss_country_probe") as r:
    base="https://data-api.ecb.europa.eu/service/data/CISS/"
    tests={
      "china D.CN": base+"D.CN.Z0Z.4F.EC.SS_CI.IDX?format=csvdata&lastNObservations=3",
      "UK D.GB":    base+"D.GB.Z0Z.4F.EC.SS_CI.IDX?format=csvdata&lastNObservations=3",
      "china alt CIN": base+"D.CN.Z0Z.4F.EC.SS_CIN.IDX?format=csvdata&lastNObservations=3",
      "UK alt CIN":    base+"D.GB.Z0Z.4F.EC.SS_CIN.IDX?format=csvdata&lastNObservations=3",
      "china GDPW":  base+"D.CN.Z0Z.4F.EC.SS_GDPW.IDX?format=csvdata&lastNObservations=3",
      # what country keys DO exist? probe the dataflow structure
    }
    for name,url in tests.items():
        r.log(f"  {name}: {probe(url)}")
    # list available CISS series via the data structure
    r.section("available CISS country keys")
    dsd=probe("https://data-api.ecb.europa.eu/service/data/CISS/D..Z0Z.4F.EC.SS_CI.IDX?format=csvdata&lastNObservations=1&detail=serieskeysonly")
    r.log(f"  all SS_CI series: {dsd}")
