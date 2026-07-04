"""ops 2830 — probe BEA T10701 (GDI) + T11200 (corp profits) line descriptions."""
import os, json, urllib.parse, urllib.request
from datetime import datetime, timezone
R={"ops":2830,"ts":datetime.now(timezone.utc).isoformat()}
BEA=os.environ.get("BEA_API_KEY","")
def probe(table,freq="Q"):
    try:
        u="https://apps.bea.gov/api/data?"+urllib.parse.urlencode({"UserID":BEA,"method":"GetData","datasetname":"NIPA","TableName":table,"Frequency":freq,"Year":"2025,2026","ResultFormat":"json"})
        d=json.loads(urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"jh"}),timeout=40).read())
        res=(d.get("BEAAPI",{}) or {}).get("Results",{}) or {}
        data=res.get("Data",[]) or []
        if not data:
            return {"error":res.get("Error") or "no data","note":str(res)[:160]}
        lines=sorted({(row.get("LineNumber"),row.get("LineDescription")) for row in data},key=lambda x:int(x[0]) if str(x[0]).isdigit() else 999)
        return {"n_rows":len(data),"lines":[{"ln":l,"d":dsc} for l,dsc in lines[:18]]}
    except Exception as e:
        return {"exception":repr(e)[:150]}
R["T10701_gdi"]=probe("T10701")
R["T11200_profits"]=probe("T11200")
# also try alternate GDI table names
for t in ("T10705","T10706","T10105"):
    R["alt_"+t]=probe(t)
print(json.dumps(R,indent=1,default=str)[:2600])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2830_bea_probe.json","w"),indent=1,default=str)
print("OPS 2830 COMPLETE")
