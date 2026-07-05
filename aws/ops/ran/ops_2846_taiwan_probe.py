"""ops 2846 — locate Taiwan MOEA export-orders + industrial-production datasets on
data.gov.tw, get machine-readable resource URLs, and inspect the file format."""
import os, json, csv, io, urllib.request
from datetime import datetime, timezone
R={"ops":2846,"ts":datetime.now(timezone.utc).isoformat()}
def _get(url,t=60,raw=False):
    req=urllib.request.Request(url,headers={"User-Agent":"jh/1.0","Accept":"*/*"})
    with urllib.request.urlopen(req,timeout=t) as r: b=r.read()
    return b if raw else b.decode("utf-8","ignore")
# 1) try the per-dataset metadata API for a known keyword search via the front API
def dataset_meta(ds_id):
    for base in ("https://data.gov.tw/api/v2/rest/dataset/","https://data.nat.gov.tw/api/v2/rest/dataset/"):
        try:
            d=json.loads(_get(base+str(ds_id),t=40))
            res=d.get("result") or d
            dist=res.get("distribution") or []
            return {"title":res.get("title"),"freq":res.get("frequency"),
                    "resources":[{"fmt":x.get("resourceFormat"),"url":x.get("resourceDownloadUrl") or x.get("downloadUrl")} for x in dist][:6]}
        except Exception as e:
            last=str(e)[:60]
    return {"err":last}
# 2) pull the full catalog and filter for export orders + industrial production
cat_hits={"外銷訂單":[], "工業生產":[], "製造業銷售":[]}
try:
    cat=_get("https://data.gov.tw/datasets/export/csv",t=90)
    rdr=csv.reader(io.StringIO(cat))
    hdr=next(rdr,None)
    # find id + name + url columns by header text
    def col(names):
        for i,h in enumerate(hdr or []):
            if any(n in (h or "") for n in names): return i
        return None
    ci_id=col(["識別碼","dataset id","id"]); ci_nm=col(["名稱","name"]); ci_url=col(["下載網址","download","url"]); ci_fmt=col(["格式","format"])
    R["catalog_cols"]={"id":ci_id,"name":ci_nm,"url":ci_url,"fmt":ci_fmt,"header_sample":hdr[:8] if hdr else None}
    for row in rdr:
        nm=row[ci_nm] if (ci_nm is not None and len(row)>ci_nm) else ""
        for kw in cat_hits:
            if kw in nm and len(cat_hits[kw])<6:
                cat_hits[kw].append({"id":row[ci_id] if ci_id is not None and len(row)>ci_id else None,
                                     "name":nm[:48],
                                     "fmt":row[ci_fmt] if ci_fmt is not None and len(row)>ci_fmt else None,
                                     "url":(row[ci_url][:120] if ci_url is not None and len(row)>ci_url else None)})
except Exception as e:
    R["catalog_err"]=str(e)[:120]
R["catalog_hits"]=cat_hits
# 3) inspect one export-orders CSV (take first hit with a CSV url)
sample=None
for h in cat_hits.get("外銷訂單",[]):
    u=h.get("url")
    if u and (u.lower().endswith(".csv") or "csv" in (h.get("fmt") or "").lower()):
        sample=h; break
if sample and sample.get("url"):
    try:
        # url column may hold multiple ;-separated; take a csv one
        u=[x for x in sample["url"].split(";") if ".csv" in x.lower() or "csv" in x.lower()]
        u=(u[0] if u else sample["url"].split(";")[0]).strip()
        raw=_get(u,t=60)
        lines=raw.splitlines()
        R["sample_export_orders"]={"dataset":sample["name"],"url":u[:120],
                                   "header":lines[0][:200] if lines else None,
                                   "first_rows":[l[:120] for l in lines[1:4]],
                                   "last_rows":[l[:120] for l in lines[-3:]],"n_lines":len(lines)}
    except Exception as e:
        R["sample_err"]=str(e)[:120]
print(json.dumps(R,ensure_ascii=False,indent=1)[:3500])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2846_taiwan_probe.json","w"),ensure_ascii=False,indent=1)
print("OPS 2846 COMPLETE")
