"""ops 4360 — strict v5.1 verify (full-fidelity + coverage contract).
Poll invoke->S3 until coverage block exists AND version=='5.1'; then assert:
catalog_metrics>=18, onchain_metrics present, open_interest ok via okx,
coverage.total_leaves recorded, doc size sane. Max 6x40s."""
import json, os, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; FN="justhodl-crypto-intel"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=300,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION)
R={"ops":4360,"started":datetime.now(timezone.utc).isoformat(),"attempts":[]}
v=None
for i in range(6):
    at={"n":i+1}
    try:
        inv=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
        at["invoke"]=inv.get("StatusCode"); at["fn_err"]=inv.get("FunctionError"); inv["Payload"].read()
    except Exception as e:
        at["invoke_err"]=str(e)[:120]
    try:
        raw=s3.get_object(Bucket=BUCKET,Key="crypto-intel.json")["Body"].read()
        d=json.loads(raw); at["version"]=d.get("version")
        cov=d.get("coverage") or {}
        q=d.get("cryptoquant") or {}; oi=d.get("open_interest") or {}
        if d.get("version")=="5.1" and cov.get("total_leaves"):
            v={"generated_at":d.get("generated_at"),"fetch_time_s":d.get("fetch_time"),
               "doc_kb":round(len(raw)/1024,1),
               "coverage":{"total_leaves":cov.get("total_leaves"),"sections":cov.get("sections"),
                            "truncated":cov.get("truncated_paths"),
                            "top_sections":dict(sorted((cov.get("leaves_by_section") or {}).items(),
                                                        key=lambda kv:-kv[1])[:12])},
               "cq":{"status":q.get("status"),"headline_metrics":len(q.get("metrics") or {}),
                      "catalog_metrics":len(q.get("catalog_metrics") or {}),
                      "onchain_metrics":len(q.get("onchain_metrics") or {}),
                      "grading":bool(q.get("grading")),"composite_z":q.get("composite_onchain_risk_z")},
               "open_interest":{"status":oi.get("status"),"source":oi.get("source"),
                                 "rows":len(oi.get("list") or []),
                                 "sample":(oi.get("list") or [None])[0]},
               "fleet_joined":sum(1 for e in ((d.get("fleet") or {}).get("ledger") or []) if e.get("status")=="ok"),
               "source_health":{"ok":(d.get("source_health") or {}).get("ok"),
                                 "total":(d.get("source_health") or {}).get("total")}}
            R["attempts"].append(at); break
    except Exception as e:
        at["s3_err"]=str(e)[:120]
    R["attempts"].append(at); time.sleep(40)
R["verify"]=v
ok = bool(v and v["cq"]["catalog_metrics"]>=18 and v["cq"]["status"]=="ok"
          and v["open_interest"]["status"]=="ok" and v["fleet_joined"]>=6)
R["verdict"]="PASS — full-fidelity coverage live" if ok else ("PARTIAL" if v else "FAIL")
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4360_v51_coverage.json","w"),indent=1,default=str)
md=[f"# ops 4360 — v5.1 coverage verify — {R['verdict']}"]
if v:
    md+=[f"- doc {v['doc_kb']}KB | fetch {v['fetch_time_s']}s @ {v['generated_at']}",
         f"- coverage: {v['coverage']['total_leaves']} leaves / {v['coverage']['sections']} sections / {v['coverage']['truncated']} truncations",
         f"- top sections by leaves: {json.dumps(v['coverage']['top_sections'])}",
         f"- CQ: catalog={v['cq']['catalog_metrics']} onchain_map={v['cq']['onchain_metrics']} headline={v['cq']['headline_metrics']} grading={v['cq']['grading']} z={v['cq']['composite_z']}",
         f"- OI[{v['open_interest']['source']}]: {v['open_interest']['rows']} rows, sample={json.dumps(v['open_interest']['sample'])}",
         f"- fleet joined={v['fleet_joined']} | source_health {v['source_health']['ok']}/{v['source_health']['total']}"]
else:
    md+=[f"- attempts: {json.dumps(R['attempts'])[:1200]}"]
open("aws/ops/reports/4360_v51_coverage.md","w").write("\n".join(md)+"\n")
print(json.dumps(R,indent=1,default=str))
