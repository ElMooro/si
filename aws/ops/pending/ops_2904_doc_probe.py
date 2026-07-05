"""ops 2904 — decisive evidence: (a) FMP /full+/light direct probes from runner; (b) the actual
cached NVDA doc's v2 blocks verbatim (present? available? error strings?); (c) verdict/model fields."""
import os, json, urllib.request, boto3, traceback
from datetime import datetime, timezone
R={"ops":2904,"ts":datetime.now(timezone.utc).isoformat()}
KEY="wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
def probe(ep,params):
    url="https://financialmodelingprep.com/stable/"+ep+"?"+"&".join(f"{k}={v}" for k,v in params.items())+f"&apikey={KEY}"
    try:
        with urllib.request.urlopen(url,timeout=25) as r:
            b=r.read(); js=json.loads(b)
            rows=js if isinstance(js,list) else js.get("historical") or []
            return {"status":r.status,"rows":len(rows),"first_keys":sorted(list(rows[0].keys()))[:10] if rows else None,
                    "shape":("list" if isinstance(js,list) else "dict:"+",".join(list(js.keys())[:4]))}
    except urllib.error.HTTPError as e:
        return {"status":e.code,"body":e.read()[:160].decode(errors="ignore")}
    except Exception as e:
        return {"err":str(e)[:120]}
R["probe_full"]=probe("historical-price-eod/full",{"symbol":"NVDA","from":"2026-06-20"})
R["probe_light"]=probe("historical-price-eod/light",{"symbol":"NVDA","from":"2026-06-20"})
try:
    s3=boto3.client("s3",region_name="us-east-1")
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="equity-research/NVDA.json")["Body"].read())
    R["generated_at"]=d.get("generated_at"); R["top_keys"]=sorted(d.keys())
    for k in ("technicals","liquidity_solvency","growth_vs_mcap","quant_risk","backlog"):
        v=d.get(k)
        if v is None and k not in d: R[k]="KEY ABSENT"
        elif isinstance(v,dict) and v.get("available") and k=="technicals":
            R[k]={"available":True,"close_pts":len((v.get("series") or {}).get("close") or []),
                  "stats_sample":{x:(v.get("stats") or {}).get(x) for x in ("last","rsi_last","beta_2y","adv_dollar_3m_musd")}}
        else:
            R[k]=json.loads(json.dumps(v,default=str)[:400]) if isinstance(v,(dict,list)) else v
    R["verdict"]=d.get("verdict"); 
    R["model_fields"]={k:d.get(k) for k in ("claude_model","model","llm_model") if k in d}
except Exception:
    R["doc_err"]=traceback.format_exc()[-300:]
R["status"]="OK"
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:3400])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2904_doc_probe.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2904 COMPLETE")
