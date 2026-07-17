"""ops 3417 — feed-shape probe: jsi-history, sector-flow-state, gamma feeds."""
import json, sys
from pathlib import Path
import boto3
from ops_report import report
S3C=boto3.client("s3","us-east-1")
def peek(key):
    try:
        j=json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",Key=key)["Body"].read())
        top=list(j.keys())[:10] if isinstance(j,dict) else f"LIST[{len(j)}]"
        samp=None
        if isinstance(j,dict):
            for k,v in j.items():
                if isinstance(v,list) and v and isinstance(v[0],dict):
                    samp={k: {kk: (str(vv)[:40] if not isinstance(vv,(int,float)) else vv) for kk,vv in list(v[0].items())[:10]}}
                    break
        elif isinstance(j,list) and j:
            samp={"[0]": {kk:(str(vv)[:40] if not isinstance(vv,(int,float)) else vv) for kk,vv in list(j[0].items())[:10]} if isinstance(j[0],dict) else str(j[0])[:60]}
        return {"top":top,"sample":samp}
    except Exception as e:
        return {"err":str(e)[:80]}
with report("3417_shapes") as rep:
    rep.heading("ops 3417 — shapes")
    out={}
    for k in ("data/jsi-history.json","data/sector-flow-state.json",
              "data/options-gamma.json","data/dealer-gex.json"):
        out[k]=peek(k); line=k+" → "+json.dumps(out[k])[:340]; print(line); rep.log(line)
    Path("aws/ops/reports/3417.json").write_text(json.dumps(out,indent=2)); sys.exit(0)
