"""ops 3447 — real auction-grades.json shape."""
import json, sys
from pathlib import Path
import boto3
from ops_report import report
S3C=boto3.client("s3","us-east-1")
with report("3447_grades_peek") as rep:
    j=json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",Key="data/auction-grades.json")["Body"].read())
    top=list(j.keys()) if isinstance(j,dict) else f"LIST[{len(j)}]"
    line="top: "+json.dumps(top)[:200]; print(line); rep.log(line)
    def first_dict_row(o,depth=0):
        if depth>3: return None
        if isinstance(o,list):
            for v in o:
                r=first_dict_row(v,depth+1)
                if r: return r
        if isinstance(o,dict):
            if "dimensions" in o or "tail" in json.dumps(list(o.keys())).lower() or "grade" in o:
                return o
            for v in o.values():
                r=first_dict_row(v,depth+1)
                if r: return r
        return None
    row=first_dict_row(j)
    if row:
        line="row keys: "+json.dumps(sorted(row.keys()))[:300]; print(line); rep.log(line)
        line="row sample: "+json.dumps({k:(str(v)[:50] if not isinstance(v,(int,float)) else v) for k,v in list(row.items())[:12]})[:400]
        print(line); rep.log(line)
    else:
        print("no grade-like row found"); rep.log("none")
    Path("aws/ops/reports/3447.json").write_text("{}"); sys.exit(0)
