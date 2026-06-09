import json, os, time, zipfile, io, boto3
from botocore.config import Config
cfg=Config(read_timeout=200,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg); s3=boto3.client("s3",region_name="us-east-1",config=cfg)
out={}
def deploy_run(name):
    buf=io.BytesIO(); src=f"aws/lambdas/{name}/source"
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf:
        for r,_,fs in os.walk(src):
            for f in fs:
                if "__pycache__" in r or f.endswith(".pyc"): continue
                zf.write(os.path.join(r,f),arcname=os.path.relpath(os.path.join(r,f),src))
    lam.update_function_code(FunctionName=name,ZipFile=buf.getvalue())
    for _ in range(25):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=name)
        if c.get("LastUpdateStatus") in ("Successful",None): break
    return lam.invoke(FunctionName=name,InvocationType="RequestResponse",Payload=b"{}")["Payload"].read().decode()[:60]
out["history_run"]=deploy_run("justhodl-ecb-history")  # refresh manifest w/ stale flags
time.sleep(3)
out["derived_run"]=deploy_run("justhodl-ecb-derived")  # build #1 + #4
time.sleep(4)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/ecb-derived.json")["Body"].read())
ind=d.get("indicators",{})
out["usd_funding"]={k:ind.get("usd_funding_stress_composite",{}).get(k) for k in ['score_0_100','composite_z','signal','err']}
out["esi"]={k:ind.get("eurodollar_stress_index",{}).get(k) for k in ['esi_0_100','tier','components','err']}
out["all_signals"]={k:v.get("signal") for k,v in ind.items()}
# confirm ecb-detail eurodollar_stress_score now populated
ed=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/ecb-detail.json")["Body"].read())
out["ecb_detail_eurodollar_stress_score"]=ed.get("eurodollar_stress_score","ABSENT")
open("aws/ops/reports/1512_r.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
