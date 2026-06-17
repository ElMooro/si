import json, boto3
s3=boto3.client("s3",region_name="us-east-1"); lam=boto3.client("lambda",region_name="us-east-1")
B="justhodl-dashboard-live"
# 1) confirm squeeze feeds exist
for k in ["data/squeeze-pretrigger.json","data/microcap-float-squeeze.json"]:
    try: print(f"{k}: EXISTS {s3.head_object(Bucket=B,Key=k)['ContentLength']}b")
    except Exception as e: print(f"{k}: MISSING ({e.__class__.__name__})")
# which lambda writes squeeze-pretrigger?
# 2) ask-desk Function URL + DESK_KEY
fn="justhodl-ask-desk"
try:
    url=lam.get_function_url_config(FunctionName=fn)["FunctionUrl"]
    print("ask-desk URL exists:",url)
except lam.exceptions.ResourceNotFoundException:
    print("ask-desk URL missing -> creating")
    url=lam.create_function_url_config(FunctionName=fn,AuthType="NONE",
        Cors={"AllowOrigins":["*"],"AllowMethods":["POST"],"AllowHeaders":["content-type","x-desk-key"]})["FunctionUrl"]
    try:
        lam.add_permission(FunctionName=fn,StatementId="FunctionURLAllowPublicAccess",Action="lambda:InvokeFunctionUrl",Principal="*",FunctionUrlAuthType="NONE")
    except Exception as e: print("  perm:",e)
    print("created:",url)
cfg=lam.get_function_configuration(FunctionName=fn)
desk_key=cfg.get("Environment",{}).get("Variables",{}).get("DESK_KEY","")
out={"url":url,"k":desk_key}
s3.put_object(Bucket=B,Key="data/askdesk-config.json",Body=json.dumps(out).encode(),ContentType="application/json",CacheControl="max-age=600")
print("wrote data/askdesk-config.json:",{"url":url[:55]+"...","k":"(set)" if desk_key else "(none/soft)"})
