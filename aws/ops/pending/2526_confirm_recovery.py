import boto3, json, time, urllib.request
lam=boto3.client("lambda","us-east-1"); ssm=boto3.client("ssm","us-east-1")
# 1) Z.ai / GLM direct test (the fallback provider)
try:
    zkey=ssm.get_parameter(Name="/justhodl/zai-api-key",WithDecryption=True)["Parameter"]["Value"]
    body=json.dumps({"model":"glm-5.1","max_tokens":16,"messages":[{"role":"user","content":"Reply OK."}]}).encode()
    req=urllib.request.Request("https://api.z.ai/api/paas/v4/chat/completions",data=body,
        headers={"Content-Type":"application/json","Authorization":f"Bearer {zkey}"})
    r=urllib.request.urlopen(req,timeout=30); d=json.loads(r.read().decode())
    print("Z.ai/GLM:", "HTTP",r.status,"OK ->",str(d.get("choices",[{}])[0].get("message",{}).get("content",""))[:40])
except urllib.error.HTTPError as e:
    print("Z.ai/GLM: HTTP",e.code,"->",e.read().decode()[:180])
except Exception as e:
    print("Z.ai/GLM: ERR",str(e)[:140])
# 2) brain-sync regime_read recovery (exercises router: GLM->Claude fallback)
r=lam.invoke(FunctionName="justhodl-brain-sync",InvocationType="RequestResponse",Payload=b"{}")
print("\nbrain-sync invoke err:",r.get("FunctionError")); time.sleep(4)
br=json.loads(boto3.client("s3","us-east-1").get_object(Bucket="justhodl-dashboard-live",Key="data/brain.json")["Body"].read())
rr=br.get("regime_read") or {}; d=br.get("directive") or {}
if rr.get("regime"):
    print("✅ regime_read RECOVERED. regime:",rr.get("regime"))
    print("   headline:",str(rr.get("headline"))[:130])
    print("   invest_in:",(rr.get("invest_in") or [])[:3])
    print("   alignment:",rr.get("alignment"))
elif rr.get("_error"): print("regime_read still erroring:",rr["_error"][:120])
else: print("regime_read:",json.dumps(rr)[:140])
print("directive populated:",bool(d),"| sector_tilts:",list((d.get("sector_tilts") or {}).keys())[:6] if d else None)
print("brain.generated_at:",br.get("generated_at"))
print("DONE 2526")
