"""ops 1999: deploy morning-brief equity sections; diagnose Telegram 403; verify section data."""
import boto3, json, time, io, os, zipfile, urllib.request, urllib.error
REGION="us-east-1"; B="justhodl-dashboard-live"
lam=boto3.client("lambda",region_name=REGION); s3=boto3.client("s3",region_name=REGION); ssm=boto3.client("ssm",region_name=REGION)

# 0) confirm the section feeds have data
print("=== SECTION DATA CHECK ===")
try:
    boom=json.loads(s3.get_object(Bucket=B,Key="data/boom-radar.json")["Body"].read())
    print(f"  boom-radar: high_conviction={len(boom.get('high_conviction') or [])} candidates={len(boom.get('boom_candidates') or [])}")
    for b in (boom.get('high_conviction') or boom.get('boom_candidates') or [])[:5]:
        print(f"    {b.get('ticker'):<6} n={b.get('n')} score={b.get('boom_score')} dims={b.get('dims')}")
except Exception as e: print("  boom err",e)
try:
    aa=json.loads(s3.get_object(Bucket=B,Key="data/analyst-actions.json")["Body"].read())
    print(f"  analyst-actions: guidance_raises={len(aa.get('guidance_raises') or [])} guidance_cuts={len(aa.get('guidance_cuts') or [])}")
    print("    raises:", [g.get('ticker') for g in (aa.get('guidance_raises') or [])[:6]])
except Exception as e: print("  aa err",e)
try:
    cc=json.loads(s3.get_object(Bucket=B,Key="data/catalyst-calendar.json")["Body"].read())
    big=[e for e in (cc.get('events') or []) if e.get('type')=='EARNINGS' and e.get('impact')=='HIGH' and 0<=(e.get('days_to') or 99)<=7]
    print(f"  catalyst big-earnings-7d: {len(big)} ->", [e.get('ticker') for e in sorted(big,key=lambda x:-(x.get('importance') or 0))[:6]])
except Exception as e: print("  cc err",e)

# 1) force-redeploy morning-brief
FN="justhodl-morning-brief-tg"; SRC=f"aws/lambdas/{FN}/source"
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    for r,_,fs in os.walk(SRC):
        for f in fs:
            if f.endswith(".py"):
                p=os.path.join(r,f); z.write(p,os.path.relpath(p,SRC))
buf.seek(0)
lam.update_function_code(FunctionName=FN,ZipFile=buf.read())
for _ in range(24):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("LastUpdateStatus")!="InProgress" and c.get("State")=="Active": break
    time.sleep(5)
print("=== morning-brief redeployed:",lam.get_function(FunctionName=FN)["Configuration"]["LastModified"],"===")

# 2) Telegram 403 diagnosis
print("\n=== TELEGRAM DIAGNOSIS ===")
cfg=lam.get_function(FunctionName=FN)["Configuration"]
env=cfg.get("Environment",{}).get("Variables",{})
tok=env.get("TELEGRAM_TOKEN","")
print("  morning-brief env TELEGRAM_TOKEN present:", bool(tok), "len",len(tok))
try:
    chat=ssm.get_parameter(Name="/justhodl/telegram/chat_id")["Parameter"]["Value"]
except Exception as e:
    chat=None; print("  chat_id SSM err:",e)
print("  chat_id:",chat)
def tg(method,payload=None,token=tok):
    url=f"https://api.telegram.org/bot{token}/{method}"
    try:
        if payload is None:
            r=urllib.request.urlopen(url,timeout=15)
        else:
            r=urllib.request.urlopen(urllib.request.Request(url,data=json.dumps(payload).encode(),headers={"Content-Type":"application/json"},method="POST"),timeout=15)
        return r.getcode(), r.read().decode()[:300]
    except urllib.error.HTTPError as e:
        return e.code, (e.read().decode()[:300] if hasattr(e,'read') else str(e))
    except Exception as e:
        return None, str(e)[:200]
if tok:
    code,body=tg("getMe"); print("  getMe:",code,body)
    if chat:
        code,body=tg("sendMessage",{"chat_id":chat,"text":"JustHodl boom-watch wiring test ✅"}); print("  sendMessage:",code,body)
else:
    print("  no token in env — cannot test")

# 3) invoke morning-brief and capture result
print("\n=== INVOKE morning-brief ===")
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
print("  status:",r["StatusCode"]); print("  payload:",r["Payload"].read().decode()[:500])
print("DONE 1999")
