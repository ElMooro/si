import boto3, json, urllib.request
s3=boto3.client("s3","us-east-1"); lam=boto3.client("lambda","us-east-1")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/signal-backtest.json")["Body"].read())
ai=d.get("ai_analysis") or {}
print("ai_analysis:",json.dumps(ai)[:300])
# function env
cfg=lam.get_function_configuration(FunctionName="justhodl-signal-backtest")
env=(cfg.get("Environment") or {}).get("Variables") or {}
print("env has ANTHROPIC_API_KEY:","ANTHROPIC_API_KEY" in env,"| has FMP_KEY:","FMP_KEY" in env)
k=env.get("ANTHROPIC_API_KEY","")
print("key prefix:",(k[:14]+"..."+k[-4:]) if k else "(empty)")
# test the key directly with a tiny call
if k:
    payload=json.dumps({"model":"claude-sonnet-4-6","max_tokens":20,"messages":[{"role":"user","content":"reply OK"}]}).encode()
    req=urllib.request.Request("https://api.anthropic.com/v1/messages",data=payload,
        headers={"Content-Type":"application/json","x-api-key":k,"anthropic-version":"2023-06-01"},method="POST")
    try:
        with urllib.request.urlopen(req,timeout=40) as r:
            rr=json.loads(r.read().decode()); print("DIRECT CALL OK:",rr.get("content",[{}])[0].get("text","")[:40],"| model:",rr.get("model"))
    except urllib.error.HTTPError as e:
        print("DIRECT CALL HTTP",e.code,":",e.read().decode()[:200])
    except Exception as e:
        print("DIRECT CALL ERR:",type(e).__name__,str(e)[:150])
print("DONE 2445")
