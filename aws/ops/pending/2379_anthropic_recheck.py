import boto3, json, urllib.request, urllib.error
lam=boto3.client("lambda","us-east-1")
env=lam.get_function_configuration(FunctionName="justhodl-fed-speak").get("Environment",{}).get("Variables",{})
key=env.get("ANTHROPIC_API_KEY") or env.get("ANTHROPIC_KEY")
print("key:",key[:14]+"..."+key[-4:])
def call(model):
    body=json.dumps({"model":model,"max_tokens":5,"messages":[{"role":"user","content":"reply with: ok"}]}).encode()
    req=urllib.request.Request("https://api.anthropic.com/v1/messages",data=body,
        headers={"Content-Type":"application/json","x-api-key":key,"anthropic-version":"2023-06-01"})
    try:
        with urllib.request.urlopen(req,timeout=30) as r:
            d=json.loads(r.read()); return 200,(d.get("content") or [{}])[0].get("text","")[:40]
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()[:160]
    except Exception as e:
        return "ERR", str(e)[:120]
for m in ["claude-haiku-4-5-20251001","claude-sonnet-4-6"]:
    c,msg=call(m); print(f"  {m}: HTTP {c} -> {msg}")
print("DONE 2379")
