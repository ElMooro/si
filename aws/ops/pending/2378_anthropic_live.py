import boto3, json, urllib.request
lam=boto3.client("lambda","us-east-1")
# find a Lambda that carries the Anthropic key
key=None; src=None
for fn in ["justhodl-fed-speak","justhodl-weekly-ai-review","justhodl-financial-secretary","justhodl-ask-desk","justhodl-cycle-clock","justhodl-khalid-metrics"]:
    try:
        env=lam.get_function_configuration(FunctionName=fn).get("Environment",{}).get("Variables",{})
        k=env.get("ANTHROPIC_API_KEY") or env.get("ANTHROPIC_KEY")
        if k: key=k; src=fn; break
    except Exception as e: print(fn,"err",str(e)[:40])
if not key:
    print("NO anthropic key found in env"); print("DONE 2378"); raise SystemExit
print("key from:",src,"| prefix:",key[:14]+"..."+key[-4:],"| len:",len(key))
def call(model):
    body=json.dumps({"model":model,"max_tokens":5,"messages":[{"role":"user","content":"ping"}]}).encode()
    req=urllib.request.Request("https://api.anthropic.com/v1/messages",data=body,
        headers={"Content-Type":"application/json","x-api-key":key,"anthropic-version":"2023-06-01"})
    try:
        with urllib.request.urlopen(req,timeout=30) as r:
            d=json.loads(r.read()); return 200, (d.get("content") or [{}])[0].get("text","")[:30]
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()[:200]
    except Exception as e:
        return "ERR", str(e)[:120]
for m in ["claude-haiku-4-5-20251001","claude-sonnet-4-6"]:
    code,msg=call(m); print(f"  {m}: HTTP {code} -> {msg}")
print("DONE 2378")
