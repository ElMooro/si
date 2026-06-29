import boto3, json, time, urllib.request
lam=boto3.client("lambda","us-east-1"); logs=boto3.client("logs","us-east-1")
# 1) Pull the live ANTHROPIC_KEY from a lambda env
env=lam.get_function_configuration(FunctionName="justhodl-brain-sync").get("Environment",{}).get("Variables",{})
KEY=env.get("ANTHROPIC_KEY") or env.get("ANTHROPIC_API_KEY") or ""
print("ANTHROPIC_KEY present on brain-sync:", bool(KEY), "len:", len(KEY))
def test(model):
    body=json.dumps({"model":model,"max_tokens":16,"messages":[{"role":"user","content":"Reply with the single word OK."}]}).encode()
    req=urllib.request.Request("https://api.anthropic.com/v1/messages",data=body,
        headers={"Content-Type":"application/json","x-api-key":KEY,"anthropic-version":"2023-06-01"})
    try:
        r=urllib.request.urlopen(req,timeout=30); d=json.loads(r.read().decode())
        return f"HTTP {r.status} OK -> {d.get('content',[{}])[0].get('text','')[:30]}"
    except urllib.error.HTTPError as e:
        return f"HTTP {e.code} -> {e.read().decode()[:240]}"
    except Exception as e:
        return f"ERR {str(e)[:120]}"
print("\n=== LIVE Anthropic test NOW (credits reportedly filled) ===")
for m in ["claude-haiku-4-5-20251001","claude-sonnet-4-6"]:
    print(f"  {m}: {test(m)}")
# 2) CloudWatch: when did failures start + exact message
print("\n=== CloudWatch failure history (brain-sync, last 14d) ===")
now=int(time.time()*1000); start=now-14*24*3600*1000
try:
    ev=logs.filter_log_events(logGroupName="/aws/lambda/justhodl-brain-sync",startTime=start,
        filterPattern='?"400" ?"credit" ?"err" ?"Error"',limit=200).get("events",[])
    print("matching events:",len(ev))
    if ev:
        import datetime as dt
        first=ev[0]; last=ev[-1]
        def ts(e): return dt.datetime.utcfromtimestamp(e["timestamp"]/1000).isoformat()
        print("  EARLIEST:",ts(first),"->",first["message"][:160].strip())
        print("  LATEST  :",ts(last),"->",last["message"][:160].strip())
except Exception as e: print("  logs err:",str(e)[:120])
print("DONE 2524")
