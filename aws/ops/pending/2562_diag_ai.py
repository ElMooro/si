"""ops 2562 — diagnose why upside-thesis AI=0: read CloudWatch + test complete() directly."""
import boto3, time
logs = boto3.client("logs", "us-east-1")
grp = "/aws/lambda/justhodl-upside-thesis"
try:
    streams = logs.describe_log_streams(logGroupName=grp, orderBy="LastEventTime", descending=True, limit=2)["logStreams"]
    for st in streams:
        ev = logs.get_log_events(logGroupName=grp, logStreamName=st["logStreamName"], limit=40, startFromHead=False)["events"]
        for e in ev:
            m = e["message"].strip()
            if any(k in m for k in ["AI ", "GLM", "router", "err", "Error", "Anthropic", "anthropic", "401", "403", "429", "credit", "balance"]):
                print("  ", m[:200])
except Exception as e:
    print("logs err:", str(e)[:100])
# direct router test from inside the same env
print("\n--- direct complete() test via the function ---")
import json
lam = boto3.client("lambda", "us-east-1")
# tiny probe: invoke a one-off by reading the function's env to test anthropic key validity
cfg = lam.get_function_configuration(FunctionName="justhodl-upside-thesis")
env = cfg.get("Environment", {}).get("Variables", {})
print("has ANTHROPIC_API_KEY:", bool(env.get("ANTHROPIC_API_KEY")), "len", len(env.get("ANTHROPIC_API_KEY","")))
# test the key directly
import urllib.request
key = env.get("ANTHROPIC_API_KEY","")
if key:
    try:
        req = urllib.request.Request("https://api.anthropic.com/v1/messages",
            data=json.dumps({"model":"claude-sonnet-4-5","max_tokens":20,"messages":[{"role":"user","content":"say OK"}]}).encode(),
            headers={"Content-Type":"application/json","x-api-key":key,"anthropic-version":"2023-06-01"})
        r = urllib.request.urlopen(req, timeout=20)
        print("anthropic test:", r.status, r.read().decode()[:120])
    except Exception as e:
        body = getattr(e,'read',lambda:b'')() if hasattr(e,'read') else b''
        print("anthropic test ERR:", str(e)[:80], body[:200] if body else "")
print("DONE 2562")
