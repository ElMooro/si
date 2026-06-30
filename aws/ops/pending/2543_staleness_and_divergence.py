"""ops 2543 — confirm true staleness + recheck the divergence-interpreter 400."""
import boto3, json, time
from datetime import datetime, timezone

s3 = boto3.client("s3", "us-east-1")
lam = boto3.client("lambda", "us-east-1")
logs = boto3.client("logs", "us-east-1")
BUCKET = "justhodl-dashboard-live"


def rd(k):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read())
    except Exception as e:
        return {"_err": str(e)[:80]}


# 1. true freshness picture (correct schema)
m = rd("data/_freshness-monitor.json")
print("=== fleet freshness (last monitor run) ===")
print("generated_at:", m.get("generated_at"))
print("n_keys_tracked:", m.get("n_keys_tracked"), "| n_stale:", m.get("n_stale"),
      "| n_alerts_raised:", m.get("n_alerts_raised"))
stale = m.get("stale") or []
if isinstance(stale, list) and stale:
    print("stale feeds:")
    for s in stale[:25]:
        if isinstance(s, dict):
            print(f"  {s.get('key'):<46} age={s.get('age_h')}h thr={s.get('max_age_h')}h")
        else:
            print("  ", s)
else:
    print("stale feeds: none reported")

# 2. recheck divergence-interpreter NOW (Anthropic is up)
print("\n=== divergence-interpreter live recheck ===")
r = lam.invoke(FunctionName="justhodl-divergence-interpreter", InvocationType="RequestResponse", Payload=b"{}")
print("FunctionError:", r.get("FunctionError"))
pl = r["Payload"].read().decode()
print("payload:", pl[:300])
time.sleep(4)
# pull the freshest log lines to see if the Claude call still 400s
try:
    lg = "/aws/lambda/justhodl-divergence-interpreter"
    st = logs.describe_log_streams(logGroupName=lg, orderBy="LastEventTime", descending=True, limit=1)["logStreams"][0]
    ev = logs.get_log_events(logGroupName=lg, logStreamName=st["logStreamName"], limit=25, startFromHead=False)["events"]
    print("recent log tail:")
    for e in ev[-12:]:
        msg = e["message"].rstrip()
        if any(k in msg for k in ("interp", "Claude", "Error", "model", "200", "400", "via", "fallback")):
            print("  " + msg[:160])
except Exception as e:
    print("log err:", str(e)[:100])

# 3. did its output update with a real model (not null)?
out = rd("data/divergence-interpretation.json")
if "_err" in out:
    for alt in ("data/divergence-interpreter.json", "data/divergence-v2.json"):
        out = rd(alt)
        if "_err" not in out:
            print("\noutput key:", alt); break
print("\ninterpreter output model:", out.get("model"), "| has interpretation:",
      bool(out.get("interpretation") or out.get("read") or out.get("ai_read")))
print("DONE 2543")
