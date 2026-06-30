"""ops 2537 — verify master-allocator shipped brain_posture and measure its effect.

(1) Confirm the DEPLOYED zip actually contains brain_posture (deploy-lambdas
    can lag; LastUpdateStatus=Successful != my code shipped). (2) Capture the
    current allocation (computed WITHOUT brain_posture). (3) Invoke. (4) Diff the
    target weights so we see the brain's defensive macro lean move gold/UST/cash.
"""
import boto3, json, io, zipfile, urllib.request, time
from botocore.config import Config

lam = boto3.client("lambda", "us-east-1", config=Config(read_timeout=200, retries={"max_attempts": 0}))
s3 = boto3.client("s3", "us-east-1")
FN = "justhodl-master-allocator"


def rd(k):
    try:
        return json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key=k)["Body"].read())
    except Exception:
        return {}


# 1. deployed code has brain_posture?
loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
src = zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(loc).read())).read("lambda_function.py").decode()
shipped = "brain_posture" in src
print("deployed allocator contains brain_posture:", shipped)
if not shipped:
    print("  -> deploy-lambdas has not shipped the change yet; re-run this ops shortly.")

# 2. before
before = rd("data/master-allocation.json")
bt = before.get("target") or {}
print("\nBEFORE target weights:", {k: round(v, 2) for k, v in bt.items()} if bt else "(none)")

# 3. invoke
r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
print("invoke err:", r.get("FunctionError"))
time.sleep(3)

# 4. after
after = rd("data/master-allocation.json")
at = after.get("target") or {}
print("AFTER  target weights:", {k: round(v, 2) for k, v in at.items()} if at else "(none)")

# brain_posture signal present?
blob = json.dumps(after)
print("\nbrain_posture present in output signals:", "brain_posture" in blob)
# try to surface its intensity
for key in ("signals", "contributing_signals", "active_signals", "signal_detail"):
    sd = after.get(key)
    if isinstance(sd, dict) and "brain_posture" in sd:
        print("  brain_posture:", sd["brain_posture"]); break
    if isinstance(sd, list):
        for s in sd:
            if isinstance(s, dict) and (s.get("signal") == "brain_posture" or s.get("name") == "brain_posture" or s.get("label") == "Brain Macro Posture"):
                print("  brain_posture:", s); break

# 5. diff
if bt and at:
    print("\nWEIGHT DELTAS (after - before), pp:")
    for a in sorted(set(bt) | set(at)):
        d = (at.get(a, 0) - bt.get(a, 0))
        if abs(d) >= 0.01:
            print(f"  {a:>14}: {bt.get(a,0):6.2f} -> {at.get(a,0):6.2f}  ({d:+.2f})")
print("\nrationale:", (after.get("rationale") or "")[:240])
print("DONE 2537")
