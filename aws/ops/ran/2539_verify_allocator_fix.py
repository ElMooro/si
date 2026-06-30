"""ops 2539 — verify the brain_posture fix: allocator runs clean, brain_posture
fires, and measure its tilt on the allocation (target_allocation / signals_used)."""
import boto3, json, time
from botocore.config import Config

lam = boto3.client("lambda", "us-east-1", config=Config(read_timeout=200, retries={"max_attempts": 0}))
s3 = boto3.client("s3", "us-east-1")
FN = "justhodl-master-allocator"


def rd(k):
    try:
        return json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key=k)["Body"].read())
    except Exception:
        return {}


before = rd("data/master-allocation.json")
bw = before.get("target_allocation") or {}

r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
print("FunctionError:", r.get("FunctionError"))
if r.get("FunctionError"):
    print("PAYLOAD:", r["Payload"].read().decode()[:600])
time.sleep(3)

after = rd("data/master-allocation.json")
aw = after.get("target_allocation") or {}
su = after.get("signals_used") or {}

bp = su.get("brain_posture")
print("\nbrain_posture in signals_used:", bool(bp))
if bp:
    print("  ", {k: bp.get(k) for k in ("value", "intensity", "label", "regime", "ic", "in_allocator")})

print("\nposture:", after.get("posture"), "| confidence:", after.get("confidence"),
      "| active_risk_bps:", after.get("active_risk_bps"))
print("\nWEIGHT DELTAS (after - before), pp:")
if bw and aw:
    for a in sorted(set(bw) | set(aw)):
        d = aw.get(a, 0) - bw.get(a, 0)
        flag = "  <- brain lean" if a in ("gold", "cash", "ust_long", "us_equity") and abs(d) >= 0.01 else ""
        print(f"  {a:>14}: {bw.get(a,0):6.2f} -> {aw.get(a,0):6.2f}  ({d:+.2f}){flag}")
else:
    print("  before had no target_allocation; AFTER weights:", {k: aw.get(k) for k in list(aw)[:12]})

print("\ndeltas_from_benchmark:", json.dumps(after.get("deltas_from_benchmark"))[:200])
print("rationale:", (after.get("rationale") or "")[:260])
print("DONE 2539")
