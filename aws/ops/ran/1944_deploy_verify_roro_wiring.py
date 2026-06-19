"""ops 1944 — Deploy + verify RORO wiring into signal-board + master-ranker.

Both edited (no custom shared imports), so force-update via boto3 then invoke and
prove the wiring:
  • signal-board: "Risk Regime (RORO)" feed present with a signal value
  • master-ranker: top-level risk_regime block + tickers carrying risk_regime_mult != 1.0
"""
import io, json, time, zipfile, os
import boto3

lam = boto3.client("lambda", "us-east-1")
s3 = boto3.client("s3", "us-east-1")
BUCKET = "justhodl-dashboard-live"
ROOT = os.getcwd()

def zb(path):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.write(path, "lambda_function.py")
    buf.seek(0); return buf.read()

def update(fn, path):
    for i in range(24):
        try:
            lam.update_function_code(FunctionName=fn, ZipFile=zb(path), Publish=False)
            print(f"  {fn}: update OK (attempt {i})"); break
        except lam.exceptions.ResourceConflictException:
            time.sleep(5)
    for _ in range(40):
        c = lam.get_function_configuration(FunctionName=fn)
        if c["State"] == "Active" and c.get("LastUpdateStatus") != "InProgress":
            return
        time.sleep(3)

# ── deploy ──
update("justhodl-signal-board", f"{ROOT}/aws/lambdas/justhodl-signal-board/source/lambda_function.py")
update("justhodl-master-ranker", f"{ROOT}/aws/lambdas/justhodl-master-ranker/source/lambda_function.py")

# ── signal-board verify ──
print("\n=== signal-board ===")
r = lam.invoke(FunctionName="justhodl-signal-board", InvocationType="RequestResponse")
print("invoke:", str(json.loads(r["Payload"].read()))[:160])
sb = json.loads(s3.get_object(Bucket=BUCKET, Key="data/signal-board.json")["Body"].read())
# find RORO feed in whatever structure holds the engines
import re
blob = json.dumps(sb)
roro_present = "Risk Regime (RORO)" in blob
print("RORO feed present:", roro_present)
# try to surface its signal/note
def _find(obj):
    found = []
    if isinstance(obj, dict):
        if obj.get("name") == "Risk Regime (RORO)" or obj.get("engine") == "Risk Regime (RORO)":
            found.append(obj)
        for v in obj.values():
            found += _find(v)
    elif isinstance(obj, list):
        for v in obj:
            found += _find(v)
    return found
for f in _find(sb)[:3]:
    print("  RORO entry:", json.dumps({k: f.get(k) for k in ("name", "category", "signal", "note", "value", "state")})[:200])

# ── master-ranker verify ──
print("\n=== master-ranker ===")
r = lam.invoke(FunctionName="justhodl-master-ranker", InvocationType="RequestResponse")
print("invoke:", str(json.loads(r["Payload"].read()))[:160])
mr = json.loads(s3.get_object(Bucket=BUCKET, Key="data/master-ranker.json")["Body"].read())
print("top-level risk_regime:", json.dumps(mr.get("risk_regime"))[:240])
tt = mr.get("top_tickers", [])
tilted = [t for t in tt if isinstance(t.get("risk_regime_mult"), (int, float)) and t["risk_regime_mult"] != 1.0]
print(f"top_tickers: {len(tt)} | with RORO tilt != 1.0: {len(tilted)}")
for t in tilted[:6]:
    print(f"  {t['ticker']:6s} score={t['score']} roro_mult={t['risk_regime_mult']} cf_mult={t.get('capital_flow_mult')}")
    # show roro note tail
    rt = t.get("rationale", "")
    if "RORO" in rt:
        print("       …", rt[rt.index("RORO"):][:80])
print("\nDONE 1944")
