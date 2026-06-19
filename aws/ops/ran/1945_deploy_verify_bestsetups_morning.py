"""ops 1945 — Deploy + verify RORO wiring into best-setups + morning-intelligence.

best-setups: pure scoring engine — invoke + prove risk_regime_mult on setups.
morning-intelligence: calls Anthropic (out of credits) + sends Telegram, so deploy
only (no live invoke) and verify the RORO brief line renders from live data offline.
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
            return c
        time.sleep(3)
    return lam.get_function_configuration(FunctionName=fn)

# ── deploy both ──
update("justhodl-best-setups", f"{ROOT}/aws/lambdas/justhodl-best-setups/source/lambda_function.py")
c2 = update("justhodl-morning-intelligence", f"{ROOT}/aws/lambdas/justhodl-morning-intelligence/source/lambda_function.py")
print("morning-intel LastModified:", c2.get("LastModified"), "size:", c2.get("CodeSize"))

# ── best-setups: invoke + verify ──
print("\n=== best-setups ===")
r = lam.invoke(FunctionName="justhodl-best-setups", InvocationType="RequestResponse")
print("invoke:", str(json.loads(r["Payload"].read()))[:160])
bs = json.loads(s3.get_object(Bucket=BUCKET, Key="data/best-setups.json")["Body"].read())
setups = bs.get("setups") or bs.get("top_setups") or []
tilted = [s for s in setups if isinstance(s.get("risk_regime_mult"), (int, float)) and s["risk_regime_mult"] != 1.0]
print(f"setups: {len(setups)} | with RORO mult != 1.0: {len(tilted)}")
for s in tilted[:6]:
    print(f"  {s['ticker']:6s} conviction={s.get('conviction')} roro_mult={s['risk_regime_mult']} verdict={s.get('verdict')}")
# show a why-text carrying RORO
for s in setups:
    if s.get("why") and "RORO" in s["why"]:
        print("  why sample:", s["why"][s["why"].index("Cross-asset RORO"):][:120]); break

# ── morning-intelligence: offline render of the RORO brief line ──
print("\n=== morning-intelligence RORO line (offline render from live data) ===")
rr = json.loads(s3.get_object(Bucket=BUCKET, Key="data/risk-regime.json")["Body"].read())
m = {"roro_score": rr.get("risk_regime_score"), "roro_regime": rr.get("risk_regime"),
     "roro_posture": (rr.get("posture") or {}).get("beta_tilt"),
     "roro_size_mult": (rr.get("posture") or {}).get("size_mult"),
     "roro_tells": (rr.get("tells") or [])[:4]}
line = ("RISK_REGIME(RORO): " + str(m.get("roro_score")) + "/100 (" + str(m.get("roro_regime")) +
        ") — cross-asset risk-on/off from Massive FX+options + FRED VIX/credit. Posture: " +
        str(m.get("roro_posture")) + " size×" + str(m.get("roro_size_mult")) + ". Tells: " +
        ("; ".join(m.get("roro_tells") or []) or "none"))
print(" ", line)
print("\nDONE 1945")
