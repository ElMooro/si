"""ops 1947 — Extend RORO to the last needed surfaces + fix the silent gate.
  1. best-setups: sector now resolved via harvested map -> RORO gate actually fires
  2. hedge-planner: NEW roro_overlay -> sleeve budget biased by risk-on/off tape
  3. risk-regime.html: openable cockpit page (verify live)
Force-deploys both lambdas via boto3 (deploy-lambdas can miss diffs), then verifies.
"""
import io, json, time, zipfile, os, urllib.request
import boto3

REGION = "us-east-1"
lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
B = "justhodl-dashboard-live"
ROOT = os.getcwd()

def zb(path):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.write(path, "lambda_function.py")
    buf.seek(0); return buf.read()

def deploy(fn, path):
    data = zb(path)
    for i in range(24):
        try:
            lam.update_function_code(FunctionName=fn, ZipFile=data, Publish=False)
            print(f"  {fn}: code update OK (attempt {i})"); break
        except lam.exceptions.ResourceConflictException:
            time.sleep(5)
    for _ in range(40):
        c = lam.get_function_configuration(FunctionName=fn)
        if c["State"] == "Active" and c.get("LastUpdateStatus") != "InProgress":
            return
        time.sleep(3)

def get(k):
    try: return json.loads(s3.get_object(Bucket=B, Key=k)["Body"].read())
    except Exception as e: return {"__err__": str(e)}

# ── 1) best-setups ──
print("=== best-setups (sector gate fix) ===")
deploy("justhodl-best-setups", f"{ROOT}/aws/lambdas/justhodl-best-setups/source/lambda_function.py")
r = lam.invoke(FunctionName="justhodl-best-setups", InvocationType="RequestResponse")
print("  invoke:", str(json.loads(r["Payload"].read()))[:140])
bs = get("data/best-setups.json")
st = bs.get("setups") or bs.get("top_setups") or []
tl = [s for s in st if isinstance(s.get("risk_regime_mult"), (int, float)) and s["risk_regime_mult"] != 1.0]
print(f"  setups={len(st)}  with RORO tilt!=1.0={len(tl)}  (was 0 before fix)")
for s in tl[:8]:
    print(f"    {s.get('ticker'):6s} conv={s.get('conviction')} roro_mult={s['risk_regime_mult']} verdict={s.get('verdict')}")

# ── 2) hedge-planner ──
print("\n=== hedge-planner (RORO sleeve bias) ===")
deploy("justhodl-hedge-planner", f"{ROOT}/aws/lambdas/justhodl-hedge-planner/source/lambda_function.py")
r = lam.invoke(FunctionName="justhodl-hedge-planner", InvocationType="RequestResponse")
print("  invoke:", str(json.loads(r["Payload"].read()))[:140])
hp = get("data/hedge-planner.json")
ov = hp.get("roro_overlay")
print("  roro_overlay present:", bool(ov))
if ov:
    print("    regime:", ov.get("risk_regime"), "score:", ov.get("risk_regime_score"))
    print("    budget_bias_mult:", ov.get("budget_bias_mult"), "urgency:", ov.get("urgency"))
    print("    budget pre->post:", ov.get("target_budget_pre_roro_pct"), "->", ov.get("target_budget_post_roro_pct"))
    print("    note:", str(ov.get("note"))[:140])

# ── 3) risk-regime.html live ──
print("\n=== risk-regime.html (live page) ===")
ok = False
for attempt in range(6):
    try:
        req = urllib.request.Request("https://justhodl.ai/risk-regime.html",
                                     headers={"User-Agent": "jh-ops"})
        body = urllib.request.urlopen(req, timeout=15).read().decode("utf-8", "ignore")
        has_title = "Risk Regime" in body and "RORO" in body
        has_fx = "FX RORO drivers" in body
        has_load = "risk-regime.json" in body
        print(f"  attempt {attempt}: 200 OK len={len(body)} title={has_title} fx_section={has_fx} loads_json={has_load}")
        ok = has_title and has_load
        break
    except Exception as e:
        print(f"  attempt {attempt}: {type(e).__name__} {str(e)[:60]} — waiting for Pages…")
        time.sleep(20)
print("  PAGE LIVE" if ok else "  PAGE not confirmed yet (Pages may still be propagating)")

print("\nDONE 1947")
