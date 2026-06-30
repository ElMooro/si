"""ops 2563 — redeploy upside-thesis (Sonnet), async invoke, verify AI theses now populate."""
import boto3, json, io, zipfile, time, glob, os, datetime
lam = boto3.client("lambda", "us-east-1"); s3 = boto3.client("s3", "us-east-1")
FN = "justhodl-upside-thesis"
def ready():
    for _ in range(36):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): return
        time.sleep(5)
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("lambda_function.py", open("aws/lambdas/justhodl-upside-thesis/source/lambda_function.py").read())
    for p in glob.glob("aws/shared/*.py"): z.writestr(os.path.basename(p), open(p).read())
lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue()); ready()
print("redeployed (Sonnet)")
lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
print("async invoked — polling…")
for i in range(12):
    time.sleep(20)
    try:
        out = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/upside-theses.json")["Body"].read())
        ga = out.get("generated_at","")
        try: age=(datetime.datetime.now(datetime.timezone.utc)-datetime.datetime.fromisoformat(ga)).total_seconds()
        except: age=9999
        print(f"  poll {i}: age~{int(age)}s n_ai={out.get('n_ai')} n_cand={out.get('n_candidates')} elapsed={out.get('elapsed_s')}")
        if age<240 and (out.get('n_ai') or 0) > 0:
            print("\n✓ AI THESES POPULATED")
            for t in out.get("top_ranked",[])[:3]:
                d=out["theses"][t]; ai=d.get("ai") or {}
                print(f"\n── {t} · CANSLIM {d['canslim']['score']} SQGLP {d['sqglp']['score']} Lynch {d['lynch']['score']} ──")
                print("  AI headline:", ai.get("headline"))
                print("  why_boom:", str(ai.get("why_boom"))[:180])
                print("  multibagger_case:", str(ai.get("multibagger_case"))[:180])
                print("  catalysts:", ai.get("catalysts"), "| best_fw:", ai.get("best_framework"), "conv", ai.get("conviction"))
            break
    except Exception as e: print(f"  poll {i}: {str(e)[:50]}")
print("DONE 2563")
