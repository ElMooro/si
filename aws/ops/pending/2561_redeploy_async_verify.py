"""ops 2561 — redeploy parallelized upside-thesis, async-invoke, poll S3 for fresh output."""
import boto3, json, io, zipfile, time, glob, os
lam = boto3.client("lambda", "us-east-1")
s3 = boto3.client("s3", "us-east-1")
FN = "justhodl-upside-thesis"
def ready():
    for _ in range(36):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): return
        time.sleep(5)
# check what the prior (timed-out) invoke left
try:
    prev = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/upside-theses.json")["Body"].read())
    print("prior output:", prev.get("generated_at"), "n_ai", prev.get("n_ai"), "n_cand", prev.get("n_candidates"))
except Exception as e: print("no prior output:", str(e)[:40])
# redeploy parallelized code
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("lambda_function.py", open("aws/lambdas/justhodl-upside-thesis/source/lambda_function.py").read())
    for p in glob.glob("aws/shared/*.py"): z.writestr(os.path.basename(p), open(p).read())
lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue()); ready()
print("redeployed")
# async invoke
lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
print("async invoked — polling S3…")
import datetime
t_start = time.time()
for i in range(14):
    time.sleep(20)
    try:
        out = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/upside-theses.json")["Body"].read())
        ga = out.get("generated_at","")
        # fresh if generated in last 5 min
        try:
            age = (datetime.datetime.now(datetime.timezone.utc) - datetime.datetime.fromisoformat(ga)).total_seconds()
        except Exception: age = 9999
        print(f"  poll {i}: gen {ga} age~{int(age)}s n_ai={out.get('n_ai')} n_cand={out.get('n_candidates')}")
        if age < 240 and out.get("n_candidates"):
            print("\nFRESH OUTPUT:")
            print("top_ranked:", out.get("top_ranked", [])[:12])
            for t in out.get("top_ranked", [])[:3]:
                d = out["theses"][t]
                print(f"\n── {t} · CANSLIM {d['canslim']['score']} SQGLP {d['sqglp']['score']} Lynch {d['lynch']['score']} · {d['n_engines']} engines ──")
                print("  why:", d["why"][:160])
                if d.get("ai"):
                    print("  AI:", d["ai"].get("headline"), "| fw:", d["ai"].get("best_framework"), "conv", d["ai"].get("conviction"))
                    print("     ", str(d["ai"].get("why_boom"))[:150])
            break
    except Exception as e:
        print(f"  poll {i}: {str(e)[:40]}")
print("DONE 2561")
