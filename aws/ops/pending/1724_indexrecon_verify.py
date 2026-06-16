import json, time, boto3
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1")
before=""
try: before=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/index-recon.json")["Body"].read()).get("generated_at","")
except Exception as e: print("pre-read:",str(e)[:60])
lam.invoke(FunctionName="justhodl-index-recon",InvocationType="Event")
print("invoked async, polling...")
out=None
for i in range(13):
    time.sleep(20)
    try:
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/index-recon.json")["Body"].read())
        if d.get("generated_at")!=before: out=d; break
    except Exception: pass
if not out: print("did not refresh in time"); raise SystemExit
ft=out.get("finviz_truth") or {}
print("finviz_truth present:", bool(ft))
if ft:
    print("  n_sp500:", ft.get("n_sp500"), "| n_russell2000:", ft.get("n_russell2000"))
    print("  recon implied-R2000 but Finviz says OUT (potential adds/boundary):", len(ft.get("implied_in__truth_out",[])), "e.g.", ft.get("implied_in__truth_out",[])[:8])
    print("  Finviz R2000 but recon ranks elsewhere (potential demote/delete):", len(ft.get("truth_in__implied_out",[])), "e.g.", ft.get("truth_in__implied_out",[])[:8])
print("n_additions:", out.get("n_additions"), "| n_deletions:", out.get("n_deletions"))
