"""ops 2673 — check if the FDR-corrected run already completed despite the client-side
60s read timeout (Lambda itself keeps running after the caller stops waiting), and if not,
re-invoke with a properly configured longer client timeout."""
import boto3, json, time
from botocore.config import Config
s3 = boto3.client("s3", region_name="us-east-1")

obj = s3.head_object(Bucket="justhodl-dashboard-live", Key="data/signal-genealogy.json")
print("current S3 object last_modified:", obj["LastModified"])
j = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/signal-genealogy.json")["Body"].read())
print("version:", j.get("version"), "has fdr_note:", "fdr_note" in j)
print("n_pairs_tested:", j.get("n_pairs_tested"), "n_significant_pairs:", j.get("n_significant_pairs"))

if "fdr_note" not in j:
    print("\n--> FDR fix not yet reflected, re-invoking with a proper long read_timeout")
    lam = boto3.client("lambda", region_name="us-east-1", config=Config(read_timeout=200, connect_timeout=10, retries={"max_attempts": 0}))
    r = lam.invoke(FunctionName="justhodl-signal-genealogy", InvocationType="RequestResponse", Payload=b"{}")
    print("INVOKE:", r.get("StatusCode"), r.get("FunctionError"))
    print("BODY:", r["Payload"].read().decode()[:400])
    time.sleep(2)
    j = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/signal-genealogy.json")["Body"].read())

print("\n=== FINAL LIVE STATE ===")
print("version:", j.get("version"), "elapsed_s:", j.get("elapsed_s"))
print("n_pairs_tested:", j.get("n_pairs_tested"), "| n_hypothesis_tests:", j.get("n_hypothesis_tests"))
print("n_significant_pairs (POST-FDR):", j.get("n_significant_pairs"))
if j.get("n_pairs_tested"):
    rate = j.get("n_significant_pairs",0)/j.get("n_pairs_tested")*100
    print(f"significant rate: {rate:.1f}% (was 41.6% pre-fix)")
print("\ntop 10 EARLIEST signals:")
for r2 in (j.get("earliest_signals") or [])[:10]:
    print(f"  {r2['signal_type']:35s} earliness={r2['earliness_index']:+.1f} n={r2['n_firings']} leads_spy={r2['leads_spy']}")
print("\ntop 10 significant cascades (FDR-survived):")
for p in (j.get("significant_cascades") or [])[:10]:
    print(f"  {p['leader']:28s} -> {p['follower']:28s}  lag={p['lag_days']}d corr={p['corr']} t={p['t']} n={p['n']}")
print("DONE 2673")
