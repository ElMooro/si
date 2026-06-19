"""ops 1948 — Items 1-3: alpha-aware, FDR-corrected edge measurement.
The scorecard now computes forward EXCESS return vs SPY for the entire
historical ledger (from prices already stored on each outcome row + a SPY
daily-history lookup) and applies Benjamini-Hochberg FDR across all engines.
Force-deploys with shared modules bundled, then proves alpha attribution works.
"""
import io, json, time, zipfile, os, glob
import boto3

REGION = "us-east-1"
lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
B = "justhodl-dashboard-live"
ROOT = os.getcwd()

def zb(main_path):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.write(main_path, "lambda_function.py")
        for sp in glob.glob(f"{ROOT}/aws/shared/*.py"):
            z.write(sp, os.path.basename(sp))
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

print("=== deploy signal-scorecard ===")
deploy("justhodl-signal-scorecard", f"{ROOT}/aws/lambdas/justhodl-signal-scorecard/source/lambda_function.py")
print("=== invoke (full ledger scan + alpha + FDR) ===")
r = lam.invoke(FunctionName="justhodl-signal-scorecard", InvocationType="RequestResponse")
pl = json.loads(r["Payload"].read())
print("  invoke:", str(pl)[:200])
time.sleep(2)

sc = json.loads(s3.get_object(Bucket=B, Key="data/signal-scorecard.json")["Body"].read())
al = sc.get("alpha", {})
print("\n=== ALPHA ATTRIBUTION (vs SPY, FDR-controlled) ===")
print("  schema:", sc.get("schema_version"), "| outcomes scanned:", sc.get("n_outcomes_scanned"),
      "| scored:", sc.get("n_outcomes_scored"))
print("  benchmark:", al.get("benchmark"), "| fdr_q:", al.get("fdr_q"), "| min_n:", al.get("min_alpha_n"))
print("  engines tested for alpha:", al.get("n_engines_tested"),
      "| ALPHA_PROVEN:", al.get("n_alpha_proven"), "| ALPHA_NEGATIVE:", al.get("n_alpha_negative"))
print("\n  TOP ALPHA LEADERBOARD (by t-stat):")
for x in al.get("leaderboard", [])[:15]:
    print(f"    {x['signal_type'][:34]:34s} {x['alpha_status']:14s} "
          f"excess={x['mean_excess_pct']}%  t={x['t_stat']}  IR={x['info_ratio']}  "
          f"beat-SPY={x['alpha_hit_rate']}  n={x['n']}")
print("\n  PROVEN positive-alpha engines:", al.get("alpha_proven_signals"))
print("  PROVEN value-destroying engines:", al.get("alpha_negative_signals"))

# confirm the downstream alpha map was published
try:
    am = json.loads(s3.get_object(B and B, Key="data/engine-alpha.json")["Body"].read())
    print("\n  data/engine-alpha.json published:", len(am.get("engines", {})), "engines mapped; proven:",
          len(am.get("alpha_proven_signals", [])))
except Exception as e:
    print("  engine-alpha.json:", e)

print("\nDONE 1948")
