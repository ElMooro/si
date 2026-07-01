"""ops 2698 — final fix in the FinViz chain: avg_volume unit normalization.

Root cause proven by ops-2697 fingerprint: universe HAS price/sma/volatility;
breadth_universe_n=16 exposed that "Average Volume" arrives in THOUSANDS of
shares, so the coil gate (>=300k) demanded 300M sh/day and the breadth gate
(>=50k) passed only the ~16 mega-liquids. Engine now auto-detects units from
the median stock avg-vol (AV_MULT). This op deploys, re-runs, and hard-proves
Coiled > 0 and a real breadth universe. Report committed to
aws/ops/reports/2698_avg_volume_units.json.
"""
import sys, os, io, json, time, zipfile
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

sys.path.insert(0, "aws/shared")

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=600, connect_timeout=15, retries={"max_attempts": 2}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2698, "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}

def sect(t): print("\n" + "=" * 8 + " " + t + " " + "=" * 8)

def wait_updatable(fn, budget=240):
    t0 = time.time()
    while time.time() - t0 < budget:
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") in (None, "Successful"):
            return
        time.sleep(5)

def retry_conflict(call, what, tries=6):
    for i in range(tries):
        try:
            return call()
        except ClientError as e:
            if e.response["Error"]["Code"] in ("ResourceConflictException", "TooManyRequestsException"):
                print("    %s conflict — retry %d" % (what, i + 1)); time.sleep(20)
            else:
                raise
    raise RuntimeError(what)

sect("1/3 DEPLOY ENGINE")
print("  settling 60s for parallel deploy-lambdas…"); time.sleep(60)
src = "aws/lambdas/justhodl-finviz-signals/source"
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    for root, _, files in os.walk(src):
        for f in files:
            z.write(os.path.join(root, f), os.path.relpath(os.path.join(root, f), src))
    for f in sorted(os.listdir("aws/shared")):
        if f.endswith(".py"):
            z.write(os.path.join("aws/shared", f), f)
wait_updatable("justhodl-finviz-signals")
retry_conflict(lambda: lam.update_function_code(FunctionName="justhodl-finviz-signals", ZipFile=buf.getvalue()), "code")
wait_updatable("justhodl-finviz-signals")
print("  engine synced")

sect("2/3 RE-RUN + HARD PROOF")
r = lam.invoke(FunctionName="justhodl-finviz-signals", InvocationType="RequestResponse")
pay = json.loads(r["Payload"].read() or b"{}")
assert not r.get("FunctionError"), "engine errored: %s" % str(pay)[:200]
doc = json.loads(s3.get_object(Bucket=BUCKET, Key="data/finviz-signals.json")["Body"].read())
cons = (doc.get("boards") or {}).get("consolidation") or {}
br = doc.get("breadth") or {}
dg = cons.get("diag") or {}
R["after"] = {"quarantined": doc.get("quarantined"),
              "coil_diag": dg,
              "coiled": len(cons.get("coiled") or []),
              "top_coils": [{"t": c["ticker"], "s": c["coil_score"], "tags": c.get("tags")}
                            for c in (cons.get("coiled") or [])[:8]],
              "wyckoff_coils": sum(1 for c in (cons.get("coiled") or [])
                                   if "WYCKOFF_ACCUMULATION" in (c.get("tags") or [])),
              "dist_watch": len(cons.get("distribution_watch") or []),
              "breadth": {k: br.get(k) for k in ("universe_n", "pct_above_sma200", "pct_above_sma50",
                                                 "pct_above_sma20", "advancers_pct", "regime",
                                                 "sp500_pct_above_sma200")},
              "sectors_n": len(br.get("sectors") or {}),
              "picks": len(doc.get("top_picks") or [])}
print(json.dumps(R["after"], indent=1)[:1200])
assert dg.get("av_unit_mult") == 1000.0, "unit detection did not trigger: %s" % dg
assert dg.get("px_vol", 0) > 500, "px_vol gate still starved: %s" % dg
assert (br.get("universe_n") or 0) > 2000, "breadth universe still tiny: %s" % br.get("universe_n")
assert len(cons.get("coiled") or []) > 0, "coiled still empty despite open gates"
print("  HARD ASSERTS PASSED")

sect("3/3 REPORT + CONSUMERS")
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2698_avg_volume_units.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("  wrote aws/ops/reports/2698_avg_volume_units.json")
for fn in ("justhodl-best-setups", "justhodl-master-ranker"):
    lam.invoke(FunctionName=fn, InvocationType="Event")
    print("  triggered", fn)
print("\nOPS 2698 COMPLETE")
