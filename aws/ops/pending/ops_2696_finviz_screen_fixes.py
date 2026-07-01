"""ops 2696 — fix the 8 quarantined FinViz screens + empty Coiled board (follow-up to 2695).

Root-cause hypotheses under test (report committed to aws/ops/reports/2696_finviz_fix.json):
  A. 20d/50d high-low are FILTERS (f=ta_highlow20d_nh) not signals (s= was silently
     ignored -> whole universe -> quarantined). The 20d pair was broken since v1.
  B. "strong horizontal" code is ta_pattern_horizontal2 (not ..._horizontalstrong).
  C. horizontal_sr / tl_support / tl_resistance are LOOSE patterns that legitimately
     tag thousands -> per-screen threshold 9500 instead of 4500.
  D. Coiled=0: _is_stock rejected everything if asset_type != exactly "Stock";
     now accepts any *stock* variant. Coil gates instrumented (diag counters).
"""
import sys, os, io, json, time, zipfile
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

sys.path.insert(0, "aws/shared")
import finviz as FV

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=360, connect_timeout=15, retries={"max_attempts": 2}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2696, "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}

def sect(t): print("\n" + "=" * 8 + " " + t + " " + "=" * 8)

# ── 1) corrected + loose codes, live raw counts ─────────────────────────
sect("1/5 LIVE-VERIFY CORRECTED CODES")
PROBE = [("new_high_50d", "f=ta_highlow50d_nh"), ("new_low_50d", "f=ta_highlow50d_nl"),
         ("new_high_20d", "f=ta_highlow20d_nh"), ("new_low_20d", "f=ta_highlow20d_nl"),
         ("horizontal_sr_strong", "f=ta_pattern_horizontal2"),
         ("horizontal_sr", "f=ta_pattern_horizontal"),
         ("tl_support", "f=ta_pattern_tlsupport"), ("tl_resistance", "f=ta_pattern_tlresistance")]
R["code_probe"] = {}
for name, qs in PROBE:
    try:
        n = len(FV.fetch_screen(qs))
    except Exception as e:
        n = "FAIL:" + str(e)[:60]
    R["code_probe"][name] = {"qs": qs, "raw": n}
    print("  %-22s %-30s raw=%s" % (name, qs, n))
    time.sleep(1.5)

# ── 2) universe probe: why was Coiled empty? ────────────────────────────
sect("2/5 UNIVERSE PROBE (asset_type + volatility fields)")
uni = FV.load_universe()
hist, have = {}, {"volatility_w": 0, "volatility_m": 0, "atr": 0, "off_52w_high_pct": 0, "avg_volume": 0}
for u in uni.values():
    hist[u.get("asset_type") or "<none>"] = hist.get(u.get("asset_type") or "<none>", 0) + 1
    for k in have:
        if u.get(k) is not None:
            have[k] += 1
R["universe"] = {"n": len(uni),
                 "asset_type_hist": dict(sorted(hist.items(), key=lambda x: -x[1])[:6]),
                 "fields_nonnull": have}
print("  n=%d asset_type=%s" % (len(uni), R["universe"]["asset_type_hist"]))
print("  fields non-null:", have)

# ── 3) capture prior feed evidence, deploy patched engine ───────────────
sect("3/5 DEPLOY PATCHED ENGINE")
try:
    prior = json.loads(s3.get_object(Bucket=BUCKET, Key="data/finviz-signals.json")["Body"].read())
    R["before"] = {"quarantined": prior.get("quarantined"),
                   "counts_raw_bad8": {k: (prior.get("counts_raw") or {}).get(k)
                                       for k in list((prior.get("quarantined") or {}))},
                   "coiled": len(((prior.get("boards") or {}).get("consolidation") or {}).get("coiled") or [])}
    print("  before:", json.dumps(R["before"])[:300])
except Exception as e:
    R["before"] = {"err": str(e)[:80]}

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

print("  settling 60s for parallel deploy-lambdas…"); time.sleep(60)
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    src = "aws/lambdas/justhodl-finviz-signals/source"
    for root, _, files in os.walk(src):
        for f in files:
            z.write(os.path.join(root, f), os.path.relpath(os.path.join(root, f), src))
    for f in sorted(os.listdir("aws/shared")):
        if f.endswith(".py"):
            z.write(os.path.join("aws/shared", f), f)
wait_updatable("justhodl-finviz-signals")
retry_conflict(lambda: lam.update_function_code(FunctionName="justhodl-finviz-signals", ZipFile=buf.getvalue()), "code")
wait_updatable("justhodl-finviz-signals")
print("  engine code synced")

# ── 4) re-run + prove ───────────────────────────────────────────────────
sect("4/5 RE-RUN ENGINE")
r = lam.invoke(FunctionName="justhodl-finviz-signals", InvocationType="RequestResponse")
pay = json.loads(r["Payload"].read() or b"{}")
assert not r.get("FunctionError"), "engine errored: %s" % str(pay)[:200]
doc = json.loads(s3.get_object(Bucket=BUCKET, Key="data/finviz-signals.json")["Body"].read())
cons = (doc.get("boards") or {}).get("consolidation") or {}
R["after"] = {"quarantined": doc.get("quarantined"),
              "counts_fixed": {k: doc["counts"].get(k) for k, _ in PROBE},
              "counts_raw_fixed": {k: (doc.get("counts_raw") or {}).get(k) for k, _ in PROBE},
              "coiled": len(cons.get("coiled") or []),
              "coil_diag": cons.get("diag"),
              "dist_watch": len(cons.get("distribution_watch") or []),
              "base_breakout_confluence": len((doc.get("confluence") or {}).get("base_breakout") or []),
              "breadth_universe_n": (doc.get("breadth") or {}).get("universe_n"),
              "pa200": (doc.get("breadth") or {}).get("pct_above_sma200")}
print(json.dumps(R["after"], indent=1)[:900])

# ── 5) persist report (auto-committed by run-ops) + retrigger consumers ─
sect("5/5 REPORT + CONSUMERS")
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2696_finviz_fix.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("  wrote aws/ops/reports/2696_finviz_fix.json")
for fn in ("justhodl-best-setups", "justhodl-master-ranker"):
    lam.invoke(FunctionName=fn, InvocationType="Event")
    print("  triggered", fn)
print("\nOPS 2696 COMPLETE")
