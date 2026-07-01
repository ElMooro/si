"""ops 2697 — root-cause + fix the missing `price` in the FinViz universe (Coiled=0,
breadth_universe_n=16) and land TL strong-variant screens. Report committed to
aws/ops/reports/2697_universe_price_fix.json.

Evidence chain from ops 2696: coil gates showed px_vol=0 with avg_volume present ->
universe records lack `price` even though CUSTOM_COLS spans 0..150 and COLMAP maps
"Price". Step 1 fingerprints the LIVE export headers to find the exact mismatch;
the shipped fix (_cmap normalized lookup + prev_close fallback) covers header
case/spacing variants; then the universe is rebuilt and the engine re-run to prove
Coiled > 0 and sane breadth.
"""
import sys, os, io, json, time, zipfile
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

sys.path.insert(0, "aws/shared")
import finviz as FV

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=600, connect_timeout=15, retries={"max_attempts": 2}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2697, "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}

def sect(t): print("\n" + "=" * 8 + " " + t + " " + "=" * 8)

# ── 1) fingerprint the live custom-export headers ───────────────────────
sect("1/6 EXPORT HEADER FINGERPRINT")
rows = FV.fetch_custom()
hdrs = list(rows[0].keys()) if rows else []
R["export"] = {"n_rows": len(rows), "n_headers": len(hdrs),
               "price_like_headers": [repr(h) for h in hdrs if "price" in (h or "").lower()],
               "sample_headers": [repr(h) for h in hdrs[:12]]}
smp = rows[0] if rows else {}
R["export"]["sample_price_vals"] = {repr(h): smp.get(h) for h in hdrs if "price" in (h or "").lower()}
print(json.dumps(R["export"], indent=1)[:800])

# ── 2) current cached universe state (before rebuild) ───────────────────
sect("2/6 CACHED UNIVERSE BEFORE")
try:
    env = json.loads(s3.get_object(Bucket=BUCKET, Key="data/finviz-universe.json")["Body"].read())
    uni0 = env.get("by_ticker", {})
    R["before"] = {"generated_at": env.get("generated_at"), "n": len(uni0),
                   "price_nonnull": sum(1 for u in uni0.values() if u.get("price") is not None),
                   "prev_close_nonnull": sum(1 for u in uni0.values() if u.get("prev_close") is not None),
                   "sma200_nonnull": sum(1 for u in uni0.values() if u.get("sma200_pct") is not None),
                   "change_nonnull": sum(1 for u in uni0.values() if u.get("change_pct") is not None)}
    stock = next((u for u in uni0.values() if not u.get("asset_type")), {})
    R["before"]["sample_stock_keys"] = sorted(stock.keys())[:40]
except Exception as e:
    R["before"] = {"err": str(e)[:100]}
print(json.dumps(R["before"], default=str)[:700])

# ── 3) deploy patched shared into universe-builder + engine ─────────────
sect("3/6 DEPLOY (shared bundle refresh)")
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
for fn in ("justhodl-finviz-universe", "justhodl-finviz-signals"):
    src = "aws/lambdas/%s/source" % fn
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(src):
            for f in files:
                z.write(os.path.join(root, f), os.path.relpath(os.path.join(root, f), src))
        for f in sorted(os.listdir("aws/shared")):
            if f.endswith(".py"):
                z.write(os.path.join("aws/shared", f), f)
    wait_updatable(fn)
    retry_conflict(lambda b=buf: lam.update_function_code(FunctionName=fn, ZipFile=b.getvalue()), fn)
    wait_updatable(fn)
    print("  %s synced (shared bundled)" % fn)

# ── 4) rebuild the universe snapshot ────────────────────────────────────
sect("4/6 REBUILD UNIVERSE")
r = lam.invoke(FunctionName="justhodl-finviz-universe", InvocationType="RequestResponse")
assert not r.get("FunctionError"), "universe builder errored: %s" % r["Payload"].read()[:200]
uni = FV.load_universe()
R["after_universe"] = {"n": len(uni),
                       "price_nonnull": sum(1 for u in uni.values() if u.get("price") is not None),
                       "prev_close_nonnull": sum(1 for u in uni.values() if u.get("prev_close") is not None),
                       "sma200_nonnull": sum(1 for u in uni.values() if u.get("sma200_pct") is not None)}
print("  ", R["after_universe"])

# ── 5) re-run the engine + prove the boards ─────────────────────────────
sect("5/6 RE-RUN ENGINE")
r = lam.invoke(FunctionName="justhodl-finviz-signals", InvocationType="RequestResponse")
pay = json.loads(r["Payload"].read() or b"{}")
assert not r.get("FunctionError"), "engine errored: %s" % str(pay)[:200]
doc = json.loads(s3.get_object(Bucket=BUCKET, Key="data/finviz-signals.json")["Body"].read())
cons = (doc.get("boards") or {}).get("consolidation") or {}
br = doc.get("breadth") or {}
R["after"] = {"quarantined": doc.get("quarantined"),
              "tl_counts_raw": {k: (doc.get("counts_raw") or {}).get(k) for k in ("tl_support", "tl_resistance")},
              "coiled": len(cons.get("coiled") or []),
              "coil_diag": cons.get("diag"),
              "top_coils": [c["ticker"] for c in (cons.get("coiled") or [])[:10]],
              "dist_watch": len(cons.get("distribution_watch") or []),
              "breadth": {k: br.get(k) for k in ("universe_n", "pct_above_sma200", "pct_above_sma50",
                                                 "pct_above_sma20", "advancers_pct", "regime",
                                                 "sp500_pct_above_sma200")},
              "picks": len(doc.get("top_picks") or [])}
print(json.dumps(R["after"], indent=1)[:1000])

# ── 6) persist report + retrigger consumers ─────────────────────────────
sect("6/6 REPORT + CONSUMERS")
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2697_universe_price_fix.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("  wrote aws/ops/reports/2697_universe_price_fix.json")
for fn in ("justhodl-best-setups", "justhodl-master-ranker"):
    lam.invoke(FunctionName=fn, InvocationType="Event")
    print("  triggered", fn)
print("\nOPS 2697 COMPLETE")
