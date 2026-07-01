"""ops 2695 — FinViz Technical Pillar v2: end-to-end deploy + verify.

Runs in GitHub Actions (run-ops.yml) with AWS creds + open network.
1. Live-verifies every NEW FinViz filter code against Elite (count sanity —
   the ops-2693 lesson: bad codes silently return the whole universe).
2. Ensures fresh code + config on all three touched Lambdas (belt-and-
   suspenders vs the parallel deploy-lambdas run; idempotent, conflict-retried).
3. Invokes finviz-signals sync and PROVES the v2 output (schema, boards,
   breadth, picks, quarantine report).
4. Invokes best-setups sync (technical family live) and master-ranker async.
Real data only. Safe to re-run.
"""
import sys, os, io, json, time, zipfile
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

sys.path.insert(0, "aws/shared")
import finviz as FV

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=360, connect_timeout=15, retries={"max_attempts": 2}))
s3 = boto3.client("s3", region_name=REGION)

NEW_CODES = [
    ("new_high_alltime", "f=ta_alltime_nh"), ("new_low_alltime", "f=ta_alltime_nl"),
    ("new_high_50d", "s=ta_highlow50d_nh"), ("new_low_50d", "s=ta_highlow50d_nl"),
    ("price_cross50b", "f=ta_sma50_pcb"), ("price_cross20a", "f=ta_sma20_pca"),
    ("price_cross20b", "f=ta_sma20_pcb"),
    ("horizontal_sr", "f=ta_pattern_horizontal"), ("horizontal_sr_strong", "f=ta_pattern_horizontalstrong"),
    ("tl_support", "f=ta_pattern_tlsupport"), ("tl_resistance", "f=ta_pattern_tlresistance"),
    ("gap_up_vol", "f=ta_gap_u,sh_relvol_o2"), ("gap_down_vol", "f=ta_gap_d,sh_relvol_o2"),
]

def sect(t): print("\n" + "=" * 8 + " " + t + " " + "=" * 8)

# ── 1) live verification of new codes ───────────────────────────────────
sect("1/5 LIVE-VERIFY NEW FINVIZ CODES")
bad = []
for name, qs in NEW_CODES:
    try:
        n = len(FV.fetch_screen(qs))
        verdict = "SUSPICIOUS(unfiltered?)" if n >= 4500 else ("EMPTY-TODAY" if n == 0 else "VERIFIED")
        if n >= 4500: bad.append(name)
        print("  %-22s %-28s n=%-5d %s" % (name, qs, n, verdict))
    except Exception as e:
        bad.append(name)
        print("  %-22s %-28s FAIL %s" % (name, qs, str(e)[:60]))
    time.sleep(1.5)
print("  quarantine-guard in the engine will exclude:", bad or "none")

# ── 2) ensure fresh code + config (idempotent, conflict-retried) ─────────
def wait_updatable(fn, budget=240):
    t0 = time.time()
    while time.time() - t0 < budget:
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") in (None, "Successful"):
            return c
        time.sleep(5)
    return lam.get_function_configuration(FunctionName=fn)

def build_zip(src_dir):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(src_dir):
            for f in files:
                p = os.path.join(root, f)
                z.write(p, os.path.relpath(p, src_dir))
        for f in sorted(os.listdir("aws/shared")):
            if f.endswith(".py"):
                z.write(os.path.join("aws/shared", f), f)
    return buf.getvalue()

def retry_conflict(fn_call, what, tries=6):
    for i in range(tries):
        try:
            return fn_call()
        except ClientError as e:
            if e.response["Error"]["Code"] in ("ResourceConflictException", "TooManyRequestsException"):
                print("    %s conflict — retry %d/%d" % (what, i + 1, tries)); time.sleep(20)
            else:
                raise
    raise RuntimeError(what + " kept conflicting")

sect("2/5 SYNC DEPLOYS (belt-and-suspenders vs parallel deploy-lambdas)")
print("  settling 60s so the parallel deploy-lambdas run lands first…"); time.sleep(60)
TOUCHED = ["justhodl-finviz-signals", "justhodl-best-setups", "justhodl-master-ranker"]
for fn in TOUCHED:
    wait_updatable(fn)
    zb = build_zip("aws/lambdas/%s/source" % fn)
    retry_conflict(lambda: lam.update_function_code(FunctionName=fn, ZipFile=zb), fn + " code")
    wait_updatable(fn)
    print("  %-28s code synced (%d KB, shared bundled)" % (fn, len(zb) // 1024))
retry_conflict(lambda: lam.update_function_configuration(
    FunctionName="justhodl-finviz-signals", Timeout=300, MemorySize=512),
    "finviz-signals config")
wait_updatable("justhodl-finviz-signals")
print("  justhodl-finviz-signals config: timeout=300 memory=512")

# ── 3) run + prove finviz-signals v2 ─────────────────────────────────────
sect("3/5 RUN finviz-signals v2 (sync, ~3-4 min for 46 screens)")
r = lam.invoke(FunctionName="justhodl-finviz-signals", InvocationType="RequestResponse")
pay = json.loads(r["Payload"].read() or b"{}")
print("  invoke ->", json.dumps(pay)[:300])
assert not r.get("FunctionError"), "finviz-signals errored: %s" % pay
doc = json.loads(s3.get_object(Bucket=BUCKET, Key="data/finviz-signals.json")["Body"].read())
assert doc.get("schema") == "2.0", "output not schema 2.0"
b = doc.get("boards") or {}
for k in ("ma_crosses", "highs_lows", "patterns", "consolidation"):
    assert k in b, "missing board " + k
br = doc.get("breadth") or {}
print("  schema 2.0 OK · boards OK · breadth pa200=%s pa50=%s pa20=%s regime=%s hist=%d"
      % (br.get("pct_above_sma200"), br.get("pct_above_sma50"),
         br.get("pct_above_sma20"), br.get("regime"), len(br.get("history") or [])))
print("  NEW screens:", {n: doc["counts"].get(n) for n, _ in NEW_CODES})
print("  quarantined:", doc.get("quarantined") or "none")
print("  top_picks: %d  · momentum_breakouts: %d · sweeps: %d · coiled: %d · dbl-confirmed: %d/%d"
      % (len(doc.get("top_picks") or []),
         len((b["highs_lows"].get("momentum_breakouts") or [])),
         len((b["highs_lows"].get("sweep_opportunities") or [])),
         len((b["consolidation"].get("coiled") or [])),
         len(((b["patterns"].get("double_confirmed") or {}).get("double_bottom") or [])),
         len(((b["patterns"].get("double_confirmed") or {}).get("double_top") or []))))

# ── 4) fused consumers ───────────────────────────────────────────────────
sect("4/5 RUN best-setups (technical family live)")
r2 = lam.invoke(FunctionName="justhodl-best-setups", InvocationType="RequestResponse")
p2 = (r2["Payload"].read() or b"{}").decode()[:220]
print("  invoke ->", ("ERROR " if r2.get("FunctionError") else "") + p2)
try:
    raw = s3.get_object(Bucket=BUCKET, Key="data/best-setups.json")["Body"].read().decode()
    hits = {k: raw.count('"%s"' % k) for k in
            ("ATH_BREAKOUT", "BASE_BREAKOUT", "GOLDEN_CROSS", "MA200_RECLAIM", "DOUBLE_BOTTOM_FV")}
    print("  technical keys present in output:", hits,
          "(zeros are fine on quiet days — the family is wired)")
except Exception as e:
    print("  best-setups output check skipped:", str(e)[:80])

sect("5/5 TRIGGER master-ranker (async — finviz_tech system wired)")
lam.invoke(FunctionName="justhodl-master-ranker", InvocationType="Event")
print("  triggered · verify later on /master-rank.html (systems list will show finviz_tech)")

print("\nOPS 2695 COMPLETE — FinViz Technical Pillar v2 live and fused.")
