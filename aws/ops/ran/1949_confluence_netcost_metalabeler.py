"""ops 1949 — Items 4, 5, 6.
  #4 best-setups: confluence now counts EFFECTIVE independent bets (factor-family
     adjusted) instead of raw signal count; STRONG BUY needs cross-family independence.
  #6 signal-scorecard: alpha is now NET of a round-trip cost; FDR 'proven' set must
     beat SPY AFTER costs.
  #5 meta-labeler: read live status (active vs warming) + uplift to decide wiring.
"""
import io, json, time, zipfile, os, glob
import boto3

REGION = "us-east-1"
lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
B = "justhodl-dashboard-live"
ROOT = os.getcwd()

def zb(main_path, shared=False):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.write(main_path, "lambda_function.py")
        if shared:
            for sp in glob.glob(f"{ROOT}/aws/shared/*.py"):
                z.write(sp, os.path.basename(sp))
    buf.seek(0); return buf.read()

def deploy(fn, path, shared=False):
    data = zb(path, shared)
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

# ── #4 best-setups ──
print("=== #4 best-setups (correlation-adjusted confluence) ===")
deploy("justhodl-best-setups", f"{ROOT}/aws/lambdas/justhodl-best-setups/source/lambda_function.py")
r = lam.invoke(FunctionName="justhodl-best-setups", InvocationType="RequestResponse")
print("  invoke:", str(json.loads(r["Payload"].read()))[:130])
bs = get("data/best-setups.json")
st = bs.get("setups") or bs.get("top_setups") or []
haircut = [s for s in st if isinstance(s.get("n_independent_bets"), (int, float))
           and s["n_independent_bets"] < s.get("n_signals", 0)]
print(f"  setups={len(st)} | with confluence haircut (eff<raw)={len(haircut)}")
for s in sorted(st, key=lambda x: -(x.get('n_signals') or 0))[:6]:
    print(f"    {s.get('ticker'):6s} raw_signals={s.get('n_signals')} indep_bets={s.get('n_independent_bets')} "
          f"families={s.get('n_factor_families')} conf×{s.get('confluence_mult')} verdict={s.get('verdict')}")
from collections import Counter
vc = Counter(s.get("verdict") for s in st)
print("  verdict distribution:", dict(vc))

# ── #6 signal-scorecard net-of-cost ──
print("\n=== #6 signal-scorecard (NET-of-cost alpha + FDR) ===")
deploy("justhodl-signal-scorecard", f"{ROOT}/aws/lambdas/justhodl-signal-scorecard/source/lambda_function.py", shared=True)
r = lam.invoke(FunctionName="justhodl-signal-scorecard", InvocationType="RequestResponse")
print("  invoke:", str(json.loads(r["Payload"].read()))[:150])
time.sleep(2)
sc = get("data/signal-scorecard.json")
al = sc.get("alpha", {})
print(f"  round_trip_cost={al.get('round_trip_cost_pct')}% | tested={al.get('n_engines_tested')} "
      f"| PROVEN(net)={al.get('n_alpha_proven')} | NEGATIVE(net)={al.get('n_alpha_negative')}")
print("  NET-of-cost leaderboard:")
for x in al.get("leaderboard", [])[:12]:
    print(f"    {x['signal_type'][:30]:30s} {x['alpha_status']:14s} gross={x['gross_excess_pct']}% "
          f"net={x['net_excess_pct']}%  net_t={x['net_t_stat']}  net_IR={x['net_info_ratio']}  n={x['n']}")
print("  PROVEN after costs:", al.get("alpha_proven_signals"))

# ── #5 meta-labeler status ──
print("\n=== #5 meta-labeler (status + uplift) ===")
r = lam.invoke(FunctionName="justhodl-meta-labeler", InvocationType="RequestResponse")
print("  invoke:", str(json.loads(r["Payload"].read()))[:120])
time.sleep(2)
ml = get("data/meta-labeler.json")
m = ml.get("model", {})
print(f"  status: {ml.get('status')} | n_training_rows: {ml.get('n_training_rows')} (activates at {ml.get('min_rows_to_activate')})")
print(f"  uplift_pp: {m.get('uplift_pp')} | take_precision: {m.get('test_take_precision')}% vs base {m.get('test_base_hit')}% "
      f"| take_rate: {m.get('test_take_rate')}% | brier: {m.get('brier')}")
print(f"  avg_excess taken vs all: {m.get('avg_excess_taken_pct')}% vs {m.get('avg_excess_all_pct')}% "
      f"| pending gated: {ml.get('n_take')}/{ml.get('n_pending_gated')} TAKE")
print("  -> WIRE IT" if ml.get("status") == "active" and (m.get("uplift_pp") or 0) > 0 else "  -> still warming or no uplift; leave advisory")

print("\nDONE 1949")
