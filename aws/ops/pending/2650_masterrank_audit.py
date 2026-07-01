"""ops 2650 — deep audit: engine-trust coverage of master-ranker's signal families,
full live master-ranker.json shape, and cross-check against the full engine-manifest
to find engines producing equity-relevant signals that master-ranker never reads."""
import boto3, json
s3 = boto3.client("s3", region_name="us-east-1")
def get(key):
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key=key)["Body"].read())
    except Exception as e: return {"__err__": str(e)[:150]}

print("="*70)
print("1) ENGINE-TRUST — is it populated? does it cover master-ranker's families?")
et = get("data/engine-trust.json")
engines = et.get("engines", []) if isinstance(et, dict) else []
print(f"   engine-trust.json: {len(engines)} graded engines")
MR_FAMILIES = ["compound","asymmetric","theme_tiers","eps_velocity","insider","smart_money",
  "deep_value","pead","nobrainers","options_flow","momentum_breakout","volatility_squeeze",
  "future_intel","supply_inflection","pre_pump","revenue_accel","massive","capital_flow",
  "risk_regime","options_confluence","flow_confluence","equity_confluence","earnings_confluence",
  "scarcity_radar","buyback"]
graded = {e.get("signal_type"): e for e in engines}
covered, uncovered = [], []
for f in MR_FAMILIES:
    hit = [k for k in graded if f in k or k in f]
    if hit:
        for h in hit: covered.append((f, h, graded[h].get("status"), graded[h].get("effective_trust")))
    else:
        uncovered.append(f)
print(f"   MR families WITH a trust grade: {len(covered)}")
for f,h,st,tr in covered[:20]: print(f"     {f:20s} -> {h:25s} status={st} trust={tr}")
print(f"   MR families with NO trust grade (would default 1.0): {len(uncovered)}")
print("    ", uncovered)

print("\n" + "="*70)
print("2) LIVE master-ranker.json — full top-level key shape")
mr = get("data/master-ranker.json")
print("   top-level keys:", list(mr.keys()) if isinstance(mr, dict) else mr)
if isinstance(mr, dict):
    tt = mr.get("top_tickers") or []
    print(f"   top_tickers: {len(tt)} entries")
    if tt: print("   first ticker full shape:", json.dumps(tt[0], indent=1)[:1500])
    rc = mr.get("regime_context")
    print("\n   regime_context:", json.dumps(rc, indent=1)[:600] if rc else None)
    macro = mr.get("macro_signals") or mr.get("top_macro_signals")
    print(f"\n   macro signals key present: {bool(macro)}, count: {len(macro) if isinstance(macro,list) else '?'}")

print("\n" + "="*70)
print("3) engine-manifest — total real engines on the platform vs the ~25 MR reads")
em = get("data/engine-manifest.json")
all_engines = em.get("engines", []) if isinstance(em, dict) else []
print(f"   total engines in manifest: {len(all_engines)}")
mr_keys_set = set("data/"+k.replace("_","-")+".json" for k in MR_FAMILIES)
# find engines whose primary key looks equity/ticker-relevant but isn't read by MR
candidates = []
for e in all_engines:
    keys = e.get("keys") or []
    if not keys: continue
    k0 = keys[0]
    already = any(mrf.replace("_","-") in k0 for mrf in MR_FAMILIES)
    if already: continue
    name = e.get("engine","")
    if any(kw in name for kw in ["insider","earnings","estimate","analyst","patent","backlog",
        "forward-order","resilience","dark-pool","accumulation","short","squeeze","gex",
        "options-analytics","rotation","sector","emergence","conviction","tape","russell",
        "spinoff","merger","pairs","index-recon","catalyst","beneish","quality","dividend",
        "sec-filing","lobbying","political","activist","13d","13f","ipo","alpha-score",
        "boom","catch-up","cannibal","dislocat","asymmetric","predictab","smart-beta",
        "magic-formula","gf-value","eva","supply-chain"]):
        candidates.append((name, k0))
print(f"\n   candidate engines NOT read by master-ranker ({len(candidates)}):")
for name, k in sorted(set(candidates)):
    print(f"     {name:40s} {k}")
print("DONE 2650")
