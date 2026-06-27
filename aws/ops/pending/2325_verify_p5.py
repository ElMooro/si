import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
def doc():
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/cycle-clock.json")["Body"].read())
    except Exception as e: return {"_e":str(e)[:60]}
b4=doc().get("generated_at")
lam.invoke(FunctionName="justhodl-cycle-clock",InvocationType="Event",Payload=b"{}")
print("regen v2.3...")
d=None
for i in range(18):
    time.sleep(12); cur=doc()
    if cur.get("generated_at")!=b4 and cur.get("version")=="2.3": d=cur; print(f"  t+{(i+1)*12}s v2.3 (dur {cur.get('duration_s')}s)"); break
    print(f"  t+{(i+1)*12}s...")
if not d: print("NO v2.3:",json.dumps(doc())[:150]); print("DONE 2325"); raise SystemExit
ca=d.get("cross_asset") or {}; conf=ca.get("confirmation") or {}; gl=d.get("global_liquidity") or {}
cy=d.get("cycle") or {}; nl=(d.get("liquidity") or {}).get("net_liquidity") or {}
print("\n=== CROSS-ASSET (sorted by 1m) ===")
for x in (ca.get("returns") or [])[:14]: print(f"  {x['ticker']:4} {x['label']:18} 1m {x['ret_1m']:+5}%  3m {x['ret_3m']:+6}%")
print("\nCONFIRMATION:", conf.get("status"), "| beating SPY:", conf.get("beating_spy"))
print("  note:", conf.get("note"))
print("\n=== GLOBAL LIQUIDITY ===")
print("  regime:", gl.get("regime"), "| index:", gl.get("global_liquidity_index"), "| impulse13w:", gl.get("global_impulse_13w_pct"))
print("  CB impulse 13w:", json.dumps(gl.get("cb_impulse_13w")))
print("\n=== NET LIQUIDITY ===")
print("  net", nl.get("net_tn"),"T | pctile_since_2018:", nl.get("percentile_since_2018"), "| sparkline pts:", len(nl.get("series") or []))
print("\n=== SCENARIO PLAYBOOK ===")
for s in (cy.get("scenario_playbook") or []): print(f"  {s['scenario']:16} {s['odds_pct']:5}%  → {', '.join(s['assets'][:3])}")
print("\n=== FORWARD DISTRIBUTION (for cone) ===")
fd=(d.get("analogs") or {}).get("forward_distribution") or {}
for h in ("21d","63d","126d"):
    if h in fd: print(f"  {h}: median {fd[h].get('median_pct')}% mean {fd[h].get('mean_pct')}% σ {fd[h].get('stdev_pct')}% [{fd[h].get('min_pct')}..{fd[h].get('max_pct')}]")
print("\ndivergences:", len(d.get("divergences") or []), "| AI:", "OK" if d.get("ai") else "null(quota)")
print("DONE 2325")
