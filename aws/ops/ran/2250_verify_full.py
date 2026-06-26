import boto3, json, time, urllib.request
lam=boto3.client("lambda","us-east-1")
for _ in range(25):
    c=lam.get_function(FunctionName="justhodl-equity-research")["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    time.sleep(3)
URL="https://6nkrwmk2ntjx54okqvtzokosb40whvfb.lambda-url.us-east-1.on.aws/"
req=urllib.request.Request(f"{URL}?ticker=LDOS&refresh=1",headers={"User-Agent":"jh","Origin":"https://justhodl.ai"})
t=time.time()
with urllib.request.urlopen(req,timeout=175) as r: b=r.read().decode()
d=json.loads(b); print(f"LDOS refresh {time.time()-t:.0f}s · keys={len(d)}")
es=d.get("executive_summary") or ""; v=d.get("verdict") or {}; da=d.get("devils_advocate") or {}; rf=d.get("risk_factors") or {}
print("\nEXEC SUMMARY:", es[:240])
print("\nVERDICT:", {k:v.get(k) for k in ("rating","conviction_grade","price_target_12m","upside_pct","confidence_pct","verdict_rationale")})
print("\nRISK FACTORS:", rf.get("title"), "| risks:", [r.get("risk") for r in (rf.get("key_risks") or [])[:4]])
print("\nDEVIL'S ADVOCATE:", da.get("title"))
print("  short_thesis:", str(da.get("short_thesis"))[:300])
print("  kill_points:", [(k.get("point"),k.get("evidence")) for k in (da.get("kill_points") or [])[:4]])
print("  what_bulls_underestimate:", str(da.get("what_bulls_underestimate"))[:160])
sc=d.get("scenarios") or {}
print("\nSCENARIOS:", {k:(sc.get(k) or {}).get("price_target_12m") for k in ("bull_case","base_case","bear_case")})
print("parsed_keys count:", len(d), "| has all core:", all(d.get(k) for k in ("executive_summary","investment_thesis","risk_factors","devils_advocate","verdict")))
print("DONE 2250")
