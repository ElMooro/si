import json, time, urllib.request, boto3
lam=boto3.client("lambda","us-east-1")
for _ in range(20):
    c=lam.get_function(FunctionName="justhodl-equity-research")["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    time.sleep(3)
URL="https://6nkrwmk2ntjx54okqvtzokosb40whvfb.lambda-url.us-east-1.on.aws/"
t=time.time()
req=urllib.request.Request(f"{URL}?ticker=LDOS&refresh=1",headers={"User-Agent":"jh","Origin":"https://justhodl.ai"})
with urllib.request.urlopen(req,timeout=270) as r: d=json.loads(r.read().decode())
print(f"generated {time.time()-t:.0f}s")
# 1) analyst estimates fixed?
ae=d.get("analyst_estimates") or []
print("\nANALYST ESTIMATES (was null):")
for e in ae[:4]: print("  ", e.get("date"), "rev_avg=", e.get("revenue_avg"), "eps_avg=", e.get("eps_avg"))
# 2) industry comparison (real PE)
ic=d.get("industry_comparison") or {}
print("\nINDUSTRY COMPARISON:")
print("  industry:", ic.get("industry"), "| industry_pe:", ic.get("industry_pe"), "| sector_pe:", ic.get("sector_pe"), "| as_of:", ic.get("as_of"))
print("  company pe:", (ic.get("company") or {}).get("pe"), "| pe_vs_industry:", ic.get("pe_vs_industry_pct"), "% | pe_vs_sector:", ic.get("pe_vs_sector_pct"),"%")
print("  industry_comparison_assessment:", str(d.get("industry_comparison_assessment"))[:240])
# 3) forward model
fm=d.get("forward_model") or {}
print("\nFORWARD MODEL present:", bool(fm))
print("  summary:", str(fm.get("summary"))[:240])
print("  revenue_projections:", [(p.get("year"),p.get("revenue_est"),p.get("growth_pct")) for p in (fm.get("revenue_projections") or [])])
print("  eps_projections:", [(p.get("year"),p.get("eps_est"),p.get("growth_pct")) for p in (fm.get("eps_projections") or [])])
pm=fm.get("price_model") or {}
print("  price_model: fwd_pe=", pm.get("forward_pe_applied"), "fair_base=", pm.get("fair_value_base"), "bull=", pm.get("fair_value_bull"), "bear=", pm.get("fair_value_bear"), "upside=", pm.get("upside_to_base_pct"),"%")
print("  dcf_cross_check:", str(pm.get("dcf_cross_check"))[:160])
print("  confidence:", fm.get("confidence"))
print("DONE 2258")
