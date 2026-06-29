import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
lam.invoke(FunctionName="justhodl-bottleneck-boom",InvocationType="Event",Payload=b"{}")
print("bottleneck-boom async; waiting 180s (4 FMP calls/ticker)..."); time.sleep(180)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom.json")["Body"].read())
print("version:",d.get("version"),"| scored:",d.get("scored_n"),"| dur:",d.get("duration_s"),"s")
print("phase counts:",d.get("capital_cycle_phase_counts"))
print("flood warnings:",d.get("capacity_flood_warnings"))
print("early signals logged:",d.get("early_signals_logged"))
print("\nINDUSTRY SUPPLY:")
for g,e in (d.get("industry_supply") or {}).items():
    print("  %s: phase=%s score=%s | capex_yoy_med=%s capex/DA_med=%s | %%money_losing=%s %%capex_cut=%s | util=%s(z%s,6mo%s)%s"%(
        g,e.get("capital_cycle_phase"),e.get("supply_cycle_score"),e.get("capex_yoy_median"),e.get("capex_to_da_median"),
        e.get("pct_money_losing"),e.get("pct_capex_cut"),e.get("cap_util"),e.get("cap_util_z"),e.get("cap_util_6mo_chg"),
        " mo_to_tight=%s"%e.get("est_months_to_tightness") if e.get("est_months_to_tightness") else ""))
print("\nEARLY BOTTLENECK CALLS (the Druckenmiller 'born in the bust' list):")
for c in (d.get("early_bottleneck_calls") or [])[:8]:
    print("  %s (%s) score %s | %s %s | capex %s%% yoy, capex/DA %s, margin %s%%, money_losing=%s"%(
        c["ticker"],c.get("group"),c["score"],c.get("phase"),"",c.get("capex_yoy_pct"),c.get("capex_to_da"),c.get("net_margin_pct"),c.get("money_losing")))
print("\nsample rank supply fields:",{k:(d.get("ranks") or [{}])[0].get(k) for k in ("ticker","capex_yoy_pct","capex_to_da","money_losing","druckenmiller_setup","capital_cycle_phase","early_bottleneck_score")})
print("DONE 2449")
