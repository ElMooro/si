import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
r=lam.invoke(FunctionName="justhodl-bottleneck-boom",InvocationType="RequestResponse",Payload=b"{}")
print("FunctionError:",r.get("FunctionError"),"| resp:",r["Payload"].read().decode()[:160])
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom.json")["Body"].read())
print("version:",d.get("version"),"| dur:",d.get("duration_s"),"s | scored:",d.get("scored_n"))
print("phase counts:",d.get("capital_cycle_phase_counts"),"| early logged:",d.get("early_signals_logged"),"| flood:",d.get("capacity_flood_warnings"))
print("\nINDUSTRY SUPPLY (capital-cycle phase per group):")
for g,e in (d.get("industry_supply") or {}).items():
    print("  %-22s phase=%-18s score=%s | capex_yoy_med=%s capex/DA=%s | %%ml=%s %%cut=%s %%druck=%s | util=%s z=%s 6mo=%s"%(
        g,e.get("capital_cycle_phase"),e.get("supply_cycle_score"),e.get("capex_yoy_median"),e.get("capex_to_da_median"),
        e.get("pct_money_losing"),e.get("pct_capex_cut"),e.get("pct_druckenmiller"),e.get("cap_util"),e.get("cap_util_z"),e.get("cap_util_6mo_chg")))
print("\nEARLY BOTTLENECK CALLS (born-in-the-bust):")
for c in (d.get("early_bottleneck_calls") or [])[:8]:
    print("  %-6s %-26s %s | %-18s | capex %s%% capex/DA %s margin %s%% ml=%s"%(
        c["ticker"],(c.get("name") or "")[:26],c["score"],c.get("phase"),c.get("capex_yoy_pct"),c.get("capex_to_da"),c.get("net_margin_pct"),c.get("money_losing")))
print("DONE 2451")
