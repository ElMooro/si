import boto3, json
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
r=lam.invoke(FunctionName="justhodl-bottleneck-boom",InvocationType="RequestResponse",Payload=b"{}")
print("FunctionError:",r.get("FunctionError"),"| resp:",r["Payload"].read().decode()[:140])
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom.json")["Body"].read())
print("version:",d.get("version"),"| dur:",d.get("duration_s"),"s | universe_n:",d.get("universe_n"),"| scored:",d.get("scored_n"))
print("phase counts:",d.get("capital_cycle_phase_counts"),"| early logged:",d.get("early_signals_logged"))
print("\nINDUSTRY SUPPLY:")
for g,e in (d.get("industry_supply") or {}).items():
    print("  %-22s phase=%-18s | capex_yoy_med=%s capex/DA=%s | %%ml=%s %%cut=%s %%druck=%s | util=%s(z%s,6mo%s)"%(
        g,e.get("capital_cycle_phase"),e.get("capex_yoy_median"),e.get("capex_to_da_median"),
        e.get("pct_money_losing"),e.get("pct_capex_cut"),e.get("pct_druckenmiller"),e.get("cap_util"),e.get("cap_util_z"),e.get("cap_util_6mo_chg")))
print("\nEARLY BOTTLENECK CALLS (now with cyclical universe + consensus gap):")
for c in (d.get("early_bottleneck_calls") or [])[:8]:
    print("  %-6s %-24s sc=%s | %-18s | capex %s%% capex/DA %s margin %s%% ml=%s | Street fwd %s%% gap=%s"%(
        c["ticker"],(c.get("name") or "")[:24],c["score"],c.get("phase"),c.get("capex_yoy_pct"),c.get("capex_to_da"),
        c.get("net_margin_pct"),c.get("money_losing"),c.get("consensus_fwd_growth_pct"),c.get("consensus_gap_score")))
print("DONE 2452")
