import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
c=lam.get_function_configuration(FunctionName="justhodl-bottleneck-boom")
print("deployed LastModified:",c["LastModified"],"| timeout:",c["Timeout"])
# synchronous invoke to force the current code + wait for full run
r=lam.invoke(FunctionName="justhodl-bottleneck-boom",InvocationType="RequestResponse",Payload=b"{}")
print("FunctionError:",r.get("FunctionError"),"| resp:",r["Payload"].read().decode()[:160])
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom.json")["Body"].read())
print("version:",d.get("version"),"| dur:",d.get("duration_s"),"s")
print("phase counts:",d.get("capital_cycle_phase_counts"))
print("early signals logged:",d.get("early_signals_logged"),"| flood:",d.get("capacity_flood_warnings"))
print("INDUSTRY SUPPLY:")
for g,e in (d.get("industry_supply") or {}).items():
    print("  %s: phase=%s score=%s capex_yoy_med=%s capex/DA=%s %%ml=%s %%cut=%s util=%s(z%s)"%(
        g,e.get("capital_cycle_phase"),e.get("supply_cycle_score"),e.get("capex_yoy_median"),e.get("capex_to_da_median"),
        e.get("pct_money_losing"),e.get("pct_capex_cut"),e.get("cap_util"),e.get("cap_util_z")))
print("EARLY CALLS:")
for c2 in (d.get("early_bottleneck_calls") or [])[:6]:
    print("  %s (%s) %s | %s | capex %s%% capex/DA %s margin %s%% ml=%s"%(c2["ticker"],c2.get("group"),c2["score"],c2.get("phase"),c2.get("capex_yoy_pct"),c2.get("capex_to_da"),c2.get("net_margin_pct"),c2.get("money_losing")))
print("DONE 2450")
