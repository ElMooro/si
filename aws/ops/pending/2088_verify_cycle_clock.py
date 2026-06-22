import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=180,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
try:
    c=lam.get_function(FunctionName="justhodl-cycle-clock")["Configuration"]
    print("function EXISTS | state",c.get("State"))
    for _ in range(20):
        c=lam.get_function(FunctionName="justhodl-cycle-clock")["Configuration"]
        if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": break
        time.sleep(3)
    print("invoke:",lam.invoke(FunctionName="justhodl-cycle-clock",InvocationType="RequestResponse")["Payload"].read().decode()[:200])
    time.sleep(2)
    d=json.loads(s3.get_object(Bucket=B,Key="data/cycle-clock.json")["Body"].read())
    print("\nVERDICT:",d["verdict"])
    cy=d["cycle"];lq=d["liquidity"]
    print(f"\nCYCLE: {cy['phase']} (phase_n {cy['phase_n']}, {cy['quadrant']}) | growth {cy['growth_direction']} infl {cy['inflation_direction']} | conf {cy['confidence']} | froth {cy['froth_markers']}")
    print(f"  US {cy['us_cycle_score']}/{cy['us_cycle_level']} · global {cy['global_phase']} CLI {cy['global_avg_cli']} · nowcast {cy['nowcast_regime']}")
    print(f"\nSQUEEZE RISK: {lq['squeeze_risk_score']} = {lq['level']} | aggregate liq {lq['aggregate_liquidity_regime']}")
    print(f"  flickers: {lq['flickers']}")
    print(f"\nDIVERGENCES ({len(d['divergences'])}):")
    for dv in d["divergences"]: print("  •",dv[:150])
    print("\navailability:",d["availability"])
    print("stale:",d["stale"])
    print("CREATED_OK")
except lam.exceptions.ResourceNotFoundException:
    print("NOT_CREATED")
print("DONE 2088")
