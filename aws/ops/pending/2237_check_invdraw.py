import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
try:
    c=lam.get_function("justhodl-inventory-drawdown" if False else "justhodl-inventory-drawdown")["Configuration"]
    env=c.get("Environment",{}).get("Variables",{})
    print("Lambda:",c.get("State"),c.get("LastUpdateStatus"),"| FRED_API_KEY set:",bool(env.get("FRED_API_KEY")))
    fredset=bool(env.get("FRED_API_KEY"))
except Exception as e:
    print("Lambda MISSING:",str(e)[:60]); fredset=False
# if FRED key missing, set it from global-liquidity (same fix as capital-inflows)
if not fredset:
    src=lam.get_function_configuration(FunctionName="justhodl-global-liquidity").get("Environment",{}).get("Variables",{})
    fk=src.get("FRED_API_KEY")
    if fk:
        cur=lam.get_function_configuration(FunctionName="justhodl-inventory-drawdown").get("Environment",{}).get("Variables",{})
        cur["FRED_API_KEY"]=fk
        lam.update_function_configuration(FunctionName="justhodl-inventory-drawdown",Environment={"Variables":cur})
        print("SET FRED_API_KEY on inventory-drawdown from global-liquidity")
        time.sleep(6)
        for _ in range(20):
            cc=lam.get_function("justhodl-inventory-drawdown")["Configuration"]
            if cc.get("LastUpdateStatus")=="Successful": break
            time.sleep(3)
# invoke + read output
try:
    r=lam.invoke(FunctionName="justhodl-inventory-drawdown",InvocationType="RequestResponse")
    print("invoke status:",r.get("StatusCode"))
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/inventory-drawdown.json")["Body"].read())
    print("generated_at:",d.get("generated_at"),"| counts:",json.dumps(d.get("counts")))
    sec=d.get("sector_drawdown") or d.get("sectors") or []
    print("SECTORS (falling I/S = drawdown):")
    for s in (sec if isinstance(sec,list) else [])[:8]:
        print("  ",s.get("sector"),"ratio_chg_12mo=",s.get("chg_12mo") or s.get("ratio_chg_12mo"),"class=",s.get("classification") or s.get("class"))
    bm=d.get("boom_setups") or d.get("boom_book") or []
    print("STOCK BOOM SETUPS (DIO falling + demand rising):",len(bm))
    for r2 in bm[:8]:
        print("  ",r2.get("ticker"),"boom=",r2.get("boom_score"),"dio_chg=",r2.get("dio_chg_pct"),"rev=",r2.get("rev_yoy") or r2.get("demand"),r2.get("sector"))
except Exception as e:
    import traceback;print("read fail:",str(e)[:100]);traceback.print_exc()
print("DONE 2237")
