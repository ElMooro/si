import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=320,connect_timeout=20,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
try:
    lam.invoke(FunctionName="justhodl-strategist",InvocationType="RequestResponse")
except Exception as e:
    print("invoke note:",str(e)[:80])
time.sleep(3)
d=json.loads(s3.get_object(Bucket=B,Key="data/strategist.json")["Body"].read())
fl=d["fleet"]
print("REBALANCED: fresh",fl["n_fresh"],"| +",fl["n_positive"],"/ -",fl["n_negative"],"/ ~",fl["n_neutral"],
      "| risk-on wt",fl["risk_on_weight"],"vs risk-off",fl["risk_off_weight"],"| CONSENSUS",fl["consensus"])
loud={i["engine"]:i for i in d["loudest_engines"]}
print("risk-ON regime engines now in read:")
for w in ["trend-engine","master-ranker","market-internals","activity-nowcast","regime-composite"]:
    i=loud.get(w); print(f"  {w:<18}",(f"dir {i['direction']}  {str(i['verdict'])[:26]}" if i else "(not in top-55)"))
print("risk-OFF engines:")
for w in ["construction-housing","stablecoin-flow","move-index","sector-tilt"]:
    i=loud.get(w); print(f"  {w:<18}",(f"dir {i['direction']}  {str(i['verdict'])[:26]}" if i else "(not in top-55)"))
it=d.get("interpretation") or {}
if not (it.get("raw") or it.get("error")):
    print("\nDRIVER:",it.get("dominant_driver"))
    print("CALL:",str(it.get("decisive_call"))[:240])
    print("CONVICTION:",it.get("conviction"))
print("most-backed:",[x['ticker'] for x in d.get("most_backed_names",[])[:10]])
print("DONE 2056")
