import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
for _ in range(20):
    c=lam.get_function(FunctionName="justhodl-strategist")["Configuration"]
    if c.get("LastUpdateStatus")=="Successful": break
    time.sleep(4)
lam.invoke(FunctionName="justhodl-strategist",InvocationType="RequestResponse")
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/strategist.json")["Body"].read())
fl=d["fleet"]
print("REBALANCED FLEET: fresh",fl["n_fresh"],"| +",fl["n_positive"],"/ -",fl["n_negative"],"/ ~",fl["n_neutral"],
      "| risk-on wt",fl["risk_on_weight"],"vs risk-off",fl["risk_off_weight"],"| CONSENSUS",fl["consensus"])
# show the risk-on regime engines now in the read
loud={i["engine"]:i for i in d["loudest_engines"]}
for w in ["trend-engine","master-ranker","market-internals","regime-composite","risk-regime","activity-nowcast","construction-housing","stablecoin-flow","move-index"]:
    i=loud.get(w)
    print(f"  {w:<20} {('dir '+str(i['direction'])+'  '+str(i['verdict'])[:24]) if i else '(not in top-55 loudest)'}")
it=d.get("interpretation") or {}
if not (it.get("raw") or it.get("error")):
    print("\nDRIVER:",it.get("dominant_driver"))
    print("CALL:",str(it.get("decisive_call"))[:220])
    print("CONVICTION:",it.get("conviction"))
print("DONE 2055")
