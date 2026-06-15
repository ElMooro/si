import json, boto3
ddb=boto3.client("dynamodb",region_name="us-east-1")
# scan for bottleneck_boom signals
items=[]; lek=None
for _ in range(20):
    kw={"TableName":"justhodl-signals","Limit":300,
        "FilterExpression":"signal_type = :t","ExpressionAttributeValues":{":t":{"S":"bottleneck_boom"}}}
    if lek: kw["ExclusiveStartKey"]=lek
    r=ddb.scan(**kw); items+=r.get("Items",[]); lek=r.get("LastEvaluatedKey")
    if not lek: break
print(f"bottleneck_boom signals found: {len(items)}")
def D(it):  # de-dynamo
    def v(x):
        t,val=list(x.items())[0]
        if t=="N": return float(val)
        if t in ("S","BOOL"): return val
        if t=="M": return {k:v(z) for k,v in val.items()} if False else {k:v2 for k,v2 in [(k2,v(z2)) for k2,z2 in val.items()]}
        if t=="L": return [v(z) for z in val]
        return val
    return {k:v(val) for k,val in it.items()}
graded=[i for i in items if i.get("outcomes")]
print(f"with outcomes: {len(graded)}")
import datetime
# show fields + a graded sample
if items:
    s=D(items[0]); print("\nfields:", sorted(s.keys()))
    print("check_windows:", s.get("check_windows"), "| baseline_price:", s.get("baseline_price"), "| benchmark:", s.get("baseline_benchmark_price"))
if graded:
    g=D(graded[0]); print("\nGRADED sample:", g.get("signal_id"))
    print("  outcomes:", json.dumps(g.get("outcomes"))[:500])
    print("  accuracy_scores:", json.dumps(g.get("accuracy_scores"))[:300])
    print("  status:", g.get("status"))
# date range
dts=sorted(D(i).get("ts_iso") or str(D(i).get("logged_at")) for i in items if i.get("ts_iso") or i.get("logged_at"))
print("\nlogged range:", dts[0] if dts else None, "->", dts[-1] if dts else None)
