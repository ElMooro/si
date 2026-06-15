import json, boto3
from boto3.dynamodb.conditions import Attr
from decimal import Decimal
tbl=boto3.resource("dynamodb",region_name="us-east-1").Table("justhodl-signals")
items=[]; lek=None
for _ in range(20):
    kw=dict(FilterExpression=Attr("signal_type").eq("bottleneck_boom"),Limit=300)
    if lek: kw["ExclusiveStartKey"]=lek
    r=tbl.scan(**kw); items+=r.get("Items",[]); lek=r.get("LastEvaluatedKey")
    if not lek: break
def f(o):
    if isinstance(o,Decimal): return float(o)
    if isinstance(o,dict): return {k:f(v) for k,v in o.items()}
    if isinstance(o,list): return [f(v) for v in o]
    return o
print(f"total bottleneck_boom: {len(items)}")
graded=[i for i in items if i.get("outcomes")]
print(f"with non-empty outcomes: {len(graded)}")
from collections import Counter
print("by logged date:", dict(Counter((i.get("logged_at") or "")[:10] for i in items)))
print("status dist:", dict(Counter(i.get("status") for i in items)))
if graded:
    g=f(graded[0])
    print("\nsample signal_id:", g.get("signal_id"))
    print("outcomes:", json.dumps(g.get("outcomes"))[:600])
    print("accuracy_scores:", json.dumps(g.get("accuracy_scores"))[:300])
    print("baseline_price:", g.get("baseline_price"), "benchmark:", g.get("benchmark"))
