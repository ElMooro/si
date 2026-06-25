import boto3, json
from datetime import date
from boto3.dynamodb.conditions import Attr
ddb=boto3.resource("dynamodb","us-east-1"); t=ddb.Table("justhodl-signals")
items=[]; kw={"FilterExpression":Attr("signal_type").begins_with("crypto_ma200")}
while True:
    r=t.scan(**kw); items+=r.get("Items",[])
    lek=r.get("LastEvaluatedKey")
    if not lek: break
    kw["ExclusiveStartKey"]=lek
print(f"crypto signals in ledger: {len(items)}\n")
bad=[]
for it in items:
    md=it.get("metadata") or {}
    ld=md.get("log_date"); la=(it.get("logged_at") or "")[:10]
    gap=None
    if ld and la:
        try: gap=(date.fromisoformat(la)-date.fromisoformat(ld)).days
        except Exception: pass
    flag="" 
    if gap is not None and gap>7: flag=" <-- STALE log_date (logged during buffer bug)"; bad.append(it["signal_id"])
    print(f"  {it['signal_id']:<32} log_date={ld} logged={la} gap={gap}d status={it.get('status')} outcomes={list((it.get('outcomes') or {}).keys())}{flag}")
print(f"\nBAD (stale log_date) to purge: {len(bad)}")
for b in bad: print("   ",b)
print("DONE 2174")
