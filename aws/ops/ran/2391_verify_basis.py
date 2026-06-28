import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=120,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
try:
    lam.get_function_configuration(FunctionName="justhodl-crypto-basis"); print("function created OK")
except Exception as e:
    print("MISSING:",str(e)[:80]); print("DONE 2391"); raise SystemExit
r=lam.invoke(FunctionName="justhodl-crypto-basis",InvocationType="RequestResponse",Payload=b"{}")
print("FunctionError:",r.get("FunctionError"),"| resp:",r["Payload"].read().decode()[:170])
time.sleep(3)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/crypto-basis.json")["Body"].read())
for c in ("btc","eth"):
    s=d.get(c) or {}
    print(f"\n{c.upper()}: index ${s.get('index')} | perp prem {s.get('perp_premium_pct')}% | funding ann {s.get('funding_annualized_pct')}%")
    print(f"  30d basis ann {s.get('basis_30d_ann_pct')}% | 3m cash&carry {s.get('cash_and_carry_yield_3m_pct')}% | {s.get('regime')}")
    print("  curve (days: ann basis%):")
    for r2 in (s.get("curve") or [])[:9]:
        print(f"     {r2['days']:>6}d  {r2['annualized_basis_pct']:>7}%  (basis {r2['basis_pct']}%)")
print("\nread:",d.get("interpretation"),"| history_n:",d.get("history_n"),"| _diag:",d.get("_diag"))
print("DONE 2391")
