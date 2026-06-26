import boto3, json
ev=boto3.client("events","us-east-1"); s3=boto3.client("s3","us-east-1")
rs=[(r["Name"],r.get("ScheduleExpression"),r.get("State")) for r in ev.list_rules(NamePrefix="justhodl-supply-inflection").get("Rules",[])]
print("supply-inflection rules:", rs or "NONE")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/supply-inflection.json")["Body"].read())
for L in ("signals","all_signals","inflections","scored","results"):
    arr=d.get(L)
    if isinstance(arr,list) and arr:
        print(f"\nINPUT SIGNALS ('{L}', n={len(arr)}), sorted by score:")
        for s in sorted(arr,key=lambda x:-(x.get('score') or 0))[:20]:
            print(f"   {str(s.get('name') or s.get('signal') or s.get('id'))[:26]:<26} score={s.get('score')} dir={s.get('direction')} etfs={s.get('themes') or s.get('etfs') or s.get('beneficiary_etfs')}")
        break
else:
    print("signal list under a different key; top keys:", list(d.keys()))
print("DONE 2227")
