import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
lam.invoke(FunctionName="justhodl-fedwatch-rate-probability",InvocationType="RequestResponse",Payload=b"{}")
time.sleep(3)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/fedwatch.json")["Body"].read())
print("6mo summary:", json.dumps(d.get("next_6mo_summary")))
print("next meeting:", (d.get("next_meeting") or {}).get("date"), (d.get("next_meeting") or {}).get("implied_move_bps"),"bps")
print("DONE 2341")
