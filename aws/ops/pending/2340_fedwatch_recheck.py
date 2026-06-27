import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
lam.invoke(FunctionName="justhodl-fedwatch-rate-probability",InvocationType="RequestResponse",Payload=b"{}")
time.sleep(3)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/fedwatch.json")["Body"].read())
print("6mo summary:", json.dumps(d.get("next_6mo_summary")))
print("per-meeting (date / incremental move / cumulative-from-today / post rate):")
for m in (d.get("meetings_ahead") or [])[:6]:
    if m.get("status")=="ok":
        print(f"  {m['date']}  incr {m.get('implied_move_bps'):+}bps  cum {m.get('cumulative_from_today_bps'):+}bps  post {m.get('implied_post_meeting_rate_pct')}%")
print("DONE 2340")
