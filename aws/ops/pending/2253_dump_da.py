import boto3, json
s3=boto3.client("s3","us-east-1")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="equity-research/LDOS.json")["Body"].read())
da=d.get("devils_advocate") or {}
print("devils_advocate keys:", list(da.keys()))
print("title:", da.get("title"))
print("short_thesis:", str(da.get("short_thesis"))[:400])
print("kill_points:", json.dumps(da.get("kill_points"))[:400])
print("what_bulls_underestimate:", str(da.get("what_bulls_underestimate"))[:200])
print("DONE 2253")
