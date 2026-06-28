import boto3, json
s3=boto3.client("s3","us-east-1")
for bkt in ["justhodl-dashboard-live"]:
  for key in ["data/crypto-dvol.json","crypto-dvol.json","crypto-intel.json","data/crypto-intel.json"]:
    try:
        d=json.loads(s3.get_object(Bucket=bkt,Key=key)["Body"].read())
        btc=(d.get("btc") or {}); rs=(d.get("risk_score") or {})
        print(f"  {bkt}/{key}: OK  btc.dvol={btc.get('dvol')} risk_score.score={rs.get('score')}")
    except Exception as e:
        print(f"  {bkt}/{key}: {str(e)[:40]}")
print("DONE 2369")
