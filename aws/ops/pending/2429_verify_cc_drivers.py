import boto3, json
s3=boto3.client("s3","us-east-1")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/cycle-clock.json")["Body"].read())
syn=d.get("synthesis") or {}
bd=[x.get("label") for x in (syn.get("bearish_drivers") or [])]
bu=[x.get("label") for x in (syn.get("bullish_drivers") or [])]
print("posture:",syn.get("posture"),syn.get("score"),"| n_risk_off:",syn.get("n_risk_off"),"n_risk_on:",syn.get("n_risk_on"))
print("crypto bearish drivers:",[l for l in bd if "Crypto" in l])
print("crypto bullish drivers:",[l for l in bu if "Crypto" in l])
print("ETF driver present:",any("ETF" in (l or "") for l in bd+bu))
print("DONE 2429")
