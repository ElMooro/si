import boto3, json, urllib.request, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
# 1) is my code deployed?
import io,zipfile
loc=lam.get_function(FunctionName="justhodl-supply-inflection-scanner")["Code"]["Location"]
code=urllib.request.urlopen(loc,timeout=30).read()
z=zipfile.ZipFile(io.BytesIO(code))
src=z.read("lambda_function.py").decode("utf-8","replace")
print("deployed code has SEMI_PPI:", "SEMI_PPI" in src, "| has PCU33443344:", "PCU33443344" in src)
print("INFLECTION_SIGNALS count in deployed code:", src.count('"src":'))
# 2) what 18 signals are in output
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/supply-inflection.json")["Body"].read())
print("output signal names:", list((d.get('signals') or {}).keys()))
fs=d.get("fetch_stats") or {}
print("fetch_stats:", json.dumps(fs)[:200])
# 3) test the engine's fred path for one new series
fk=lam.get_function_configuration(FunctionName="justhodl-supply-inflection-scanner").get("Environment",{}).get("Variables",{})
print("engine env has FRED_KEY:", bool(fk.get("FRED_KEY")), "| FRED_API_KEY:", bool(fk.get("FRED_API_KEY")))
print("DONE 2228")
