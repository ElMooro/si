import boto3, json
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
r=lam.invoke(FunctionName="justhodl-bottleneck-boom",InvocationType="RequestResponse",Payload=b"{}")
print("FunctionError:",r.get("FunctionError"))
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom.json")["Body"].read())
print("version:",d.get("version"),"dur:",d.get("duration_s"))
P=d.get("ppi_pricing") or {}
print("=== #5 PPI PRICING POWER ===")
for o in P.get("ppi_inputs",[]):
    print("  %-44s flag=%-16s ind=%-15s names=%s"%(str(o.get("desc") or o.get("input"))[:44],o.get("flag"),o.get("industry"),o.get("capturing_names")))
print("tightening_inputs:",P.get("tightening_inputs"),"| pricing_power_building:",P.get("pricing_power_building"))
L=d.get("leading_bottleneck_read") or {}
print("CAPSTONE:",L.get("forward_state"),L.get("n_confirmations"),"confirms")
print("DONE 2483")
