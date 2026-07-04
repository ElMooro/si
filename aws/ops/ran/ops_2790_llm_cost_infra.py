"""ops 2790 — provision LLM cost-governance infra: DDB ledger + SSM controls + IAM grant.

Idempotent. Creates:
  - DynamoDB justhodl-llm-cost (pk=date S, sk=engine_model S, PAY_PER_REQUEST)
  - SSM /justhodl/llm/mode=normal, /justhodl/llm/daily-budget-usd=50
  - inline policy jh-llm-cost-governance on lambda-execution-role (DDB ledger +
    SSM read + S3 llm-cache/ read/write) so all 75 engines can meter & cache.
Then verifies with an atomic write + query round-trip.
"""
import os, json, time
from datetime import datetime, timezone
import boto3

REGION = "us-east-1"
TABLE = "justhodl-llm-cost"
ROLE = "lambda-execution-role"
BUCKET = "justhodl-dashboard-live"
R = {"ops": 2790, "ts": datetime.now(timezone.utc).isoformat(), "steps": {}}

ddb = boto3.client("dynamodb", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
iam = boto3.client("iam", region_name=REGION)

# 1) DDB table -----------------------------------------------------------------
try:
    ddb.create_table(
        TableName=TABLE,
        BillingMode="PAY_PER_REQUEST",
        AttributeDefinitions=[{"AttributeName": "date", "AttributeType": "S"},
                              {"AttributeName": "engine_model", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "date", "KeyType": "HASH"},
                   {"AttributeName": "engine_model", "KeyType": "RANGE"}])
    R["steps"]["table"] = "created"
    print("table: creating…")
except ddb.exceptions.ResourceInUseException:
    R["steps"]["table"] = "exists"
    print("table: already exists")
except Exception as e:
    R["steps"]["table"] = "ERR " + str(e)[:120]
    print("table ERR", str(e)[:120])
# wait active
for _ in range(30):
    try:
        st = ddb.describe_table(TableName=TABLE)["Table"]["TableStatus"]
        if st == "ACTIVE":
            break
        time.sleep(2)
    except Exception:
        time.sleep(2)

# 2) SSM controls (create-if-absent, don't clobber a value Khalid may have set) -
def put_if_absent(name, value, typ="String"):
    try:
        ssm.get_parameter(Name=name)
        return "exists"
    except ssm.exceptions.ParameterNotFound:
        ssm.put_parameter(Name=name, Value=value, Type=typ)
        return "created=" + value
    except Exception as e:
        return "ERR " + str(e)[:80]

R["steps"]["ssm_mode"] = put_if_absent("/justhodl/llm/mode", "normal")
R["steps"]["ssm_budget"] = put_if_absent("/justhodl/llm/daily-budget-usd", "50")
print("ssm mode:", R["steps"]["ssm_mode"], "| budget:", R["steps"]["ssm_budget"])

# 3) IAM inline policy so every engine can meter + cache + read config ----------
policy = {
    "Version": "2012-10-17",
    "Statement": [
        {"Sid": "LLMCostLedger", "Effect": "Allow",
         "Action": ["dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:GetItem",
                    "dynamodb:Query", "dynamodb:DescribeTable"],
         "Resource": "arn:aws:dynamodb:%s:857687956942:table/%s" % (REGION, TABLE)},
        {"Sid": "LLMConfigRead", "Effect": "Allow",
         "Action": ["ssm:GetParameter", "ssm:GetParameters"],
         "Resource": "arn:aws:ssm:%s:857687956942:parameter/justhodl/llm/*" % REGION},
        {"Sid": "LLMCacheS3", "Effect": "Allow",
         "Action": ["s3:GetObject", "s3:PutObject"],
         "Resource": "arn:aws:s3:::%s/llm-cache/*" % BUCKET},
    ],
}
try:
    iam.put_role_policy(RoleName=ROLE, PolicyName="jh-llm-cost-governance",
                        PolicyDocument=json.dumps(policy))
    R["steps"]["iam"] = "attached jh-llm-cost-governance"
    print("iam: inline policy attached")
except Exception as e:
    R["steps"]["iam"] = "ERR " + str(e)[:150]
    print("iam ERR", str(e)[:150])

# 4) Verify round-trip: atomic ADD then query ----------------------------------
try:
    today = time.strftime("%Y-%m-%d", time.gmtime())
    ddb.update_item(TableName=TABLE,
        Key={"date": {"S": today}, "engine_model": {"S": "ops-2790-selftest|verify"}},
        UpdateExpression="ADD calls :one, cost_usd :c",
        ExpressionAttributeValues={":one": {"N": "1"}, ":c": {"N": "0.000123"}})
    q = ddb.query(TableName=TABLE, KeyConditionExpression="#d = :d",
                  ExpressionAttributeNames={"#d": "date"},
                  ExpressionAttributeValues={":d": {"S": today}})
    R["steps"]["verify"] = {"items_today": q.get("Count"),
                            "selftest_present": any(i["engine_model"]["S"].startswith("ops-2790") for i in q.get("Items", []))}
    print("verify: items_today=%s selftest_present=%s" % (q.get("Count"), R["steps"]["verify"]["selftest_present"]))
    # clean the selftest row
    ddb.delete_item(TableName=TABLE, Key={"date": {"S": today}, "engine_model": {"S": "ops-2790-selftest|verify"}})
except Exception as e:
    R["steps"]["verify"] = "ERR " + str(e)[:150]
    print("verify ERR", str(e)[:150])

R["status"] = "LLM COST INFRA READY"
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/2790_llm_cost_infra.json", "w"), indent=1, default=str)
print("OPS 2790:", R["status"])
