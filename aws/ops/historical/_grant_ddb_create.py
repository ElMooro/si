"""Grant dynamodb:CreateTable + waiter perms to github-actions-justhodl user."""
import json
import boto3
from ops_report import report

iam = boto3.client("iam")
USER = "github-actions-justhodl"
POLICY_NAME = "justhodl-ddb-create-and-manage"


def main():
    with report("grant_ddb_create") as r:
        r.heading("Grant DynamoDB management to github-actions-justhodl")

        policy_doc = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "dynamodb:CreateTable",
                        "dynamodb:DescribeTable",
                        "dynamodb:UpdateTable",
                        "dynamodb:DeleteTable",
                        "dynamodb:ListTables",
                        "dynamodb:TagResource",
                        "dynamodb:UntagResource",
                        "dynamodb:UpdateTimeToLive",
                        "dynamodb:DescribeTimeToLive",
                        "dynamodb:DescribeContinuousBackups",
                        "dynamodb:UpdateContinuousBackups",
                        "dynamodb:GetItem",
                        "dynamodb:PutItem",
                        "dynamodb:UpdateItem",
                        "dynamodb:DeleteItem",
                        "dynamodb:Query",
                        "dynamodb:Scan",
                        "dynamodb:BatchGetItem",
                        "dynamodb:BatchWriteItem",
                    ],
                    "Resource": "*",
                }
            ],
        }

        try:
            iam.put_user_policy(
                UserName=USER,
                PolicyName=POLICY_NAME,
                PolicyDocument=json.dumps(policy_doc),
            )
            r.ok(f"  ✓ inline policy {POLICY_NAME} attached to {USER}")
        except Exception as e:
            r.log(f"  ✗ {e}")
            raise

        # Confirm
        resp = iam.get_user_policy(UserName=USER, PolicyName=POLICY_NAME)
        actions = resp["PolicyDocument"]["Statement"][0]["Action"]
        r.log(f"  ✓ verified {len(actions)} DDB actions granted")


if __name__ == "__main__":
    main()
