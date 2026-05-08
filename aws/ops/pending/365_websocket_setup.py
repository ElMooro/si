#!/usr/bin/env python3
"""
Step 365 — bootstrap WebSocket pipeline.

  1. Create DynamoDB table WebSocketConnections (PK=connectionId, on-demand)
  2. Create API Gateway v2 WebSocket API "justhodl-wss"
  3. Create 3 routes ($connect, $disconnect, $default) wired to handler Lambda
  4. Deploy stage "prod"
  5. Add invoke permission for API Gateway → handler Lambda
  6. Set env vars on handler + broadcast Lambdas (DDB_TABLE, WS_API_ID, WS_STAGE)
  7. IAM updates on lambda-execution-role:
       - DDB read/write on WebSocketConnections
       - execute-api:ManageConnections on the new WS API
  8. Create Function URL on broadcast Lambda (CORS)
  9. Add S3 event notifications on tracked keys → invoke broadcast Lambda
 10. Add Lambda permission for S3 to invoke broadcast Lambda

Idempotent — safe to re-run; only creates what's missing.
Writes the WSS endpoint URL to SSM /justhodl/wss/endpoint for frontend to fetch.
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

REPORT = "aws/ops/reports/365_websocket_setup.json"
ACCOUNT = "857687956942"
REGION = "us-east-1"
ROLE_NAME = "lambda-execution-role"
HANDLER_FN = "openbb-websocket-handler"
BROADCAST_FN = "openbb-websocket-broadcast"
DDB_TABLE = "WebSocketConnections"
API_NAME = "justhodl-wss"
STAGE_NAME = "prod"
S3_BUCKET = "justhodl-dashboard-live"
TRACKED_KEYS = [
    "data/report.json", "data/macro-nowcast.json", "data/compound-signals.json",
    "data/cross-asset-regime.json", "data/options-flow.json",
    "data/eurodollar-stress.json", "data/nobrainers.json", "data/narrative-density.json",
]

ddb = boto3.client("dynamodb", region_name=REGION)
apigw = boto3.client("apigatewayv2", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
iam = boto3.client("iam")
s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


def step_create_ddb(out):
    try:
        ddb.describe_table(TableName=DDB_TABLE)
        out["ddb"] = "already_exists"
        return
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
    ddb.create_table(
        TableName=DDB_TABLE,
        AttributeDefinitions=[{"AttributeName": "connectionId", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "connectionId", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
        Tags=[{"Key": "project", "Value": "justhodl"}, {"Key": "feature", "Value": "websocket"}],
    )
    ddb.get_waiter("table_exists").wait(TableName=DDB_TABLE)
    out["ddb"] = "created"


def step_create_websocket_api(out):
    # Look for existing
    apis = apigw.get_apis()
    existing = [a for a in apis.get("Items", []) if a.get("Name") == API_NAME and a.get("ProtocolType") == "WEBSOCKET"]
    if existing:
        api_id = existing[0]["ApiId"]
        out["wss_api"] = {"id": api_id, "endpoint": existing[0].get("ApiEndpoint"), "preexisting": True}
        return api_id
    resp = apigw.create_api(
        Name=API_NAME,
        ProtocolType="WEBSOCKET",
        RouteSelectionExpression="$request.body.action",
        Description="JustHodl.AI live data push channel",
        Tags={"project": "justhodl", "feature": "websocket"},
    )
    api_id = resp["ApiId"]
    out["wss_api"] = {"id": api_id, "endpoint": resp.get("ApiEndpoint"), "preexisting": False}
    return api_id


def step_create_routes(out, api_id):
    # Get existing routes
    routes = apigw.get_routes(ApiId=api_id).get("Items", [])
    existing_keys = {r["RouteKey"]: r for r in routes}
    handler_arn = lam.get_function(FunctionName=HANDLER_FN)["Configuration"]["FunctionArn"]
    integration_uri = f"arn:aws:apigateway:{REGION}:lambda:path/2015-03-31/functions/{handler_arn}/invocations"

    # Get or create integration
    integrations = apigw.get_integrations(ApiId=api_id).get("Items", [])
    int_match = [i for i in integrations if i.get("IntegrationUri") == integration_uri]
    if int_match:
        integration_id = int_match[0]["IntegrationId"]
    else:
        integration_id = apigw.create_integration(
            ApiId=api_id, IntegrationType="AWS_PROXY",
            IntegrationUri=integration_uri, IntegrationMethod="POST",
            ContentHandlingStrategy="CONVERT_TO_TEXT",
        )["IntegrationId"]

    out["integration_id"] = integration_id
    out["routes"] = {}
    for key in ["$connect", "$disconnect", "$default"]:
        if key in existing_keys:
            out["routes"][key] = "already_exists"
            continue
        apigw.create_route(
            ApiId=api_id, RouteKey=key,
            Target=f"integrations/{integration_id}",
            AuthorizationType="NONE",
        )
        out["routes"][key] = "created"


def step_deploy(out, api_id):
    # Find existing stage
    try:
        apigw.get_stage(ApiId=api_id, StageName=STAGE_NAME)
        # Just create a deployment to push latest changes
        dep = apigw.create_deployment(ApiId=api_id, Description="365 redeploy")
        apigw.update_stage(ApiId=api_id, StageName=STAGE_NAME, DeploymentId=dep["DeploymentId"])
        out["stage"] = "redeployed"
    except apigw.exceptions.NotFoundException:
        dep = apigw.create_deployment(ApiId=api_id, Description="initial deploy")
        apigw.create_stage(ApiId=api_id, StageName=STAGE_NAME, DeploymentId=dep["DeploymentId"],
                           AutoDeploy=True, Description="JustHodl WSS prod")
        out["stage"] = "created"


def step_lambda_invoke_perm(out, api_id):
    """API Gateway must be allowed to invoke the handler Lambda."""
    statement_id = "apigateway-wss-invoke"
    source_arn = f"arn:aws:execute-api:{REGION}:{ACCOUNT}:{api_id}/*/*"
    try:
        lam.add_permission(
            FunctionName=HANDLER_FN, StatementId=statement_id,
            Action="lambda:InvokeFunction", Principal="apigateway.amazonaws.com",
            SourceArn=source_arn,
        )
        out["lambda_invoke_perm"] = "added"
    except lam.exceptions.ResourceConflictException:
        out["lambda_invoke_perm"] = "already_exists"


def step_lambda_envs(out, api_id):
    """Update env vars on handler + broadcast Lambdas."""
    # Wait for any pending updates to settle, then update.
    for fn in [HANDLER_FN, BROADCAST_FN]:
        try:
            lam.get_waiter("function_updated").wait(FunctionName=fn)
        except Exception:
            pass
        env = {"DDB_TABLE": DDB_TABLE, "WS_API_ID": api_id, "WS_STAGE": STAGE_NAME}
        lam.update_function_configuration(FunctionName=fn, Environment={"Variables": env})
    out["lambda_envs"] = "updated"


def step_iam(out, api_id):
    """Two inline policies on lambda-execution-role."""
    # DDB
    ddb_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem",
                       "dynamodb:DeleteItem", "dynamodb:Scan"],
            "Resource": f"arn:aws:dynamodb:{REGION}:{ACCOUNT}:table/{DDB_TABLE}",
        }],
    }
    iam.put_role_policy(RoleName=ROLE_NAME, PolicyName="ddb-websocket-connections",
                        PolicyDocument=json.dumps(ddb_policy))
    # ManageConnections
    mc_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": ["execute-api:ManageConnections"],
            "Resource": f"arn:aws:execute-api:{REGION}:{ACCOUNT}:{api_id}/*/*/@connections/*",
        }],
    }
    iam.put_role_policy(RoleName=ROLE_NAME, PolicyName="execute-api-manage-connections",
                        PolicyDocument=json.dumps(mc_policy))
    out["iam"] = "policies_attached"


def step_broadcast_function_url(out):
    cors = {
        "AllowOrigins": ["https://justhodl.ai", "https://www.justhodl.ai"],
        "AllowMethods": ["GET", "POST"],
        "AllowHeaders": ["Content-Type", "X-Justhodl-Admin-Token"],
        "ExposeHeaders": [], "MaxAge": 3600, "AllowCredentials": False,
    }
    try:
        resp = lam.create_function_url_config(
            FunctionName=BROADCAST_FN, AuthType="NONE", Cors=cors, InvokeMode="BUFFERED",
        )
        out["broadcast_function_url"] = {"created": True, "url": resp["FunctionUrl"]}
    except lam.exceptions.ResourceConflictException:
        existing = lam.get_function_url_config(FunctionName=BROADCAST_FN)
        out["broadcast_function_url"] = {"created": False, "url": existing["FunctionUrl"]}
        try:
            lam.update_function_url_config(FunctionName=BROADCAST_FN, AuthType="NONE",
                                           Cors=cors, InvokeMode="BUFFERED")
        except Exception:
            pass
    # Public invoke perm
    try:
        lam.add_permission(
            FunctionName=BROADCAST_FN, StatementId="FunctionURLAllowPublicAccess",
            Action="lambda:InvokeFunctionUrl", Principal="*",
            FunctionUrlAuthType="NONE",
        )
        out["broadcast_public_perm"] = "added"
    except lam.exceptions.ResourceConflictException:
        out["broadcast_public_perm"] = "already_exists"


def step_s3_event_notifications(out):
    """Allow S3 to invoke broadcast Lambda + add notification config."""
    # 1. Lambda permission for S3
    try:
        lam.add_permission(
            FunctionName=BROADCAST_FN, StatementId="s3-invoke-broadcast",
            Action="lambda:InvokeFunction", Principal="s3.amazonaws.com",
            SourceArn=f"arn:aws:s3:::{S3_BUCKET}",
        )
        out["s3_lambda_perm"] = "added"
    except lam.exceptions.ResourceConflictException:
        out["s3_lambda_perm"] = "already_exists"
    # 2. Bucket notification configuration
    broadcast_arn = lam.get_function(FunctionName=BROADCAST_FN)["Configuration"]["FunctionArn"]
    # Read existing config
    existing = s3.get_bucket_notification_configuration(Bucket=S3_BUCKET)
    lambda_configs = existing.get("LambdaFunctionConfigurations", []) or []
    # Replace any of our previous configs (Id starts with "ws-broadcast-")
    lambda_configs = [c for c in lambda_configs if not (c.get("Id") or "").startswith("ws-broadcast-")]
    # Add one config per tracked key
    for i, key in enumerate(TRACKED_KEYS):
        lambda_configs.append({
            "Id": f"ws-broadcast-{i:02d}-{key.split('/')[-1]}",
            "LambdaFunctionArn": broadcast_arn,
            "Events": ["s3:ObjectCreated:*"],
            "Filter": {"Key": {"FilterRules": [{"Name": "prefix", "Value": key}]}},
        })
    new_config = {k: v for k, v in existing.items() if k != "ResponseMetadata" and v}
    new_config["LambdaFunctionConfigurations"] = lambda_configs
    s3.put_bucket_notification_configuration(Bucket=S3_BUCKET, NotificationConfiguration=new_config)
    out["s3_event_configs"] = len(lambda_configs)


def step_save_endpoint_in_ssm(out, api_id):
    endpoint = f"wss://{api_id}.execute-api.{REGION}.amazonaws.com/{STAGE_NAME}"
    ssm.put_parameter(
        Name="/justhodl/wss/endpoint", Value=endpoint, Type="String",
        Description="WebSocket endpoint for live data push", Overwrite=True,
    )
    out["wss_endpoint"] = endpoint
    # Patch wss-client.js with real URL so frontend connects without an extra fetch
    client_path = "wss-client.js"
    if os.path.isfile(client_path):
        with open(client_path) as f:
            content = f.read()
        new_content = content.replace(
            "wss://__WS_API_ID__.execute-api.us-east-1.amazonaws.com/prod",
            endpoint,
        )
        if new_content != content:
            with open(client_path, "w") as f:
                f.write(new_content)
            out["wss_client_patched"] = "yes"
        else:
            out["wss_client_patched"] = "no_placeholder_found (already patched or missing)"


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "steps": {}}
    try:
        step_create_ddb(out["steps"])
        api_id = step_create_websocket_api(out["steps"])
        step_create_routes(out["steps"], api_id)
        step_lambda_invoke_perm(out["steps"], api_id)
        step_iam(out["steps"], api_id)
        # Update Lambda code from local source (deploy-lambdas only updates existing
        # functions and these DO exist, so the standard workflow handles code).
        # Just update env vars here.
        step_lambda_envs(out["steps"], api_id)
        step_deploy(out["steps"], api_id)
        step_broadcast_function_url(out["steps"])
        step_s3_event_notifications(out["steps"])
        step_save_endpoint_in_ssm(out["steps"], api_id)
        out["status"] = "success"
    except Exception as e:
        out["status"] = "error"
        out["error"] = f"{type(e).__name__}: {e}"
        print(f"[365] FAIL: {e}")
        raise
    finally:
        out["finished"] = datetime.now(timezone.utc).isoformat()
        os.makedirs(os.path.dirname(REPORT), exist_ok=True)
        with open(REPORT, "w") as f:
            json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
