#!/usr/bin/env python3
"""ops 1042 — probe github-actions-justhodl IAM permissions"""
import json, boto3, os
from datetime import datetime, timezone

REGION = 'us-east-1'
ACCOUNT = '857687956942'

sts = boto3.client('sts', region_name=REGION)
sqs = boto3.client('sqs', region_name=REGION)
sns = boto3.client('sns', region_name=REGION)
lam = boto3.client('lambda', region_name=REGION)
iam = boto3.client('iam', region_name=REGION)

report = {'started_at': datetime.now(timezone.utc).isoformat()}

# Who am I?
identity = sts.get_caller_identity()
report['identity'] = {'arn': identity['Arn'], 'account': identity['Account']}

# What policies does this user/role have?
arn = identity['Arn']
if ':user/' in arn:
    user_name = arn.split('/')[-1]
    try:
        attached = iam.list_attached_user_policies(UserName=user_name).get('AttachedPolicies', [])
        report['attached_managed_policies'] = [p['PolicyName'] for p in attached]
        inline = iam.list_user_policies(UserName=user_name).get('PolicyNames', [])
        report['inline_policies'] = inline
        # Dump each inline policy
        report['inline_policy_docs'] = {}
        for pn in inline:
            d = iam.get_user_policy(UserName=user_name, PolicyName=pn)
            report['inline_policy_docs'][pn] = d.get('PolicyDocument')
    except Exception as e:
        report['iam_list_error'] = str(e)[:300]

# Probe specific permissions
checks = {}

# 1. SQS create
try:
    test = sqs.create_queue(QueueName='justhodl-permcheck-temp-' + str(int(datetime.now().timestamp())))
    checks['sqs_create_queue'] = 'ALLOWED'
    # Clean up
    sqs.delete_queue(QueueUrl=test['QueueUrl'])
    checks['sqs_delete_queue'] = 'ALLOWED'
except Exception as e:
    checks['sqs_create_queue'] = f'DENIED: {str(e)[:200]}'

# 2. SNS create
try:
    t = sns.create_topic(Name='justhodl-permcheck-temp')
    checks['sns_create_topic'] = 'ALLOWED'
    sns.delete_topic(TopicArn=t['TopicArn'])
except Exception as e:
    checks['sns_create_topic'] = f'DENIED: {str(e)[:200]}'

# 3. Lambda update config
try:
    # Try a no-op update
    funcs = lam.list_functions(MaxItems=1)
    fn_name = funcs['Functions'][0]['FunctionName'] if funcs.get('Functions') else None
    if fn_name:
        # don't actually do it, just check permission via dry-run-style approach
        cfg = lam.get_function_configuration(FunctionName=fn_name)
        checks['lambda_get_function_configuration'] = 'ALLOWED'
except Exception as e:
    checks['lambda_get_function_configuration'] = f'DENIED: {str(e)[:200]}'

# 4. List existing queues to see if any pre-existing DLQ exists
try:
    qs = sqs.list_queues(QueueNamePrefix='justhodl-')
    report['existing_justhodl_queues'] = qs.get('QueueUrls', [])
except Exception as e:
    report['queue_list_error'] = str(e)[:200]

# 5. List existing SNS topics matching prefix
try:
    topics = sns.list_topics()
    matching = [t['TopicArn'] for t in topics.get('Topics',[]) if 'justhodl' in t['TopicArn']]
    report['existing_justhodl_topics'] = matching
except Exception as e:
    report['topic_list_error'] = str(e)[:200]

report['permission_checks'] = checks

# Write
os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1042.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

print(json.dumps(report, indent=2, default=str))
