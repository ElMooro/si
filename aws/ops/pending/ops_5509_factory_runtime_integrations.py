#!/usr/bin/env python3
"""Grant the existing deploy lane's exact DLQ and X-Ray prerequisites."""
import json
import sys
from pathlib import Path
import boto3
from ops_report import report


def main(rep):
    iam = boto3.client('iam')
    queue = 'arn:aws:sqs:us-east-1:857687956942:justhodl-dlq-default'
    checks = {}
    for name in ('justhodl-student-rsi-role', 'justhodl-factory-grader-role'):
        role = iam.get_role(RoleName=name)['Role']
        if {t['Key']:t['Value'] for t in role.get('Tags', [])}.get('JustHodlComponent') != 'factory-gear-a':
            raise RuntimeError('unmanaged_role')
        before = iam.simulate_principal_policy(PolicySourceArn=role['Arn'], ActionNames=['sqs:SendMessage'], ResourceArns=[queue])['EvaluationResults'][0]['EvalDecision']
        iam.put_role_policy(RoleName=name, PolicyName='factory-runtime-integrations', PolicyDocument=json.dumps({
            'Version':'2012-10-17', 'Statement':[
                {'Effect':'Allow','Action':'sqs:SendMessage','Resource':queue},
                {'Effect':'Allow','Action':['xray:PutTraceSegments','xray:PutTelemetryRecords'],'Resource':'*'}]}))
        checks[name] = {'dlq_before':before, 'dlq_target':queue, 'trace_write_only':True}
    Path('aws/ops/reports/5509.json').write_text(json.dumps({'ok':True,'checks':checks},indent=2)+'\n')
    rep.kv(**checks)
    rep.ok('Existing DLQ and tracing supported. No read/receive/delete queue rights or management rights granted.')


if __name__ == '__main__':
    try:
        with report('5509_factory_runtime_integrations') as rep:
            main(rep)
    except Exception as exc:
        print('Factory runtime integration failed:',type(exc).__name__)
        sys.exit(1)
