"""Read FR2004 runtime and routing metadata only; never invoke a producer."""
from pathlib import Path
import sys
import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report


def main():
    client = boto3.client('lambda', region_name='us-east-1')
    events = boto3.client('events', region_name='us-east-1')
    with report('ops_5846_fr2004_runtime_probe') as r:
        for name in ('justhodl-settlement-fails', 'justhodl-nyfed-pd',
                     'nyfed-primary-dealer-fetcher'):
            try:
                conf = client.get_function_configuration(FunctionName=name)
            except ClientError as exc:
                if exc.response['Error']['Code'] != 'ResourceNotFoundException':
                    raise
                r.kv(function=name, exists=False)
                continue
            public = {key: conf.get(key) for key in
                      ('FunctionName', 'Runtime', 'Handler', 'MemorySize', 'Timeout',
                       'Role', 'Architectures', 'CodeSha256', 'LastModified')}
            public['layers'] = [item['Arn'] for item in conf.get('Layers', [])]
            rules = []
            args = {'TargetArn': conf['FunctionArn']}
            while True:
                page = events.list_rule_names_by_target(**args)
                rules.extend(page.get('RuleNames', []))
                if not page.get('NextToken'):
                    break
                args['NextToken'] = page['NextToken']
            public['eventbridge_rules_default_bus'] = sorted(rules)
            try:
                url = client.get_function_url_config(FunctionName=name)
                public['function_url'] = {key: url.get(key) for key in ('FunctionUrl', 'AuthType')}
            except ClientError as exc:
                if exc.response['Error']['Code'] != 'ResourceNotFoundException':
                    raise
                public['function_url'] = None
            r.kv(**public)
        r.kv(invocations=0, environment_values_disclosed=0, paid_ai_calls=0,
             private_account_reads=0, portfolio_writes=0,
             scope='Runtime configuration, unqualified function URL and default-bus rules only; API Gateway, aliases and other event buses are not audited.')


if __name__ == '__main__':
    try:
        main()
    except Exception:
        print('FR2004 runtime probe failed; inspect committed runner report.')
        sys.exit(1)
