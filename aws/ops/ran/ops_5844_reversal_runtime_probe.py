"""Read runtime capacity only; do not invoke engines or read environment values."""
from pathlib import Path
import sys
import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report


def main():
    client = boto3.client('lambda', region_name='us-east-1')
    with report('ops_5844_reversal_runtime_probe') as r:
        for name in ('justhodl-liquidity-reversal', 'justhodl-catalyst',
                     'justhodl-physical-econ', 'justhodl-stock-buying'):
            conf = client.get_function_configuration(FunctionName=name)
            public = {key: conf.get(key) for key in
                      ('FunctionName', 'Runtime', 'Handler', 'MemorySize', 'Timeout',
                       'Role', 'Architectures', 'CodeSha256', 'LastModified')}
            public['layers'] = [item['Arn'] for item in conf.get('Layers', [])]
            r.kv(**public)
        r.kv(invocations=0, environment_values_read=0, paid_ai_calls=0,
             private_account_reads=0, portfolio_writes=0)


if __name__ == '__main__':
    try:
        main()
    except Exception:
        print('Reversal runtime probe failed; inspect committed runner report.')
        sys.exit(1)
