"""Four runner-only technical reads; no billing, metric/log queries or object bodies."""
import importlib.util
import json
import os
from pathlib import Path
import signal
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'aws/ops'))
SPEC = importlib.util.spec_from_file_location('cadence_probe', Path(__file__).with_name('ops_6422_cloudwatch_cadence_probe.py'))
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)
MONITOR = 'justhodl-fleet-error-monitor'
DETECTOR = 'justhodl-cost-anomaly'
BUCKET = 'justhodl-dashboard-live'
KEY = 'data/_fleet-monitor.json'
MAX_CALLS = 4


def inspect(lam, s3, out, reader):
    usage = reader.read(lam.get_account_settings)
    count = (usage or {}).get('AccountUsage', {}).get('FunctionCount')
    if type(count) is not int or count < 1:
        raise probe.Stop('function_count_unavailable')
    out.kv(regional_function_count=count,
           repository_function_directories=sum(p.is_dir() for p in (ROOT/'aws/lambdas').iterdir()),
           repository_config_count=len(list((ROOT/'aws/lambdas').glob('*/config.json'))),
           scope='Regional function inventory only; not proof of completed monitoring coverage')
    item = reader.read(s3.head_object, Bucket=BUCKET, Key=KEY)
    # No body, user-defined metadata, object identifier or response dump.
    out.kv(monitor_report_exists=item is not None,
           monitor_report_last_modified=(item or {}).get('LastModified'),
           monitor_report_bytes=(item or {}).get('ContentLength'))
    for name, keys in ((MONITOR, ('LOOKBACK_MINUTES','MIN_INVOCATIONS','ERROR_RATE_THRESHOLD',
                                 'DLQ_DEPTH_THRESHOLD','DEDUPE_WINDOW_MINUTES')),
                       (DETECTOR, ('MONTHLY_BUDGET_USD',))):
        item = reader.read(lam.get_function_configuration, FunctionName=name)
        if not item or (item.get('Environment') or {}).get('Error'):
            raise probe.Stop('configuration_unavailable')
        config = json.loads((ROOT/'aws/lambdas'/name/'config.json').read_text(encoding='utf-8'))
        declared = config['env']
        env = (item.get('Environment') or {}).get('Variables') or {}
        matches = {key: env.get(key) == declared[key] for key in keys}
        if name == DETECTOR:
            out.kv(cost_detector_release_controls={
                'other_declared_environment_matches': all(env.get(k) == v for k,v in declared.items() if k != 'MONTHLY_BUDGET_USD'),
                'memory_matches': item.get('MemorySize') == config['memory'],
                'timeout_matches': item.get('Timeout') == config['timeout'],
                'architectures_match': item.get('Architectures') == config['architectures'],
                'tracing_already_active': item.get('TracingConfig', {}).get('Mode') == 'Active',
                'dlq_already_standard': item.get('DeadLetterConfig', {}).get('TargetArn') == 'arn:aws:sqs:us-east-1:857687956942:justhodl-dlq-default'})
        out.kv(function=name, declared_setting_matches=matches,
               code_sha256=item.get('CodeSha256'), state=item.get('State'),
               memory_mb=item.get('MemorySize'), timeout_seconds=item.get('Timeout'))
    out.kv(completed=True, api_calls=reader.calls, billing_reads=0, metric_queries=0,
           log_reads=0, object_body_reads=0, aws_writes=0)


def main():
    if os.environ.get('GITHUB_ACTIONS') != 'true':
        raise SystemExit('runner_only')
    import boto3
    from botocore.config import Config
    from ops_report import report
    probe.MAX_CALLS = MAX_CALLS
    reader = probe.Reader()
    def deadline(*_):
        raise probe.Stop('time_bound_reached')
    signal.signal(signal.SIGALRM, deadline)
    signal.alarm(60)
    with report(Path(__file__).stem) as out:
        try:
            cfg = Config(connect_timeout=3, read_timeout=5, retries={'total_max_attempts':1})
            inspect(boto3.client('lambda', region_name='us-east-1',config=cfg),
                    boto3.client('s3', region_name='us-east-1',config=cfg),out,reader)
        except probe.Stop as exc:
            out.kv(completed=False,api_calls=reader.calls,stop_reason=str(exc))
            out._failed=True
            raise SystemExit(1) from None
        except Exception:
            out.kv(completed=False,api_calls=reader.calls,stop_reason='unexpected_failure_details_withheld')
            out._failed=True
            raise SystemExit(1) from None
        finally:
            signal.alarm(0)


if __name__ == '__main__': main()
