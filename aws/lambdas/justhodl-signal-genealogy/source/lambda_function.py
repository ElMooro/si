"""Scheduled public-original chronology; no private ledger or market acquisition.

The old ledger-derived result remains preserved in storage. This producer only
publishes the new research namespace after complete retained-original replay.
The unchanged 06:40 UTC trigger supplies no data or budget overrides.
"""
from datetime import datetime,timezone
from pathlib import Path
import tempfile

from genealogy_native_runtime import run

VERSION='2.0.0'


def lambda_handler(event,context):
    import boto3
    from botocore.config import Config
    limits={'inventory':150000,'restore':130000,'calculate':110000,
            'retain':90000,'replay':60000,'publication':15000}

    def guard(phase):
        remaining=context.get_remaining_time_in_millis()
        if type(remaining) is not int or remaining<limits[phase]:
            raise RuntimeError('genealogy_remaining_runtime_budget_'+phase)

    guard('inventory')
    cutoff=datetime.now(timezone.utc).isoformat()
    client=boto3.client('s3',region_name='us-east-1',config=Config(
        max_pool_connections=8,connect_timeout=10,read_timeout=30,
        retries={'mode':'standard','total_max_attempts':2}))
    with tempfile.TemporaryDirectory(prefix='genealogy-native-') as directory:
        result=run(client,client,cutoff,Path(directory)/'run',guard)
    return {'statusCode':200,'version':VERSION,'published':result['published'],
            'reason':result['reason'],'generated_at':result['packet']['generated_at'],
            'original_objects':result['original_objects'],'original_bytes':result['original_bytes'],
            'coverage':result['packet']['coverage'],'replay':result['retained_manifest'],
            'timings_seconds':result['timings_seconds'],'calls_eligible':False,'sizing_eligible':False}
