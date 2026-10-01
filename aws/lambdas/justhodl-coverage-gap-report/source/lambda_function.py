"""Coverage inventory from the existing four platform artifacts.

Counts describe received source populations and explicitly reported summary
fields. They do not certify market completeness, provider freshness, historical
issuer/security relationships or Bloomberg/Refinitiv parity. Failed or malformed
inputs remain unavailable instead of becoming zero. A scoped empty population
can produce a zero inventory count without asserting zero market coverage.

Complete received inputs, compiler files, prior reports and publication inputs
are retained in the existing protected audit namespace. The public report holds
references and narrowly defined counts, not the underlying source documents.
Its generation clock is distinct from each source's reported clock. A successful
conditional write confirms publication acknowledgement; it is not independent
original-source or normal-schedule replay verification.

The original data/audit/coverage-gap.json path, metric names, source keys,
resources and daily schedule are preserved. No provider, model, notification,
account discovery or native invocation is introduced by this handler.
"""
import json
import os
from pathlib import Path

import boto3
from botocore.config import Config
from coverage_store import publish

BUCKET=os.environ.get('S3_BUCKET','justhodl-dashboard-live')
s3=boto3.client('s3',region_name='us-east-1',config=Config(
    connect_timeout=2,read_timeout=3,retries={'total_max_attempts':1}))


def lambda_handler(event,context):
    source=Path(__file__).resolve().parent
    compilers={name:source/name for name in
               ('lambda_function.py','coverage_model.py','coverage_store.py')}
    packet,_=publish(s3,BUCKET,compilers,context)
    result={'ok':True,'summary':{row['metric']:row['actual'] for row in packet['metrics']},
            'status':packet['quality']['status'],
            'unavailable_metric_count':packet['quality']['unavailable_metric_count'],
            'investment_authority':False}
    print(json.dumps(result,allow_nan=False))
    return {'statusCode':200,'body':json.dumps(result,allow_nan=False)}
