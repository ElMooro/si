"""Scheduled Sentinel research with complete privately retained source replay."""
import json,os
import boto3
from botocore.config import Config
from managed_secret import managed_secret
from sentinel_store import run


def lambda_handler(event=None,context=None):
    key=(os.environ.get('FRED_API_KEY') or os.environ.get('FRED_KEY')
         or managed_secret(('FRED_API_KEY','FRED_KEY'),('/justhodl/fred/api-key',)))
    client=boto3.client('s3',region_name='us-east-1',config=Config(connect_timeout=4,read_timeout=20,
        retries={'max_attempts':2,'mode':'standard'}))
    result=run(client,'justhodl-dashboard-live',key)
    return {'statusCode':200,'body':json.dumps(result,allow_nan=False)}
