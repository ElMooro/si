"""Inspect the failed 6034 claim and exact source sizes without recollection."""
from pathlib import Path
import json,sys
import boto3
ROOT=Path(__file__).resolve().parents[3];sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged')]
from ops_report import report
import ops_6034_offexchange_source_preflight as audit
def main():
    s3=boto3.client('s3',region_name='us-east-1')
    with report('ops_6035_offexchange_retention_diagnosis') as r:
        status=audit.strict(audit.get(s3,audit.STATUS));assert status['request_id']==audit.REQUEST
        sizes={}
        for key in audit.PACKETS:
            try:head=s3.head_object(Bucket=audit.BUCKET,Key=key)
            except Exception as exc:
                if not audit.missing(exc):raise
                sizes[key]={'status':'missing'};continue
            sizes[key]={'status':'present','bytes':head['ContentLength'],'etag':head['ETag'],'last_modified':head['LastModified'].isoformat(),
                'over_6034_limit':head['ContentLength']>audit.MAX,'empty':head['ContentLength']==0}
        retained=[]
        for page in s3.get_paginator('list_objects_v2').paginate(Bucket=audit.BUCKET,Prefix=audit.PRIVATE):
            retained.extend({'key':v['Key'],'bytes':v['Size']} for v in page.get('Contents',[]));assert len(retained)<=100,'Bounded failed-attempt inventory'
        r.kv(failed_request=status,source_objects=sizes,retained_objects=retained,
            provider_requests=0,engine_invocations=0,public_head_writes=0,original_recollection=0,private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0,schedules_changed=0)
if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
