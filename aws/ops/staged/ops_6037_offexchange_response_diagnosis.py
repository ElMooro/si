"""Inspect retained 6036 evidence; never re-request a provider or invoke an engine."""
from pathlib import Path
import json,sys
import boto3
ROOT=Path(__file__).resolve().parents[3];sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged')]
from ops_report import report
import ops_6036_offexchange_whole_source_preflight as audit

def main():
    s3=boto3.client('s3',region_name='us-east-1')
    with report('ops_6037_offexchange_response_diagnosis') as r:
        status=audit.strict(audit.get(s3,audit.STATUS));assert status['request_id']==audit.REQUEST
        linked={v['original']['key'] for v in status.get('parents',{}).values() if v.get('original')}
        linked.update(v['original']['key'] for v in status.get('captures',{}).values() if v.get('original'))
        retained=[]
        for page in s3.get_paginator('list_objects_v2').paginate(Bucket=audit.BUCKET,Prefix=audit.PRIVATE):
            for item in page.get('Contents',[]):
                row={'key':item['Key'],'bytes':item['Size'],'journalled':item['Key'] in linked}
                if item['Key'].endswith('.bin') and item['Key'] not in linked and item['Size']<8*1024*1024:
                    raw=audit.get(s3,item['Key']);assert audit.sha(raw)+'.bin'==item['Key'].split('/')[-1]
                    try:
                        doc=audit.strict(raw);row['root_type']=type(doc).__name__
                        row['fields']=list(doc)[:30] if isinstance(doc,dict) else sorted({k for v in doc[:3] if isinstance(v,dict) for k in v}) if isinstance(doc,list) else []
                        row['sample']=doc[:1] if isinstance(doc,list) else doc if isinstance(doc,dict) and len(raw)<20000 else None
                    except (ValueError,UnicodeError):row['text_prefix']=raw[:180].decode('utf-8',errors='replace')
                retained.append(row);assert len(retained)<=100,'Bounded failed-attempt inventory'
        r.kv(failed_request=status,retained_objects=retained,provider_requests=0,engine_invocations=0,
            public_head_writes=0,original_recollection=0,private_account_reads=0,paid_ai_calls=0,
            notifications_sent=0,portfolio_writes=0,schedules_changed=0)
if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
