#!/usr/bin/env python3
"""Ensure only the newly added factory evidence requires authenticated access."""
import hashlib
import json
import sys
from pathlib import Path
import boto3
from botocore import UNSIGNED
from botocore.config import Config
from ops_report import report


def main(rep):
    bucket='justhodl-dashboard-live'
    s3=boto3.client('s3',region_name='us-east-1')
    anonymous=boto3.client('s3',region_name='us-east-1',config=Config(signature_version=UNSIGNED))
    keys=['student-state.json','data/student-state.json','factory/salon/season.json']
    def probe(key):
        try:
            anonymous.head_object(Bucket=bucket,Key=key)
            return 'public'
        except Exception as exc:
            code=getattr(exc,'response',{}).get('Error',{}).get('Code')
            if code in ('403','AccessDenied'):return 'denied'
            raise
    before={key:probe(key) for key in keys}
    changed=False
    if 'public' in before.values():
        current=json.loads(s3.get_bucket_policy(Bucket=bucket)['Policy'])
        statement={'Sid':'RequireIdentityForFactoryEvidence','Effect':'Deny','Principal':'*','Action':'s3:GetObject',
            'Resource':['arn:aws:s3:::'+bucket+'/factory/*','arn:aws:s3:::'+bucket+'/student-state.json',
                        'arn:aws:s3:::'+bucket+'/data/student-state.json'],
            'Condition':{'Null':{'aws:PrincipalArn':'true'}}}
        matching=[s for s in current['Statement'] if s.get('Sid')==statement['Sid']]
        if matching and matching!=[statement]:raise RuntimeError('existing_boundary_requires_review')
        if not matching:
            old=json.dumps(current,sort_keys=True).encode()
            try:
                s3.put_object(Bucket='justhodl-ai-857687956942',Key='factory/releases/identity-policy-before-'+hashlib.sha256(old).hexdigest()+'.json',
                    Body=old,ContentType='application/json',ServerSideEncryption='AES256',IfNoneMatch='*')
            except Exception as exc:
                if getattr(exc,'response',{}).get('Error',{}).get('Code') not in ('PreconditionFailed','412'):raise
            if json.loads(s3.get_bucket_policy(Bucket=bucket)['Policy'])!=current:raise RuntimeError('policy_changed_concurrently')
            current['Statement'].append(statement)
            s3.put_bucket_policy(Bucket=bucket,Policy=json.dumps(current))
            changed=True
    after={key:probe(key) for key in keys}
    if any(value!='denied' for value in after.values()):raise RuntimeError('anonymous_factory_evidence_still_readable')
    # IAM reads still work; existing Brain feed and site assets are untouched.
    for key in keys:s3.head_object(Bucket=bucket,Key=key)
    s3.head_object(Bucket=bucket,Key='data/ai.json')
    result={'ok':True,'anonymous_before':before,'anonymous_after':after,'restriction_added':changed,
        'scope':'new factory/* and two student-state keys only','existing_brain_feed_preserved':True,
        'public_access_granted':False,'bucket_policy_existing_statements_preserved':True}
    Path('aws/ops/reports/5512.json').write_text(json.dumps(result,indent=2)+'\n')
    rep.kv(**result)
    rep.ok('Factory evidence requires identity; existing Brain data and website assets preserved.')


if __name__=='__main__':
    try:
        with report('5512_factory_private_view_boundary') as rep:main(rep)
    except Exception as exc:
        print('Factory identity boundary failed:',type(exc).__name__)
        sys.exit(1)
