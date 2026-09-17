"""Recover absent receipts only after exact deployed ZIP/source readback, then refresh."""
import base64
import hashlib
import io
import json
import os
import runpy
import subprocess
import sys
import time
import urllib.request
import zipfile
from pathlib import Path
import boto3
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from ops_report import report
ROOT=Path(__file__).resolve().parents[3]
BUCKET='justhodl-dashboard-live'
TARGETS=(('justhodl-us10y-sentinel','5d0c09d4de5aaee9895aed269ce0004f5202522d'),
         ('justhodl-yield-curve','7e33d1b52a6521e4596b8fd7f9ede0debc6b5d12'))


def git(*args):return subprocess.check_output(['git',*args],cwd=ROOT)


def main():
    lam=boto3.client('lambda',region_name='us-east-1')
    s3=boto3.client('s3',region_name='us-east-1')
    with report('ops_5635_yield_release_readback') as r:
        for fn,commit in TARGETS:
            function=lam.get_function(FunctionName=fn)
            cfg=function['Configuration']
            # Presigned URL stays in memory and is never logged.
            raw=urllib.request.urlopen(function['Code']['Location'],timeout=60).read()
            digest=hashlib.sha256(raw).digest()
            code_sha=base64.b64encode(digest).decode()
            assert code_sha==cfg['CodeSha256'],'Downloaded ZIP differs from AWS CodeSha256'
            expected={};inventory={}
            for tree in ('aws/shared/',f'aws/lambdas/{fn}/source/'):
                paths=git('ls-tree','-r','-z','--name-only',commit,'--',tree).decode().split('\0')
                for path in paths:
                    if not path or '__pycache__' in path:continue
                    relative=path[len(tree):]
                    if tree=='aws/shared/' and ('/' in relative or not relative.endswith('.py')):continue
                    data=git('show',commit+':'+path)
                    expected[relative]=hashlib.sha256(data).hexdigest()
                    if tree!='aws/shared/':inventory[relative]={'bytes':len(data),'sha256':expected[relative]}
            archive=zipfile.ZipFile(io.BytesIO(raw))
            actual={n:hashlib.sha256(archive.read(n)).hexdigest() for n in archive.namelist()
                    if not n.endswith('/') and '__pycache__' not in n.split('/')}
            differences=[n for n in sorted(set(expected)|set(actual)) if expected.get(n)!=actual.get(n)]
            r.kv(function=fn,expected_commit=commit,code_sha256=code_sha,last_modified=cfg['LastModified'],
                 expected_file_count=len(expected),actual_file_count=len(actual),differing_files=differences)
            assert not differences,'Deployed source differs from pinned commit; no receipt written or invoke performed'
            stamp=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())
            receipt={'schema':'release-receipt.v1','function':fn,'commit':commit,'verified':True,
                     'code_sha256':code_sha,'zip_bytes':len(raw),'zip_sha256_hex':digest.hex(),'source':inventory,
                     'deployed_at':cfg['LastModified'],'verified_at':stamp,'workflow':'run-ops.yml',
                     'run_id':os.environ.get('GITHUB_RUN_ID'),'actor':os.environ.get('GITHUB_ACTOR'),
                     'run_url':'https://github.com/ElMooro/si/actions/runs/'+os.environ.get('GITHUB_RUN_ID',''),
                     'verification_method':'Independent deployed ZIP readback: AWS CodeSha256 and every source/shared file equal pinned git commit'}
            # Preserve an existing matching receipt. Recover only the absent proof.
            try:existing=json.loads(s3.get_object(Bucket=BUCKET,Key=f'data/ops/releases/{fn}.json')['Body'].read())
            except s3.exceptions.NoSuchKey:existing={}
            if existing.get('commit')!=commit or existing.get('code_sha256')!=code_sha:
                body=json.dumps(receipt,indent=2).encode()
                for key in (f'data/ops/releases/{fn}.json',f'data/ops/releases/history/{fn}/readback-{stamp.replace(":","")}.json'):
                    s3.put_object(Bucket=BUCKET,Key=key,Body=body,ContentType='application/json',CacheControl='no-cache, max-age=60')
                r.ok('Exact deployed bytes verified; missing release proof recovered')
            else:r.ok('Existing receipt independently confirmed against live AWS ZIP')
    script=ROOT/'aws/ops/pending/ops_5634_yield_integrity_verify.py'
    if not script.exists():script=ROOT/'aws/ops/ran/ops_5634_yield_integrity_verify.py'
    return runpy.run_path(str(script))['main']()


if __name__=='__main__':sys.exit(main())
