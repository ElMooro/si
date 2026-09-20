"""Verify the public scenario producer, immutable model and deployed pages."""
from datetime import datetime, timezone
from pathlib import Path
import base64
import hashlib
import io
import json
import re
import subprocess
import sys
import urllib.error
import urllib.request
import zipfile
import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT/'aws/lambdas/justhodl-position-sizer/source'
sys.path[:0] = [str(ROOT/'aws/ops'), str(ROOT/'aws/shared'), str(SOURCE)]
from ops_report import report
from acceptance_invoke import invoke_when_available
import scenario_publication as model

FN, BUCKET = 'justhodl-position-sizer', 'justhodl-dashboard-live'
PAGES = ('position-sizer.html','cockpit.html','jh-portfolio-scenario.js','jh-portfolio-scenario-page.js')


def public(key, method='GET'):
    url = key if key.startswith('https://') else 'https://justhodl.ai/'+key
    with urllib.request.urlopen(urllib.request.Request(url,method=method,headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=45) as response:
        raw=response.read(32*1024*1024+1)
    assert len(raw)<=32*1024*1024
    return raw


def denied(url):
    try: public(url,'HEAD');return False
    except urllib.error.HTTPError as exc:return exc.code in (401,403,404)


def main():
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=90,retries={'max_attempts':0}))
    s3=boto3.client('s3',region_name='us-east-1')
    with report('ops_5894_scenario_acceptance') as r:
        commit=subprocess.check_output(['git','log','-1','--format=%H','--',str(SOURCE/'scenario_publication.py')],cwd=ROOT,text=True).strip()
        receipt=json.loads(public('data/ops/releases/'+FN+'.json'))
        cfg=lam.get_function_configuration(FunctionName=FN)
        assert receipt['commit']==commit and receipt['code_sha256']==cfg['CodeSha256']
        assert cfg['State']=='Active' and cfg['LastUpdateStatus']=='Successful'
        with urllib.request.urlopen(lam.get_function(FunctionName=FN)['Code']['Location'],timeout=45) as response: archive=response.read(32*1024*1024+1)
        assert len(archive)<=32*1024*1024 and base64.b64encode(hashlib.sha256(archive).digest()).decode()==cfg['CodeSha256']
        with zipfile.ZipFile(io.BytesIO(archive)) as z:
            for name in model.FILES:assert z.read(name)==(SOURCE/name).read_bytes(), 'Packaged source differs: '+name
        r.kv(exact_runtime_commit=commit,code_sha256=cfg['CodeSha256'],paid_ai_calls=0,private_account_reads=0,notifications_sent=0,portfolio_writes=0)
        before=model.read(s3,BUCKET,model.CURRENT)[0]
        assert json.loads(public(model.CURRENT))==json.loads(before)
        sha=model.digest(before);key=model.PRIVATE+sha+'.bin'
        model.immutable(s3,BUCKET,key,before)
        assert denied('https://'+BUCKET+'.s3.amazonaws.com/'+key) and denied('https://justhodl.ai/'+key)
        r.kv(whole_preceding_product_preserved={'sha256':sha,'bytes':len(before),'anonymous_denied':True})
        previous=json.loads(before)
        invocations=0
        if previous.get('contract')!=model.CONTRACT or previous.get('compilers')!=model.sources()[0]:
            started=datetime.now(timezone.utc)
            response,rejections=invoke_when_available(lam,dict(FunctionName=FN,InvocationType='RequestResponse',Payload=b'{"notify":false}'),wait_seconds=90)
            value=json.loads(response['Payload'].read());invocations=1
            r.kv(engine_invocations=invocations,invocation_started=started.isoformat(),request_id=response.get('ResponseMetadata',{}).get('RequestId'),function_error=response.get('FunctionError'),throttle_rejections=rejections)
            assert not response.get('FunctionError') and value.get('statusCode')==200, 'Read report before another invocation'
        out=json.loads(public(model.CURRENT))
        assert out['contract']==model.CONTRACT and not out['sized_positions'] and out['suggested_gross_top15_pct'] is None
        assert all(v is False for v in out['permissions'].values())
        assert datetime.fromisoformat(out['generated_at'].replace('Z','+00:00'))>=datetime.fromisoformat(receipt['deployed_at'].replace('Z','+00:00'))
        manifest=model.verified(out['replay'],public,'runs')
        assert model.replay(manifest,public)=={k:v for k,v in out.items() if k!='replay'}
        assert public(out['scenario_model']['key'])==(ROOT/'jh-portfolio-scenario.js').read_bytes()
        # A separately executed, reviewed Node verifier rejects altered model/results.
        subprocess.run(['node','--test','tests/portfolio-scenario.test.js'],cwd=ROOT,check=True,stdout=subprocess.DEVNULL)
        build=json.loads(public('build-manifest.json'));pages_commit=build['commit_sha']
        subprocess.run(['git','merge-base','--is-ancestor',commit,pages_commit],cwd=ROOT,check=True)
        subprocess.run(['git','diff','--quiet',commit,pages_commit,'--','.',':(exclude)aws/ops/**',':(exclude)docs/**'],cwd=ROOT,check=True)
        for name in PAGES:
            body=public(name)
            built,count=re.subn(rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?',b'',body)
            assert count<=1 and model.digest(built)==build['files_sha256'][name], 'Built asset differs: '+name
            if name=='position-sizer.html':
                assert b'jh-portfolio-scenario.js' in body and b'portfolio/sizer-v2.json' not in body
                assert b'<meta charset="utf-8">' in body[:1024]
        assert public('jh-portfolio-scenario.js')==(SOURCE/'scenario_model.js').read_bytes()
        assert lam.get_function_configuration(FunctionName=FN)['CodeSha256']==cfg['CodeSha256']
        assert json.loads(public('data/ops/releases/'+FN+'.json'))['commit']==commit
        proof={'contract':'portfolio-scenario-verification.v1','generated_at':datetime.now(timezone.utc).isoformat(),
            'commit':commit,'code_sha256':cfg['CodeSha256'],'engine_invocations':invocations,
            'publication_generated_at':out['generated_at'],'replay':out['replay'],'model':out['scenario_model'],
            'pages_commit':pages_commit,'built_assets':{k:build['files_sha256'][k] for k in PAGES},
            'whole_preceding_product':{'sha256':sha,'bytes':len(before),'anonymous_denied':True},
            'paid_ai_calls':0,'notifications_sent':0,'private_account_reads':0,'portfolio_writes':0,
            'scope':'Public model publication and explicit hypothetical arithmetic. No expected-return, allocation, portfolio-account or execution qualification.'}
        key='data/scenario-model-verification.json'
        s3.put_object(Bucket=BUCKET,Key=key,Body=model.encoded(proof),ContentType='application/json',CacheControl='no-store')
        assert json.loads(public(key))==proof
        r.kv(proof_key=key,publication_generated_at=out['generated_at'],replay=out['replay'],model=out['scenario_model'],pages_commit=pages_commit)


if __name__=='__main__':
    try:main()
    except Exception:
        print('Scenario acceptance failed; read the committed report before any retry or reinvocation.')
        sys.exit(1)
