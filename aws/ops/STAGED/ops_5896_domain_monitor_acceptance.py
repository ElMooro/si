"""Verify domain runtime/pages and rehearse public inputs without private invocation."""
from datetime import datetime, timezone
from pathlib import Path
import base64, hashlib, io, json, re, runpy, subprocess, sys, types, urllib.request, zipfile
from unittest.mock import patch
import boto3
import urllib.error

ROOT=Path(__file__).resolve().parents[3]
SOURCE=ROOT/'aws/lambdas/justhodl-domain-barometers/source'
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/shared'),str(SOURCE)]
from ops_report import report
from barometer_integrity import CONTRACT

FN='justhodl-domain-barometers';BUCKET='justhodl-dashboard-live'
PAGES=('tradingview.html','jh-domain-monitor.js')


def public(key):
    with urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/'+key,headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=45) as response:
        raw=response.read(32*1024*1024+1)
    assert len(raw)<=32*1024*1024
    return raw


def sha(raw):return hashlib.sha256(raw).hexdigest()


def denied(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,method='HEAD',headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=30):return False
    except urllib.error.HTTPError as exc:return exc.code in (401,403,404)


def main():
    lam=boto3.client('lambda',region_name='us-east-1');s3=boto3.client('s3',region_name='us-east-1')
    scheduler=boto3.client('scheduler',region_name='us-east-1')
    with report('ops_5896_domain_monitor_acceptance') as r:
        commit=subprocess.check_output(['git','log','-1','--format=%H','--',str(SOURCE/'lambda_function.py')],cwd=ROOT,text=True).strip()
        receipt=json.loads(public('data/ops/releases/'+FN+'.json'));cfg=lam.get_function_configuration(FunctionName=FN)
        assert receipt['commit']==commit and receipt['code_sha256']==cfg['CodeSha256']
        assert cfg['State']=='Active' and cfg['LastUpdateStatus']=='Successful'
        with urllib.request.urlopen(lam.get_function(FunctionName=FN)['Code']['Location'],timeout=45) as response:archive=response.read(32*1024*1024+1)
        assert len(archive)<=32*1024*1024 and base64.b64encode(hashlib.sha256(archive).digest()).decode()==cfg['CodeSha256']
        with zipfile.ZipFile(io.BytesIO(archive)) as z:
            for name in ('lambda_function.py','barometer_integrity.py'):assert z.read(name)==(SOURCE/name).read_bytes()
            assert z.read('public_brain_projection.py')==(ROOT/'aws/shared/public_brain_projection.py').read_bytes()
        schedule=scheduler.get_schedule(Name='domain-barometers-daily',GroupName='default')
        assert schedule['State']=='ENABLED' and schedule['Target']['Arn'] in (cfg['FunctionArn'],cfg['FunctionArn']+':$LATEST')
        assert schedule['ScheduleExpression']=='cron(20 12 * * ? *)' and schedule['ScheduleExpressionTimezone']=='UTC'
        r.kv(exact_runtime_commit=commit,code_sha256=cfg['CodeSha256'],engine_invocations=0,private_account_reads=0,
             paid_ai_calls=0,notifications_sent=0,portfolio_writes=0,schedule={k:schedule[k] for k in ('Name','ScheduleExpression','ScheduleExpressionTimezone','State')})
        raw={name:public('data/'+name+'.json') for name in ('domain-barometers','tradingview','risk-gate','rotation-dashboard')}
        docs={name:json.loads(body) for name,body in raw.items()}
        retained={}
        for name,body in raw.items():
            key='audit-private/20260909-originals/domain-monitor-rehearsal/'+sha(body)+'.bin'
            try:s3.put_object(Bucket=BUCKET,Key=key,Body=body,IfNoneMatch='*',ContentType='application/octet-stream',CacheControl='no-store')
            except Exception as exc:
                if getattr(exc,'response',{}).get('Error',{}).get('Code') not in ('PreconditionFailed','ConditionalRequestConflict'):raise
            stream=s3.get_object(Bucket=BUCKET,Key=key)['Body']
            try:assert stream.read(32*1024*1024+1)==body
            finally:stream.close()
            assert denied('https://'+BUCKET+'.s3.amazonaws.com/'+key) and denied('https://justhodl.ai/'+key)
            retained[name]={'sha256':sha(body),'bytes':len(body),'retention':'private_audit_copy_of_existing_public_data'}
        with patch.dict(sys.modules,{'boto3':types.SimpleNamespace(client=lambda *a,**k:None)}):
            env=runpy.run_path(str(SOURCE/'lambda_function.py'))
        previous=docs['domain-barometers'];rows=docs['tradingview']['symbols']
        domains={v['symbol']:v['domain'] for v in previous['symbols'] if v.get('tier')!='T6'}
        cats={v['symbol']:v.get('category') for v in rows}
        bar=env['build_barometers'](rows,domains,docs['risk-gate'],cats,{})
        mapping=env['predict'](bar,docs['risk-gate'],docs['rotation-dashboard'])
        for v in bar.values():
            assert v['n_favourable']+v['n_adverse']==v['n_directional_comparisons']
            assert v['n_directional_comparisons']+v['n_unchanged']+v['n_excluded_comparisons']==len(v['driver_comparisons'])
            assert not v['worst_movers'] and not v['best_movers']
        rehearsal={'input_references':retained,'rows':len(rows),'classification_generated_at':previous.get('generated_at'),
            'result_sha256':sha(json.dumps({'barometers':bar,'rule_mapping':mapping},sort_keys=True,allow_nan=False).encode()),
            'counts':{d:{k:v[k] for k in ('n_favourable','n_adverse','n_unchanged','n_comparison_unavailable','n_excluded_comparisons','excluded_provider_roots')} for d,v in bar.items()},
            'scope':'Public-input calculation rehearsal using retained public classification labels. The private classifier and scheduled handler were not invoked.'}
        r.kv(public_input_rehearsal=rehearsal)
        build=json.loads(public('build-manifest.json'));actual=build['commit_sha']
        subprocess.run(['git','merge-base','--is-ancestor',commit,actual],cwd=ROOT,check=True)
        subprocess.run(['git','diff','--quiet',commit,actual,'--','.',':(exclude)aws/ops/**',':(exclude)docs/**'],cwd=ROOT,check=True)
        for name in PAGES:
            body=public(name);built,count=re.subn(rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?',b'',body)
            assert count<=1 and sha(built)==build['files_sha256'][name]
        assert b'jh-domain-monitor.js' in public('tradingview.html')
        current=json.loads(public('data/domain-barometers.json'));stamp=current.get('generated_at')
        qualified=current.get('contract')==CONTRACT and isinstance(stamp,str) and datetime.fromisoformat(stamp.replace('Z','+00:00'))>=datetime.fromisoformat(receipt['deployed_at'].replace('Z','+00:00'))
        if qualified:
            assert current['quality']['status']=='unvalidated_monitor' and current['permissions']['sizing_eligible'] is False
            assert current['grading']['history_write_status']=='paused_pending_native_observation_and_price_identity'
        proof={'contract':'domain-monitor-verification.v1','generated_at':datetime.now(timezone.utc).isoformat(),
            'runtime_commit':commit,'code_sha256':cfg['CodeSha256'],'pages_commit':actual,
            'built_assets':{k:build['files_sha256'][k] for k in PAGES},'rehearsal':rehearsal,
            'public_generated_at':stamp,'publication_acceptance':'observed_new_contract' if qualified else 'awaiting_natural_publication',
            'engine_invocations':0,'paid_ai_calls':0,'private_account_reads':0,'notifications_sent':0,'portfolio_writes':0,
            'remaining':'Native observation/price identities, full private-classifier replay, original-source snapshots and predictive validation remain unqualified. No allocation authority.'}
        key='data/domain-monitor-verification.json'
        s3.put_object(Bucket=BUCKET,Key=key,Body=json.dumps(proof,sort_keys=True,allow_nan=False).encode(),ContentType='application/json',CacheControl='no-store')
        assert json.loads(public(key))==proof
        r.kv(proof_key=key,publication_acceptance=proof['publication_acceptance'],public_generated_at=stamp,pages_commit=actual)


if __name__=='__main__':
    try:main()
    except Exception:
        print('Domain-monitor verification failed. Read the committed report; do not invoke a private-context producer to retry acceptance.')
        sys.exit(1)
