"""One recorded provider check after a doubled cooldown; never resume a batch.

A repeated 429 leaves acquisition closed. No provider requests before thirty
minutes have elapsed since the retained failure, and no automatic retry.
"""
from pathlib import Path
from datetime import datetime,timezone
import json,sys,urllib.request,urllib.error
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/staged','aws/ops/checks')]
from ops_report import report
from ops_5998_option_population_retained_acceptance import denied_with_retry
from market_runtime_evidence import runtime
import share_structure_sources as source
import share_structure_campaign as campaign
import share_structure_batch_runner as batches
import ops_6081_share_structure_retained_baseline as baseline

REQUEST='chatgpt-share-structure-batch3-quota-check-6098'
STATUS=source.request_key(REQUEST,'diagnostic')
FAILED={'key':source.PRIVATE+'88803ebc27d63e2f1f31fa75892815f3e9d76036fe0abf7dad47b13055d0bf5d.bin',
    'sha256':'88803ebc27d63e2f1f31fa75892815f3e9d76036fe0abf7dad47b13055d0bf5d','bytes':184445}


def main():
    s3,lam,events,scheduler=(boto3.client(name,region_name='us-east-1') for name in ('s3','lambda','events','scheduler'))
    with report('ops_6098_share_structure_batch3_limit_check') as r:
        assert '**Status:** success' in (ROOT/'aws/ops/reports/latest/ops_6097_share_structure_batch3_diagnosis.md').read_text(encoding='utf-8')
        failed=json.loads(source.read(s3,FAILED));assert failed['status']=='failed' and failed['part']==3 and len(failed['captures'])==597
        request=source.spec('HCM','cash-flow-statement','quarter');assert set(failed['source_errors'])=={request['url']}
        old=campaign.read_journal(s3,source.request_key(batches.PARENT,request['url']))
        assert old['status']=='failed' and old['http_status']==429
        elapsed=(datetime.now(timezone.utc)-datetime.fromisoformat(old['received_at'])).total_seconds()
        assert elapsed>=1800,'Thirty-minute minimum recorded cooldown required'
        progress={'request_id':REQUEST,'status':'claimed','failed_batch':FAILED,'elapsed_cooldown_seconds':elapsed,'planned_provider_requests':1}
        source.journal(s3,STATUS,progress,True)
        before=runtime(lam,s3,events,scheduler,baseline.FUNCTION)
        env=lam.get_function_configuration(FunctionName=baseline.FUNCTION).get('Environment',{}).get('Variables',{})
        credential=env.get('FMP_API_KEY') or env.get('FMP_KEY')
        if not credential:credential=boto3.client('ssm',region_name='us-east-1').get_parameter(Name='/justhodl/fmp/api-key',WithDecryption=True)['Parameter']['Value']
        rate_headers={};opener=urllib.request.build_opener(source.NoRedirect())
        def transport(req,timeout):
            try:response=opener.open(req,timeout=timeout)
            except urllib.error.HTTPError as exc:response=exc
            for key in ('Retry-After','X-RateLimit-Limit','X-RateLimit-Remaining','X-RateLimit-Reset'):
                value=response.headers.get(key)
                if value is not None:rate_headers[key.lower()]=str(value)[:120]
            return response
        try:result=source.capture(s3,REQUEST,request,{'HCM'},credential,source.Rate(interval=2),baseline.now,transport)
        except RuntimeError:
            retained=campaign.read_journal(s3,source.request_key(REQUEST,request['url']))
            assert retained.get('original') and retained.get('http_status')==429,'Unexpected failure requires inspection, never retry'
            code=429;capture=None;original=retained['original'];outcome='provider_limit_persists'
        else:
            code=200;capture=result['retained_capture'];original=result['original'];outcome='single_source_request_recovered'
        protected={STATUS,FAILED['key'],original['key'],source.request_key(REQUEST,request['url'])}
        if capture:protected.add(capture['key'])
        for key in protected:
            assert denied_with_retry('https://justhodl.ai/'+key) and denied_with_retry('https://'+source.BUCKET+'.s3.amazonaws.com/'+key)
        assert runtime(lam,s3,events,scheduler,baseline.FUNCTION)==before
        document={**progress,'status':'complete','outcome':outcome,'http_status':code,'capture':capture,'original':original,
            'rate_limit_headers':rate_headers,'account_remaining_quota_verified':False,'campaign_resumed':False}
        source.journal(s3,STATUS,document)
        r.kv(**document,provider_requests=1,producer_invocations=0,consumer_invocations=0,public_writes=0,
            private_account_reads=0,signal_writes=0,paid_ai_calls=0,notifications_sent=0,schedules_changed=0,
            protected_artifacts_checked=len(protected))


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
