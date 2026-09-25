"""One reviewed request after the retained 429 cooldown; never a retry loop.

This diagnostic cannot resume a campaign or publish an engine. A continuing
429 is recorded as a closed acquisition gate, not an empty company dataset.
"""
from pathlib import Path
from datetime import datetime,timezone
import base64,hashlib,json,sys,urllib.request
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/staged','aws/ops/checks')]
from ops_report import report
from market_runtime_evidence import runtime,bounded
from ops_5998_option_population_retained_acceptance import denied_with_retry
import share_structure_sources as source
import share_structure_campaign as campaign
import share_structure_batch_runner as batches
import ops_6081_share_structure_retained_baseline as baseline

REQUEST='chatgpt-share-structure-quota-check-6092'
STATUS=source.request_key(REQUEST,'diagnostic')
CONSUMERS=('justhodl-short-book','justhodl-opportunity-engine','justhodl-cannibals','justhodl-comeback-screener')
FAILED={'key':source.PRIVATE+'43a3b3ea12ac52acf54b4e13642263919477531d9b0579c3201f8b769e5004f4.bin',
    'sha256':'43a3b3ea12ac52acf54b4e13642263919477531d9b0579c3201f8b769e5004f4','bytes':761}


def main():
    s3=boto3.client('s3',region_name='us-east-1')
    with report('ops_6092_share_structure_limit_check') as r:
        assert '**Status:** success' in (ROOT/'aws/ops/reports/latest/ops_6091_share_structure_batch_diagnosis.md').read_text(encoding='utf-8')
        failed=json.loads(source.read(s3,FAILED));assert failed['status']=='failed' and failed['part']==2
        request=source.spec('CCO','income-statement','annual')
        assert request['url'] in failed['source_errors']
        clocks=[]
        for url in failed['source_errors']:
            old=campaign.read_journal(s3,source.request_key(batches.PARENT,url))
            assert old['http_status']==429 and old['status']=='failed'
            clocks.append(datetime.fromisoformat(old['received_at']))
        elapsed=(datetime.now(timezone.utc)-max(clocks)).total_seconds()
        assert elapsed>=900, 'Fifteen-minute minimum recorded cooldown required before the single request'
        source.journal(s3,STATUS,{'request_id':REQUEST,'status':'claimed','failed_batch':FAILED,
            'elapsed_cooldown_seconds':elapsed,'planned_provider_requests':1},True)
        lam,events,scheduler=(boto3.client(name,region_name='us-east-1') for name in ('lambda','events','scheduler'))
        predecessors={}
        for function in CONSUMERS:
            evidence=runtime(lam,s3,events,scheduler,function)
            deployed=lam.get_function(FunctionName=function)
            raw=bounded(urllib.request.urlopen(deployed['Code']['Location'],timeout=40))
            assert base64.b64encode(hashlib.sha256(raw).digest()).decode()==evidence['code_sha256']
            predecessors[function]={'runtime':evidence,'whole_zip':source.retain(s3,raw)}
        source.journal(s3,STATUS,{'request_id':REQUEST,'status':'predecessors_retained','consumer_predecessors':predecessors})
        env=lam.get_function_configuration(FunctionName=baseline.FUNCTION).get('Environment',{}).get('Variables',{})
        credential=env.get('FMP_API_KEY') or env.get('FMP_KEY')
        if not credential:credential=boto3.client('ssm',region_name='us-east-1').get_parameter(Name='/justhodl/fmp/api-key',WithDecryption=True)['Parameter']['Value']
        request_key=source.request_key(REQUEST,request['url'])
        try:result=source.capture(s3,REQUEST,request,{request['symbol']},credential,source.Rate(interval=2),baseline.now)
        except RuntimeError:
            retained=campaign.read_journal(s3,request_key)
            assert retained.get('original') and retained.get('http_status')==429, 'Inspect unexpected failure; no retry'
            outcome='provider_limit_persists';capture_ref=None;raw_ref=retained['original'];code=429
        else:
            outcome='single_source_request_recovered';capture_ref=result['retained_capture'];raw_ref=result['original'];code=200
        protected={STATUS,request_key,raw_ref['key'],FAILED['key'],*(v['whole_zip']['key'] for v in predecessors.values())}
        if capture_ref:protected.add(capture_ref['key'])
        for key in protected:
            assert denied_with_retry('https://justhodl.ai/'+key)
            assert denied_with_retry('https://'+source.BUCKET+'.s3.amazonaws.com/'+key)
        document={'request_id':REQUEST,'status':'complete','outcome':outcome,'http_status':code,
            'elapsed_cooldown_seconds':elapsed,'capture':capture_ref,'original':raw_ref,
            'provider_requests':1,'account_remaining_quota_verified':False,'campaign_resumed':False,
            'consumer_predecessors':predecessors}
        assert all(runtime(lam,s3,events,scheduler,function)==v['runtime'] for function,v in predecessors.items())
        source.journal(s3,STATUS,document)
        r.kv(**document,protected_artifacts_checked=len(protected),producer_invocations=0,
            consumer_invocations=0,private_account_reads=0,public_writes=0,signal_writes=0,
            paid_ai_calls=0,notifications_sent=0,schedules_changed=0)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
