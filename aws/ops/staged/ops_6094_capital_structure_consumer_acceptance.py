"""Verify four native consumer guard packages without invoking consumers.

Whole predecessor ZIPs, exact release commits, actual package bytes, unchanged
runtime/schedules and public-packet status. A stale public consumer packet is
reported honestly; this operation never refreshes signals or notifications.
"""
from pathlib import Path
import base64,hashlib,io,json,subprocess,sys,urllib.request,zipfile
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/shared','aws/ops','aws/ops/staged','aws/ops/checks')]
from ops_report import report
from market_runtime_evidence import runtime,bounded
from ops_5998_option_population_retained_acceptance import denied_with_retry
import share_structure_sources as source
import share_structure_campaign as campaign
import ops_6092_share_structure_limit_check as baseline

PACKETS={'justhodl-short-book':'data/short-book.json','justhodl-opportunity-engine':'data/opportunities.json',
    'justhodl-cannibals':'data/cannibals.json','justhodl-comeback-screener':'data/comeback-screener.json'}


def main():
    s3,lam,events,scheduler=(boto3.client(name,region_name='us-east-1') for name in ('s3','lambda','events','scheduler'))
    with report('ops_6094_capital_structure_consumer_acceptance') as r:
        expected=subprocess.check_output(['git','log','-1','--format=%H','--',
            'aws/ops/staged/ops_6094_capital_structure_consumer_acceptance.py'],cwd=ROOT,text=True).strip()
        saved=campaign.read_journal(s3,baseline.STATUS)
        assert saved['status']=='complete' and set(saved['consumer_predecessors'])==set(PACKETS)
        accepted={};public={}
        for function,key in PACKETS.items():
            prior=saved['consumer_predecessors'][function];old=source.read(s3,prior['whole_zip'])
            assert base64.b64encode(hashlib.sha256(old).digest()).decode()==prior['runtime']['code_sha256']
            current=runtime(lam,s3,events,scheduler,function)
            assert current['receipt']['commit']==expected
            for field in ('timeout','memory_mb','schedules','function_name','runtime','handler','architectures','role','ephemeral_storage_mb'):
                assert current[field]==prior['runtime'][field], (function,field,'changed unexpectedly')
            location=lam.get_function(FunctionName=function)['Code']['Location']
            raw=bounded(urllib.request.urlopen(location,timeout=40))
            assert base64.b64encode(hashlib.sha256(raw).digest()).decode()==current['code_sha256']
            with zipfile.ZipFile(io.BytesIO(raw)) as archive:
                helper=archive.read('capital_structure_context.py')
            assert helper==(ROOT/'aws/shared/capital_structure_context.py').read_bytes()
            scope={};exec(compile(helper,'verified-capital-structure-context','exec'),scope)
            view=scope['decision_view']({'tickers':{'ABC':{'buyback_net_yield_pct':99,'flags':['BUYBACK_BLUFF']}},'call':'LONG'})
            assert view['tickers']=={} and view['calls_eligible'] is False and view['independent_investment_votes']==0
            assert denied_with_retry('https://justhodl.ai/'+prior['whole_zip']['key'])
            assert denied_with_retry('https://'+source.BUCKET+'.s3.amazonaws.com/'+prior['whole_zip']['key'])
            request=urllib.request.Request('https://justhodl.ai/'+key,
                headers={'User-Agent':'JustHodl-research-acceptance/1.0','Cache-Control':'no-cache'})
            response=urllib.request.urlopen(request,timeout=30);body=bounded(response)
            packet=json.loads(body)
            public[function]={'key':key,'http_status':response.status,'bytes':len(body),'sha256':hashlib.sha256(body).hexdigest(),
                'generated_at':packet.get('generated_at') or packet.get('as_of'),
                'guard_output_observed':packet.get('capital_structure_context',{}).get('contract')=='capital-structure-context.v1',
                'packet_refreshed_by_acceptance':False}
            accepted[function]=current
        r.kv(source_commit=expected,native_packages=accepted,public_packets=public,
            actual_zip_bytes_verified=True,predecessor_zips_reverified=4,consumer_invocations=0,
            producer_invocations=0,provider_requests=0,private_account_reads=0,public_writes=0,
            signal_writes=0,portfolio_writes=0,paid_ai_calls=0,notifications_sent=0,schedules_changed=0,
            all_public_guard_outputs_observed=all(v['guard_output_observed'] for v in public.values()),
            forecast_qualified=False,sizing_qualified=False)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
