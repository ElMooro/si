"""Retain the current SEC ticker index and inspect every accounting identity.

One durably claimed SEC GET; no FMP request, invocation, account read, public
write, signal or schedule change. Never rewrite an original provider CIK.
"""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from collections import Counter,defaultdict
import json,subprocess,sys,urllib.request,urllib.error
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/shared','aws/ops','aws/ops/staged','aws/ops/checks')]
from ops_report import report
from market_runtime_evidence import runtime
from ops_5998_option_population_retained_acceptance import denied_with_retry
import ops_6071_financial_statement_source_inventory as baseline
import ops_6072_financial_statement_original_probe as probe
import ops_6074_statement_research_candidate as candidate
import financial_statement_campaign as campaign
import statement_research_source as source
import statement_identity_inventory as inventory
BUCKET=baseline.BUCKET
REQUEST='chatgpt-statement-identity-sources-6075'
STATUS=campaign.request_key(REQUEST,'identity')


def main():
    s3=boto3.client('s3',region_name='us-east-1',config=Config(max_pool_connections=16,retries={'max_attempts':2}))
    lam,events,scheduler=(boto3.client(n,region_name='us-east-1') for n in ('lambda','events','scheduler'))
    with report('ops_6075_statement_identity_sources') as r:
        subprocess.run([sys.executable,str(ROOT/'tests/test_statement_identity_inventory.py')],cwd=ROOT,check=True)
        prior={fn:runtime(lam,s3,events,scheduler,fn) for fn in baseline.FUNCTIONS}
        assert prior==json.loads(campaign.read(s3,probe.BASELINE))['runtime']
        manifest=json.loads(campaign.read(s3,candidate.SOURCE))
        progress={'request_id':REQUEST,'source_manifest':candidate.SOURCE,'status':'claimed','url':inventory.INDEX_URL,
            'requested_at':baseline.now()}
        campaign.journal(s3,STATUS,progress,True)
        try:
            req=urllib.request.Request(inventory.INDEX_URL,headers={'User-Agent':'JustHodl Research raafouis@gmail.com',
                'Accept':'application/json','Accept-Encoding':'identity'})
            try:response=urllib.request.build_opener(campaign.NoRedirect()).open(req,timeout=30)
            except urllib.error.HTTPError as exc:response=exc
            status=response.status
            headers={k.lower():v for k,v in response.headers.items() if k.lower() in ('date','content-type','content-length','etag','last-modified')}
            body=candidate.store.bounded(response)
            progress.update(status='response_retained',received_at=baseline.now(),http_status=status,headers=headers,original=campaign.retain(s3,body))
            campaign.journal(s3,STATUS,progress)
            if status!=200 or not body:raise ValueError('SEC index unavailable; complete response retained; no retry')
            index=inventory.ticker_index(body)
            index_ref=campaign.retain(s3,source.encoded(index))
            def inspect(item):
                url,ref=item;cap=json.loads(campaign.read(s3,ref));assert cap['spec']['url']==url and cap['http_status']==200
                rows=source.strict(campaign.read(s3,cap['original']))
                assert isinstance(rows,list) and len(rows)==cap['inventory']['rows']
                return inventory.compare(cap,rows,index)
            checked=[]
            with ThreadPoolExecutor(max_workers=8) as pool:
                for rows in pool.map(inspect,sorted(manifest['captures'].items())):checked.extend(rows)
            assert len(checked)==20941
            results={'contract':'statement-identity-source-diagnostic.v1','generated_at':baseline.now(),
                'source_manifest':candidate.SOURCE,'sec_index_capture':dict(progress),'sec_index_inventory':index_ref,'rows':checked,
                'current_index_does_not_establish_historical_security_continuity':True,'native_publication_qualified':False}
            result_ref=campaign.retain(s3,source.encoded(results))
            by_symbol=defaultdict(Counter)
            for row in checked:
                by_symbol[row['requested_symbol']][row['current_identity_status']]+=1
                if row['statement_identity_problem']:by_symbol[row['requested_symbol']][row['statement_identity_problem']]+=1
            problems={symbol:dict(counts) for symbol,counts in sorted(by_symbol.items()) if any(k!='current_ticker_cik_pair_corroborated' for k in counts)}
            counts={'original_rows':len(checked),'sec_index_rows':len(index['rows']),
                'current_identity_statuses':dict(Counter(v['current_identity_status'] for v in checked)),
                'statement_identity_problems':dict(Counter(v['statement_identity_problem'] for v in checked if v['statement_identity_problem'])),
                'clock_issues':dict(Counter(problem for v in checked for problem in v['clock_issues']))}
            for ref in (progress['original'],index_ref,result_ref):
                campaign.read(s3,ref)
            for key in (STATUS,progress['original']['key'],index_ref['key'],result_ref['key']):
                assert denied_with_retry('https://justhodl.ai/'+key) and denied_with_retry('https://'+BUCKET+'.s3.amazonaws.com/'+key)
            assert {fn:runtime(lam,s3,events,scheduler,fn) for fn in baseline.FUNCTIONS}==prior
            campaign.journal(s3,STATUS,{**progress,'status':'complete','diagnostic':result_ref,'index_inventory':index_ref,'counts':counts})
            examples=[v for v in checked if v['statement_identity_problem'] or v['current_identity_status']!='current_ticker_cik_pair_corroborated']
            r.kv(sec_index_capture=progress,diagnostic=result_ref,index_inventory=index_ref,counts=counts,problems_by_symbol=problems,
                examples=examples[:12],provider_requests=1,fmp_requests=0,engine_invocations=0,consumer_invocations=0,
                private_account_reads=0,public_writes=0,signal_writes=0,notifications_sent=0,paid_ai_calls=0,schedules_changed=0,
                current_index_only=True,historical_security_continuity_verified=False,native_publication_qualified=False)
        except Exception as exc:
            campaign.journal(s3,STATUS,{**progress,'status':'failed','error_type':type(exc).__name__})
            raise


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
