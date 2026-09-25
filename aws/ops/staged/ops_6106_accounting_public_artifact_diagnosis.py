"""Diagnose 6104's public/origin byte mismatch without repeating acquisition.

Every exact immutable public path is compared with its retained origin. Whole
unexpected responses are protected for review; no source or engine is invoked.
This diagnoses edge delivery only and does not certify accounting publication.
"""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import json,sys,urllib.request,urllib.error
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/shared','aws/ops','aws/ops/staged','aws/ops/checks')]
from ops_report import report
from ops_5998_option_population_retained_acceptance import denied_with_retry
import financial_statement_campaign as capture
import statement_research_source as source
import statement_research_store_v2 as store
import statement_producer as producer

REQUEST='chatgpt-accounting-public-artifact-diagnosis-6106'
STATUS=capture.request_key(REQUEST,'diagnostic')
PRIOR=capture.request_key('scheduled-accounting-source:36157251200','refresh')


def main():
    client=boto3.client('s3',region_name='us-east-1',config=Config(max_pool_connections=12,retries={'max_attempts':2}))
    bucket=capture.BUCKET
    with report('ops_6106_accounting_public_artifact_diagnosis') as r:
        failed=(ROOT/'aws/ops/reports/latest/ops_6104_scheduled_accounting_source_acceptance.md').read_bytes()
        assert b'**Status:** failure' in failed and b'assert store.bounded(response)==' in failed
        progress={'request_id':REQUEST,'status':'claimed','failed_report':capture.retain(client,failed)}
        capture.journal(client,STATUS,progress,True)
        try:
            original=store.bounded(client.get_object(Bucket=bucket,Key=PRIOR)['Body'])
            original_ready=store.bounded(client.get_object(Bucket=bucket,Key=producer.READY)['Body'])
            prior,ready=source.strict(original),source.strict(original_ready)
            assert prior['status']=='complete' and ready['request_id']=='scheduled-accounting-source:36157251200'
            assert prior['result']['replay']==ready['replay']
            read=store.reader(client,bucket);reference=ready['replay'];run=store.verified_run(reference,read)
            packet=store.checked(run['output'],'outputs',read)
            assert packet['reported_names']==500 and packet['provider_responses']==3000
            public={reference['manifest_key'],run['input']['key'],run['output']['key'],packet['identity_index']['original']['key'],
                *(row['record']['key'] for row in packet['issuers']),*(row['key'] for row in run['compilers'].values())}
            assert len(public)==511
            progress.update(status='checking',whole_source_journal=capture.retain(client,original),
                whole_ready_document=capture.retain(client,original_ready),replay=reference,public_keys=sorted(public))
            capture.journal(client,STATUS,progress)
            def check(key):
                expected=store.bounded(client.get_object(Bucket=bucket,Key=key)['Body'])
                assert source.sha(expected)==key.rsplit('/',1)[1].split('.')[0], 'Origin does not match immutable identity: '+key
                try:
                    req=urllib.request.Request('https://justhodl.ai/'+key,headers={'User-Agent':'JustHodl-research-acceptance/1.0','Cache-Control':'no-cache','Accept-Encoding':'identity'})
                    try:response=urllib.request.urlopen(req,timeout=35)
                    except urllib.error.HTTPError as exc:response=exc
                    status=response.status;headers={k.lower():v for k,v in response.headers.items() if k.lower() in
                        ('content-type','content-length','content-encoding','cache-control','etag','x-edge-cache','cf-cache-status','cf-ray')}
                    body=store.bounded(response);matched=status==200 and body==expected
                    row={'key':key,'status':status,'headers':headers,'origin_sha256':source.sha(expected),'origin_bytes':len(expected),
                        'public_sha256':source.sha(body),'public_bytes':len(body),'matched':matched}
                    if not matched:
                        row.update(whole_origin=capture.retain(client,expected),whole_public_response=capture.retain(client,body))
                    return row
                except Exception as exc:return {'key':key,'matched':False,'error_type':type(exc).__name__}
            results=[]
            with ThreadPoolExecutor(max_workers=4) as pool:
                for row in pool.map(check,sorted(public)):
                    results.append(row)
                    if len(results)%25==0:
                        progress.update(checked=len(results),results=results);capture.journal(client,STATUS,progress)
                        print(json.dumps({'checked':len(results),'mismatches':sum(not v['matched'] for v in results)}),flush=True)
            mismatches=[row for row in results if not row['matched']]
            complete={**progress,'status':'complete','results':results,'checked':len(results),'mismatches':mismatches,
                'public_original_bytes_match':not mismatches,'generated_at':packet['generated_at']}
            ref=capture.retain(client,source.encoded(complete))
            protected={STATUS,ref['key'],progress['failed_report']['key'],progress['whole_source_journal']['key'],progress['whole_ready_document']['key'],
                *(row[name]['key'] for row in mismatches for name in ('whole_origin','whole_public_response') if name in row)}
            for key in protected:
                assert denied_with_retry('https://justhodl.ai/'+key) and denied_with_retry('https://'+bucket+'.s3.amazonaws.com/'+key)
            capture.journal(client,STATUS,{**complete,'manifest':ref})
            r.kv(manifest=ref,replay=reference,generated_at=packet['generated_at'],public_artifacts_checked=len(results),
                public_original_bytes_match=not mismatches,mismatches=mismatches,protected_artifacts_checked=len(protected),
                source_journal=progress['whole_source_journal'],ready_document=progress['whole_ready_document'],
                provider_requests=0,producer_invocations=0,consumer_invocations=0,private_account_reads=0,
                current_head_writes=0,paid_ai_calls=0,notifications_sent=0,source_arithmetic_repeated=False)
        except Exception as exc:
            capture.journal(client,STATUS,{**progress,'status':'failed','error_type':type(exc).__name__});raise


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
