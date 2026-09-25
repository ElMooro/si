"""Retain all nine complete submissions cited by the accepted 6105 baseline.

One identified SEC GET per distinct accession/issuer, at one request per second.
Every embedded exhibit is retained; this is not a market-wide discovery census.
No FMP, AI, account, native invocation, public replacement or notification.
"""
from pathlib import Path
from datetime import datetime,timezone
import json,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/staged','aws/ops/checks')]
from ops_report import report
from ops_5998_option_population_retained_acceptance import denied_with_retry
from market_runtime_evidence import runtime
from financial_statement_campaign import Rate
import buyback_filing_sources as source

REQUEST='chatgpt-buyback-reported-filings-6109'
STATUS=source.request_key(REQUEST,'all-reported-filings')
BASELINE={'key':source.PRIVATE+'c900fe7e31c7608ab634c66a30abc546340c7c5b629a5649a064f6a73ec70ae4.bin',
    'sha256':'c900fe7e31c7608ab634c66a30abc546340c7c5b629a5649a064f6a73ec70ae4','bytes':10254}
now=lambda:datetime.now(timezone.utc).isoformat()


def main():
    subprocess.run([sys.executable,str(ROOT/'tests/test_buyback_filing_sources.py')],cwd=ROOT,check=True)
    s3,lam,events,scheduler=(boto3.client(name,region_name='us-east-1') for name in ('s3','lambda','events','scheduler'))
    with report('ops_6109_buyback_reported_filing_capture') as r:
        assert '**Status:** success' in (ROOT/'aws/ops/reports/latest/ops_6105_buyback_original_source_baseline.md').read_text(encoding='utf-8')
        baseline=json.loads(source.read(s3,BASELINE))
        scanner_ref=baseline['captures']['data/buyback-scanner.json']['original']
        packet=json.loads(source.read(s3,scanner_ref));plan=source.plan(packet)
        assert plan['reported_rows']==9 and len(plan['requests'])==9 and not plan['unresolved_rows']
        progress={'request_id':REQUEST,'status':'claimed','started_at':now(),'baseline':BASELINE,
            'whole_scanner':scanner_ref,'plan':source.retain(s3,source.encoded(plan)),
            'captures':{},'inspector_source':source.retain(s3,Path(source.__file__).read_bytes())}
        source.journal(s3,STATUS,progress,True)
        try:
            functions=baseline['native_predecessors']
            for function,prior in functions.items():assert runtime(lam,s3,events,scheduler,function)==prior['runtime']
            rate=Rate(1.0)
            for item in plan['requests']:
                request=item['spec'];capture=source.capture(s3,REQUEST,request,rate,now)
                progress['captures'][request['url']]=capture['retained_capture']
                source.journal(s3,STATUS,progress)
                print(json.dumps({'complete_filings':len(progress['captures']),'expected_filings':len(plan['requests']),
                    'retained_documents':len(capture['inventory']['documents'])}),flush=True)
            records=[];protected={STATUS,BASELINE['key'],scanner_ref['key'],progress['plan']['key'],progress['inspector_source']['key']}
            for item in plan['requests']:
                request=item['spec'];ref=progress['captures'][request['url']]
                capture=json.loads(source.read(s3,ref));body=source.read(s3,capture['original'])
                checked=source.inspect(body,request);assert checked==capture['inventory'] and capture['http_status']==200
                # Independently check every declared raw document/text coordinate.
                for document in checked['documents']:
                    for start,end,digest,size in (('byte_start','byte_end','sha256','bytes'),
                            ('text_byte_start','text_byte_end','text_sha256','text_bytes')):
                        raw=body[document[start]:document[end]]
                        assert len(raw)==document[size] and source.sha(raw)==document[digest]
                    for word in document['literal_keyword_occurrences']:
                        assert body[word['byte_start']:word['byte_end']].decode('ascii')==word['reported_text']
                records.append({'request':request,'source_rows':item['source_rows'],'capture':ref,'original':capture['original'],
                    'received_at':capture['received_at'],'filing_date':checked['filing_date'],'documents':len(checked['documents']),
                    'literal_keyword_occurrences':checked['literal_keyword_occurrences'],**source.FLAGS})
                protected.update((ref['key'],capture['original']['key'],capture['request_status_key']))
            manifest=source.retain(s3,source.encoded({**progress,'status':'captured','records':records,'completed_at':now(),
                'all_reported_rows_conserved':True,'all_declared_documents_retained':True,**source.FLAGS}))
            protected.add(manifest['key'])
            for key in sorted(protected):
                assert denied_with_retry('https://justhodl.ai/'+key) and denied_with_retry('https://'+source.BUCKET+'.s3.amazonaws.com/'+key)
            for function,prior in functions.items():assert runtime(lam,s3,events,scheduler,function)==prior['runtime']
            result={'manifest':manifest,'plan':progress['plan'],'baseline':BASELINE,'reported_rows':plan['reported_rows'],
                'distinct_filings':len(records),'documents':sum(row['documents'] for row in records),
                'original_bytes':sum(row['original']['bytes'] for row in records),'records':records,'protected_paths_checked':len(protected),
                'all_reported_rows_conserved':True,'all_declared_documents_retained':True,'native_packages_unchanged':True,
                'sec_requests':len(records),'fmp_requests':0,'producer_invocations':0,'consumer_invocations':0,
                'private_account_reads':0,'public_writes':0,'notifications_sent':0,'paid_ai_calls':0,'signal_writes':0,**source.FLAGS}
            source.journal(s3,STATUS,{**progress,'status':'complete','result':result});r.kv(**result)
        except Exception as exc:
            source.journal(s3,STATUS,{**progress,'status':'failed','error_type':type(exc).__name__});raise


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
