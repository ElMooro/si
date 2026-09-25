"""Independent whole-byte/SGML verification of the accepted document catalog.

Uses retained originals only, without importing the production inspector in
the verifier. No vendor request, native invocation, public write or message.
"""
from pathlib import Path
import json,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('scripts','aws/ops','aws/ops/staged','aws/ops/checks')]
from ops_report import report
from market_runtime_evidence import runtime
from ops_5998_option_population_retained_acceptance import denied_with_retry
import buyback_filing_sources as source
import verify_buyback_documents as independent
import ops_6112_buyback_document_evidence as prior
import ops_6109_buyback_reported_filing_capture as capture

REQUEST='chatgpt-buyback-documents-independent-6113'
STATUS=source.request_key(REQUEST,'acceptance')


def main():
    subprocess.run([sys.executable,str(ROOT/'tests/test_buyback_document_verifier.py')],cwd=ROOT,check=True)
    s3,lam,events,scheduler=(boto3.client(name,region_name='us-east-1') for name in ('s3','lambda','events','scheduler'))
    with report('ops_6113_buyback_document_independent_acceptance') as r:
        assert '**Status:** success' in (ROOT/'aws/ops/reports/latest/ops_6112_buyback_document_evidence.md').read_text(encoding='utf-8')
        state=json.loads(source.bounded(s3.get_object(Bucket=source.BUCKET,Key=prior.STATUS)['Body'],source.MAX))
        assert state['status']=='complete' and state['manifest']==prior.MANIFEST
        result=state['result'];projection=result['projection'];catalog=result['catalog']
        baseline=json.loads(source.read(s3,capture.BASELINE))
        for fn,old in baseline['native_predecessors'].items():
            assert runtime(lam,s3,events,scheduler,fn)==old['runtime']
        verifier=source.retain(s3,Path(independent.__file__).read_bytes())
        progress={'request_id':REQUEST,'status':'claimed','catalog':projection,'verifier':verifier}
        source.journal(s3,STATUS,progress,True)
        try:
            raw=source.read(s3,projection);assert raw==source.encoded(catalog)
            refs={f['original_sha256']:{'key':source.PRIVATE+f['original_sha256']+'.bin',
                'sha256':f['original_sha256'],'bytes':f['original_bytes']} for f in catalog['filings']}
            proof=independent.verify(raw,projection['sha256'],lambda digest:source.read(s3,refs[digest]))
            assert (proof['verified_submissions'],proof['verified_documents'],proof['verified_original_bytes'])==(9,116,2997796)
            assert proof['reported_rows_conserved']==9 and not proof['production_inspector_imported']
            retained_proof=source.retain(s3,source.encoded(proof))
            for key in (STATUS,verifier['key'],retained_proof['key']):
                assert denied_with_retry('https://justhodl.ai/'+key)
                assert denied_with_retry('https://'+source.BUCKET+'.s3.amazonaws.com/'+key)
            for fn,old in baseline['native_predecessors'].items():
                assert runtime(lam,s3,events,scheduler,fn)==old['runtime']
            final={'catalog':projection,'verifier':verifier,'proof':proof,'retained_proof':retained_proof,
                'native_packages_unchanged':True,'protected_paths_checked':3,'provider_requests':0,
                'producer_invocations':0,'consumer_invocations':0,'public_writes':0,'private_account_reads':0,
                'notifications_sent':0,'paid_ai_calls':0,'signal_writes':0,'schedules_changed':0,
                'authorization_amount_qualified':False,'buyback_execution_qualified':False,'forecast_qualified':False,'sizing_qualified':False}
            source.journal(s3,STATUS,{**progress,'status':'complete','result':final});r.kv(**final)
        except Exception as exc:
            source.journal(s3,STATUS,{**progress,'status':'failed','error_type':type(exc).__name__});raise


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
