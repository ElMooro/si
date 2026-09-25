"""Reconstruct every document behind accepted 6109 filing evidence.

Retained originals only. No vendor requests, native/consumer invocations,
public S3 writes, account reads, notifications, AI or schedule changes.
Report only an explicit public SEC metadata projection, never whole journals.
"""
from pathlib import Path
import json, subprocess, sys
import boto3
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/p) for p in ('aws/ops','aws/ops/staged','aws/ops/checks')]
from ops_report import report
from market_runtime_evidence import runtime
from ops_5998_option_population_retained_acceptance import denied_with_retry
import buyback_filing_sources as source
import buyback_document_evidence as model
import ops_6109_buyback_reported_filing_capture as accepted

REQUEST = 'chatgpt-buyback-document-evidence-6112'
STATUS = source.request_key(REQUEST, 'reconstruction')
MANIFEST = {'key': source.PRIVATE+'460569af0f790eea8307e770017eb5a6efa19897700712c2d682571d1dd1359f.bin',
    'sha256':'460569af0f790eea8307e770017eb5a6efa19897700712c2d682571d1dd1359f', 'bytes':13545}


def main():
    subprocess.run([sys.executable,str(ROOT/'tests/test_buyback_document_evidence.py')],cwd=ROOT,check=True)
    s3,lam,events,scheduler=(boto3.client(name,region_name='us-east-1') for name in ('s3','lambda','events','scheduler'))
    with report('ops_6112_buyback_document_evidence') as r:
        assert '**Status:** success' in (ROOT/'aws/ops/reports/latest/ops_6109_buyback_reported_filing_capture.md').read_text(encoding='utf-8')
        prior=json.loads(source.bounded(s3.get_object(Bucket=source.BUCKET,Key=accepted.STATUS)['Body'],source.MAX))
        assert prior['status']=='complete' and prior['result']['manifest']==MANIFEST
        baseline=json.loads(source.read(s3,accepted.BASELINE))
        for fn,state in baseline['native_predecessors'].items():
            assert runtime(lam,s3,events,scheduler,fn)==state['runtime']
        compiler=source.retain(s3,Path(model.__file__).read_bytes())
        progress={'request_id':REQUEST, 'status':'claimed', 'manifest':MANIFEST, 'compiler':compiler}
        source.journal(s3,STATUS,progress,True)
        try:
            catalog=model.compile_output(MANIFEST,lambda ref:source.read(s3,ref))
            encoded=source.encoded(catalog)
            replay=model.compile_output(MANIFEST,lambda ref:source.read(s3,ref))
            assert source.encoded(replay)==encoded
            assert (catalog['reported_rows'],catalog['distinct_filings'],catalog['documents'],catalog['original_bytes'])==(9,9,116,2997796)
            assert catalog['source_packet']['sha256']=='3dc753ede042809d11d3cf8639e6d854d0031940adc1c112b4d75c9da9712704'
            assert b'audit-private/' not in encoded
            projection=source.retain(s3,encoded)
            protected={STATUS,compiler['key'],projection['key']}
            for key in protected:
                assert denied_with_retry('https://justhodl.ai/'+key)
                assert denied_with_retry('https://'+source.BUCKET+'.s3.amazonaws.com/'+key)
            for fn,state in baseline['native_predecessors'].items():
                assert runtime(lam,s3,events,scheduler,fn)==state['runtime']
            result={'source_manifest':MANIFEST,'compiler':compiler,'projection':projection,'catalog':catalog,
                'complete_original_replay_verified':True,'document_ranges_rechecked':116,
                'protected_paths_checked':len(protected),'native_packages_unchanged':True,
                'provider_requests':0,'producer_invocations':0,'consumer_invocations':0,
                'private_account_reads':0,'public_writes':0,'notifications_sent':0,'paid_ai_calls':0,
                'signal_writes':0,'schedules_changed':0,**model.FLAGS}
            source.journal(s3,STATUS,{**progress,'status':'complete','result':result});r.kv(**result)
        except Exception as exc:
            source.journal(s3,STATUS,{**progress,'status':'failed','error_type':type(exc).__name__});raise


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
