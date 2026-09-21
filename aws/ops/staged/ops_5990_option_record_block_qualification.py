"""Replay and compact qualified option records privately, without recollection."""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
import json, subprocess, sys, time
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged'),str(ROOT/'aws/shared')]
from ops_report import report
import ops_5987_options_dependency_preflight as baseline
import ops_5988_option_originals_qualification as source_audit
import option_contract_research as model
import option_research_rows as codec

PREFIX=baseline.PREFIX;BUCKET=baseline.BUCKET
QUALIFIED={'key':PREFIX+'fc0aeef1b0b18b4e0c344b479dbc1b1f52ff0cd8ccc2f22d46ac6d7674752e59.bin',
    'sha256':'fc0aeef1b0b18b4e0c344b479dbc1b1f52ff0cd8ccc2f22d46ac6d7674752e59','bytes':8426}


def main():
    import resource
    s3=boto3.client('s3',region_name='us-east-1');started=time.monotonic()
    with report('ops_5990_option_record_block_qualification') as r:
        subprocess.run([sys.executable,str(ROOT/'tests/test_option_research_rows.py')],cwd=ROOT,check=True)
        qualified=json.loads(source_audit.checked(s3,QUALIFIED))
        for name,module in (('option_contract_research',model),('option_snapshot_capture',model.capture)):
            assert source_audit.checked(s3,qualified['compilers'][name])==Path(module.__file__).read_bytes()
        inputs=json.loads(source_audit.checked(s3,qualified['source_manifest']))
        protected={QUALIFIED['key'],qualified['source_manifest']['key']}
        result={};total_rows=total_bytes=total_blocks=0
        for symbol,chain in inputs['option_snapshots'].items():
            assert chain['pagination_complete'];source_audit.summarize_chain(s3,chain)
            pages=[]
            for meta in chain['pages']:
                raw=source_audit.checked(s3,meta['original']);protected.add(meta['original']['key'])
                pages.append({**meta,'raw':raw})
            output=model.compile_rows(symbol,pages,True)
            original=baseline.evidence.encoded(output)
            assert len(original)==qualified['outputs'][symbol]['bytes']
            assert model.capture.sha(original)==qualified['outputs'][symbol]['sha256']
            groups=defaultdict(list)
            for row in output['rows']:groups[row['evidence']['page']].append(row)
            blocks=[];restored=[];sizes=[]
            for page,rows in groups.items():
                block=codec.pack(rows);raw=codec.encoded(block);ref=baseline.retain(s3,raw)
                decoded=codec.unpack(json.loads(source_audit.checked(s3,ref)))
                assert decoded==rows
                restored.extend(decoded);protected.add(ref['key']);sizes.append(len(raw))
                blocks.append({**ref,'source_page':page,'rows':len(rows),'expanded_sha256':block['expanded_sha256']})
            assert restored==output['rows']
            total_rows+=len(restored);total_bytes+=sum(sizes);total_blocks+=len(blocks)
            result[symbol]={'blocks':blocks,'rows':len(restored),'block_bytes':sum(sizes),'largest_block_bytes':max(sizes),
                'qualified_expanded_output_sha256':qualified['outputs'][symbol]['sha256'],
                'qualified_expanded_output_bytes':len(original),'lossless_roundtrip':True}
            del pages,output,original,groups,restored
        compiler=baseline.retain(s3,Path(codec.__file__).read_bytes());protected.add(compiler['key'])
        manifest={'contract':'option-record-block-qualification.v1','generated_at':model.capture.now(),
            'original_qualification':QUALIFIED,'codec':compiler,'results':result,
            'rows':total_rows,'blocks':total_blocks,'block_bytes':total_bytes,
            'elapsed_s':round(time.monotonic()-started,3),'peak_runner_rss_kib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            'scope':'Lossless bounded record blocks of previously qualified retained originals. No current public packet changed.'}
        ref=baseline.retain(s3,baseline.evidence.encoded(manifest));protected.add(ref['key'])
        def check(key):
            assert baseline.evidence.denied('https://justhodl.ai/'+key) and baseline.evidence.denied('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        with ThreadPoolExecutor(max_workers=6) as pool:list(pool.map(check,sorted(protected)))
        r.kv(retained_manifest=ref,summary={k:{n:v for n,v in row.items() if n!='blocks'} for k,row in result.items()},
            total_rows=total_rows,total_blocks=total_blocks,total_block_bytes=total_bytes,
            elapsed_s=manifest['elapsed_s'],peak_runner_rss_kib=manifest['peak_runner_rss_kib'],
            protected_artifacts_checked=len(protected),originals_anonymously_denied=True,
            provider_requests=0,engine_invocations=0,private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0,schedules_changed=0)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
