"""Inspect the one retained option candidate without recollecting or publishing."""
from pathlib import Path
from collections import Counter
import json,sys,time
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/shared')]
from ops_report import report
import option_flow_store as store
import option_flow_research as model
BUCKET='justhodl-dashboard-live'
REQUEST='chatgpt-ops-5991-option-flow-candidate'


def main():
    s3=boto3.client('s3',region_name='us-east-1',config=Config(max_pool_connections=24))
    with report('ops_5992_option_candidate_replay_diagnosis') as r:
        status=json.loads(store.bounded(s3.get_object(Bucket=BUCKET,Key=store.request_key(REQUEST))['Body']))
        r.kv(request_state={k:status.get(k) for k in ('status','phase','failure_class','retained_input','candidate_replay','started_at','completed_at','provider_requests')},
            selected_underlyings=len(status.get('selected',[])))
        if not status.get('retained_input'):
            checkpoints={}
            for symbol in status.get('selected',[]):
                key=store.request_key(REQUEST)[:-5]+'/chains/'+symbol+'.json'
                try:chain=json.loads(store.bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body']))
                except Exception as exc:
                    if not store.missing(exc):raise
                    checkpoints[symbol]={'status':'no_checkpoint'};continue
                checkpoints[symbol]={'stop':chain.get('stop'),'pages':len(chain.get('pages',[])),'request_budget':chain.get('request_budget'),
                    'underlying':chain.get('underlying'),'started_at':chain.get('started_at'),'completed_at':chain.get('completed_at')}
            r.kv(checkpoints=checkpoints,provider_requests_this_audit=0,engine_invocations=0,publications=0)
            return
        read=store.reader(s3,BUCKET);inputs=model.checked(status['retained_input'],read,'inputs');results={};counts=Counter();started=time.monotonic()
        for symbol,chain in inputs['chains'].items():
            phase='reconstruct';part=None
            try:
                output=model.reconstruct(symbol,chain,read,inputs['generated_at']);counts[output['research_status']]+=1
                phase='record_blocks';groups={}
                for row in output['rows']:groups.setdefault(row['evidence']['page'],[]).append(row)
                for part,rows in groups.items():model.codec.pack(rows)
                results[symbol]={'status':'replayed','rows':len(output['rows']),'pages':len(chain['pages']),'stop':chain['stop']}
            except Exception as exc:
                # These are pure reviewed parsers of already protected source bytes;
                # no live provider, credential, request URL or raw body is printed.
                results[symbol]={'status':'failed','phase':phase,'source_page':part,'failure_class':type(exc).__name__,
                    'reason':str(exc)[:200] if isinstance(exc,(ValueError,KeyError,TypeError,AssertionError)) else 'Inspect retained source with reviewed parser'}
        r.kv(chain_replays=results,capture_status_counts=dict(counts),replay_seconds=round(time.monotonic()-started,3),
            provider_requests_this_audit=0,engine_invocations=0,publications=0,private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
