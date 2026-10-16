"""Offline synthetic disjoint-identity stress; actual writer with in-memory storage.
No cloud client, provider fetch, latency emulation, or production capacity claim.
"""
from pathlib import Path
import argparse,json,resource,sys,time
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/shared/tests')]
import etf_desk_store as desk
import etf_holdings_model as model
from test_etf_ownership_summary import pair,GENERATED
from test_etf_holdings_store import Storage
FUNDS=sorted(desk.model.SUPPLEMENT_FUNDS)


def accumulator(current,prior):
    acc=model.OwnershipSummary(GENERATED,16)
    for i,ticker in enumerate(FUNDS):
        a,b=pair()
        for role,snapshot,total in ((0,a,current),(1,b,prior)):
            size=total//16+(i<total%16); row=snapshot['rows'][0]
            snapshot['ticker']=ticker
            snapshot['rows']=[{**row,'identity_key':format(i*1000000+role*500000+j,'064x'),
                              'row_id':format(i*1000000+role*500000+j,'064x')} for j in range(size)]
            snapshot['quality']['returned_rows']=size
            snapshot['effective_dates']={next(iter(snapshot['effective_dates'])):size}
        acc.add(ticker,a,b,model.native.compare(a,b),{'synthetic':'current'},{'synthetic':'prior'},{'synthetic':'comparison'},{})
    return acc


def run(current,prior):
    db=Storage();read=desk.reader(db,'offline');start=time.perf_counter()
    acc=accumulator(current,prior);accumulated=time.perf_counter()-start
    tick=time.perf_counter()
    with desk.ArtifactWriter(db,'offline',read) as writer: ref=acc.finish(writer)
    publish=time.perf_counter()-tick
    if ref['status']!='complete':return {'status':ref['status'],'reason':ref.get('reason'),'emitted_objects':len(db.objects)}
    manifest=json.loads(read(ref['manifest']['key']));writes=len(db.writes);reads=len(db.reads)
    tick=time.perf_counter()
    def verify(k,v):assert read(k)==v
    assert accumulator(current,prior).finish(verify)==ref
    replay=time.perf_counter()-tick
    return {'synthetic':True,'case':'disjoint-all-qualified','current_rows':current,'prior_rows':prior,
            'records':manifest['record_count'],'pages':manifest['page_count'],
            'manifest_bytes':ref['manifest']['bytes'],'payload_bytes':sum(map(len,db.objects.values())),
            'put_attempts':writes,'write_readback_gets':reads,'replay_put_attempts':len(db.writes)-writes,
            'replay_network_gets_in_cache_fit_fixture':len(db.reads)-reads,
            'accumulator_including_comparison_seconds':accumulated,'finish_in_memory_writer_seconds':publish,
            'replay_including_comparison_seconds':replay,'peak_process_rss_mib':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1024,
            'network_latency_measured':False,'lambda_capacity_established':False,
            'vendor_requests':0,'aws_calls':0}


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--record-cap',action='store_true');args=p.parse_args()
    print(json.dumps(run(33333,33334) if args.record_cap else run(22809,25252),sort_keys=True))
