"""Offline synthetic/retained-fixture benchmark; never creates a cloud client.
Run each case in a new process so ru_maxrss describes that case's peak RSS.
"""
from pathlib import Path
import argparse,copy,gzip,json,resource,sys,time
from unittest import mock
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/shared/tests')]
import etf_holdings_model as m
import etf_holdings_store as store
import etf_desk_store as desk
from test_etf_holdings_store import Storage,fixture
from test_etf_ownership_summary import pair,GENERATED


def run(case,n):
    start=time.perf_counter();attempts=0;objects={};result={}
    if case in ('producer-old','producer-new','producer-scale','producer-scale-old'):
        with mock.patch.dict(m.catalog.ETF_UNIVERSE,{'SPY':{'category':'broad'}},clear=True):
            db,i=fixture()
            if case in ('producer-scale','producer-scale-old'):
                from test_etf_holdings_native import collection,row
                count=max(1,(n+4999)//5000)
                current,raw=collection([row(figi='SYNTHETIC-'+str(j)) for j in range(n)],pages=count)
                prior,old=collection([row(figi='SYNTHETIC-'+str(j),processed_date='2026-08-20',effective_date='2026-08-19') for j in range(n)],processed='2026-08-20',pages=count)
                prior['cutoff']='2026-08-22';prior['selection']['url']=m.native.selection_url('SPY','2026-08-22')
                db.objects.update(raw);db.objects.update(old);i['collections']={'SPY':{'current':current,'prior':prior}}
            if case in ('producer-new','producer-scale'):i['ownership_summary_policy']=m.OWNERSHIP_POLICY
            original=db.put_object
            def write(**kw):
                nonlocal attempts
                attempts+=1;return original(**kw)
            db.put_object=write;reader=store.reader(db,'fixture');before=set(db.objects)
            tick=time.perf_counter();out=store.compile_output(i,reader,lambda k,v:store.immutable(db,'fixture',k,v));compile_s=time.perf_counter()-tick
            ref=store.retain(db,'fixture',i,out,reader)
            tick=time.perf_counter();assert store.replay(ref,reader)==out;replay_s=time.perf_counter()-tick
            objects={k:v for k,v in db.objects.items() if k not in before}
            result={'producer_put_attempts':attempts,'compile_seconds':compile_s,'replay_seconds':replay_s,'status':out.get('ownership_summary',{}).get('status','legacy')}
            if case in ('producer-new','producer-scale'):
                packet={**out,'replay':ref};db.objects[m.CURRENT]=m.encoded(packet)
                look={'contract':'etf-lookthrough-inputs.v1','kind':'lookthrough','generated_at':GENERATED,'canonical_source':store.snapshot(db,'fixture',m.CURRENT),'previous':None}
                before_calls=attempts;tick=time.perf_counter();store.compile_output(look,reader,lambda *a:None)
                result.update(lookthrough_seconds=time.perf_counter()-tick,lookthrough_put_attempts=attempts-before_calls)
    elif case=='desk-predecessor':
        f=json.loads(gzip.decompress((ROOT/'tests/fixtures/holdings-summary-desk-predecessor-synthetic.json.gz').read_bytes()))
        db=Storage();db.objects={k:v.encode() for k,v in f['objects'].items()}
        with mock.patch.object(desk.catalog,'DESK',('SPY','VOO','BND')),mock.patch.dict(m.catalog.ETF_UNIVERSE,{'SPY':{'category':'broad'},'VOO':{'category':'broad'}},clear=True):
            tick=time.perf_counter();out=desk.replay(f['packet']['replay'],desk.reader(db,'fixture'))
            result={'replay_seconds':time.perf_counter()-tick,'status':'old_desk_byte_equal'}
            assert out=={k:v for k,v in f['packet'].items() if k!='replay'}
    else:
        a,b=pair();base=a['rows'][0];old=b['rows'][0]
        a['rows']=[{**base,'identity_key':format(i,'064x'),'row_id':format(i,'064x')} for i in range(n)]
        b['rows']=[{**old,'identity_key':format(i,'064x'),'row_id':format(i+n,'064x')} for i in range(n)]
        a['quality']['returned_rows']=b['quality']['returned_rows']=n
        a['effective_dates']={'2026-09-17':n};b['effective_dates']={'2026-08-19':n}
        comparison=m.native.compare(a,b);tick=time.perf_counter()
        def build(emit):
            accumulator=m.OwnershipSummary(GENERATED,1)
            accumulator.add('SPY',a,b,comparison,{'synthetic':'current'},{'synthetic':'prior'},{'synthetic':'comparison'},{})
            return accumulator.finish(emit)
        def put(k,v):
            nonlocal attempts
            attempts+=1;objects[k]=v
        ref=build(put);compile_s=time.perf_counter()-tick;first=dict(objects)
        def verify(k,v):assert first[k]==v
        tick=time.perf_counter();assert build(verify)==ref;replay_s=time.perf_counter()-tick
        result={'synthetic_positions_per_snapshot':n,'compile_seconds':compile_s,'replay_seconds':replay_s,'status':ref['status'],'reason':ref.get('reason')}
        if ref['status']=='complete':
            manifest=json.loads(objects[ref['manifest']['key']]);result.update(records=manifest['record_count'],pages=manifest['page_count'],manifest_bytes=ref['manifest']['bytes'])
    result.update(case=case,total_seconds=time.perf_counter()-start,peak_rss_mib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1024,
                  emitted_objects=len(objects),payload_bytes=sum(map(len,objects.values())),attempted_puts=attempts,
                  vendor_requests=0,aws_invocations=0,synthetic=True)
    return result


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('case',choices=['producer-old','producer-new','producer-scale','producer-scale-old','desk-predecessor','worst']);parser.add_argument('--rows',type=int,default=40000)
    args=parser.parse_args();print(json.dumps(run(args.case,args.rows),sort_keys=True))
