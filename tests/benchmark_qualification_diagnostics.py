"""Offline invented 300-fund diagnostic payload benchmark; no network or real holdings."""
import json, sys, time
from pathlib import Path
sys.path[:0] = [str(Path(__file__).resolve().parents[1]/'aws/shared'),str(Path(__file__).resolve().parents[1]/'aws/shared/tests')]
import etf_holdings_model as model
import etf_holdings_native as native
from test_etf_ownership_summary import pair, GENERATED


def run(policy, scenario):
    a,b=pair()
    if scenario=='overlapping':
        for s in (a,b):
            s['quality'].update(status='partial',pagination_complete=False,missing_identity_rows=1,duplicate_identity_rows=1,rows_with_field_errors=1)
            s['source_valid_until']=GENERATED;s['effective_dates']['2020-01-01']=1;s['processed_date']='2099-01-01'
    ref=lambda kind: {'key':model.PREFIX+kind+'/'+'a'*64+'.json','sha256':'a'*64,'bytes':123456}
    out={};acc=model.OwnershipSummary(GENERATED,300,policy);start=time.perf_counter()
    for fund,tags in sorted(model.catalog.ETF_UNIVERSE.items()):
        acc.add(fund,a,b,native.compare(a,b),ref('snapshots'),ref('snapshots'),ref('comparisons'),
                {k:tags.get(k) for k in ('category','subcategory','region')})
    r=acc.finish(lambda k,v:out.__setitem__(k,v))
    assert r['status']=='complete',r
    return {'manifest_bytes':r['manifest']['bytes'],'total_bytes':sum(map(len,out.values())),
            'objects':len(out),'page_objects':json.loads(out[r['manifest']['key']])['page_count'],'seconds':round(time.perf_counter()-start,6)},out,r


def benchmark():
    results={}
    for scenario in ('qualified','overlapping'):
        old,ob,orr=run(None,scenario);new,nb,nrr=run(model.DIAGNOSTIC_POLICY,scenario)
        pages={k:v for k,v in ob.items() if k!=orr['manifest']['key']}
        assert all(nb[k]==v for k,v in pages.items())
        results[scenario]={'old':old,'diagnostic':new,'additional_bytes':new['total_bytes']-old['total_bytes'],
                           'additional_objects_per_new_run':new['objects']-old['objects'],'row_pages_identical':True}
    return {'synthetic_only':True,'funds':300,'provider_calls':0,'new_schedules':0,'results':results}

if __name__=='__main__': print(json.dumps(benchmark(),indent=2,sort_keys=True))
