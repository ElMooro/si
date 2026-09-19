"""Retain original TIC definitions, complete monthly histories and release calendar; no engine invocation."""
from datetime import datetime,timezone
from decimal import Decimal
from pathlib import Path
import io,json,sys,time,urllib.error,urllib.parse,urllib.request,zipfile
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/ops')]
from managed_secret import managed_secret
from evidence_store import capture,read_verified
from ops_report import report

SERIES=('FORLTTOTALNET99996','FORLTTREASNET99996','FORLTAGCYNET99996','FORLTCORPNET99996',
        'FORLTEQTYNET99996','FORSTTREASNET99996','USLTTOTALNET99996','FORLTTOTALNET99990','FORLTTOTALNET99991')


def main():
    s3=boto3.client('s3',region_name='us-east-1');bucket='justhodl-dashboard-live'
    key=managed_secret(('FRED_API_KEY','FRED_KEY'),('/justhodl/fred/api-key',))
    assert key,'configured FRED credential unavailable'
    vintage=datetime.now(timezone.utc).date().isoformat();refs={};errors={};docs={}
    def fetch(name,path,params):
        url='https://api.stlouisfed.org/fred/'+path+'?'+urllib.parse.urlencode({'file_type':'json',**params})
        actual=url+'&'+urllib.parse.urlencode({'api_key':key})
        # Sequential pacing; no parallel fleet burst from this diagnostic.
        time.sleep(1)
        try:
            req=urllib.request.Request(actual,headers={'User-Agent':'JustHodl original TIC audit','Accept':'application/json'})
            with urllib.request.build_opener().open(req,timeout=25) as response:raw=response.read(4*1024*1024+1)
            if not raw or len(raw)>4*1024*1024:raise ValueError('source response bound')
            if len(key)>=12 and key.encode() in raw:raise ValueError('credential echoed')
            stamp=datetime.now(timezone.utc);doc=json.loads(raw)
            ref=capture(s3,bucket,'tic',url,raw,stamp)
            assert read_verified(s3,bucket,ref)==raw
            refs[name]={'url':url,'acquired_at':stamp.isoformat(),'evidence':ref};docs[name]=doc
            return doc
        except Exception as exc:
            errors[name]='HTTP_'+str(exc.code) if isinstance(exc,urllib.error.HTTPError) else type(exc).__name__
            return None
    with report('ops_5856_tic_original_source_probe') as r:
        r.kv(production_engines_invoked=0,paid_ai_calls=0,notifications_sent=0,private_account_reads=0,portfolio_writes=0)
        for sid in SERIES:
            doc=fetch(sid+':definition','series',{'series_id':sid,'realtime_start':vintage,'realtime_end':vintage})
            if not doc:continue
            definitions=doc.get('seriess') or []
            assert len(definitions)==1 and definitions[0]['id']==sid,'definition identity differs'
            meta=definitions[0]
            observations=fetch(sid+':observations','series/observations',{'series_id':sid,'realtime_start':vintage,'realtime_end':vintage,
                'units':'lin','sort_order':'desc','limit':4000,'observation_start':meta['observation_start'],'observation_end':vintage})
            rows=(observations or {}).get('observations') or []
            r.kv(series_id=sid,definition=meta,rows=len(rows),reported_count=(observations or {}).get('count'),
                 newest=rows[:2],oldest=rows[-1:] if rows else [],originals={k:v for k,v in refs.items() if k.startswith(sid+':')})
        fetch('series_release','series/release',{'series_id':SERIES[0]})
        fetch('release_dates','release/dates',{'release_id':3,'realtime_start':'2025-01-01','realtime_end':'2027-12-31',
            'sort_order':'desc','limit':1000,'include_release_dates_with_no_data':'true'})
        for name in ('series_release','release_dates'):
            if name in docs:r.kv(release_kind=name,document=docs[name],original=refs[name])
        # Treasury links this complete CSLT archive directly. Compare the same
        # nine identities to the FRED originals, without substituting either.
        url='https://ticdata.treasury.gov/resource-center/data-chart-center/tic/Documents/cslt.zip'
        req=urllib.request.Request(url,headers={'User-Agent':'JustHodl original TIC audit'})
        with urllib.request.build_opener().open(req,timeout=45) as response:raw=response.read(32*1024*1024+1)
        assert 0<len(raw)<=32*1024*1024,'CSLT compressed bound'
        archive=zipfile.ZipFile(io.BytesIO(raw));members=archive.infolist()
        assert len(members)==1 and members[0].filename=='cslt.json' and members[0].file_size<=192*1024*1024,'CSLT archive shape/bound'
        bulk=json.loads(archive.read('cslt.json'));assert bulk.get('releaseID')=='3','CSLT release identity'
        ref=capture(s3,bucket,'tic',url,raw,datetime.now(timezone.utc));assert read_verified(s3,bucket,ref)==raw
        seen=set();comparisons=[]
        for item in bulk['series']:
            sid=item['source_id'].replace('_','').upper()
            if sid not in SERIES:continue
            assert sid not in seen,'duplicate core CSLT identity'
            seen.add(sid)
            native=dict(item['observations']);fred={v['date']:v['value'] for v in (docs.get(sid+':observations') or {}).get('observations',[])}
            common=sorted(set(native)&set(fred));different=[d for d in common if (None if native[d] in ('','.','NA',None) else Decimal(native[d]))!=(None if fred[d] in ('','.','NA',None) else Decimal(fred[d]))]
            comparisons.append({'id':sid,'native_source_id':item['source_id'],'native_rows':len(native),'fred_rows':len(fred),
                'same_periods':set(native)==set(fred),'different_values':len(different),'first_differences':different[:5]})
        assert seen==set(SERIES),'CSLT core identities incomplete'
        r.kv(cslt_original=ref,cslt_series=len(bulk['series']),cslt_transmission_dt=bulk.get('transmissionDt'),
             transmission_is_not_verified_publication_time=True,core_cross_source_comparisons=comparisons)
        r.kv(source_status_codes=errors,retained_responses=len(refs))
        assert all(sid+':observations' in refs for sid in SERIES),'required transaction originals incomplete'
        assert all(docs[sid+':observations'].get('count')==len(docs[sid+':observations'].get('observations',[])) for sid in SERIES),'full histories not returned'


if __name__=='__main__':
    try:main()
    except Exception:
        print('TIC source probe failed; inspect sanitized committed report.')
        sys.exit(1)
