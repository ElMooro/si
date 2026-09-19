"""Retain original country identities and the official holdings table; no engine invocation."""
from datetime import datetime,timezone
from pathlib import Path
import io,json,sys,time,urllib.error,zipfile
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/ops'),str(ROOT/'aws/lambdas/justhodl-capital-inflows/source')]
from managed_secret import managed_secret
from ops_report import report
from tic_store import acquire,raw_reader
import tic_original as native

LEGACY=('JPNNFAQ027S','MHCMM027S','MFFICHQ027S','MFFICQQ027S','MFFICOQ027S','MFFICBQ027S',
        'MFFICCQ027S','MFFICTQ027S','FANTPDQ027S')
CHECK=('FORLTTREASPOS10308','FORLTTREASPOS10251','FORLTTREASPOS11703','FORTREASPOS10251',
       'FORTREASPOS11703','FORTREASNET69995','FORTREASNET99996','FORLTEQTYNET69995','FORLTEQTYNET99996','FORLTTREASPOS35319')
TABLES=('https://ticdata.treasury.gov/Publish/slt_table5.html',
        'https://ticdata.treasury.gov/resource-center/data-chart-center/tic/Documents/slt_table5.txt')

def main():
    s3=boto3.client('s3',region_name='us-east-1');bucket='justhodl-dashboard-live';read=raw_reader(s3,bucket)
    fred_key=managed_secret(('FRED_API_KEY','FRED_KEY'),('/justhodl/fred/api-key',));assert fred_key,'configured FRED credential unavailable'
    vintage=datetime.now(timezone.utc).date().isoformat();deadline=time.monotonic()+240;refs={};errors={};definitions={}
    with report('ops_5858_tic_country_identity_probe') as r:
        r.kv(production_engines_invoked=0,paid_ai_calls=0,notifications_sent=0,private_account_reads=0,portfolio_writes=0)
        ref=acquire(s3,bucket,native.CSLT_URL,deadline=deadline);refs['bulk']=ref
        raw=native.original(ref,read,native.CSLT_URL,datetime.now(timezone.utc).isoformat())
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            info=archive.infolist();assert len(info)==1 and info[0].filename=='cslt.json' and info[0].file_size<=native.MAX_DOCUMENT
            doc=native.strict_json(archive.read('cslt.json'))
        assert doc['releaseID']=='3' and len(doc['series'])>=4000
        seen=set();catalog=[];selected=[]
        for index,row in enumerate(doc['series']):
            sid=row['source_id'];assert sid not in seen;seen.add(sid)
            meta=row['metadata'];extra=meta.get('additional') or {};geo=extra.get('geography') or {}
            if sid.startswith(('for_treas_pos_','for_lt_treas_pos_','for_lt_eqty_pos_')):
                dates=[str(v[0]) for v in row['observations']]
                catalog.append({'source_id':sid,'series_index':index,'title':meta['title'],'geography':geo,
                    'source_status':extra.get('status'),'frequency':meta['frequency'],'unit':meta['units'],
                    'observations':len(dates),'earliest':min(dates) if dates else None,'latest':max(dates) if dates else None})
            if sid.replace('_','').upper() in CHECK:
                selected.append({'source_id':sid,'metadata':meta,'observations':len(row['observations']),
                    'newest_reported_rows':row['observations'][:2],'oldest_reported_rows':row['observations'][-1:]})
        r.kv(native_archive=ref,country_holdings_catalog=catalog,selected_identities=selected,
             source_series_count=len(doc['series']),archive_transmission_is_not_publication_time=True)
        for index,url in enumerate(TABLES):
            ref=acquire(s3,bucket,url,deadline=deadline);refs['table5:'+str(index)]=ref
            raw=read(ref['evidence']['key']);assert b'Major Foreign Holders' in raw and b'2026-' in raw,'holdings table identity differs'
            r.kv(official_table=ref,bytes=len(raw),format='html' if url.endswith('.html') else 'text')
        for sid in (*LEGACY,*CHECK):
            try:
                url=native.definition_url(sid,vintage);ref=acquire(s3,bucket,url,fred_key,deadline);refs[sid]=ref
                doc=native.strict_json(native.original(ref,read,url,datetime.now(timezone.utc).isoformat()))
                rows=doc.get('seriess') or [];assert len(rows)==1 and rows[0]['id']==sid
                definitions[sid]=rows[0];r.kv(series_id=sid,definition=rows[0],original=ref)
            except Exception as exc:
                errors[sid]='HTTP_'+str(exc.code) if isinstance(exc,urllib.error.HTTPError) else type(exc).__name__
        r.kv(source_status_codes=errors,retained_responses=len(refs),audit_scope='Original identity and coverage audit only; no live packet or recommendation modified.')
        assert all(sid in definitions for sid in CHECK),'required current/legacy comparison definitions incomplete'

if __name__=='__main__':
    try:main()
    except Exception:
        print('TIC country identity probe failed; inspect sanitized committed report.')
        sys.exit(1)
