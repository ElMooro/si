"""Retain original H.4.1 and exact custody identities; no engine invocation."""
from datetime import datetime,timezone
from pathlib import Path
from urllib.parse import urlencode
import hashlib,io,json,sys,time,urllib.request,urllib.error,zipfile
import xml.etree.ElementTree as ET
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/ops')]
from managed_secret import managed_secret
from evidence_store import capture,read_verified
from ops_report import report

XML_URL='https://www.federalreserve.gov/releases/h41/data/FRB_h41_xml.zip'
HTML_URL='https://www.federalreserve.gov/releases/h41/current/default.htm'
SERIES=('WLRRAFOIAL','WMTSECL1','WMTSEC1','WMTSEC','WMTSECL')

def acquire(s3,bucket,url,key=None):
    actual=url+('&'+urlencode({'api_key':key}) if key else '')
    request=urllib.request.Request(actual,headers={'User-Agent':'JustHodl original H41 identity audit'})
    with urllib.request.urlopen(request,timeout=60) as response:raw=response.read(32*1024*1024+1)
    if not raw or len(raw)>32*1024*1024:raise ValueError('source response bound')
    if key and key.encode() in raw:raise ValueError('credential echoed')
    at=datetime.now(timezone.utc);ref={'url':url,'acquired_at':at.isoformat(),'evidence':capture(s3,bucket,'h41',url,raw,at)}
    if read_verified(s3,bucket,ref['evidence'])!=raw:raise ValueError('retained original differs')
    return ref,raw

def endpoint(path,**params):return 'https://api.stlouisfed.org/fred/'+path+'?'+urlencode({'file_type':'json',**params})

def main():
    s3=boto3.client('s3',region_name='us-east-1');bucket='justhodl-dashboard-live'
    key=managed_secret(('FRED_KEY','FRED_API_KEY'),('/justhodl/fred/api-key',));assert key,'configured FRED credential unavailable'
    vintage=datetime.now(timezone.utc).date().isoformat();refs={};errors={}
    with report('ops_5861_official_h41_source_probe') as r:
        r.kv(production_engines_invoked=0,paid_ai_calls=0,notifications_sent=0,private_account_reads=0,portfolio_writes=0)
        for label,url in (('release_html',HTML_URL),('native_archive',XML_URL)):
            try:
                ref,raw=acquire(s3,bucket,url);refs[label]=ref
                if label=='release_html':
                    assert b'Securities held in custody' in raw and b'Foreign official' in raw
                    r.kv(source=label,original=ref,bytes=len(raw))
                else:
                    catalog=[];samples=[];total=0
                    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
                        members=[{'name':v.filename,'bytes':v.file_size,'compressed_bytes':v.compress_size} for v in archive.infolist()]
                        assert sum(v['bytes'] for v in members)<=256*1024*1024,'expanded archive bound'
                        for member in archive.infolist():
                            if not member.filename.lower().endswith('.xml'):continue
                            with archive.open(member) as stream:
                                for event,element in ET.iterparse(stream,events=('end',)):
                                    if element.tag.rsplit('}',1)[-1]!='Series':continue
                                    total+=1;attrs=dict(element.attrib);children=[{'tag':v.tag.rsplit('}',1)[-1],'attributes':dict(v.attrib),'text':''.join(v.itertext())[:1200]} for v in list(element) if v.tag.rsplit('}',1)[-1]!='Obs']
                                    observations=[dict(v.attrib) for v in list(element) if v.tag.rsplit('}',1)[-1]=='Obs']
                                    title=json.dumps(attrs)+' '+json.dumps(children)
                                    if any(word in title.lower() for word in ('custody','foreign official','reverse repurchase')):
                                        catalog.append({'member':member.filename,'attributes':attrs,'metadata':children,'observations':len(observations),'first_rows':observations[:2],'last_rows':observations[-2:]})
                                    if len(samples)<2:samples.append({'attributes':attrs,'metadata':children,'first_observations':observations[:2]})
                                    element.clear()
                    r.kv(source=label,original=ref,members=members,native_series_count=total,matching_catalog=catalog[:80],matching_series=len(catalog),structure_samples=samples)
            except Exception as exc:errors[label]='HTTP_'+str(exc.code) if isinstance(exc,urllib.error.HTTPError) else type(exc).__name__
        for sid in SERIES:
            try:
                url=endpoint('series',series_id=sid,realtime_start=vintage,realtime_end=vintage);ref,raw=acquire(s3,bucket,url,key);definition=json.loads(raw)['seriess']
                assert len(definition)==1 and definition[0]['id']==sid
                refs[sid+':definition']=ref;r.kv(source=sid+':definition',original=ref,definition=definition[0]);time.sleep(1)
                url=endpoint('series/observations',series_id=sid,realtime_start=vintage,realtime_end=vintage,observation_start=definition[0]['observation_start'],sort_order='asc',limit=100000,offset=0)
                ref,raw=acquire(s3,bucket,url,key);doc=json.loads(raw);rows=doc['observations'];assert doc['count']==len(rows) and doc['offset']==0 and rows
                assert len({v['date'] for v in rows})==len(rows)
                refs[sid+':observations']=ref;r.kv(source=sid+':observations',original=ref,observations=len(rows),first_rows=rows[:2],last_rows=rows[-2:],frequency=definition[0]['frequency']);time.sleep(1)
            except Exception as exc:errors[sid]='HTTP_'+str(exc.code) if isinstance(exc,urllib.error.HTTPError) else type(exc).__name__
        for label,url in (('series_release',endpoint('series/release',series_id='WMTSECL1')),('release_dates',endpoint('release/dates',release_id=20,realtime_start='2025-01-01',realtime_end='2027-12-31',sort_order='desc',limit=1000,include_release_dates_with_no_data='true'))):
            try:
                ref,raw=acquire(s3,bucket,url,key);refs[label]=ref;doc=json.loads(raw)
                r.kv(source=label,original=ref,release_identity=doc.get('releases'),count=doc.get('count'),dates=(doc.get('release_dates') or [])[:8]);time.sleep(1)
            except Exception as exc:errors[label]='HTTP_'+str(exc.code) if isinstance(exc,urllib.error.HTTPError) else type(exc).__name__
        r.kv(source_status_codes=errors,retained_responses=len(refs),audit_scope='Native H41 source and exact FRED identity audit only; no current research packet modified.')
        assert all(sid+':observations' in refs for sid in ('WLRRAFOIAL','WMTSECL1','WMTSEC1')),'required Wednesday/average definitions unavailable'

if __name__=='__main__':
    try:main()
    except Exception:
        print('H41 source audit failed; inspect sanitized committed report.')
        sys.exit(1)
