"""Retain the bounded AGG workbook and catalogue-linked State Street histories."""
from concurrent.futures import ThreadPoolExecutor,as_completed
from datetime import datetime,timezone
from pathlib import Path
import hashlib,importlib.util,io,json,re,sys,urllib.request,urllib.error,zipfile,xml.etree.ElementTree as ET
import boto3
ROOT=Path(__file__).resolve().parents[3]
spec=importlib.util.spec_from_file_location('etf_probe',ROOT/'aws/ops/STAGED/ops_5866_etf_original_source_probe.py');prior=importlib.util.module_from_spec(spec);spec.loader.exec_module(prior)
from evidence_store import capture,read_verified
from ops_report import report

def acquire(s3,bucket,url):
    with urllib.request.urlopen(urllib.request.Request(url,headers={'User-Agent':'JustHodl original ETF research audit (ops@justhodl.ai)'}),timeout=60) as response:
        raw=response.read(64*1024*1024+1)
    assert 0<len(raw)<=64*1024*1024,'source exceeds explicit64MiB bound'
    at=datetime.now(timezone.utc);ref={'url':url,'acquired_at':at.isoformat(),'evidence':capture(s3,bucket,'etf_original',url,raw,at)}
    assert read_verified(s3,bucket,ref['evidence'])==raw
    return ref,raw

def xlsx_summary(raw):
    ns={'s':'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        assert sum(v.file_size for v in archive.infolist())<=48*1024*1024 and len(archive.infolist())<=60
        text=archive.read('xl/sharedStrings.xml');sheet=archive.read('xl/worksheets/sheet1.xml')
        assert not any(b'<!DOCTYPE' in v.upper() or b'<!ENTITY' in v.upper() for v in (text,sheet))
        strings=[''.join(v.itertext()) for v in ET.fromstring(text).findall('s:si',ns)]
        rows=[]
        for row in ET.fromstring(sheet).findall('s:sheetData/s:row',ns):
            cells={}
            for c in row.findall('s:c',ns):
                value=c.findtext('s:v',default='',namespaces=ns);value=strings[int(value)] if c.get('t')=='s' and value else value
                if value:cells[c.get('r')]=value
            if cells:rows.append(cells)
        return {'nonempty_rows':len(rows),'first_rows':rows[:6],'last_rows':rows[-5:]}

def main():
    s3=boto3.client('s3',region_name='us-east-1');bucket='justhodl-dashboard-live';errors={};count=0
    with report('ops_5868_etf_remaining_issuer_sources') as r:
        r.kv(production_engines_invoked=0,paid_ai_calls=0,notifications_sent=0,private_account_reads=0,portfolio_writes=0)
        url='https://www.blackrock.com/varnish-api/blk-one01-product-data/product-data/api/v1/get-fund-document?appSubType=ISHARES&appType=PRODUCT_PAGE&component=fundDownload&locale=en_US&portfolioId=239458&targetSite=us-ishares&userType=individual'
        ref,raw=acquire(s3,bucket,url);r.kv(source='ishares_AGG',original=ref,bytes=len(raw),reason_for_larger_bound='Observed HEAD length28428480 exceeds prior24MiB probe bound; original retained complete.')
        ref,raw=acquire(s3,bucket,prior.CONSTANTS['SSGA_FUNDFINDER']);catalog=json.loads(raw);r.kv(source='ssga_catalog',original=ref)
        universe={t for members in prior.CONSTANTS['ETFS'].values() for t in members};selected=[]
        for v in catalog['data']['funds']['etfs']['datas']:
            if v.get('fundTicker') not in universe or v.get('domicile')!='US':continue
            paths=[d['path'] for doc in v.get('documentPdf',[]) if doc.get('docType')=='Navhist' for d in doc.get('docs',[])]
            if len(paths)!=1:
                errors[v['fundTicker']]='native_nav_link_missing_or_ambiguous';continue
            path=paths[0];assert re.fullmatch(r'/library-content/products/fund-data/etfs/us/navhist-us-en-[a-z0-9-]+\.xlsx',path)
            selected.append((v,'https://www.ssga.com'+path))
        assert 1<=len(selected)<=40 and len({v['fundTicker'] for v,url in selected})==len(selected)
        def fetch(item):
            v,url=item;ref,raw=acquire(s3,bucket,url)
            return {'source':'ssga_'+v['fundTicker'],'identity':{'ticker':v['fundTicker'],'fund_name':v['fundName'],'catalog_date':v['asOfDate'],'nav':v['nav'],'aum':v['aum'],'native_url':url},'original':ref,'summary':xlsx_summary(raw)}
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures={pool.submit(fetch,item):item[0]['fundTicker'] for item in selected}
            for future in as_completed(futures):
                try:r.kv(**future.result());count+=1
                except Exception as exc:errors[futures[future]]='HTTP_'+str(exc.code) if isinstance(exc,urllib.error.HTTPError) else type(exc).__name__
        r.kv(source_status_codes=errors,retained_histories=count,selected_histories=len(selected),audit_scope='Source collection only; no current engine output changed.')
        assert count==len(selected),'State Street source histories require investigation'

if __name__=='__main__':
    try:main()
    except Exception:
        print('ETF remaining issuer audit failed; inspect sanitized committed report.')
        sys.exit(1)
