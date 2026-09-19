"""Audit complete dated issuer histories for the configured ETF universe."""
from concurrent.futures import ThreadPoolExecutor,as_completed
from pathlib import Path
import importlib.util,io,json,re,sys,zipfile,xml.etree.ElementTree as ET
import boto3
ROOT=Path(__file__).resolve().parents[3]
source=ROOT/'aws/ops/STAGED/ops_5866_etf_original_source_probe.py'
spec=importlib.util.spec_from_file_location('etf_probe',source);prior=importlib.util.module_from_spec(spec);spec.loader.exec_module(prior)
from ops_report import report
NS={'s':'urn:schemas-microsoft-com:office:spreadsheet'}
DOWNLOAD='https://www.blackrock.com/varnish-api/blk-one01-product-data/product-data/api/v1/get-fund-document?appSubType=ISHARES&appType=PRODUCT_PAGE&component=fundDownload&locale=en_US&portfolioId={pid}&targetSite=us-ishares&userType=individual'

def inspect_xml(raw):
    if b'<!DOCTYPE' in raw.upper() or b'<!ENTITY' in raw.upper():raise ValueError('external declaration')
    # Issuer emits bare ampersands in hyperlink attributes; originals stay intact.
    fixed,n=re.subn(rb'&(?!amp;|lt;|gt;|quot;|apos;|#\d+;|#x[0-9A-Fa-f]+;)',b'&amp;',raw)
    root=ET.fromstring(fixed);sheets={}
    for ws in root.findall('s:Worksheet',NS):
        rows=[[(''.join(cell.itertext())) for cell in row.findall('s:Cell/s:Data',NS)] for row in ws.findall('s:Table/s:Row',NS)]
        name=ws.get('{'+NS['s']+'}Name');sheets[name]={'rows':len(rows),'first':rows[:6],'last':rows[-2:]}
    return {'ampersands_escaped_for_xml_parser':n,'sheets':{k:v for k,v in sheets.items() if k in ('Historical','Holdings')},'all_sheet_names':list(sheets)}

def main():
    s3=boto3.client('s3',region_name='us-east-1');bucket='justhodl-dashboard-live';errors={};count=0
    with report('ops_5867_etf_issuer_history_probe') as r:
        ref,raw=prior.acquire(s3,bucket,prior.CONSTANTS['ISHARES_SCREENER']);catalog=json.loads(raw)
        universe={ticker for values in prior.CONSTANTS['ETFS'].values() for ticker in values}
        selected=[v for v in catalog.values() if v.get('localExchangeTicker') in universe]
        assert 1<=len(selected)<=70 and len({v['localExchangeTicker'] for v in selected})==len(selected)
        r.kv(source='ishares_catalog',original=ref,selected=len(selected),production_engines_invoked=0,paid_ai_calls=0,notifications_sent=0,private_account_reads=0,portfolio_writes=0)
        def fetch(v):
            pid=v['portfolioId'];assert isinstance(pid,int) and 100000<=pid<=999999
            ref,raw=prior.acquire(s3,bucket,DOWNLOAD.format(pid=pid));summary=inspect_xml(raw)
            return v['localExchangeTicker'],{'portfolio_id':pid,'fund_name':v['fundName'],'isin':v.get('isin'),'cusip':v.get('cusip'),'product_page':v['productPageUrl']},ref,summary
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures={pool.submit(fetch,v):v['localExchangeTicker'] for v in selected}
            for future in as_completed(futures):
                ticker=futures[future]
                try:
                    ticker,identity,ref,summary=future.result();count+=1;r.kv(source='ishares_'+ticker,identity=identity,original=ref,summary=summary)
                except Exception as exc:errors[ticker]='HTTP_'+str(exc.code) if isinstance(exc,prior.urllib.error.HTTPError) else type(exc).__name__
        for label,url in (('ssga_spy_page','https://www.ssga.com/us/en/institutional/etfs/state-street-spdr-sp-500-etf-trust-spy'),('ssga_spy_nav','https://www.ssga.com/library-content/products/fund-data/etfs/us/navhist-us-en-spy.xlsx')):
            try:
                ref,raw=prior.acquire(s3,bucket,url);summary={'bytes':len(raw)}
                if label.endswith('_nav'):
                    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
                        assert sum(v.file_size for v in archive.infolist())<32*1024*1024
                        summary['members']=[v.filename for v in archive.infolist()]
                        for name in ('xl/workbook.xml','xl/sharedStrings.xml','xl/worksheets/sheet1.xml'):
                            if name in summary['members']:summary[name]=archive.read(name)[:12000].decode('utf-8')
                r.kv(source=label,original=ref,summary=summary)
            except Exception as exc:errors[label]='HTTP_'+str(exc.code) if isinstance(exc,prior.urllib.error.HTTPError) else type(exc).__name__
        r.kv(source_status_codes=errors,retained_histories=count,selected_histories=len(selected),audit_scope='Complete source histories only. No engine invocation or production research output changed.')
        assert count==len(selected),'some configured iShares original histories need investigation'

if __name__=='__main__':
    try:main()
    except Exception:
        print('ETF issuer-history audit failed; inspect sanitized committed report.')
        sys.exit(1)
