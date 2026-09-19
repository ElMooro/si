"""Retain complete bounded fiscal and auction inputs already used by foreign-flows."""
from datetime import datetime,timezone
from pathlib import Path
import json,sys,time
from urllib.parse import urlencode
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/ops'),str(ROOT/'aws/lambdas/justhodl-capital-inflows/source')]
from tic_store import acquire,raw_reader
from ops_report import report
import tic_original as native
MSPD='https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/debt/mspd/mspd_table_1?'+urlencode({
    'fields':'record_date,security_type_desc,security_class_desc,debt_held_public_mil_amt',
    'filter':'security_type_desc:eq:Marketable','sort':'record_date','page[size]':10000,'page[number]':1})
AUCTION='https://www.treasurydirect.gov/TA_WS/securities/auctioned?format=json&days=60&type='

def main():
    s3=boto3.client('s3',region_name='us-east-1');bucket='justhodl-dashboard-live';read=raw_reader(s3,bucket);deadline=time.monotonic()+180
    with report('ops_5859_tic_fiscal_auction_source_probe') as r:
        r.kv(production_engines_invoked=0,paid_ai_calls=0,notifications_sent=0,private_account_reads=0,portfolio_writes=0)
        for name,url in (('mspd',MSPD),('auction_note',AUCTION+'Note'),('auction_bond',AUCTION+'Bond')):
            ref=acquire(s3,bucket,url,deadline=deadline)
            doc=native.strict_json(native.original(ref,read,url,datetime.now(timezone.utc).isoformat()))
            if name=='mspd':
                rows=doc['data'];assert rows and isinstance(rows,list);dates=sorted({v['record_date'] for v in rows})
                r.kv(source=name,original=ref,rows=len(rows),months=len(dates),first_date=dates[0],last_date=dates[-1],
                    metadata=doc.get('meta'),links=doc.get('links'),classes=sorted({v['security_class_desc'] for v in rows}),first_rows=rows[:6],last_rows=rows[-6:])
                assert doc['meta']['total-count']==len(rows) and doc['meta']['total-pages']==1,'MSPD full bounded response incomplete'
            else:
                assert isinstance(doc,list) and 0<len(doc)<2000,'auction response shape'
                dates=sorted({v['auctionDate'] for v in doc});fields=('cusip','auctionDate','issueDate','securityType','securityTerm','inflationIndexSecurity','floatingRate',
                    'indirectBidderAccepted','directBidderAccepted','primaryDealerAccepted','competitiveAccepted','noncompetitiveAccepted','totalAccepted','bidToCoverRatio','highYield')
                r.kv(source=name,original=ref,rows=len(doc),first_date=dates[0],last_date=dates[-1],
                    source_fields=sorted(doc[0]),sample=[{k:v.get(k) for k in fields} for v in doc[:3]])

if __name__=='__main__':
    try:main()
    except Exception:
        print('TIC fiscal/auction source probe failed; inspect sanitized committed report.')
        sys.exit(1)
