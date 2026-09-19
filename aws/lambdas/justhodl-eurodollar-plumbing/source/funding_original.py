"""Strict parsing of retained funding responses; no scores or inferred crises."""
import csv
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
from html.parser import HTMLParser
import io
import json
import re
from evidence_store import public_source_url

OFR_BASE = 'https://data.financialresearch.gov/v1/series/full?mnemonic='
OFR_IDS = tuple('REPO-'+venue+'_'+kind+'_TOT-'+vintage
                for venue in ('DVP','GCF','TRI') for kind in ('AR','TV') for vintage in ('F','P')) + ('MMF-MMF_RP_TOT-M',)
ECB_KEY = 'ILM.W.U2.C.A030000.U2.Z06'
URLS = {sid: OFR_BASE+sid for sid in OFR_IDS}
URLS.update(ofr_fsi='https://www.financialresearch.gov/financial-stress-index/data/fsi.csv',
    ecb_fx_claims='https://data-api.ecb.europa.eu/service/data/ILM/W.U2.C.A030000.U2.Z06?format=csvdata&lastNObservations=260',
    cnh_hibor='https://www.tma.org.hk/en_market_more_ib.aspx')
FX = ('USDHKD','USDCNH','USDCNY','USDJPY')
URLS.update({sid:'https://financialmodelingprep.com/stable/quote?symbol='+sid for sid in FX})
TENORS = ('ON','1WK','2WK','1M','2M','3M','6M','12M')
FSI_COLUMNS = ('OFR FSI','Credit','Equity valuation','Safe assets','Funding','Volatility',
               'United States','Other advanced economies','Emerging markets')


def clock(value):
    if not isinstance(value,str):raise ValueError('clock string required')
    result=datetime.fromisoformat(value.replace('Z','+00:00'))
    if result.tzinfo is None:raise ValueError('timezone required')
    return result.astimezone(timezone.utc)


def strict_json(raw):
    def pairs(items):
        out={}
        for k,v in items:
            if k in out:raise ValueError('duplicate JSON key')
            out[k]=v
        return out
    def bad(value):raise ValueError('nonfinite JSON')
    return json.loads(raw,object_pairs_hook=pairs,parse_float=str,parse_constant=bad)


def amount(value,negative=True):
    if value is None or value in ('','.','*','N/A'):return None
    if isinstance(value,bool):raise ValueError('boolean amount')
    if len(str(value))>60:raise ValueError('numeric length bound')
    try:out=Decimal(str(value))
    except InvalidOperation:raise ValueError('invalid numeric value')
    if not out.is_finite() or abs(out)>Decimal('1e18') or (not negative and out<0):raise ValueError('numeric bound')
    return str(out)


def rows_checked(rows,at):
    seen=set()
    if not rows or len(rows)>20000:raise ValueError('original row bound')
    for row in rows:
        if not isinstance(row['date'],str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}',row['date']):raise ValueError('ISO observation date required')
        d=date.fromisoformat(row['date'])
        if d.isoformat() in seen or d>clock(at).date():raise ValueError('duplicate or future observation')
        seen.add(d.isoformat())
    return sorted(rows,key=lambda r:r['date'])


def entry(identifier,label,unit,frequency,rows,definition,evidence,acquired,at,age_days,role='descriptive_measurement'):
    rows=rows_checked(rows,at);latest=rows[-1];age=(clock(at).date()-date.fromisoformat(latest['date'])).days
    acquisition=(clock(at)-clock(acquired)).total_seconds()
    if acquisition<0:raise ValueError('future source acquisition')
    status='incomplete' if latest['value_decimal'] is None else 'stale' if age>age_days else 'stale_source' if acquisition>26*3600 else 'fresh'
    return {'id':identifier,'label':label,'unit':unit,'frequency':frequency,'as_of':latest['date'],
        'value_decimal':latest['value_decimal'],'value':float(latest['value_decimal']) if latest['value_decimal'] is not None else None,
        'original_row_index':latest['row_index'],'rows':rows,'definition':definition,'evidence':evidence,
        'quality':{'status':status,'observation_date':latest['date'],'observation_age_days':age,'max_observation_age_days':age_days,
                   'acquired_at':acquired,'max_acquisition_age_hours':26,'actual_publication_time_verified':False,
                   'holiday_calendar_verified':False,'basis':'explicit_calendar_age_limit_and_original_acquisition'},
        'role':role,'call':None,'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False}


def ofr(raw,sid,evidence,acquired,at):
    doc=strict_json(raw)
    if not isinstance(doc,dict) or set(doc)!={sid}:raise ValueError('OFR identity differs')
    meta=doc[sid]['metadata'];unit=meta['unit'];desc=meta['description'];schedule=meta['schedule']
    if meta.get('mnemonic')!=sid or unit.get('magnitude')!=0:raise ValueError('OFR unit magnitude or identity differs')
    is_rate='_AR_' in sid;is_mmf=sid=='MMF-MMF_RP_TOT-M'
    expected='Percent' if is_rate else 'USD'
    if unit.get('name')!=expected:raise ValueError('OFR native unit differs')
    freq='M' if is_mmf else 'D';wanted='Monthly' if is_mmf else 'Daily'
    if schedule.get('observation_frequency')!=wanted:raise ValueError('OFR frequency differs')
    if not is_mmf and desc.get('vintage')!=('Final' if sid.endswith('-F') else 'Preliminary'):raise ValueError('OFR vintage differs')
    series=doc[sid]['timeseries']['aggregation'];rows=[]
    for index,row in enumerate(series):
        if not isinstance(row,list) or len(row)!=2:raise ValueError('OFR row shape differs')
        value=amount(row[1],negative=is_rate)
        rows.append({'date':row[0],'value_decimal':value,'row_index':index,'status':'observed' if value is not None else 'missing_or_disclosure_edited'})
    result=entry(sid,desc['name'],expected,freq,rows,meta,evidence,acquired,at,120 if is_mmf else 7)
    result['source_vintage']='monthly_report' if is_mmf else desc['vintage'].lower()
    if sid.endswith('-F'):
        if result['value_decimal'] is not None:result['quality']['status']='historical_final'
        result['role']='final_vintage_reference'
    result['limitation']=('Monthly reported MMF holdings; not a daily cash-flow measure.' if is_mmf else
        'OFR venue averages and transaction populations differ. These are not executable reference rates or a measure of unique collateral across venues.')
    return {sid:result}


def fsi(raw,evidence,acquired,at):
    reader=csv.DictReader(io.StringIO(raw.decode('utf-8-sig')))
    if reader.fieldnames!=['Date',*FSI_COLUMNS]:raise ValueError('OFR FSI headers differ')
    original=list(reader);out={}
    for column in FSI_COLUMNS:
        identifier='ofr_fsi:'+column.lower().replace(' ','_');rows=[]
        for index,row in enumerate(original):
            if None in row:raise ValueError('OFR FSI row width differs')
            value=amount(row[column]);rows.append({'date':row['Date'],'value_decimal':value,'row_index':index,
                                                    'status':'observed' if value is not None else 'missing'})
        out[identifier]=entry(identifier,'OFR FSI · '+column,'index_points','D',rows,
            {'column':column,'definition_url':'https://www.financialresearch.gov/financial-stress-index/',
             'basis':'Publisher stress index or contribution; not a locally calibrated probability or independent evidence vote'},
            evidence,acquired,at,7)
    return out


def ecb(raw,evidence,acquired,at):
    reader=csv.DictReader(io.StringIO(raw.decode('utf-8-sig')));rows=[];definition=None
    for index,row in enumerate(reader):
        if None in row or row.get('KEY')!=ECB_KEY or row.get('UNIT')!='EUR' or row.get('UNIT_MULT')!='6' or row.get('FREQ')!='W':
            raise ValueError('ECB series or native unit differs')
        if row.get('TITLE_COMPL')!='Euro area (changing composition), Eurosystem reporting sector - Claims on euro area residents denominated in foreign currency, All currencies except EUR - Euro area (changing composition) counterpart':
            raise ValueError('ECB definition requires review')
        period=row['TIME_PERIOD']
        if not re.fullmatch(r'\d{4}-W\d{2}',period):raise ValueError('ECB weekly period differs')
        year,week=map(int,period.split('-W'));day=date.fromisocalendar(year,week,5).isoformat()
        value=amount(row['OBS_VALUE'],negative=False)
        # Do not silently authorize values with unreviewed publication status.
        usable=value if row.get('OBS_STATUS') in ('A','P') and row.get('OBS_CONF') in ('F','') else None
        rows.append({'date':day,'source_period':period,'value_decimal':usable,'reported_decimal':value,'row_index':index,
                     'status':row.get('OBS_STATUS'),'confidentiality_status':row.get('OBS_CONF')})
        definition={k:row[k] for k in ('KEY','TITLE','TITLE_COMPL','UNIT','UNIT_MULT','FREQ','COLLECTION')}
    result=entry(ECB_KEY,'Eurosystem foreign-currency claims on euro-area residents','EUR_millions','W',rows,definition,evidence,acquired,at,21)
    result['limitation']='All foreign currencies expressed in EUR; not isolated USD lending, swap-line use or QE. Friday period end is not the publication timestamp.'
    return {ECB_KEY:result}


class Tables(HTMLParser):
    def __init__(self):
        super().__init__();self.tables=[];self.table=None;self.row=None;self.cell=None
    def handle_starttag(self,tag,attrs):
        if tag=='table':
            if self.table is not None:raise ValueError('nested benchmark table')
            self.table=[]
        elif tag=='tr' and self.table is not None:self.row=[]
        elif tag in ('td','th') and self.row is not None:self.cell=[]
    def handle_data(self,data):
        if self.cell is not None:self.cell.append(data)
    def handle_endtag(self,tag):
        if tag in ('td','th') and self.cell is not None:
            self.row.append(' '.join(''.join(self.cell).split()));self.cell=None
        elif tag=='tr' and self.row is not None:
            self.table.append(self.row);self.row=None
        elif tag=='table' and self.table is not None:
            self.tables.append(self.table);self.table=None


def tma(raw,evidence,acquired,at):
    html=raw.decode('utf-8');parser=Tables();parser.feed(html)
    candidates=[t for t in parser.tables if t and t[0] and t[0][0]=='Date' and len(t)==9 and tuple(r[0] for r in t[1:])==TENORS]
    if len(candidates)!=1 or 'CNH Hong Kong Interbank Offered Rate' not in html:raise ValueError('CNH benchmark table identity differs')
    table=candidates[0];dates=[datetime.strptime(v,'%d/%m/%Y').date().isoformat() for v in table[0][1:]]
    if not 1<=len(dates)<=10 or dates!=sorted(set(dates),reverse=True):raise ValueError('CNH date columns differ')
    out={}
    for row_index,row in enumerate(table[1:],1):
        if len(row)!=len(dates)+1:raise ValueError('CNH benchmark column alignment differs')
        tenor=row[0];rows=[]
        for column,(day,value) in enumerate(zip(dates,row[1:]),1):
            number=amount(value);rows.append({'date':day,'value_decimal':number,'row_index':row_index,'column_index':column,
                'status':'observed' if number is not None else 'missing'})
        identifier='CNH_HIBOR:'+tenor
        out[identifier]=entry(identifier,'CNH HIBOR '+tenor,'Percent','D',rows,
            {'benchmark':'CNH Hong Kong Interbank Offered Rate','tenor':tenor,'source_url':URLS['cnh_hibor'],
             'specifications_url':'https://benchmark.tma.org.hk/benchmark/governance/specifications',
             'dissemination':'Delayed public website; not a real-time quote'},evidence,acquired,at,5)
        out[identifier]['limitation']='Delayed CNH benchmark, not USD borrowing or proof of a policy intervention. Only the displayed dated fixing window is retained; fallback-fixing status is not supplied by this table.'
    return out


def fx(raw,sid,evidence,acquired,at):
    doc=strict_json(raw)
    if not isinstance(doc,list) or len(doc)!=1 or doc[0].get('symbol')!=sid:raise ValueError('FX quote identity differs')
    row=doc[0];value=amount(row.get('price'),negative=False);stamp=row.get('timestamp')
    if type(stamp) is not int or stamp<=0 or value is None or Decimal(value)<=0:raise ValueError('dated positive FX quote required')
    quoted=datetime.fromtimestamp(stamp,timezone.utc)
    if quoted>clock(acquired):raise ValueError('FX quote is future dated')
    result=entry(sid,sid[:3]+'/'+sid[3:]+' indicative spot',sid[3:]+'_per_'+sid[:3],'quote',
        [{'date':quoted.date().isoformat(),'value_decimal':value,'row_index':0,'quoted_at':quoted.isoformat(),'status':'observed'}],
        {'symbol':sid,'basis':'FMP indicative last price; no executable bid/ask or forward tenor'},evidence,acquired,at,4)
    result['quoted_at']=quoted.isoformat();return {sid:result}


def parse(name,raw,evidence,acquired,at):
    if name in OFR_IDS:return ofr(raw,name,evidence,acquired,at)
    if name in FX:return fx(raw,name,evidence,acquired,at)
    return {'ofr_fsi':fsi,'ecb_fx_claims':ecb,'cnh_hibor':tma}[name](raw,evidence,acquired,at)


def load(name,descriptor,read,at):
    if descriptor.get('url')!=URLS[name]:raise ValueError('original request differs')
    ref=descriptor['evidence'];raw=read(ref['key'])
    if ref.get('contract')!='source-evidence.v1' or ref.get('captured') is not True:raise ValueError('verified capture receipt required')
    source_url=public_source_url(URLS[name])
    if ref.get('source_url')!=source_url or len(raw)!=ref['bytes'] or hashlib.sha256(raw).hexdigest()!=ref['sha256']:
        raise ValueError('original response differs')
    key='data/evidence/funding/'+hashlib.sha256(source_url.encode()).hexdigest()+'/'+ref['sha256']+'.bin.gz'
    if ref['key']!=key:raise ValueError('original evidence path differs')
    return parse(name,raw,ref,descriptor['acquired_at'],at)
