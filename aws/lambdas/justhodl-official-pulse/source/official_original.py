"""Exact H.4.1 identities and originals; custody balances are not purchases."""
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from html.parser import HTMLParser
from urllib.parse import urlencode
from zoneinfo import ZoneInfo
import hashlib, io, json, re, zipfile
import xml.etree.ElementTree as ET
import evidence_store

XML_URL='https://www.federalreserve.gov/releases/h41/data/FRB_h41_xml.zip'
HTML_URL='https://www.federalreserve.gov/releases/h41/current/default.htm'
FAQ_URL='https://home.treasury.gov/data/treasury-international-capital-tic-system-home-page/frequently-asked-questions-regarding/ticfaq2'
MAX_ARCHIVE=32*1024*1024
MAX_EXPANDED=256*1024*1024
ROOT='FEDERAL_RESERVE:H41'
# Exact reviewed series, not a popularity search or a currentness-only match.
SERIES={
 'foreign_rrp':('RESPPLLRF_N.WW','LIABCAP','LIAB','RRPF','L','WLRRAFOIAL','Foreign official and international accounts: reverse-repo liability','cash liability'),
 'rrp_total':('RESPPLLR_N.WW','LIABCAP','LIAB','RRP','L',None,'Total reverse-repo liabilities','cash liability'),
 'rrp_other':('RESPPLLRD_N.WW','LIABCAP','LIAB','RRPO','L',None,'Other reverse-repo liabilities','cash liability'),
 'custody':('RESH4FG_N.WW','MEMO','CH','USTSN','L','WMTSECL1','Treasury securities in foreign official and international custody','current face value'),
 'custody_average':('RESH4FG_XAW_N.WW','MEMO','CH','USTSN','A','WMTSEC1','Treasury custody: weekly average','current face value'),
 'custody_total':('RESH4F_N.WW','MEMO','CH','TCS','L',None,'Total securities in foreign official and international custody','current face value'),
 'custody_agency':('RESH4FA_N.WW','MEMO','CH','FASN','L',None,'Agency debt and MBS in custody','current face value'),
 'custody_other':('RESH4FO_N.WW','MEMO','CH','OTH','L',None,'Other securities in custody','current face value'),
}

def clock(value):
    out=datetime.fromisoformat(value.replace('Z','+00:00'))
    if out.tzinfo is None:raise ValueError('timezone required')
    return out.astimezone(timezone.utc)

def encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def digest(value):return hashlib.sha256(encoded(value)).hexdigest()

def amount(value):
    if value in (None,'','.','NA','ND','NC'):return None
    if isinstance(value,bool) or len(str(value))>30:raise ValueError('H41 amount shape')
    try:out=Decimal(str(value))
    except InvalidOperation:raise ValueError('H41 amount invalid')
    if not out.is_finite() or abs(out)>Decimal('1e12') or out!=out.to_integral_value():raise ValueError('H41 integer-million precision differs')
    return str(int(out))

def original(ref,read,url,at):
    rec=ref['evidence'];acquired=clock(ref['acquired_at'])
    if ref['url']!=url or rec.get('source_url')!=evidence_store.public_source_url(url):raise ValueError('H41 request identity differs')
    if acquired>clock(at) or clock(rec['first_received_at'])>acquired:raise ValueError('H41 acquisition clock differs')
    sha=rec.get('sha256');request=hashlib.sha256(rec['source_url'].encode()).hexdigest()
    if rec.get('contract')!='source-evidence.v1' or rec.get('captured') is not True or rec.get('provider')!='h41':raise ValueError('H41 evidence contract differs')
    if not isinstance(sha,str) or not re.fullmatch('[a-f0-9]{64}',sha) or rec['key']!=f'data/evidence/h41/{request}/{sha}.bin.gz':raise ValueError('H41 evidence path differs')
    raw=read(rec['key'])
    if len(raw)!=rec['bytes'] or hashlib.sha256(raw).hexdigest()!=sha:raise ValueError('H41 original bytes differ')
    return raw

def archive(ref,read,at):
    raw=original(ref,read,XML_URL,at);wanted={v[0]:k for k,v in SERIES.items()};result={};total=0
    if not 0<len(raw)<=MAX_ARCHIVE:raise ValueError('H41 archive size')
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        members=z.infolist();names=[v.filename for v in members]
        if len(names)!=len(set(names)) or set(names)!={'H41_H41.xsd','H41_data.xml','H41_struct.xml','frb_common.xsd'}:raise ValueError('H41 archive members differ')
        if any(v.flag_bits&1 or v.file_size<=0 for v in members) or sum(v.file_size for v in members)>MAX_EXPANDED:raise ValueError('H41 expanded bound')
        # Disallow DTD/entity expansion, including a token crossing chunk boundaries.
        for name in ('H41_struct.xml','H41_data.xml'):
            with z.open(name) as stream:
                tail=b''
                while True:
                    part=stream.read(1024*1024)
                    if not part:break
                    check=(tail+part).upper()
                    if b'<!DOCTYPE' in check or b'<!ENTITY' in check:raise ValueError('H41 entity declarations forbidden')
                    tail=check[-16:]
        structure=ET.fromstring(z.read('H41_struct.xml'))
        statuses={}
        for element in structure.iter():
            if element.attrib.get('id')=='CL_OBS_STATUS':
                statuses={c.attrib['value']:' '.join(''.join(c.itertext()).split()) for c in element if 'value' in c.attrib}
        if statuses!={'A':'Normal','NC':'Not calculable','NA':'Not available','ND':'No data'}:raise ValueError('H41 observation statuses changed')
        with z.open('H41_data.xml') as stream:
            for _,element in ET.iterparse(stream,events=('end',)):
                if element.tag.rsplit('}',1)[-1]!='Series':continue
                total+=1
                if total>10000:raise ValueError('H41 series count bound')
                sid=element.attrib.get('SERIES_NAME')
                if sid in wanted:
                    name=wanted[sid];spec=SERIES[name]
                    expected=dict(zip(('SERIES_NAME','CATEGORY','SUBCATEGORY','COMPONENT','SERIESTYPE'),spec[:5]))
                    expected.update(FREQ='19',DISTRIBUTION='TOT',UNIT='Currency',UNIT_MULT='1000000',CURRENCY='USD')
                    if element.attrib!=expected or name in result:raise ValueError('H41 exact series identity differs: '+name)
                    rows=[];previous=None
                    for child in element:
                        if child.tag.rsplit('}',1)[-1]!='Obs':continue
                        day=date.fromisoformat(child.attrib['TIME_PERIOD']);status=child.attrib['OBS_STATUS'];value=amount(child.attrib.get('OBS_VALUE'))
                        if day.weekday()!=2 or day>clock(at).date() or previous and day<=previous or status not in statuses:raise ValueError('H41 observation identity differs')
                        if status!='A' and value is not None:raise ValueError('H41 non-normal numeric observation')
                        if len(rows)>=5000:raise ValueError('H41 weekly history bound')
                        # Treasury documents back-history at current face value from July 2007.
                        # Keep earlier literal zeros, but never admit them to level-change analytics.
                        start='2007-07-11' if spec[4]=='A' else '2007-07-04'
                        valid_scope=not name.startswith('custody') or day.isoformat()>=start
                        rows.append({'date':day.isoformat(),'value_decimal':value,'raw_value':child.attrib.get('OBS_VALUE'),
                          'observation_status':status,'row_index':len(rows),'analysis_eligible':valid_scope and status=='A' and value is not None,
                          'scope_status':'reported_coverage' if valid_scope else 'outside_documented_current_face_history'})
                        previous=day
                    if not rows:raise ValueError('H41 selected history empty')
                    title=' '.join(''.join(c.itertext()) for c in element if c.tag.rsplit('}',1)[-1]=='Annotations')
                    result[name]={'native_id':sid,'fred_id':spec[5],'name':spec[6],'valuation_basis':spec[7],'unit':'usd_million',
                        'statistic':'week_average' if spec[4]=='A' else 'wednesday_level','attributes':expected,'description':' '.join(title.split()),
                        'rows':rows,'series_index':total-1,'member':'H41_data.xml','original':ref,'independent_evidence_root':ROOT}
                element.clear()
    if set(result)!=set(SERIES):raise ValueError('H41 required native identities missing')
    return result,{'archive_series':total,'selected_series':len(result),'observation_status_definitions':statuses,
        'members':[{'name':v.filename,'bytes':v.file_size} for v in members]}

class ReleaseHTML(HTMLParser):
    def __init__(self):super().__init__();self.cells={};self.current=None;self.title=False;self.title_parts=[]
    def handle_starttag(self,tag,attrs):
        attrs=dict(attrs)
        if tag=='title':self.title=True
        if tag in ('td','th') and attrs.get('id'):self.current=attrs['id'];self.cells[self.current]=[]
    def handle_data(self,data):
        if self.title:self.title_parts.append(data)
        if self.current:self.cells[self.current].append(data)
    def handle_endtag(self,tag):
        if tag=='title':self.title=False
        if tag in ('td','th'):self.current=None

def release(ref,read,at,series):
    raw=original(ref,read,HTML_URL,at);parser=ReleaseHTML();parser.feed(raw.decode('utf-8'))
    title=' '.join(parser.title_parts);match=re.search(r'H\.4\.1 - ([A-Za-z]+ \d{1,2}, \d{4})',title)
    if not match:raise ValueError('H41 printed release date unavailable')
    printed=datetime.strptime(match.group(1),'%B %d, %Y').date()
    if printed>clock(at).astimezone(ZoneInfo('America/New_York')).date():raise ValueError('H41 future printed release')
    cells={k:' '.join(' '.join(v).split()) for k,v in parser.cells.items()}
    def observed(cell,prefix):
        value=cells.get(cell,'')
        if not value.startswith(prefix):raise ValueError('H41 printed observation header differs')
        return datetime.strptime(value[len(prefix):].strip(),'%b %d, %Y').date().isoformat()
    wednesday=observed('t3h1c3','Wednesday');average_end=observed('t3h2c1','Week ended')
    if date.fromisoformat(wednesday).weekday()!=2 or wednesday>=printed.isoformat():raise ValueError('H41 printed observation date differs')
    checks=[]
    for name,row,label in [('custody_total',1,'Securities held in custody'),('custody',2,'Marketable U.S. Treasury'),
                           ('custody_agency',3,'Federal agency'),('custody_other',4,'Other securities')]:
        if not cells.get(f't3r{row}c1','').startswith(label):raise ValueError('H41 release table identity differs')
        value=amount(cells.get(f't3r{row}c5','').replace(',','').replace(' ',''));last=series[name]['rows'][-1]
        checks.append({'measurement':name,'native_date':last['date'],'release_date':printed.isoformat(),'native_usd_million_decimal':last['value_decimal'],
            'html_observation_date':wednesday,'html_usd_million_decimal':value,'matches':value==last['value_decimal'] and last['date']==wednesday,'cell_id':f't3r{row}c5'})
    avg=amount(cells.get('t3r2c2','').replace(',','').replace(' ',''));last=series['custody_average']['rows'][-1]
    checks.append({'measurement':'custody_average','native_date':last['date'],'release_date':printed.isoformat(),
        'native_usd_million_decimal':last['value_decimal'],'html_observation_date':average_end,'html_usd_million_decimal':avg,'matches':avg==last['value_decimal'] and last['date']==average_end,'cell_id':'t3r2c2'})
    if any(c['html_usd_million_decimal'] is None for c in checks):raise ValueError('H41 printed table value missing')
    return {'printed_release_date':printed.isoformat(),'checks':checks,'original':ref,
        'actual_publication_time_verified':False,'rule':'Printed date and current table checked; acquisition time is not initial publication time.'}

def calendar_urls(at):
    year=clock(at).year
    def url(path,**kw):return 'https://api.stlouisfed.org/fred/'+path+'?'+urlencode({'file_type':'json',**kw})
    return {'series_release':url('series/release',series_id='WMTSECL1'),
      'release_dates':url('release/dates',release_id=20,realtime_start=f'{year-1}-01-01',realtime_end=f'{year+1}-12-31',sort_order='desc',limit=1000,include_release_dates_with_no_data='true')}

def calendar(refs,read,at):
    docs={k:json.loads(original(refs[k],read,url,at)) for k,url in calendar_urls(at).items()}
    ids=docs['series_release'].get('releases') or []
    if len(ids)!=1 or ids[0].get('id')!=20 or 'H.4.1' not in ids[0].get('name',''):raise ValueError('H41 FRED release identity differs')
    doc=docs['release_dates'];rows=doc.get('release_dates') or []
    if doc.get('count')!=len(rows) or doc.get('offset')!=0 or not rows:raise ValueError('H41 release calendar incomplete')
    days=[]
    for row in rows:
        d=date.fromisoformat(row['date']).isoformat()
        if row.get('release_id')!=20 or d in days:raise ValueError('H41 release calendar identity differs')
        days.append(d)
    today=clock(at).astimezone(ZoneInfo('America/New_York')).date().isoformat();prior=sorted(d for d in days if d<today);future=sorted(d for d in days if d>=today)
    last=prior[-1] if prior else None;next_day=future[0] if future else None
    expected=None
    if last:
        d=date.fromisoformat(last)-timedelta(days=1);expected=(d-timedelta(days=(d.weekday()-2)%7)).isoformat()
    expires=datetime.combine(date.fromisoformat(next_day)+timedelta(days=1),datetime.min.time(),tzinfo=ZoneInfo('America/New_York')).astimezone(timezone.utc).isoformat() if next_day else None
    return {'dates':sorted(days),'latest_completed_calendar_date':last,'next_calendar_date':next_day,'nominal_expected_wednesday':expected,
      'calendar_expires_at':expires,'originals':{k:refs[k] for k in calendar_urls(at)},'observation_mapping_verified':False,
      'rule':'Prior calendar release maps to the preceding Wednesday. Current-day releases have a next-New-York-midnight grace; exceptional schedules require review.'}

def distribution(item,native):
    sid=native['fred_id'];defs=item['definition'].get('seriess') or [];doc=item['observations'];rows=doc.get('observations') or []
    if len(defs)!=1 or defs[0].get('id')!=sid or defs[0].get('units')!='Millions of U.S. Dollars' or defs[0].get('frequency')!='Weekly, As of Wednesday' or defs[0].get('seasonal_adjustment')!='Not Seasonally Adjusted':raise ValueError('H41 canonical definition differs')
    if doc.get('count')!=len(rows) or doc.get('offset')!=0 or not rows:raise ValueError('H41 canonical query incomplete')
    order=doc.get('sort_order')
    if order not in ('asc','desc') or doc.get('order_by')!='observation_date':raise ValueError('H41 canonical ordering differs')
    values={};previous=None
    for row in rows:
        d=date.fromisoformat(row['date']).isoformat()
        if d in values or previous and (d<=previous if order=='asc' else d>=previous):raise ValueError('H41 canonical dates duplicate/unordered')
        values[d]=amount(row['value']);previous=d
    native_values={r['date']:r['value_decimal'] for r in native['rows']};checks=[]
    for day in sorted(set(values)|set(native_values)):
        status='match' if day in values and day in native_values and values[day]==native_values[day] else 'different_value' if day in values and day in native_values else 'fred_missing' if day not in values else 'native_missing'
        checks.append({'date':day,'native_usd_million_decimal':native_values.get(day),'fred_usd_million_decimal':values.get(day),'status':status})
    return {'fred_id':sid,'definition':defs[0],'originals':item['evidence'],'acquired_at':item['acquired_at'],'rows':checks,
      'matches':sum(r['status']=='match' for r in checks),'different_values':sum(r['status']=='different_value' for r in checks),
      'missing_dates':sum(r['status'].endswith('_missing') for r in checks),'independent_evidence_root':ROOT,'additional_independent_votes':0}
