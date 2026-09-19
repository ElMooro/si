"""Exact issuer observations, retained without inventing dates or share units."""
from datetime import datetime,timezone
from decimal import Decimal,InvalidOperation,localcontext
import csv,hashlib,io,json,re,zipfile,xml.etree.ElementTree as ET
from evidence_store import public_source_url

MAX_BYTES=64*1024*1024
MAX_ROWS=18000
NS={'s':'urn:schemas-microsoft-com:office:spreadsheet'}
PRO_HEADER=['Date','ProShares Name','Ticker','NAV','Prior NAV','NAV Change (%)','NAV Change ($)','Shares Outstanding (000)','Assets Under Management']
SPLIT_HEADER=['Symbol','Name','Pre Split Cusip','Post Split Cusip','Split Type','Ratio','Date of Split']
ISHARES_HEADER=['As Of','NAV per Share','Ex-Dividends','Shares Outstanding']
ISHARES_URL=('https://www.ishares.com/us/product-screener/product-screener-v3.1.jsn'
 '?dcrPath=/templatedata/config/product-screener-v3/data/en/us-ishares/ishares-product-screener-backend-config&siteEntryPassthrough=true')
SSGA_URL='https://www.ssga.com/bin/v1/ssmp/fund/fundfinder?country=us&language=en&role=intermediary&product=etfs&ui=fund-finder'
DOWNLOAD=('https://www.blackrock.com/varnish-api/blk-one01-product-data/product-data/api/v1/get-fund-document?appSubType=ISHARES&appType=PRODUCT_PAGE&component=fundDownload&locale=en_US&portfolioId={pid}&targetSite=us-ishares&userType=individual')
PRO_URL='https://accounts.profunds.com/etfdata/ByFund/{ticker}-historical_nav.csv'
SPLIT_URL='https://accounts.profunds.com/etfdata/etf_splits.csv'

def clock(value):
    dt=datetime.fromisoformat(str(value).replace('Z','+00:00'))
    if dt.tzinfo is None:raise ValueError('timezone-aware clock required')
    return dt.astimezone(timezone.utc)

def dec(value,missing=False):
    if value is None or str(value).strip() in ('','--','-'):
        if missing:return None
        raise ValueError('required source amount missing')
    text=str(value).strip().replace(',','')
    if isinstance(value,bool) or not re.fullmatch(r'-?\d+(?:\.\d+)?(?:[Ee][+-]?\d+)?',text):raise ValueError('decimal source value required')
    out=Decimal(text)
    if not out.is_finite() or abs(out)>Decimal('1e30'):raise ValueError('source number bound')
    return out

def ds(value):return None if value is None else format(value,'f')
def quantum(value):return Decimal(1).scaleb(dec(value).as_tuple().exponent)

def observation_date(value,fmt):return datetime.strptime(value,fmt).date().isoformat()

def original(ref,read,at,expected_url):
    ev=ref['evidence'];url=ref['url']
    if url!=expected_url or ev.get('contract')!='source-evidence.v1' or ev.get('captured') is not True:raise ValueError('source receipt identity differs')
    sha=ev.get('sha256','');source=public_source_url(url);request=hashlib.sha256(source.encode()).hexdigest()
    if ev.get('provider')!='etf_original' or ev.get('source_url')!=source or not re.fullmatch('[a-f0-9]{64}',sha) or ev.get('key')!=f'data/evidence/etf_original/{request}/{sha}.bin.gz':raise ValueError('original evidence key differs')
    if clock(ev['first_received_at'])>clock(ref['acquired_at']) or clock(ref['acquired_at'])>clock(at):raise ValueError('future source receipt')
    raw=read(ev['key'])
    if not 0<len(raw)<=MAX_BYTES or len(raw)!=ev['bytes'] or hashlib.sha256(raw).hexdigest()!=sha:raise ValueError('original source bytes differ')
    return raw

def json_original(raw):return json.loads(raw,parse_float=Decimal)

def ishares_catalog(raw,universe):
    doc=json_original(raw)
    if not isinstance(doc,dict) or not 1<=len(doc)<=2000:raise ValueError('iShares catalog structure changed')
    out={}
    for pid,v in doc.items():
        ticker=v.get('localExchangeTicker')
        if ticker not in universe:continue
        if ticker in out or str(v.get('portfolioId'))!=pid:raise ValueError('duplicate or ambiguous issuer identity')
        if not re.fullmatch(r'/us/products/'+pid+r'/[a-z0-9-]+',v.get('productPageUrl','')):raise ValueError('product identity URL differs')
        if not re.fullmatch(r'US[A-Z0-9]{10}',v.get('isin','')):raise ValueError('US fund identity missing')
        out[ticker]={'issuer':'iShares','ticker':ticker,'portfolio_id':int(pid),'fund_name':v['fundName'],
            'isin':v['isin'],'cusip':v.get('cusip'),'product_url':'https://www.ishares.com'+v['productPageUrl'],
            'nav_decimal':ds(dec(v['navAmount']['r'],True)),
            'nav_date':observation_date(str(v['navAmountAsOf']['r']),'%Y%m%d'),
            'net_assets_decimal':ds(dec(v['totalNetAssets']['r'],True)),
            'net_assets_date':observation_date(str(v['totalNetAssetsFundAsOf']['r']),'%Y%m%d')}
    return out

def ssga_catalog(raw,universe):
    doc=json_original(raw);records=doc['data']['funds']['etfs']['datas'];out={}
    if not isinstance(records,list) or not 1<=len(records)<=1000:raise ValueError('State Street catalogue differs')
    for v in records:
        ticker=v.get('fundTicker')
        if ticker not in universe:continue
        if ticker in out or v.get('domicile')!='US' or not re.fullmatch(r'/us/en/intermediary/etfs/[a-z0-9-]+',v.get('fundUri','')):raise ValueError('ambiguous State Street fund identity')
        urls=[d['path'] for group in v['documentPdf'] if group.get('docType')=='Navhist' for d in group.get('docs',[])]
        if len(urls)!=1 or not re.fullmatch(r'/library-content/products/fund-data/etfs/us/navhist-us-en-'+ticker.lower()+r'\.xlsx',urls[0]):raise ValueError('native NAV workbook identity differs')
        if not isinstance(v['nav'],list) or len(v['nav'])!=2 or not str(v['nav'][0]).startswith('$') or not isinstance(v['aum'],list) or not str(v['aum'][0]).startswith('$') or not str(v['aum'][0]).endswith(' M'):raise ValueError('USD/million catalogue units differ')
        out[ticker]={'issuer':'State Street','ticker':ticker,'fund_name':v['fundName'],'currency':'USD',
          'history_url':'https://www.ssga.com'+urls[0],'product_url':'https://www.ssga.com'+v['fundUri'],
          'nav_decimal':ds(dec(v['nav'][1])),'nav_date':observation_date(v['asOfDate'][1],'%Y-%m-%d'),
          'net_assets_decimal':ds(dec(v['aum'][1])*1000000),'net_assets_date':observation_date(v['asOfDate'][1],'%Y-%m-%d')}
    return out

def ssga_history(raw,identity,at):
    ns={'s':'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        members=archive.infolist()
        if len(members)>60 or sum(v.file_size for v in members)>48*1024*1024 or len({v.filename for v in members})!=len(members):raise ValueError('spreadsheet archive bound')
        texts=[archive.read(p) for p in ('xl/sharedStrings.xml','xl/worksheets/sheet1.xml')]
        if any(b'<!DOCTYPE' in v.upper() or b'<!ENTITY' in v.upper() for v in texts):raise ValueError('external XML declarations rejected')
        strings=[''.join(v.itertext()) for v in ET.fromstring(texts[0]).findall('s:si',ns)]
        cells={};source_rows={}
        for row in ET.fromstring(texts[1]).findall('s:sheetData/s:row',ns):
            index=int(row.get('r'))
            if index in source_rows or not 1<=index<=MAX_ROWS:raise ValueError('spreadsheet row identity differs')
            values={}
            for cell in row.findall('s:c',ns):
                address=cell.get('r','')
                if address in cells or not re.fullmatch(r'[A-Z]{1,2}'+str(index),address) or cell.find('s:f',ns) is not None:raise ValueError('spreadsheet formula or cell identity differs')
                value=cell.findtext('s:v',default='',namespaces=ns)
                if cell.get('t')=='s' and value:
                    si=int(value)
                    if not 0<=si<len(strings):raise ValueError('spreadsheet shared string index differs')
                    value=strings[si]
                elif cell.get('t') not in (None,'n','s'):raise ValueError('unreviewed spreadsheet cell type')
                cells[address]=value;values[re.sub(r'\d+','',address)]=value
            source_rows[index]=values
    if cells.get('A1')!='Fund Name:' or cells.get('A2')!='Ticker Symbol:' or cells.get('B2')!=identity['ticker'] or cells.get('B1')!=identity['fund_name']:raise ValueError('State Street native fund identity differs')
    if [cells.get(k+'4') for k in 'ABCD']!=['Date','NAV','Shares Outstanding','Total Net Assets']:raise ValueError('State Street history columns differ')
    rows=[];ended=False;non_observation=0
    for index,values in sorted(source_rows.items()):
        if index<=4:continue
        day=values.get('A','')
        if not re.fullmatch(r'\d{1,2}-[A-Za-z]{3}-\d{4}',day):
            if any(values.get(k,'') for k in 'BCD'):raise ValueError('unrecognized populated historical date row')
            ended=True;non_observation+=1;continue
        if ended:raise ValueError('observations resume after end of source table')
        nav=dec(values.get('B'),True);shares=dec(values.get('C'),True);aum=dec(values.get('D'),True)
        if nav is not None and nav<0 or shares is not None and shares<0 or aum is not None and aum<0:raise ValueError('invalid issuer balance')
        qsh=quantum(values['C']) if shares is not None else None;qn=quantum(values['B']) if nav is not None else None
        residual=aum-nav*shares if all(v is not None for v in (aum,nav,shares)) else None
        sensitivity=(nav*qsh+shares*qn+qsh*qn+quantum(values['D'])) if residual is not None else None
        rows.append({'date':observation_date(day,'%d-%b-%Y'),'nav_decimal':ds(nav),'shares_decimal':ds(shares),
           'net_assets_decimal':ds(aum),'distribution_decimal':None,'distribution_status':'not_in_this_source',
           'share_display_quantum_decimal':ds(qsh),'nav_display_quantum_decimal':ds(qn),'aum_residual_decimal':ds(residual),
           'aum_display_precision_sensitivity_decimal':ds(sensitivity),'aum_within_display_precision':abs(residual)<=sensitivity if residual is not None else None,'source_row':index})
    return {'rows':validate_dates(rows,at),'non_observation_rows':non_observation,'scope':'Native USD NAV, reported shares and net assets; whole source vintage. Cash dividends are not added to share changes.'}

def xml_sheets(raw):
    if b'<!DOCTYPE' in raw.upper() or b'<!ENTITY' in raw.upper():raise ValueError('external XML declarations rejected')
    fixed,escaped=re.subn(rb'&(?!amp;|lt;|gt;|quot;|apos;|#\d+;|#x[0-9A-Fa-f]+;)',b'&amp;',raw)
    root=ET.fromstring(fixed)
    if root.tag!='{'+NS['s']+'}Workbook':raise ValueError('issuer workbook root differs')
    sheets={}
    for ws in root.findall('s:Worksheet',NS):
        name=ws.get('{'+NS['s']+'}Name')
        if name in sheets:raise ValueError('duplicate source worksheet')
        rows=[]
        for row in ws.findall('s:Table/s:Row',NS):
            cells=[]
            for cell in row.findall('s:Cell',NS):
                index=int(cell.get('{'+NS['s']+'}Index',len(cells)+1))
                if not len(cells)<index<=100:raise ValueError('source cell index differs')
                cells.extend(['']*(index-len(cells)-1));data=cell.find('s:Data',NS)
                cells.append(''.join(data.itertext()).strip() if data is not None else '')
            rows.append(cells)
        if len(rows)>MAX_ROWS:raise ValueError('issuer worksheet row bound')
        sheets[name]=rows
    return sheets,escaped

def validate_dates(rows,at):
    dates=[v['date'] for v in rows]
    if not 1<=len(dates)<=MAX_ROWS or dates!=sorted(dates,reverse=True) or len(set(dates))!=len(dates):raise ValueError('source history order or duplicates differ')
    if dates[0]>clock(at).date().isoformat():raise ValueError('future issuer observation')
    return list(reversed(rows))

def ishares_history(raw,identity,at):
    sheets,escaped=xml_sheets(raw);source=sheets['Historical']
    if source[0] not in (ISHARES_HEADER,ISHARES_HEADER+['Non-FV NAV']):raise ValueError('iShares historical columns differ')
    names=[r[0] for r in sheets.get('Holdings',[])[:4] if r]+[r[0] for r in sheets.get('Performance',[])[:1] if r]
    if identity['fund_name'] not in names:raise ValueError('workbook fund name differs from catalog')
    rows=[]
    for index,values in enumerate(source[1:],1):
        if len(values)!=len(source[0]):raise ValueError('historical source row width differs')
        day,nav,dividend,shares=values[:4];nav=dec(nav,True);shares=dec(shares,True);dividend=dec(dividend,True)
        if nav is not None and nav<=0 or shares is not None and shares<0:raise ValueError('invalid issuer balance')
        rows.append({'date':observation_date(day,'%b %d, %Y'),'nav_decimal':ds(nav),'shares_decimal':ds(shares),
            'distribution_decimal':ds(dividend),'distribution_status':'reported' if dividend is not None else 'not_reported',
            'non_fair_value_nav_decimal':ds(dec(values[4],True)) if len(values)==5 else None,
            'net_assets_decimal':None,'share_display_quantum_decimal':ds(quantum(values[3])) if shares is not None else None,
            'nav_display_quantum_decimal':ds(quantum(values[1])) if nav is not None else None,'source_row':index})
    return {'rows':validate_dates(rows,at),'xml_bare_ampersands_escaped':escaped,'source_columns':source[0],
            'scope':'Issuer-published historical shares and NAV; one complete current source vintage. No price substitution or synthetic observations.'}

def proshares_history(raw,ticker,at):
    records=list(csv.reader(io.StringIO(raw.decode('utf-8-sig'))))
    if records[0]!=PRO_HEADER:raise ValueError('ProShares historical columns differ')
    rows=[];names=set()
    for index,v in enumerate(records[1:],1):
        if len(v)!=len(PRO_HEADER) or v[2]!=ticker:raise ValueError('fund identity or row width differs')
        nav=dec(v[3]);shares=dec(v[7])*1000;aum=dec(v[8]);prior=dec(v[4]);names.add(v[1])
        if nav<=0 or shares<0 or aum<0 or prior<=0:raise ValueError('invalid issuer balance')
        qsh=quantum(v[7])*1000;qn=quantum(v[3]);qa=quantum(v[8])
        # Sensitivity to one last displayed unit, not a statistical confidence interval.
        sensitivity=abs(nav*qsh)+abs(shares*qn)+abs(qsh*qn)+qa
        residual=aum-nav*shares
        rows.append({'date':observation_date(v[0],'%m/%d/%Y'),'nav_decimal':ds(nav),'shares_decimal':ds(shares),
            'shares_original_thousands_decimal':v[7],'net_assets_decimal':ds(aum),'prior_nav_decimal':ds(prior),
            'distribution_decimal':None,'distribution_status':'not_in_this_source',
            'share_display_quantum_decimal':ds(qsh),'nav_display_quantum_decimal':ds(qn),
            'aum_residual_decimal':ds(residual),'aum_display_precision_sensitivity_decimal':ds(sensitivity),
            'aum_within_display_precision':abs(residual)<=sensitivity,'source_row':index})
    return {'rows':validate_dates(rows,at),'source_columns':records[0],'reported_names':sorted(names),
       'scope':'Issuer current historical basis. Reported shares in thousands multiplied by exactly1000; no additional split ratios applied.'}

def proshares_splits(raw,at):
    records=list(csv.reader(io.StringIO(raw.decode('utf-8-sig'))))
    if records[0]!=SPLIT_HEADER or len(records)>3000:raise ValueError('split file structure differs')
    out={};seen=set()
    for index,v in enumerate(records[1:],1):
        if len(v)!=len(SPLIT_HEADER) or v[4] not in ('Reverse','Forward') or dec(v[5])<=1:raise ValueError('split row differs')
        day=observation_date(v[6],'%m/%d/%Y');identity=(v[0],day)
        if identity in seen:raise ValueError('duplicate split identity')
        seen.add(identity);out.setdefault(v[0],[]).append({'date':day,'type':v[4],'ratio_decimal':ds(dec(v[5])),
           'pre_cusip':v[2] or None,'post_cusip':v[3] or None,'source_row':index,'announced_future':day>clock(at).date().isoformat()})
    return out

def flow_history(rows,reference_dates,actions):
    by_date={v['date']:v for v in rows};ref_index={day:i for i,day in enumerate(reference_dates)};action_days={v['date'] for v in actions};out=[]
    with localcontext() as ctx:
        ctx.prec=50
        for row in rows:
            day=row['date'];entry={**row,'net_share_change_decimal':None,'nav_valued_share_change_decimal':None,'flow_precision_sensitivity_decimal':None}
            i=ref_index.get(day);previous=by_date.get(reference_dates[i-1]) if i is not None and i>0 else None
            reason=None
            if previous is None:reason='missing_preceding_reference_observation'
            elif day in action_days:reason='corporate_action_transition_requires_review'
            elif any(v.get('nav_decimal') is None or v.get('shares_decimal') is None or dec(v['nav_decimal'])<=0 or dec(v['shares_decimal'])<=0 or v.get('aum_within_display_precision') is False for v in (previous,row)):reason='invalid_or_insufficient_source_precision'
            else:
                nav=dec(row['nav_decimal']);pn=dec(previous['nav_decimal']);shares=dec(row['shares_decimal']);ps=dec(previous['shares_decimal'])
                nr=nav/pn;sr=shares/ps
                if (nr>Decimal('1.5') and sr<Decimal('0.67')) or (sr>Decimal('1.5') and nr<Decimal('0.67')):reason='possible_split_or_unreviewed_action'
                else:
                    change=shares-ps;sensitivity=(dec(row['share_display_quantum_decimal'])+dec(previous['share_display_quantum_decimal']))*nav+abs(change)*dec(row['nav_display_quantum_decimal'])
                    entry.update(net_share_change_decimal=ds(change),nav_valued_share_change_decimal=ds(change*nav),flow_precision_sensitivity_decimal=ds(sensitivity))
            entry['flow_status']=reason or 'descriptive_estimate';entry['previous_date']=previous['date'] if previous else None
            out.append(entry)
    return out

def window(rows,reference_dates,n):
    if not rows:return {'status':'missing_history','value_decimal':None}
    end=rows[-1]['date'];index={day:i for i,day in enumerate(reference_dates)}.get(end)
    if index is None or index<n:return {'status':'insufficient_reference_observations','value_decimal':None,'end_date':end}
    expected=reference_dates[index-n+1:index+1];by_date={v['date']:v for v in rows};selected=[by_date.get(day) for day in expected]
    missing=[day for day,row in zip(expected,selected) if row is None or row['flow_status']!='descriptive_estimate']
    result={'start_date':reference_dates[index-n],'end_date':end,'observations_required':n,'observations_available':n-len(missing),
            'excluded_dates':missing,'value_decimal':None,'precision_sensitivity_decimal':None}
    if missing:return {**result,'status':'incomplete_or_unreviewed'}
    with localcontext() as ctx:
        ctx.prec=50
        total=sum((dec(row['nav_valued_share_change_decimal']) for row in selected),Decimal(0))
        precision=sum((dec(row['flow_precision_sensitivity_decimal']) for row in selected),Decimal(0))
        return {**result,'status':'complete_descriptive_estimate','value_decimal':ds(total),'precision_sensitivity_decimal':ds(precision)}
