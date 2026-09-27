"""Exact CAD freight-column/calendar observations from the whole workbook.

CAD's published tonnage excludes air mail. Its rounded levels and published
YoY (calculated from unrounded levels) remain separate measurements. Neither
describes monetary cargo value, commodity mix or an equity forecast.
"""
from io import BytesIO
from decimal import Decimal,InvalidOperation
from datetime import date
import hashlib,math,re,zipfile,xml.etree.ElementTree as ET
NS='http://schemas.openxmlformats.org/spreadsheetml/2006/main'
RNS='http://schemas.openxmlformats.org/officeDocument/2006/relationships'
PNS='http://schemas.openxmlformats.org/package/2006/relationships'
MONTHS='january february march april may june july august september october november december'.split()
MONTH_INDEX={name:i+1 for i,name in enumerate(MONTHS)}
MONTH_INDEX.update({name[:3]:i+1 for i,name in enumerate(MONTHS)})
LIMIT=64*1024*1024
CONTRACT='cad-air-freight-calendar.v1'
EXPECTED={'A1':'Hong Kong International Airport','A2':'Civil International Air Transport','A3':'Movements of Aircraft, Passenger and Freight','A8':'Year','B8':'Month','L7':'Freight β (Tonne)','L8':'Unloaded','M8':'Loaded','N8':'Total','O8':'Year-on-year % change'}
NOTES=('Hong Kong International Airport at Chek Lap Kok commenced operation on 6 July 1998.','the amount of freight excludes air mail','provisional figures','revised figures','incomplete yearly total','Figures may not add up to the respective totals due to rounding.','Year-on-year percentage changes are derived from unrounded figures.','Source : Airport Authority Hong Kong')


class WorkbookError(ValueError):pass


def xml(raw):
    if len(raw)>LIMIT or b'<!DOCTYPE' in raw.upper() or b'<!ENTITY' in raw.upper():raise WorkbookError('Unreviewed XML declaration or size')
    return ET.fromstring(raw)


def normalized(value):return ' '.join(str(value).split())


def workbook_cells(raw):
    if len(raw)>16*1024*1024:raise WorkbookError('Whole workbook exceeds bound')
    with zipfile.ZipFile(BytesIO(raw)) as z:
        members=z.infolist();names=[i.filename for i in members]
        if len(names)!=len(set(names)) or len(names)>1000 or sum(i.file_size for i in members)>128*1024*1024 or any(i.file_size>LIMIT or i.flag_bits&1 for i in members):raise WorkbookError('Ambiguous, encrypted or oversized workbook')
        book=xml(z.read('xl/workbook.xml'));sheets=book.findall('{'+NS+'}sheets/{'+NS+'}sheet')
        if len(sheets)!=1 or sheets[0].get('name')!='Eng':raise WorkbookError('One exact English source sheet required')
        rid=sheets[0].get('{'+RNS+'}id');rels=xml(z.read('xl/_rels/workbook.xml.rels'))
        matches=[n for n in rels if n.get('Id')==rid]
        if len(matches)!=1 or matches[0].get('TargetMode')=='External' or matches[0].get('Target') not in ('worksheets/sheet1.xml','/xl/worksheets/sheet1.xml'):raise WorkbookError('Exact sheet relationship required')
        shared=xml(z.read('xl/sharedStrings.xml'))
        strings=[''.join(t.text or '' for t in si.iter('{'+NS+'}t')) for si in shared.findall('{'+NS+'}si')]
        if shared.get('uniqueCount') is not None and int(shared.get('uniqueCount'))!=len(strings):raise WorkbookError('Shared-string population differs')
        raw_sheet=z.read('xl/worksheets/sheet1.xml')
        if b'<!DOCTYPE' in raw_sheet.upper() or b'<!ENTITY' in raw_sheet.upper():raise WorkbookError('Unreviewed XML declaration')
        rows={};seen=set()
        for _,row in ET.iterparse(BytesIO(raw_sheet),events=('end',)):
            if row.tag!='{'+NS+'}row':continue
            number=row.get('r')
            if not number or not number.isdigit() or int(number)<1 or number in seen:raise WorkbookError('Unique positive row required')
            seen.add(number);cells={};cell_ids=set()
            for cell in row.findall('{'+NS+'}c'):
                ref=cell.get('r','');match=re.fullmatch(r'([A-Z]+)([1-9][0-9]*)',ref)
                if not match or match[2]!=number or ref in cell_ids:raise WorkbookError('Unique cell and row identity required')
                cell_ids.add(ref);v=cell.find('{'+NS+'}v');kind=cell.get('t','n');f=cell.find('{'+NS+'}f')
                if v is None:
                    if kind=='inlineStr':value=''.join(t.text or '' for t in cell.iter('{'+NS+'}t'))
                    elif f is not None:value=None
                    else:continue
                else:
                    value=v.text
                    if kind=='s':
                        if value is None or not value.isdigit() or int(value)>=len(strings):raise WorkbookError('Invalid shared string index')
                        value=strings[int(value)]
                cells[match[1]]={'cell':ref,'value':value,'type':kind,'formula':f.text if f is not None else None}
            if cells:rows[int(number)]=cells
            row.clear()
    return rows


def number(cell):
    if cell is None or cell['value'] in (None,''):return None
    if cell['type']!='n' or cell['formula'] is not None:raise WorkbookError('Direct published numeric cell required')
    try:d=Decimal(cell['value'])
    except InvalidOperation:raise WorkbookError('Invalid published number') from None
    if not d.is_finite() or abs(d)>Decimal('1e15'):raise WorkbookError('Finite bounded published number required')
    return d


def output_number(d):
    if d is None:return None
    result=int(d) if d==d.to_integral_value() else float(d)
    if not math.isfinite(result) or result==0 and d!=0:raise WorkbookError('Numeric representation loses nonzero value')
    return result


def measure(raw,at):
    cutoff=date.fromisoformat(at[:10])
    try:rows=workbook_cells(raw)
    except (zipfile.BadZipFile,ET.ParseError,KeyError):raise WorkbookError('Invalid or incomplete workbook container') from None
    for address,text in EXPECTED.items():
        m=re.fullmatch(r'([A-Z]+)(\d+)',address);cell=rows.get(int(m[2]),{}).get(m[1])
        if cell is None or normalized(cell['value'])!=text:raise WorkbookError('Freight header or unit changed: '+address)
    notes=[{'cell':c['cell'],'text':c['value']} for r,cs in sorted(rows.items()) for c in cs.values() if r>8 and isinstance(c['value'],str) and any(n in c['value'] for n in NOTES)]
    if any(not any(n in x['text'] for x in notes) for n in NOTES):raise WorkbookError('Source scope or revision notes changed')
    monthly=[];annual=[];carried=None;seen=set();annual_years=set()
    for r,cs in sorted(rows.items()):
        if r<=8:continue
        a=cs.get('A');b=cs.get('B');label=normalized((b or {}).get('value','')).lower();month=MONTH_INDEX.get(label)
        year=None
        if a and re.fullmatch(r'(?:19|20)\d{2}',str(a['value'])):
            year=int(a['value'])
            if a['formula'] is not None or not 1998<=year<=cutoff.year:raise WorkbookError('Source year outside declared calendar')
        if month is None:
            if year is not None:
                if label not in ('','#¶','*','#','¶','§'):raise WorkbookError('Unrecognized annual period label')
                if year in annual_years:raise WorkbookError('Duplicate annual period')
                annual_years.add(year)
                annual.append({'year':year,'row':r,'period_marker':(b or {}).get('value'),'cells':{k:v for k,v in cs.items() if k in ('A','B','C','L','M','N','O')}})
            elif any(cs.get(k,{}).get('type')=='n' for k in ('L','M','N','O')):raise WorkbookError('Freight numeric row has no exact period')
            continue
        if a is not None and a['value'] not in (None,'') and year is None:raise WorkbookError('Monthly year cell is ambiguous')
        if year is not None:carried=year
        if carried is None:raise WorkbookError('Monthly year missing; no inference from other metrics')
        observed=date(carried,month,1)
        if observed.strftime('%Y-%m')>=cutoff.strftime('%Y-%m') or observed in seen:raise WorkbookError('Duplicate or incomplete/future monthly observation')
        seen.add(observed)
        values={k:number(cs.get(col)) for k,col in [('unloaded','L'),('loaded','M'),('total','N'),('reported_yoy_pct','O')]}
        if any(v is not None and v<0 for k,v in values.items() if k!='reported_yoy_pct'):raise WorkbookError('Negative freight amount')
        marker=(cs.get('C') or {}).get('value') or ''
        if not isinstance(marker,str) or any(x not in '#§* ' for x in marker):raise WorkbookError('Unrecognized monthly revision marker')
        row={'month':observed.strftime('%Y-%m'),'row':r,'unit':'tonnes','excludes_air_mail':True,'seasonal_adjustment':'not stated in source','provisional':'#' in marker,'revised':'§' in marker,'source_marker':marker,'cells':{k:cs.get(k) for k in ('A','B','C','L','M','N','O')},**{k:output_number(v) for k,v in values.items()}}
        row['loaded_plus_unloaded_minus_total_tonnes']=output_number(values['loaded']+values['unloaded']-values['total']) if all(values[k] is not None for k in ('loaded','unloaded','total')) else None
        monthly.append(row)
    monthly.sort(key=lambda r:r['month'])
    if not monthly:raise WorkbookError('No exact monthly freight rows')
    last_row=max(r['row'] for r in monthly)
    notes=[{'cell':c['cell'],'text':c['value']} for r,cs in sorted(rows.items()) for c in cs.values() if r>last_row and c['type'] in ('s','inlineStr') and c['value']]
    by={r['month']:r for r in monthly}
    for row in monthly:
        key=str(int(row['month'][:4])-1)+row['month'][4:];prior=by.get(key);current=number(row['cells']['N']);old=number(prior['cells']['N']) if prior else None
        row['prior_year_month']=key
        row['airport_calendar_scope']='before Chek Lap Kok opening' if row['month']<'1998-07' else 'airport transition month' if row['month']=='1998-07' else 'after Chek Lap Kok opening'
        row['yoy_from_published_levels_pct']=output_number((current/old-1)*100) if current is not None and old is not None and old!=0 else None
        row['comparison_status']='available' if row['yoy_from_published_levels_pct'] is not None else 'latest_missing' if current is None else 'prior_month_missing' if old is None else 'zero_prior'
        if prior is not None and (row['month']<='1998-07' or key<='1998-07'):
            row['yoy_from_published_levels_pct']=None;row['comparison_status']='airport_transition_comparability_unverified'
    return {'contract':CONTRACT,'workbook_sha256':hashlib.sha256(raw).hexdigest(),'sheet':'Eng','headers':EXPECTED,'notes':notes,'monthly_observations':monthly,'monthly_count':len(monthly),'annual_rows':annual,'annual_count':len(annual),'latest_month':monthly[-1]['month'],'calendar_scope':'Every published monthly freight row; annual and incomplete-year rows stay separate.','yoy_definition':'Reported YoY uses unrounded source figures. The independent comparison uses displayed same-month levels and can differ due to rounding.','interpretation':'Tonnage does not establish monetary cargo value, commodity mix, sector demand or a financial-market forecast.','original_vintage_verified':False}
