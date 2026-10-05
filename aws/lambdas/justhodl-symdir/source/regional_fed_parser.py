"""Bounded, source-specific parsers for official regional Federal Reserve tables.

No formulas are executed, external workbook targets opened, observations filled,
or percentage growth inferred from diffusion-index comparisons.
"""
from collections import Counter
from datetime import datetime,timedelta
from decimal import Decimal,InvalidOperation
from html.parser import HTMLParser
import csv,io,math,posixpath,re,zipfile,xml.etree.ElementTree as ET

MAX_WIRE=4000000
NS={'s':'http://schemas.openxmlformats.org/spreadsheetml/2006/main','r':'http://schemas.openxmlformats.org/officeDocument/2006/relationships'}
CFNAI_COLUMNS=['Date','P_I','EU_H','C_H','SO_I','CFNAI','CFNAI_MA3','DIFFUSION']
KC_LABELS=['Composite Index','Production','Volume of shipments','Volume of new orders','Backlog of orders','Number of employees','Average employee workweek','Prices received for finished product','Prices paid for raw materials','Capital expenditures','New orders for exports','Supplier delivery time','Inventories: Materials','Inventories: Finished goods']
KC_METRICS=['composite','production','shipments','new-orders','backlog','employment','workweek','prices-received','prices-paid','capital-expenditures','export-orders','delivery-time','materials-inventories','finished-inventories']
KC_BLOCKS=[('month-sa',4,'Versus a Month Ago','(seasonally adjusted)'),('month-nsa',20,'Versus a Month Ago','(not seasonally adjusted)'),('year-nsa',36,'Versus a Year Ago','(not seasonally adjusted)'),('six-month-expectation-sa',52,'Expected in Six Months','(seasonally adjusted)'),('six-month-expectation-nsa',68,'Expected in Six Months','(not seasonally adjusted)')]

def number(text):
    if not isinstance(text,str) or len(text)>80 or not re.fullmatch(r'[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?',text):return None
    try:
        d=Decimal(text);v=float(d)
        return v if d.is_finite() and math.isfinite(v) and (v!=0 or d==0) else None
    except (ValueError,InvalidOperation,OverflowError):return None

def observation(ordinal,period,lexeme,location,kind='numeric',formula=False):
    v=number(lexeme) if kind=='numeric' else None
    rejection=None if v is not None else 'source_missing_value' if lexeme in ('','NaN') else 'invalid_source_numeric_cell'
    return {'ordinal':ordinal,'period':period,'anchor':period+'-01','value':v,'original_value':lexeme,'source_location':location,'source_cell_type':kind,'cached_formula_value':formula,'rejection':rejection,'source_flags':[],'binary64_rounding':v is not None and Decimal(str(v))!=Decimal(lexeme)}

def parse_cfnai(raw):
    if not isinstance(raw,bytes) or len(raw)>MAX_WIRE:raise ValueError('CFNAI response exceeds bounds')
    rows=list(csv.reader(io.StringIO(raw.decode('utf-8-sig')),strict=True))
    if not rows or rows[0]!=CFNAI_COLUMNS or not 2<=len(rows)<=2500:raise ValueError('CFNAI exact table schema changed')
    out={key:[] for key in CFNAI_COLUMNS[1:]};periods=[]
    for i,row in enumerate(rows[1:],2):
        if len(row)!=8 or not re.fullmatch(r'(?:19|20|21)\d{2}/(?:0[1-9]|1[0-2])',row[0]):raise ValueError('CFNAI row or monthly period invalid')
        period=row[0].replace('/','-');periods.append(period)
        for j,key in enumerate(CFNAI_COLUMNS[1:],1):out[key].append(observation(i-2,period,row[j],{'row':i,'column':key}))
    if len(set(periods))!=len(periods):raise ValueError('CFNAI duplicate reference month')
    return out

def xml(raw):
    if len(raw)>20000000 or b'<!DOCTYPE' in raw.upper() or b'<!ENTITY' in raw.upper():raise ValueError('Unsafe or oversized workbook XML')
    return ET.fromstring(raw)

def workbook_cells(raw,sheet_name):
    if not isinstance(raw,bytes) or len(raw)>MAX_WIRE:raise ValueError('Workbook response exceeds bounds')
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        members=archive.infolist();names=[m.filename for m in members]
        if len(names)>500 or len(names)!=len(set(names)) or sum(m.file_size for m in members)>20000000:raise ValueError('Workbook archive exceeds bounds or repeats members')
        if any(m.flag_bits&1 or m.filename.startswith('/') or '..' in m.filename.split('/') for m in members):raise ValueError('Unsafe workbook archive member')
        wb=xml(archive.read('xl/workbook.xml'));props=wb.find('s:workbookPr',NS)
        if props is not None and props.get('date1904') not in (None,'0','false'):raise ValueError('Unreviewed Excel date system')
        sheets=wb.findall('s:sheets/s:sheet',NS);matched=[s for s in sheets if s.get('name')==sheet_name]
        if len(matched)!=1:raise ValueError('Exact source worksheet absent or repeated')
        rid=matched[0].get('{'+NS['r']+'}id');rels=xml(archive.read('xl/_rels/workbook.xml.rels'))
        links=[r for r in rels if r.get('Id')==rid]
        if len(links)!=1 or links[0].get('TargetMode')=='External':raise ValueError('Source worksheet relationship invalid')
        target=posixpath.normpath(posixpath.join('xl',links[0].get('Target','')))
        if not re.fullmatch(r'xl/worksheets/sheet\d+\.xml',target):raise ValueError('Unreviewed source worksheet target')
        strings=[]
        if 'xl/sharedStrings.xml' in names:
            strings=[''.join(t.text or '' for t in si.iter('{'+NS['s']+'}t')) for si in xml(archive.read('xl/sharedStrings.xml'))]
            if len(strings)>50000:raise ValueError('Workbook strings exceed bounds')
        cells={}
        for c in xml(archive.read(target)).findall('s:sheetData/s:row/s:c',NS):
            ref=c.get('r','');kind=c.get('t','n');v=c.find('s:v',NS);text=v.text if v is not None and v.text is not None else ''
            if not re.fullmatch('[A-Z]{1,3}[1-9][0-9]{0,4}',ref) or ref in cells:raise ValueError('Invalid or duplicate workbook cell')
            if kind=='s':
                if not re.fullmatch('[0-9]+',text) or int(text)>=len(strings):raise ValueError('Workbook string reference invalid')
                text=strings[int(text)]
            elif kind=='inlineStr':text=''.join(t.text or '' for t in c.findall('.//s:t',NS))
            cells[ref]={'text':text,'kind':'numeric' if kind=='n' else kind,'formula':c.find('s:f',NS) is not None,'style':c.get('s')}
        if len(cells)>50000:raise ValueError('Workbook cell budget exceeded')
        return cells

def column(index):
    out=''
    while index:index,r=divmod(index-1,26);out=chr(65+r)+out
    return out

def parse_kc(raw):
    cells=workbook_cells(raw,'MSURVEY Extended Table2');value=lambda ref:cells.get(ref,{}).get('text','')
    if value('A2')!='Historical Manufacturing Survey Indexes':raise ValueError('Kansas City workbook identity changed')
    dated=[]
    for ref,cell in cells.items():
        if re.fullmatch('[A-Z]{1,3}3',ref) and ref!='A3' and cell['text']:
            if cell['kind']!='numeric' or cell['formula'] or not re.fullmatch('[0-9]{4,6}',cell['text']):raise ValueError('Invalid Kansas City monthly Excel date')
            serial=int(cell['text'])
            if not 30000<=serial<=100000:raise ValueError('Kansas City Excel date outside reviewed interval')
            date=datetime(1899,12,30)+timedelta(days=serial);col=ref[:-1];index=0
            for letter in col:index=index*26+ord(letter)-64
            dated.append((index,date.strftime('%Y-%m'),cell['text'],date.date().isoformat()))
    dated.sort()
    if not 1<=len(dated)<=1500 or [i for i,*_ in dated]!=list(range(2,len(dated)+2)) or len({p for _,p,_,_ in dated})!=len(dated):raise ValueError('Kansas City missing or duplicate monthly columns')
    out={}
    for block,start,label,adjustment in KC_BLOCKS:
        if value('A'+str(start))!=label or value('A'+str(start+1))!=adjustment:raise ValueError('Kansas City comparison/adjustment block changed')
        for i,(metric,expected) in enumerate(zip(KC_METRICS,KC_LABELS)):
            row=start+2+i
            if value('A'+str(row))!=expected:raise ValueError('Kansas City exact metric row changed')
            records=[]
            for n,(col,period,serial,date) in enumerate(dated):
                ref=column(col)+str(row);c=cells.get(ref,{'text':'','kind':'numeric','formula':False})
                item=observation(n,period,c['text'],{'sheet':'MSURVEY Extended Table2','cell':ref,'date_cell':column(col)+'3','original_excel_serial':serial,'original_excel_date':date},c['kind'],c['formula'])
                if item['value'] is not None and not -100<=item['value']<=100:
                    if adjustment=='(not seasonally adjusted)':item.update(value=None,rejection='unadjusted_diffusion_index_outside_percentage_point_bounds')
                    else:item['source_flags'].append('seasonal_adjustment_can_exceed_unadjusted_diffusion_bounds')
                records.append(item)
            out[block+':'+metric]=records
    return out

class MonthlyLink(HTMLParser):
    def __init__(self):super().__init__();self.link=None;self.text=[];self.matches=[]
    def handle_starttag(self,tag,attrs):
        if tag in ('a','mnt-link'):self.link=dict(attrs).get('href');self.text=[]
    def handle_data(self,text):
        if self.link:self.text.append(text)
    def handle_endtag(self,tag):
        if tag in ('a','mnt-link') and self.link:
            if ' '.join(' '.join(self.text).split())=='Historical Monthly Data':self.matches.append(self.link)
            self.link=None

def kc_workbook_url(raw):
    if not isinstance(raw,bytes) or len(raw)>MAX_WIRE:raise ValueError('Kansas City discovery page exceeds bounds')
    parser=MonthlyLink();parser.feed(raw.decode('utf-8'));links=set(parser.matches)
    if len(links)!=1:raise ValueError('Exactly one official historical monthly workbook is required')
    path=next(iter(links))
    if not re.fullmatch(r'/documents/[0-9]+/[A-Za-z0-9_.-]+\.xlsx',path):raise ValueError('Kansas City workbook link is outside reviewed public document path')
    return 'https://www.kansascityfed.org'+path
