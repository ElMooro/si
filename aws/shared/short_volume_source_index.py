"""Parse FINRA's published daily-file index; never guess trading-calendar dates.

Only literal CNMS links are selected. Different files for one date require
explicit revision review. A captured index proves its acquisition-time listing,
not historical publication availability or security-identity continuity.
"""
from datetime import date,timedelta
from html.parser import HTMLParser
from urllib.parse import urlencode
import re

URL='https://www.finra.org/finra-data/browse-catalog/short-sale-volume-data/daily-short-sale-volume-files'
PATH='/finra-data/browse-catalog/short-sale-volume-data/daily-short-sale-volume-files'
MONTH='custom_month[month]';YEAR='custom_year[year]'
FILE=re.compile(r'https://cdn\.finra\.org/equity/regsho/daily/CNMSshvol(\d{8})\.txt')

class IndexParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.forms=[];self.options={};self.links=[];self.select=None;self.option=None;self.anchor=None
    def handle_starttag(self,tag,attributes):
        attrs=dict(attributes)
        if tag=='form' and attrs.get('action')==PATH:self.forms.append({'action':attrs['action'],'method':attrs.get('method','').lower()})
        if tag=='select':
            self.select=attrs.get('name')
            if self.select in (MONTH,YEAR):
                if self.select in self.options:raise ValueError('Duplicate source filter')
                self.options[self.select]=[]
        if tag=='option' and self.select in (MONTH,YEAR):
            self.option={'value':attrs.get('value'),'label':'','selected':'selected' in attrs};self.options[self.select].append(self.option)
        if tag=='a':
            href=attrs.get('href','')
            if 'CNMSshvol' in href:
                if not FILE.fullmatch(href):raise ValueError('Unreviewed CNMS revision or destination in source index')
                self.anchor={'url':href,'text':'','source_line':self.getpos()[0],'source_column':self.getpos()[1]};self.links.append(self.anchor)
    def handle_endtag(self,tag):
        if tag=='select':self.select=None
        if tag=='option':self.option=None
        if tag=='a':self.anchor=None
    def handle_data(self,data):
        if self.option is not None:self.option['label']+=data
        if self.anchor is not None:self.anchor['text']+=data

def parse(raw,cutoff,period=None):
    if not isinstance(raw,bytes) or not 0<len(raw)<=2*1024*1024:raise ValueError('Whole bounded index required')
    if type(cutoff) is not date:raise ValueError('Explicit source selection cutoff required')
    if period is not None and (not re.fullmatch(r'\d{4}-\d{2}',period) or date.fromisoformat(period+'-01')>cutoff):raise ValueError('Reviewed historical month required')
    parser=IndexParser();parser.feed(raw.decode('utf-8-sig'));parser.close()
    if parser.forms!=[{'action':PATH,'method':'get'}] or set(parser.options)!={MONTH,YEAR}:raise ValueError('Official filter form differs')
    year_values={}
    for item in parser.options[YEAR]:
        label=item['label'].strip();value=item['value']
        if label.isdigit():
            if len(label)!=4 or label in year_values or not isinstance(value,str) or not re.fullmatch(r'\d{1,3}',value):raise ValueError('Ambiguous source year filter')
            year_values[label]=value
    month_values=[v['value'] for v in parser.options[MONTH] if v['value']!='any']
    if sorted(month_values)!=[f'{v:02d}' for v in range(1,13)]:raise ValueError('Source month filter differs')
    rows={}
    for link in parser.links:
        compact=FILE.fullmatch(link['url'])[1];stamp=date(int(compact[:4]),int(compact[4:6]),int(compact[6:8])).isoformat()
        if date.fromisoformat(stamp)>cutoff:raise ValueError('Future listed daily file')
        if period is not None and not stamp.startswith(period+'-'):raise ValueError('Source ignored the requested month')
        if stamp in rows:raise ValueError('Duplicate daily listing needs revision review')
        rows[stamp]={'observation_date':stamp,**link}
    if len(rows)>31:raise ValueError('Bounded single-month listing required')
    if len({v[:7] for v in rows})>1:raise ValueError('Index spans unexpected months')
    return {'year_values':year_values,'period':period or (next(iter(rows))[:7] if rows else None),
        'listed_files':[rows[v] for v in sorted(rows)],'listed_files_count':len(rows),
        'listing_status':'files_listed' if rows else 'no_files_listed',
        'historical_availability_verified':False,'security_identity_continuity_verified':False}

def month_requests(today,index,count=4):
    if type(today) is not date or type(count) is not int or not 1<=count<=4:raise ValueError('Bounded dated month plan required')
    cursor=today.replace(day=1);result=[]
    for _ in range(count):
        year=str(cursor.year)
        if year not in index['year_values']:raise ValueError('Requested year absent from source options')
        result.append({'period':cursor.strftime('%Y-%m'),'url':URL+'?'+urlencode({MONTH:f'{cursor.month:02d}',YEAR:index['year_values'][year]})})
        cursor=(cursor-timedelta(days=1)).replace(day=1)
    return result

def selected_files(indexes,latest_date,count=61):
    if type(latest_date) is not date or type(count) is not int or not 2<=count<=61:raise ValueError('Bounded explicit selection required')
    files={};months=set()
    for index in indexes:
        if index['period'] in months:raise ValueError('Duplicate retained month')
        months.add(index['period'])
        for row in index['listed_files']:
            stamp=row['observation_date']
            if stamp in files:raise ValueError('Overlapping retained source dates')
            if date.fromisoformat(stamp)>latest_date:raise ValueError('Future source selection')
            files[stamp]=row
    dates=sorted(files,reverse=True)[:count]
    if len(dates)!=count:raise ValueError('Insufficient published files for the declared window')
    return [files[v] for v in sorted(dates)]
