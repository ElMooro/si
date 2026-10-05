"""Parse explicitly reviewed public survey tables without inferred metrics.

Only the named worksheet and exact ordered column schema are interpreted.
Source numbers, Excel formula-cache markers and original period cells survive.
No workbook formula is executed and no absent observation is filled.
"""
from datetime import datetime,timedelta
import calendar,csv,io,re
import regional_fed_parser as base

MONTHS={name:i+1 for i,name in enumerate(('Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'))}

def period(text,schema):
    mode=schema['date_format'];first=schema['first_period']
    if mode=='iso_month_end':
        if not re.fullmatch(r'\d{4}-\d{2}-\d{2}',text):raise ValueError('Survey date format changed')
        date=datetime.strptime(text,'%Y-%m-%d');month=text[:7]
        if date.day!=calendar.monthrange(date.year,date.month)[1]:raise ValueError('Survey month-end reference-date convention changed')
    elif mode=='mon_yy':
        names=dict(MONTHS,**schema.get('additional_month_names',{}));match=re.fullmatch(r'([A-Z][a-z]+)-([0-9]{2})',text)
        if match is None or match[1] not in names:raise ValueError('Survey short month format changed')
        # Source-specific century window avoids the standard parser's 1969
        # pivot turning the first Philadelphia May-68 observation into 2068.
        start=int(first[:4]);year=start//100*100+int(match[2]);year+=100 if year<start else 0
        month=f'{year:04d}-{names[match[1]]:02d}'
        date=None
    elif mode=='excel_serial':
        if not re.fullmatch('[0-9]{4,6}',text):raise ValueError('Survey Excel reference date invalid')
        serial=int(text)
        if not 20000<=serial<=100000:raise ValueError('Survey Excel date outside reviewed interval')
        date=datetime(1899,12,30)+timedelta(days=serial);month=date.strftime('%Y-%m')
    else:raise ValueError('Unreviewed survey date format')
    if month<first or month>'2067-12':raise ValueError('Survey reference month outside reviewed century window')
    return month,date.date().isoformat() if date else None

def observation(index,month,cell,location,field,missing):
    lexeme=cell['text'];item=base.observation(index,month,lexeme,location,cell['kind'],cell['formula'])
    if lexeme in missing:
        item.update(value=None,rejection='source_missing_value')
    low,high=field['numeric_bounds']
    if item['value'] is not None and not low<=item['value']<=high:
        if field.get('outside_bounds_policy')=='retain_flagged':item['source_flags'].append('published_adjusted_value_outside_unadjusted_bounds')
        else:item.update(value=None,rejection='source_value_outside_reviewed_measurement_bounds')
    return item

def parse_csv(raw,schema):
    if not isinstance(raw,bytes) or len(raw)>base.MAX_WIRE:raise ValueError('Survey CSV exceeds bounds')
    rows=list(csv.reader(io.StringIO(raw.decode('utf-8-sig')),strict=True));columns=schema['columns']
    if not 2<=len(rows)<=2500 or rows[0]!=columns:raise ValueError('Exact survey CSV column schema changed')
    result={key:[] for key in columns[1:]};periods=[]
    for index,row in enumerate(rows[1:]):
        if len(row)!=len(columns):raise ValueError('Survey CSV row width changed')
        month,date=period(row[0],schema);periods.append(month)
        for col,key in enumerate(columns[1:],1):
            location={'row':index+2,'column':key,'original_reference_period':row[0]}
            if date is not None:location['original_reference_date']=date
            result[key].append(observation(index,month,{'text':row[col],'kind':'numeric','formula':False},location,schema['fields'][key],schema['missing_lexemes']))
    if len(periods)!=len(set(periods)) or periods!=sorted(periods):raise ValueError('Survey periods are repeated or unordered')
    return result

def parse_xlsx(raw,schema):
    cells=base.workbook_cells(raw,schema['sheet']);columns=schema['columns'];empty={'text':'','kind':'numeric','formula':False}
    header={}
    for ref,cell in cells.items():
        if re.fullmatch('[A-Z]+1',ref) and cell['text'].strip():header[ref[:-1]]=cell['text']
    expected={base.column(i+1):key for i,key in enumerate(columns)}
    if header!=expected:raise ValueError('Exact survey worksheet column schema changed')
    grouped={}
    for ref,cell in cells.items():
        rownum=int(re.search('[0-9]+',ref)[0])
        if rownum>1:grouped.setdefault(rownum,{})[ref]=cell
    rownums=sorted(grouped)
    if len(rownums)>2500:raise ValueError('Survey worksheet row budget exceeded')
    result={key:[] for key in columns[1:]};periods=[]
    for rownum in rownums:
        row=grouped[rownum]
        datecell=cells.get('A'+str(rownum),empty)
        if not datecell['text'].strip():
            if any(c['text'].strip() for c in row.values()):raise ValueError('Source values lack a reference period')
            continue
        if datecell['formula']:raise ValueError('Reference period cannot be a formula cache')
        if schema['date_format']=='excel_serial' and datecell['kind']!='numeric':raise ValueError('Reference Excel date must be numeric')
        if any(ref[:-len(str(rownum))] not in expected and c['text'].strip() for ref,c in row.items()):raise ValueError('Unlabelled worksheet data columns')
        date_schema=schema
        if schema.get('allow_numeric_excel_date') is True and datecell['kind']=='numeric':date_schema=dict(schema,date_format='excel_serial')
        month,date=period(datecell['text'],date_schema);index=len(periods);periods.append(month)
        for col,key in enumerate(columns[1:],2):
            ref=base.column(col)+str(rownum);cell=cells.get(ref,empty)
            location={'sheet':schema['sheet'],'cell':ref,'date_cell':'A'+str(rownum),'original_reference_period':datecell['text']}
            if date is not None:location['original_reference_date']=date
            result[key].append(observation(index,month,cell,location,schema['fields'][key],schema['missing_lexemes']))
    if not periods or len(periods)!=len(set(periods)) or periods!=sorted(periods):raise ValueError('Survey periods are absent, repeated or unordered')
    return result

def parse(raw,schema):
    if set(schema['fields'])!=set(schema['columns'][1:]) or len(schema['columns'])!=len(set(schema['columns'])):raise ValueError('Exact survey field definitions required')
    if schema['format']=='survey_csv':return parse_csv(raw,schema)
    if schema['format']=='survey_xlsx':return parse_xlsx(raw,schema)
    raise ValueError('Unreviewed survey format')
