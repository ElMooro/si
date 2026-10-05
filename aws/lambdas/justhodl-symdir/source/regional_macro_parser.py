"""Exact published Cleveland inflation and Dallas WEI tables.

Published percentages, fractions, weekly dates and vintage columns are kept
distinct. No annualization, YoY inference, formula execution or gap filling.
"""
from datetime import datetime
import csv,io,re
import regional_fed_parser as base

EMPTY={'text':'','kind':'numeric','formula':False}

def monthly_period(text,style):
    if style=='iso_first':
        if not re.fullmatch(r'\d{4}-\d{2}-01',text):raise ValueError('Cleveland monthly date convention changed')
        date=datetime.strptime(text,'%Y-%m-%d')
    elif style=='us_first':
        if not re.fullmatch(r'\d{1,2}/1/\d{4}',text):raise ValueError('Cleveland monthly date convention changed')
        date=datetime.strptime(text,'%m/%d/%Y')
    else:raise ValueError('Unreviewed Cleveland date convention')
    if not 1983<=date.year<=2100:raise ValueError('Cleveland reference month outside reviewed range')
    return date.strftime('%Y-%m')

def cleveland(raw,schema):
    if not isinstance(raw,bytes) or len(raw)>base.MAX_WIRE:raise ValueError('Cleveland CSV exceeds bounds')
    rows=list(csv.reader(io.StringIO(raw.decode('utf-8-sig')),strict=True));cols=schema['columns']
    if not 2<=len(rows)<=2500 or rows[0]!=cols:raise ValueError('Exact Cleveland column schema changed')
    selected=schema['selected_columns']
    if not selected or not set(selected)<=set(cols[1:]):raise ValueError('Unreviewed Cleveland selected field')
    out={key:[] for key in selected};periods=[]
    for ordinal,row in enumerate(rows[1:]):
        if len(row)!=len(cols):raise ValueError('Cleveland CSV row width changed')
        period=monthly_period(row[0],schema['date_format']);periods.append(period)
        for key in selected:
            item=base.observation(ordinal,period,row[cols.index(key)],{'row':ordinal+2,'column':key,'original_reference_period':row[0]})
            if (schema['comparison']=='year_over_year' and '2025-10'<=period<='2026-10') or (schema['comparison']=='monthly' and period in ('2025-10','2025-11')):
                item['source_flags'].append('publisher_interpolated_october_november_2025_underlying_change')
            out[key].append(item)
    if periods!=sorted(periods) or len(set(periods))!=len(periods):raise ValueError('Cleveland periods repeated or unordered')
    return out

def dallas_wei(raw,schema):
    cells=base.workbook_cells(raw,schema['sheet']);cols=schema['columns']
    expected={base.column(i+1):key for i,key in enumerate(cols)}
    actual={ref[:-1]:cell['text'] for ref,cell in cells.items() if re.fullmatch('[A-Z]+1',ref) and cell['text'].strip()}
    if actual!=expected:raise ValueError('Exact Dallas WEI vintage headers changed')
    rows={}
    for ref,cell in cells.items():
        rownum=int(re.search('[0-9]+',ref)[0])
        if rownum>1:rows.setdefault(rownum,{})[ref]=cell
    if not 1<=len(rows)<=7000:raise ValueError('Dallas WEI row budget exceeded')
    out={key:[] for key in cols[1:]};periods=[]
    for rownum,row in sorted(rows.items()):
        datecell=cells.get('A'+str(rownum),EMPTY);text=datecell['text']
        if not text.strip():
            if any(c['text'].strip() for c in row.values()):raise ValueError('Dallas WEI values lack reference date')
            continue
        if datecell['formula'] or not re.fullmatch(r'\d{2}/\d{2}/\d{4}',text):raise ValueError('Dallas WEI reference date convention changed')
        date=datetime.strptime(text,'%m/%d/%Y')
        if not 2008<=date.year<=2100 or date.weekday()!=5:raise ValueError('Dallas WEI weekly reference must be Saturday')
        if any(ref[:-len(str(rownum))] not in expected and c['text'].strip() for ref,c in row.items()):raise ValueError('Unlabelled Dallas WEI data columns')
        period=date.date().isoformat();ordinal=len(periods);periods.append(period)
        for col,key in enumerate(cols[1:],2):
            ref=base.column(col)+str(rownum);cell=cells.get(ref,EMPTY)
            item=base.observation(ordinal,period[:7],cell['text'],{'sheet':schema['sheet'],'cell':ref,'date_cell':'A'+str(rownum),'original_reference_period':text},cell['kind'],cell['formula'])
            item.update(period=period,anchor=period)
            if key!='WEI':item['source_flags'].append('publisher_labelled_vintage_not_independently_release_time_verified')
            out[key].append(item)
    if periods!=sorted(periods) or len(set(periods))!=len(periods):raise ValueError('Dallas WEI dates repeated or unordered')
    return out

def parse(raw,schema):
    if schema['format']=='cleveland_csv':return cleveland(raw,schema)
    if schema['format']=='dallas_wei_xlsx':return dallas_wei(raw,schema)
    raise ValueError('Unreviewed regional macro table')
