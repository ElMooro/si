"""Pure ICI release-table research candidate; no network, storage or execution.

Current retrieved releases may revise prior weeks. They do not establish a
historical as-known-at vintage, a flow between asset classes or investment edge.
"""
from datetime import date,datetime,timezone,timedelta
from decimal import Decimal
from html.parser import HTMLParser
import hashlib,re

MMF=[('Government','government'),('Retail','government_retail'),('Institutional','government_institutional'),
     ('Prime','prime'),('Retail','prime_retail'),('Institutional','prime_institutional'),
     ('Tax-exempt','tax_exempt'),('Retail','tax_exempt_retail'),('Institutional','tax_exempt_institutional'),
     ('Total','total'),('Retail','retail'),('Institutional','institutional')]
FLOW=[('Equity','equity'),('Domestic','equity_domestic'),('World','equity_world'),('Hybrid','hybrid'),
      ('Bond','bond'),('Taxable','bond_taxable'),('Municipal','bond_municipal'),('Commodity','commodity'),('Total','total')]
SOURCES={'mmf':{'url':'https://www.ici.org/research/stats/mmf','unit':'usd_bn','labels':MMF,'unit_text':'Billions of dollars'},
         'combined_flows':{'url':'https://www.ici.org/research/stats/combined_flows','unit':'usd_mn','labels':FLOW,'unit_text':'Millions of dollars'}}

def clean(text):return re.sub(r'\s+',' ',text.replace('\xa0',' ')).strip()

class Tables(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True);self.tables=[];self.text=[];self.table=None;self.row=None;self.cell=None;self.skip=0
    def handle_starttag(self,tag,attrs):
        if tag in ('script','style','head'):self.skip+=1
        if self.skip:return
        attrs=dict(attrs)
        if tag=='table':
            if self.table is not None:raise ValueError('Nested source tables unsupported')
            self.table={'index':len(self.tables),'rows':[],'prefix':clean(' '.join(self.text))[-1000:],'text':[]};self.tables.append(self.table)
        elif tag=='tr' and self.table is not None:
            if self.row is not None:raise ValueError('Unclosed source row')
            self.row=[];self.table['rows'].append(self.row)
        elif tag in ('td','th') and self.row is not None:
            if self.cell is not None:raise ValueError('Unclosed source cell')
            if attrs.get('rowspan','1')!='1' or attrs.get('colspan','1')!='1':raise ValueError('Spanned source cells require explicit review')
            self.cell=[]
    def handle_endtag(self,tag):
        if tag in ('script','style','head') and self.skip:self.skip-=1;return
        if self.skip:return
        if tag in ('td','th') and self.cell is not None:self.row.append(clean(''.join(self.cell)));self.cell=None
        elif tag=='tr' and self.row is not None:
            if self.cell is not None:raise ValueError('Incomplete source row')
            self.row=None
        elif tag=='table' and self.table is not None:
            if self.cell is not None or self.row is not None:raise ValueError('Incomplete source table')
            self.table=None
    def handle_data(self,text):
        if self.skip:return
        self.text.append(text)
        if self.table is not None:self.table['text'].append(text)
        if self.cell is not None:self.cell.append(text)
    def finish(self,raw):
        if not isinstance(raw,bytes) or not 0<len(raw)<=8*1024*1024:raise ValueError('Complete bounded release bytes required')
        self.feed(raw.decode('utf-8-sig'));self.close()
        if self.table is not None or self.row is not None or self.cell is not None:raise ValueError('Truncated source markup')
        return self

def number(text):
    text=clean(text).replace('\u2212','-')
    if text in ('','—','–','-','N/A','NA'):return None
    if re.fullmatch(r'\([\d,]+(?:\.\d+)?\)',text):text='-'+text[1:-1]
    if not re.fullmatch(r'-?(?:\d+|\d{1,3}(?:,\d{3})+)(?:\.\d+)?',text):raise ValueError('Unreviewed numeric source cell')
    value=Decimal(text.replace(',',''))
    if not value.is_finite() or abs(value)>Decimal('1e12'):raise ValueError('Numeric source bound exceeded')
    return value

def observation(text):
    match=re.fullmatch(r'(\d{1,2})/(\d{1,2})/(\d{4})',text)
    if not match:return None
    m,d,y=map(int,match.groups());return date(y,m,d).isoformat()

def stamp(text):
    if not isinstance(text,str):raise ValueError('UTC acquisition clock required')
    instant=datetime.fromisoformat(text.replace('Z','+00:00'))
    if instant.tzinfo is None or instant.utcoffset()!=timedelta(0):raise ValueError('UTC acquisition clock required')
    return instant

def parse_release(raw,kind,acquired_at):
    if kind not in SOURCES:raise ValueError('Reviewed release kind required')
    spec=SOURCES[kind];parser=Tables().finish(raw);text=clean(' '.join(parser.text));clock=stamp(acquired_at)
    dates=set(re.findall(r'Washington,?\s+DC;?\s+([A-Z][a-z]+ \d{1,2}, \d{4})',text))
    if len(dates)!=1:raise ValueError('One unambiguous publisher release date required')
    release=datetime.strptime(next(iter(dates)),'%B %d, %Y').date()
    if release>clock.date():raise ValueError('Future source release')
    candidates=[]
    for table in parser.tables:
        rows=table['rows']
        if len(rows)>1 and [clean(r[0]).casefold() if r else '' for r in rows[1:]]==[x[0].casefold() for x in spec['labels']]:candidates.append(table)
    if len(candidates)!=1:raise ValueError('Complete uniquely identified classification table required')
    table=candidates[0];rows=table['rows'];header=rows[0]
    context=table['prefix']+' '+clean(' '.join(table['text']))
    if spec['unit_text'].casefold() not in context.casefold():raise ValueError('Source unit declaration missing')
    if not header or header[0] not in ('',' '):raise ValueError('Reviewed observation header required')
    columns=[];change=[]
    for column,label in enumerate(header[1:],1):
        day=observation(label)
        if day:
            if date.fromisoformat(day)>release:raise ValueError('Observation after release date')
            columns.append((column,day))
        elif kind=='mmf' and clean(label).replace('*','').casefold()=='$ change':change.append(column)
        else:raise ValueError('Unknown observation column')
    if not columns or len({d for _,d in columns})!=len(columns) or (kind=='mmf' and len(change)!=1):raise ValueError('Unique dated observations required')
    observations=[];reported_changes=[]
    for index,(label,series) in enumerate(spec['labels'],1):
        row=rows[index]
        if len(row)!=len(header):raise ValueError('Source row width differs')
        for column,day in columns:
            value=number(row[column])
            if kind=='mmf' and value is not None and value<0:raise ValueError('Negative asset stock')
            observations.append({'series_id':'ici:'+kind+':'+series,'series':series,'date':day,
                'value':float(value) if value is not None else None,'decimal':str(value) if value is not None else None,
                'unit':spec['unit'],'source_cell':{'table':table['index'],'row':index,'column':column,'text':row[column]}})
        for column in change:
            value=number(row[column]);reported_changes.append({'series':series,'decimal':str(value) if value is not None else None,
                'unit':spec['unit'],'source_cell':{'table':table['index'],'row':index,'column':column,'text':row[column]}})
    all_dates=sorted(d for _,d in columns)
    return {'source_id':'ICI:'+kind,'url':spec['url'],'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),
        'acquired_at':acquired_at,'release_date':release.isoformat(),'observation_dates':all_dates,'unit':spec['unit'],
        'classification_rows':len(spec['labels']),'table_index':table['index'],'table_header':header,
        'observations':observations,'reported_changes':reported_changes,'vintage_scope':'current_retrieved_release',
        'original_as_known_at_history':False,'estimated_flows':kind=='combined_flows'}

def reconcile(parsed,kind):
    rows={(r['series'],r['date']):None if r['decimal'] is None else Decimal(r['decimal']) for r in parsed['observations']}
    if kind=='mmf':
        checks=[(x,[x+'_retail',x+'_institutional']) for x in ('government','prime','tax_exempt')]
        checks += [('total',['government','prime','tax_exempt']),('total',['retail','institutional']),
            ('retail',['government_retail','prime_retail','tax_exempt_retail']),('institutional',['government_institutional','prime_institutional','tax_exempt_institutional'])]
        quantum=Decimal('.01')
    else:checks=[('equity',['equity_domestic','equity_world']),('bond',['bond_taxable','bond_municipal']),('total',['equity','hybrid','bond','commodity'])];quantum=Decimal('1')
    out=[]
    def check(name,day,values,residual,terms):
        tolerance=quantum*Decimal(terms)/2
        out.append({'identity':name,'date':day,'residual_decimal':str(residual) if residual is not None else None,
            'rounding_tolerance_decimal':str(tolerance),'unit':parsed['unit'],
            'status':'missing_input' if any(v is None for v in values) else 'within_reported_rounding' if abs(residual)<=tolerance else 'outside_reported_rounding'})
    for day in parsed['observation_dates']:
        for parent,children in checks:
            values=[rows[(x,day)] for x in [parent,*children]];delta=values[0]-sum(values[1:]) if all(v is not None for v in values) else None
            check(parent+' = '+' + '.join(children),day,values,delta,len(values))
    if kind=='mmf':
        # The explicit change column compares the first two printed date columns,
        # not an arbitrary earlier available observation or a flow into equities.
        dates=[observation(label) for label in parsed['table_header'][1:] if observation(label)]
        if len(dates)<2 or (date.fromisoformat(dates[0])-date.fromisoformat(dates[1])).days!=7:raise ValueError('Weekly reported-change endpoints required')
        for item in parsed['reported_changes']:
            values=[rows[(item['series'],dates[0])],rows[(item['series'],dates[1])],None if item['decimal'] is None else Decimal(item['decimal'])]
            delta=values[0]-values[1]-values[2] if all(v is not None for v in values) else None
            check(item['series']+' reported weekly asset change',dates[0],values,delta,3)
    return out

def compile_releases(sources,generated_at):
    if set(sources)!=set(SOURCES):raise ValueError('Both complete official source records required')
    at=stamp(generated_at);parsed={};checks={}
    for kind,entry in sources.items():
        acquired=stamp(entry['acquired_at'])
        if not 0<=(at-acquired).total_seconds()<=180:raise ValueError('Bounded acquisition window required')
        parsed[kind]=parse_release(entry['raw'],kind,entry['acquired_at']);checks[kind]=reconcile(parsed[kind],kind)
    def get(kind,series,day):
        return next((Decimal(r['decimal']) if r['decimal'] is not None else None for r in parsed[kind]['observations'] if r['series']==series and r['date']==day),None)
    md=parsed['mmf']['observation_dates'][-1];fd=parsed['combined_flows']['observation_dates'][-1]
    value=lambda v:float(v) if v is not None else None
    mmf={name:value(get('mmf',series,md)) for name,series in [('total_b','total'),('govt_b','government'),('prime_b','prime'),('retail_b','retail'),('inst_b','institutional'),('tax_exempt_b','tax_exempt')]}
    total=get('mmf','total',md);prior=get('mmf','total',(date.fromisoformat(md)-timedelta(days=7)).isoformat());gov=get('mmf','government',md)
    mmf.update(date=md,wow_b=value(total-prior) if total is not None and prior is not None else None,
        govt_share_pct=value(100*gov/total) if gov is not None and total is not None and total>0 else None,
        chg_13w_b=None,z_13w=None,yoy_pct=None,weeks_n=len(parsed['mmf']['observation_dates']),
        history=[{'date':day,'value':value(get('mmf','total',day))} for day in parsed['mmf']['observation_dates']])
    classes={}
    for name,series in [('eq_dom','equity_domestic'),('eq_world','equity_world'),('hybrid','hybrid'),('bond','bond'),('muni','bond_municipal'),('commodity','commodity'),('total','total')]:
        days=[(date.fromisoformat(fd)-timedelta(days=7*i)).isoformat() for i in range(4)]
        numbers=[get('combined_flows',series,d) for d in days]
        classes[name]={'date':fd,'latest_w_m':value(get('combined_flows',series,fd)),
            'sum_4w_m':value(sum(numbers)) if all(x is not None for x in numbers) else None,
            'window_dates':sorted(days),'z_4w':None,'weeks_n':len(parsed['combined_flows']['observation_dates'])}
    equity=[get('combined_flows','equity',(date.fromisoformat(fd)-timedelta(days=7*i)).isoformat()) for i in range(4)]
    bad=sum(x['status']!='within_reported_rounding' for rows in checks.values() for x in rows)
    return {'contract':'ici-research.v1','generated_at':generated_at,'version':'2.0.0','sources':parsed,'reconciliation':checks,
        'mmf':mmf,'long_term':{'classes':classes,'equity_sum_4w_m':value(sum(equity)) if all(x is not None for x in equity) else None,
        'equity_z_4w':None,'eq_minus_bond_4w_z':None},'quality':{'status':'measurement_reconciliation_failed' if bad else 'dated_measurements',
            'reconciliation_issues':bad,'observation_freshness':'not_certified_by_generation_time','historical_release_vintages_verified':False},
        'regime':None,'signal':None,'call':None,'provisional':True,'decision':{'verb':'WAIT','action':'abstain'},
        'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False,'forecast_qualified':False,
        'limitations':['Current retrieved weekly estimates can revise earlier observations.',
            'Money-market assets are stocks; their changes do not identify where investors move money.',
            'Combined mutual-fund cash-flow estimates and ETF net issuance are not pure ETF flows.',
            'Municipal bond flows are a subset of bond flows, not an additional total component.',
            'Unvintaged legacy histories are not merged; 13-week, year-on-year and predictive statistics remain unavailable.']}
