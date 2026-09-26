"""Whole-workbook ACM research candidate; no IO, publication or investment authority."""
from copy import deepcopy
from datetime import datetime,timezone,date
from decimal import Decimal,localcontext,ROUND_HALF_EVEN
from fractions import Fraction
import hashlib,json,math,re
import xlrd

CONTRACT='term-premium-candidate.v1'
URL='https://www.newyorkfed.org/medialibrary/media/research/data_indicators/ACMTermPremium.xls'
HEADERS=['DATE']+[prefix+str(i).zfill(2) for prefix in ('ACMY','ACMTP','ACMRNY') for i in range(1,11)]
SHEETS={'ACM Daily':{'frequency':'D','observation_age_days':7,'steps':(1,5,21,63,252)},
    'ACM Monthly':{'frequency':'M','observation_age_days':70,'steps':(1,3,12)}}
AUTHORITY={name:False for name in ('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified','point_in_time_backtest_qualified')}
MAX=64*1024*1024
encoded=lambda value:json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
digest=lambda raw:hashlib.sha256(raw).hexdigest()


def clock(value):
    if not isinstance(value,str) or 'T' not in value:raise ValueError('Timezone-bearing acquisition/publication required')
    stamp=datetime.fromisoformat(value.replace('Z','+00:00'))
    if stamp.tzinfo is None:raise ValueError('Timezone required')
    return stamp.astimezone(timezone.utc)


def number(value,kind):
    if kind in (xlrd.XL_CELL_EMPTY,xlrd.XL_CELL_BLANK):return None
    if kind==xlrd.XL_CELL_TEXT and value in ('','.'):return None
    if kind!=xlrd.XL_CELL_NUMBER or type(value) not in (int,float) or not math.isfinite(value):
        raise ValueError('Unreviewed nonnumeric or nonfinite measurement cell')
    if abs(value)>1000:raise ValueError('Measurement outside reviewed numeric safety bound')
    return Fraction(value)


def scalar(value):
    if value is None:return {'value':None,'exact_decimal':None}
    with localcontext() as ctx:
        # IEEE doubles can include subnormal values. This covers exact binary
        # inputs and their differences, even when distant exponents cancel.
        ctx.prec=1100;ctx.rounding=ROUND_HALF_EVEN
        dec=Decimal(value.numerator)/Decimal(value.denominator)
    return {'value':float(dec),'exact_decimal':str(dec)}


def row_date(value,kind,datemode):
    if kind==xlrd.XL_CELL_DATE:
        y,m,d,*rest=xlrd.xldate_as_tuple(value,datemode)
        if any(rest):raise ValueError('Intraday date cell is not a daily/monthly observation')
        return date(y,m,d).isoformat()
    if kind==xlrd.XL_CELL_TEXT:return datetime.strptime(value,'%d-%b-%Y').date().isoformat()
    raise ValueError('Unreviewed observation date cell')


def parse(raw):
    if not isinstance(raw,bytes) or not 0<len(raw)<=MAX:raise ValueError('Whole bounded workbook required')
    book=xlrd.open_workbook(file_contents=raw)
    if set(book.sheet_names())!=set(SHEETS) or book.nsheets!=2:raise ValueError('Reviewed daily and monthly workbook required')
    tables={}
    for name,spec in SHEETS.items():
        sheet=book.sheet_by_name(name)
        if sheet.ncols!=31 or sheet.row_values(0)!=HEADERS:raise ValueError('Exact complete 31-column definition required')
        if not 1<sheet.nrows<=50000:raise ValueError('Whole sheet row boundary exceeded')
        rows=[];seen=set()
        for r in range(1,sheet.nrows):
            values=sheet.row_values(r);types=[sheet.cell(r,c).ctype for c in range(sheet.ncols)]
            day=row_date(values[0],types[0],book.datemode)
            if day in seen:raise ValueError('Duplicate observation date')
            seen.add(day)
            for value,kind in zip(values[1:],types[1:]):number(value,kind)
            rows.append({'original_row':r,'observation_date':day,'cells':values,'cell_types':types})
        tables[name]={'frequency':spec['frequency'],'headers':sheet.row_values(0),
            'header_cell_types':[sheet.cell(0,c).ctype for c in range(sheet.ncols)],'rows':rows,'datemode':book.datemode,
            'original_rows_including_header':sheet.nrows,'original_columns':sheet.ncols,
            'history_scope':'Every returned cell in this acquired model vintage, without source rounding, filtering or downsampling.'}
    return tables


def measure(table,column,generated_at,acquired_at,spec):
    now=clock(generated_at);acquired=clock(acquired_at)
    past=sorted((r for r in table['rows'] if r['observation_date']<=str(acquired.date())),key=lambda r:r['observation_date'])
    last=past[-1] if past else None
    def value(row):return number(row['cells'][column],row['cell_types'][column])
    def point(row):return None if row is None else {'observation_date':row['observation_date'],'original_row':row['original_row'],**scalar(value(row))}
    observed=point(last);age=(now.date()-date.fromisoformat(last['observation_date'])).days if last else None
    status=('unavailable' if observed is None or observed['value'] is None else 'stale_observation' if age>spec['observation_age_days'] else
        'stale_acquisition' if (now-acquired).total_seconds()>26*3600 else 'within_age_ceiling')
    numeric=[r for r in past if value(r) is not None];endpoint=numeric[-1] if numeric else None
    comparisons={}
    for steps in spec['steps']:
        baseline=numeric[-steps-1] if len(numeric)>steps else None
        start=baseline['observation_date'] if baseline else None;end=endpoint['observation_date'] if endpoint else None
        comparisons[str(steps)]={'numeric_observation_steps':steps,'current':point(endpoint),'baseline':point(baseline),
            'elapsed_calendar_days':(date.fromisoformat(end)-date.fromisoformat(start)).days if start else None,
            'missing_observation_dates':sum(start<r['observation_date']<=end and value(r) is None for r in past) if start else None,
            'change_bps':scalar(100*(value(endpoint)-value(baseline)) if baseline else None)}
    return {'last_observed':observed,'current':deepcopy(observed) if status=='within_age_ceiling' else None,
        'historical_comparisons':comparisons,'current_comparisons':deepcopy(comparisons) if status=='within_age_ceiling' else None,
        'quality':{'status':status,'observation_age_days':age,'max_observation_age_days':spec['observation_age_days'],
            'acquisition_age_seconds':(now-acquired).total_seconds(),'max_acquisition_age_seconds':26*3600,'release_calendar_verified':False},**AUTHORITY}


def build(raw,source,generated_at):
    if (source.get('source_url')!=URL or source.get('sha256')!=digest(raw) or source.get('bytes')!=len(raw)
        or not isinstance(source.get('acquired_at'),str)):raise ValueError('Exact acquired original workbook identity required')
    if clock(source['acquired_at'])>clock(generated_at):raise ValueError('Future acquisition')
    tables=parse(raw);series={};identities={}
    for name,table in tables.items():
        spec=SHEETS[name]
        for col,header in enumerate(HEADERS[1:],1):
            family='risk_neutral_yield' if header.startswith('ACMRNY') else 'term_premium' if header.startswith('ACMTP') else 'fitted_yield'
            sid=spec['frequency']+':'+header
            series[sid]={'series_id':sid,'table':name,'column':col,'native_header':header,'tenor_years':int(header[-2:]),
                'family':family,'unit':'percent','frequency':spec['frequency'],'model':'Adrian-Crump-Moench',
                'instrument_basis':'Model-implied zero-coupon yield decomposition; not Treasury par constant-maturity rates.',
                **measure(table,col,generated_at,source['acquired_at'],spec)}
        checked=0;missing=0;max_residual=Fraction(0)
        for row in table['rows']:
            for tenor in range(1,11):
                y,tp,rn=(number(row['cells'][offset+tenor],row['cell_types'][offset+tenor]) for offset in (0,10,20))
                if None in (y,tp,rn):missing+=1;continue
                residual=abs(y-tp-rn);max_residual=max(max_residual,residual);checked+=1
                if residual>Fraction(1,10**10):raise ValueError('Model yield decomposition does not reconcile')
        identities[name]={'formula':'ACMYxx - ACMTPxx - ACMRNYxx','checked':checked,'unavailable':missing,
            'maximum_absolute_residual_pct':scalar(max_residual),'tolerance_pct':'0.0000000001',
            'meaning':'Internal model arithmetic check; not independent economic validation.'}
    current=sum(row['quality']['status']=='within_age_ceiling' for row in series.values())
    return {'contract':CONTRACT,'candidate_only':True,'publication_eligible':False,'generated_at':generated_at,
        'source':deepcopy(source),'tables':tables,'series':series,'identities':identities,
        'quality':{'status':'within_age_ceiling' if current==60 else 'degraded' if current else 'unavailable','requested_series':60,'current_series':current},
        'method_sources':[URL,'https://libertystreeteconomics.newyorkfed.org/2014/05/treasury-term-premia-1961-present/'],
        'dependency_graph':{'model_roots':['NYFED:ACM'],'independent_votes':0,
            'note':'Daily and monthly values, maturities and decomposition legs share one fitted model; they are not independent signals.'},
        'limitations':['Current acquired model vintage; historical availability and revisions are not reconstructed.',
            'Percentiles and classifications are not forecast probabilities. No causal attribution or return forecast is qualified.'],
        'signals':[],'call':None,'decision':{'verb':'WAIT','meaning':'abstain'},
        'portfolio_consequences':{'status':'UNAVAILABLE','target_weights':None},**AUTHORITY}
