"""Native official conditions and funding research with explicit source clocks."""
from copy import deepcopy
import csv,io,re
from datetime import date,timedelta
from decimal import Decimal,localcontext,ROUND_HALF_EVEN
from plumbing_research_catalog import GROUPS,SERIES,NOTES
from report_observations import measurement,decimal as amount,months_before
from research_brief_model import clock,digest,encoded,row_status,SOURCE_CONTRACT,AGE_LIMITS,MAX_PACKET_AGE_SECONDS

CONTRACT='plumbing-research.v1'
PREFIX='data/plumbing-research/'
CURRENT='data/crisis-plumbing.json'
AUTHORITY={'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False,'forecast_eligible':False}
OFR_URL='https://www.financialresearch.gov/financial-stress-index/data/fsi.csv'
OFR_COLUMNS=('OFR FSI','Credit','Equity valuation','Safe assets','Funding','Volatility',
             'United States','Other advanced economies','Emerging markets')


def native(source,originals,at):
    now=clock(at);source_at=clock(source['generated_at']);age=(now-source_at).total_seconds()
    if source.get('contract')!=SOURCE_CONTRACT or age<0:raise ValueError('canonical macro source clock required')
    ref=source.get('replay') or {}
    if not re.fullmatch(r'data/report-research/runs/[a-f0-9]{64}\.json',str(ref.get('manifest_key',''))):raise ValueError('macro replay required')
    if digest({k:v for k,v in source.items() if k!='replay'})!=ref.get('output_sha256'):raise ValueError('macro content differs')
    out={}
    for sid in SERIES:
        original=originals.get(sid);row=source.get('measurements',{}).get(sid)
        if row:
            if not original:raise ValueError('original absent: '+sid)
            with localcontext() as ctx:
                ctx.prec=28;ctx.rounding=ROUND_HALF_EVEN
                rebuilt=measurement(sid,original['definition'],original['observations'],original['evidence'],source['generated_at'],original['acquired_at'])
            if rebuilt!=row:raise ValueError('original reconstruction differs: '+sid)
        row=row or {};status=row_status(row,now,age);fresh=status=='fresh';records=[]
        if original:
            for n,r in enumerate(original['observations']['observations']):
                if date.fromisoformat(r['date'])>source_at.date():continue
                value=amount(r.get('value'))
                records.append({'date':r['date'],'value_decimal':str(value) if value is not None else None,'source_row_index':n})
            records.sort(key=lambda r:r['date'])
        out[sid]={'id':sid,'series_id':sid,'label':row.get('name') or sid,'unit':row.get('unit'),'frequency':row.get('frequency'),
            'value_decimal':row.get('current_decimal') if fresh else None,'value':row.get('current') if fresh else None,
            'last_observed_value':row.get('current_decimal'),'observation_date':row.get('date'),
            'source_row_index':row.get('current_row_index'),'acquired_at':row.get('acquired_at'),
            'source_generated_at':source['generated_at'],'provider_updated_at':row.get('provider_updated_at'),'first_publication_at':None,
            'seasonal_adjustment':row.get('seasonal_adjustment'),'definition':deepcopy(row.get('definition')),
            'vintage':deepcopy(row.get('vintage')),'evidence':deepcopy(row.get('evidence')),'source_replay':ref,
            'quality':{'status':status,'evaluated_at':at,'maximum_observation_age_days':AGE_LIMITS.get(row.get('frequency')),
                'maximum_acquisition_age_hours':MAX_PACKET_AGE_SECONDS/3600,
                'basis':'Native observation/acquisition ceilings; first-publication availability is unverified'},
            'changes':deepcopy(row.get('changes',{})) if fresh else {},'rows':records,
            'note':NOTES.get(sid),'reason':source.get('errors',{}).get(sid,'not_collected') if not row else None,**AUTHORITY}
    return out


def comparison(rows,left,right,label,limitation,anchor=None):
    anchor=anchor or left;a,b=rows[left],rows[right];base=rows[anchor];day=base['observation_date']
    out={'id':left+'__'+right,'label':label,'series_ids':[left,right],'anchor_series':anchor,'observation_date':day,
         'source_latest_dates':{left:a['observation_date'],right:b['observation_date']},'source_rows':{},
         'value_decimal':None,'value':None,'unit':'basis_points','formula':'100 * (left percent quote - right percent quote)',
         'limitation':limitation,'reason':'Both inputs must have current, date-aligned daily percent observations',**AUTHORITY}
    if any(r['quality']['status']!='fresh' or r['frequency']!='D' or r['unit']!='Percent' for r in (a,b)):return out
    pair={sid:next((r for r in rows[sid]['rows'] if r['date']==day),None) for sid in (left,right)}
    out['source_rows']={sid:r['source_row_index'] if r else None for sid,r in pair.items()}
    if any(not r or r['value_decimal'] is None for r in pair.values()):
        out['reason']='A leg is missing on the anchor observation date; no forward fill or older date substitution';return out
    with localcontext() as ctx:
        ctx.prec=50;ctx.rounding=ROUND_HALF_EVEN
        value=100*(Decimal(pair[left]['value_decimal'])-Decimal(pair[right]['value_decimal']))
    out.update(value_decimal=format(value,'f'),value=float(value),reason=None)
    return out


def official_rank(row,years):
    """Dated descriptive midrank; bounded histories cannot claim absent years."""
    result={'years':years,'as_of':row['observation_date'],'value':None,'fraction':None,'observations':0,
        'formula':'100 * (count below current + 0.5 * count equal current) / count of prior observations',
        'scope':'Current source vintage; prior native observations only. Not an independent vote, forecast or probability.',
        'reason':'Current source and complete native span required',**AUTHORITY}
    if row['quality']['status']!='fresh' or not row['rows']:return result
    end=date.fromisoformat(row['observation_date']);start=months_before(end,12*years)
    tolerance={'D':4,'W':6,'BW':13,'M':31,'Q':92}.get(row['frequency'])
    if tolerance is None or date.fromisoformat(row['rows'][0]['date'])>start+timedelta(days=tolerance):return result
    sample=[r for r in row['rows'] if start.isoformat()<=r['date']<end.isoformat() and r['value_decimal'] is not None]
    minimum={'D':200,'W':45,'BW':22,'M':10,'Q':3}[row['frequency']]*years
    result['observations']=len(sample)
    if len(sample)<minimum:return result
    value=Decimal(row['value_decimal']);below=sum(Decimal(r['value_decimal'])<value for r in sample);equal=sum(Decimal(r['value_decimal'])==value for r in sample)
    numerator=100*(2*below+equal);denominator=2*len(sample)
    with localcontext() as ctx:
        ctx.prec=50;ctx.rounding=ROUND_HALF_EVEN
        result['value']=float((Decimal(numerator)/Decimal(denominator)).quantize(Decimal('0.000001')))
    result.update(fraction={'numerator':numerator,'denominator':denominator},from_date=start.isoformat(),reason=None)
    return result


def ofr_native(packet,original,at):
    """Reconstruct all nine OFR publisher columns from the exact retained CSV."""
    if packet.get('contract')!='funding-original-research.v1':raise ValueError('canonical OFR carrier required')
    now=clock(at);source_at=clock(packet['generated_at']);acquired=clock(original['acquired_at']);ref=original['evidence']
    if not acquired<=source_at<=now or ref.get('source_url')!=OFR_URL:raise ValueError('OFR source clocks/request differ')
    reader=csv.DictReader(io.StringIO(original['raw'].decode('utf-8-sig')))
    if reader.fieldnames!=['Date',*OFR_COLUMNS]:raise ValueError('OFR original columns differ')
    records=list(reader)
    if not 1<=len(records)<=20000:raise ValueError('OFR row bound')
    seen=set();out={}
    for r in records:
        if None in r or not re.fullmatch(r'\d{4}-\d{2}-\d{2}',r['Date']):raise ValueError('OFR row shape differs')
        day=date.fromisoformat(r['Date'])
        if day.isoformat() in seen or day>source_at.date():raise ValueError('OFR duplicate/future observation')
        seen.add(day.isoformat())
    for column in OFR_COLUMNS:
        sid='ofr_fsi:'+column.lower().replace(' ','_');rows=[]
        for n,r in enumerate(records):
            value=amount(r[column])
            if value is None and r[column] not in ('','.','*','N/A'):raise ValueError('OFR invalid numeric value')
            rows.append({'date':r['Date'],'value_decimal':str(value) if value is not None else None,'source_row_index':n})
        rows.sort(key=lambda r:r['date']);last=rows[-1];old=packet['measurements'].get(sid,{})
        if (old.get('unit')!='index_points' or old.get('as_of')!=last['date'] or old.get('original_row_index')!=last['source_row_index']
                or old.get('evidence')!=ref or old.get('quality',{}).get('acquired_at')!=original['acquired_at']
                or old.get('value_decimal')!=last['value_decimal']):raise ValueError('OFR original row binding differs')
        age=(now.date()-date.fromisoformat(last['date'])).days
        status='unavailable' if last['value_decimal'] is None else 'stale_observation' if age>7 else 'stale_source' if (now-acquired).total_seconds()>26*3600 else 'fresh'
        out[sid]={'id':sid,'series_id':sid,'label':'OFR FSI · '+column,'unit':'index_points','frequency':'D',
            'value_decimal':last['value_decimal'] if status=='fresh' else None,'value':float(last['value_decimal']) if status=='fresh' else None,
            'last_observed_value':last['value_decimal'],'observation_date':last['date'],'source_row_index':last['source_row_index'],
            'acquired_at':original['acquired_at'],'source_generated_at':packet['generated_at'],'first_publication_at':None,
            'quality':{'status':status,'evaluated_at':at,'maximum_observation_age_days':7,'maximum_acquisition_age_hours':26},
            'definition':{'column':column,'url':'https://www.financialresearch.gov/financial-stress-index/',
                'release_note':'Publisher states a two-business-day lag; this product does not claim a verified historical release calendar.'},
            'evidence':ref,'source_replay':packet['replay'],'rows':rows,'changes':{},
            'note':'Headline, category contributions and region contributions share one publisher index. They are not nine independent stress votes.',**AUTHORITY}
    return out


def compact(output):
    artifacts={}
    for row in output['measurements'].values():
        records=row.pop('rows')
        if not records:row['history']=None;continue
        doc={'contract':'plumbing-history.v1','series_id':row['id'],'unit':row['unit'],'frequency':row['frequency'],
             'columns':['date','value_decimal','source_row_index'],'rows':[[r['date'],r['value_decimal'],r['source_row_index']] for r in records]}
        raw=encoded(doc);sha=digest(doc);key=PREFIX+'histories/'+sha+'.json';artifacts[key]=raw
        row['history']={'key':key,'sha256':sha,'bytes':len(raw),'observations':len(records),'first':records[0]['date'],'last':records[-1]['date']}
    return artifacts


def build(source,originals,at,ofr=None,context=None):
    rows=native(source,originals,at)
    pairs={key:comparison(rows,*args) for key,args in {
        'sofr_iorb':('SOFR','IORB','SOFR minus IORB','Secured transaction median versus administered reserve rate; no reserve-scarcity diagnosis.'),
        'obfr_iorb':('OBFR','IORB','OBFR minus IORB','Defined unsecured bank transactions versus an administered rate; not the entire offshore dollar market.'),
        'hy_ig':('BAMLH0A0HYM2','BAMLC0A0CM','High-yield minus investment-grade OAS','Different issuer, maturity and duration populations.'),
        'ccc_bb':('BAMLH0A3HYC','BAMLH0A1HYBB','CCC-and-lower minus BB OAS','Different and economically related issuer universes; not independent votes.'),
        'usd_eur_overnight':('SOFR','ECBESTRVOLWGTTRMDMNRT','USD SOFR minus euro €STR','Different currencies, secured/unsecured populations and trading windows; no forward FX, covered-interest basis or executable carry.'),
        'usd_three_month_eur_overnight':('DGS3MO','ECBESTRVOLWGTTRMDMNRT','US three-month yield minus euro overnight rate','Different currency, maturity and instrument. This quote difference is not a funding basis.'),
        'usd_jpy_three_month':('DGS3MO','IR3TIB01JPM156N','US and Japanese three-month rate context','Japanese monthly observations are not daily quotes. No forward fill or inferred basis.'),
    }.items()}
    ranks={sid:{str(y):official_rank(rows[sid],y) for y in (5,10)} for sid in ('STLFSI4','NFCI','ANFCI','KCFSI')}
    rows.update(ofr or {});fresh=sum(row['quality']['status']=='fresh' for row in rows.values())
    output={'engine':'justhodl-crisis-plumbing','version':'2.0.0','contract':CONTRACT,'generated_at':at,'source_generated_at':source['generated_at'],
        'source_clocks':{'macro':source['generated_at'],'ofr':next(iter(ofr.values()))['source_generated_at'] if ofr else None},
        'context':context or {},
        'measurements':rows,'comparisons':pairs,'official_historical_ranks':ranks,
        'quality':{'status':'fresh' if fresh==len(SERIES)+len(OFR_COLUMNS) else 'degraded' if fresh else 'unavailable',
                   'fresh_measurements':fresh,'requested_fred_identities':len(SERIES),'ofr_publisher_columns':len(ofr or {}),'expected_measurements':len(SERIES)+len(OFR_COLUMNS)},
        'groups':{**{k:list(v) for k,v in GROUPS.items()},'ofr_publisher':list(ofr or {})},
        'dependency_graph':{'roots':{sid:{'provider':'OFR' if sid.startswith('ofr_fsi:') else 'FRED','series_id':sid,'source_replay':row['source_replay']} for sid,row in rows.items()},
             'shared_families':{'chicago_conditions':['NFCI','ANFCI','NFCICREDIT','NFCILEVERAGE','NFCIRISK','NFCINONFINLEVERAGE'],
                  'ofr_financial_stress':list(ofr or {}),'ice_credit':list(GROUPS['credit'])},
             'independent_votes':0,'note':'Publisher composites share markets and input series; root and family counts are not effective independent evidence.'},
        'composite':{'composite_stress_score':None,'consensus_count':None,'agreement_signal':'RESEARCH_ONLY','flagged_indices':[]},
        'crisis_indices':{},'plumbing_tier2':{},'funding_credit_signals':{},'xcc_basis_proxy':{},'yield_curve':{},'enrichment':{},'mmf_composition':None,
        'unavailable_claims':{'cross_currency_basis':'Matched forward FX, tenor, settlement and funding conventions are absent.',
            'money_fund_migration':'RRP or bank-deposit stock changes do not identify prime-to-government flows.',
            'crisis_probability':'No JustHodl crisis forecast has passed independent prospective qualification.'},
        'decision':{'verb':'WAIT','meaning':'abstain'},'call':None,
        'portfolio_consequences':{'status':'SCENARIOS_ONLY','scenario_page':'/position-sizer.html','target_weights':None,'sizing_multiplier':None,
             'forced_liquidation':False,'reason':'Inspect rate, currency, credit and funding shocks using explicit holdings and assumptions. These observations do not authorize a position.'},
        'methodology':{'source':'All former requested identities remain visible, including unavailable and discontinued series.',
            'history':'Latest bounded native observations, never the earliest fixed-size page mislabeled current. Original missing rows and source indices survive.',
            'comparisons':'Exact anchor date, native daily percent units, no forward fill. Differences retain instrument/tenor limitations.',
            'rights':'Use and redistribution conditions vary by source. Publisher definitions and notices are retained; FRED is not a blanket public-domain license.'},**AUTHORITY}
    return output,compact(output)
