"""Contemporaneous observations and explicit evaluation gaps; no predictive validation."""
import re,zlib,math
from datetime import date
from decimal import Decimal,localcontext
from context_evidence_store import clock,strict,validate_ref,sha
from provider_flow_research import covered

CONTRACT='cascade-snapshot-observations.v1'
HEAD='data/theme-cascade-backtest.json'
PRIVATE='audit-private/20260909-originals/momentum-leaders-research/backtest-context/'
INPUTS={'momentum':'data/momentum-leaders.json','theme_rotation':'data/theme-momentum.json'}
EXCLUDED='etf-flows/stock-exposure-lookup.json'
FLAGS=dict.fromkeys(('calls_eligible','ranking_eligible','sizing_eligible','execution_eligible','forecast_qualified'),False)


def symbol(value):return value if isinstance(value,str) and re.fullmatch(r'[A-Z0-9][A-Z0-9.\-^]{0,24}',value) else None
def pointer(value):return value.replace('~','~0').replace('/','~1')


def roster(doc,ref):
    out={'status':'unavailable','source_ref':ref,'theme_occurrences':[],'membership_occurrences':[],
         'theme_rows':None,'distinct_reported_etfs':None,'membership_rows':None,'distinct_reported_pairs':None,
         'holdings_verified':False,'market_coverage_verified':False,'independent_roots':None,**FLAGS}
    if not isinstance(doc,dict) or doc.get('status')=='error' or doc.get('error'):return out
    themes=doc.get('all_themes');breadth=doc.get('breadth_details')
    if not isinstance(themes,list):return out
    if len(themes)>10000:raise ValueError('Whole theme roster exceeds declared projection bound')
    seen={};theme_rows=[]
    for i,row in enumerate(themes):
        etf=symbol(row.get('ticker')) if isinstance(row,dict) else None
        reason='invalid_literal_etf' if etf is None else 'duplicate_reported_etf' if etf in seen else 'reported_etf'
        theme_rows.append({'etf':etf,'source_pointer':'/all_themes/'+str(i),'source_index':i,'status':reason,
            'prior_occurrence_index':seen.get(etf) if etf else None})
        if etf is not None:seen.setdefault(etf,i)
    out.update(status='reported_roster_unqualified',theme_occurrences=theme_rows,theme_rows=len(themes),distinct_reported_etfs=len(seen))
    if not isinstance(breadth,dict):return out
    memberships=[];pairs=set();shape_issues=[]
    for etf,block in breadth.items():
        root='/breadth_details/'+pointer(etf);valid_etf=symbol(etf)
        values=block.get('constituents_perf') if isinstance(block,dict) else None
        if not isinstance(values,list):shape_issues.append({'source_pointer':root,'status':'missing_or_invalid_constituents'});continue
        for i,row in enumerate(values):
            if len(memberships)>=100000:raise ValueError('Whole membership roster exceeds projection bound')
            ticker=symbol(row.get('symbol')) if isinstance(row,dict) else None
            pair=(valid_etf,ticker);valid=valid_etf is not None and ticker is not None
            memberships.append({'etf':valid_etf,'ticker':ticker,'source_pointer':root+'/constituents_perf/'+str(i),
                'source_index':i,'status':'invalid_literal_membership' if not valid else 'duplicate_reported_pair' if pair in pairs else 'reported_membership',
                'etf_in_reported_roster':valid_etf in seen if valid_etf else False})
            if valid:pairs.add(pair)
    out.update(membership_occurrences=memberships,membership_rows=len(memberships) if not shape_issues else None,
        extracted_membership_occurrences=len(memberships),distinct_reported_pairs=len(pairs),membership_shape_issues=shape_issues,
        membership_status='partial_source_shape' if shape_issues else 'reported_memberships_unqualified')
    return out


def build(attempts,sources,generated_at):
    generated=clock(generated_at)
    if generated is None or not isinstance(attempts,dict) or set(attempts)!=set(INPUTS) or not covered(EXCLUDED):
        raise ValueError('Exact original context graph and existing flow exclusion required')
    records=[];docs={};refs={};objects=0;groups={}
    for name,key in INPUTS.items():
        a=attempts[name]
        if not isinstance(a,dict) or a.get('source_key')!=key:raise ValueError('Unexpected context source')
        first=clock(a.get('requested_at'));last=clock(a.get('received_at'))
        if first is None or last is None or not first<=last<=generated:raise ValueError('Ordered source clocks required')
        row={'source':name,'source_key':key,'requested_at':a['requested_at'],'received_at':a['received_at'],
             'status':'source_read_unavailable','original_ref':None,'source_generated_at':None,
             'source_generation_age_seconds':None,'source_clock_status':'unknown','observation_freshness':'unqualified',**FLAGS}
        if a.get('status')=='source_read_unavailable':
            if a.get('original_ref') is not None:raise ValueError('Unavailable source cannot claim complete body')
        elif a.get('status')=='received':
            ref=validate_ref(a.get('original_ref'),PRIVATE,'sources');raw=sources.get(ref['key'])
            if not isinstance(raw,bytes) or len(raw)!=ref['bytes'] or sha(raw)!=ref['sha256']:raise ValueError('Complete original differs')
            refs[name]=ref;row['original_ref']=dict(ref);groups.setdefault(ref['sha256'],[]).append(name)
            try:doc=strict(raw,a.get('content_encoding',''))
            except (ValueError,UnicodeError,zlib.error):row['status']='invalid_json'
            else:
                if not isinstance(doc,dict) or not doc:row['status']='unrecognized_shape'
                elif doc.get('status')=='error' or doc.get('error'):row['status']='source_error'
                else:
                    docs[name]=doc;objects+=1;row['status']='received_object_unqualified';stamp=clock(doc.get('generated_at'))
                    if stamp is not None:
                        row['source_generated_at']=stamp.isoformat()
                        if stamp>generated:row['source_clock_status']='future'
                        else:
                            row['source_clock_status']='reported_publication_clock'
                            row['source_generation_age_seconds']=round((generated-stamp).total_seconds(),3)
        else:raise ValueError('Unknown source acquisition')
        records.append(row)
    if not objects:raise ValueError('No structured context; preserve prior head')
    records.append({'source':'exposure','source_key':EXCLUDED,'status':'not_read_existing_flow_exclusion',
        'original_ref':None,'source_generated_at':None,'source_clock_status':'unknown','observation_freshness':'unqualified',**FLAGS})
    theme_roster=roster(docs.get('theme_rotation'),refs.get('theme_rotation'))
    theme_clock=clock(docs.get('theme_rotation',{}).get('generated_at'))
    p={'engine':'theme-cascade-backtest','schema_version':'2.0','measurement_contract':CONTRACT,'generated_at':generated_at,
        'status':'research_only','call':'WAIT','call_semantics':'abstain',**FLAGS,'sources':records,
        'reported_theme_roster':theme_roster,
        'snapshot_observations':snapshot(docs.get('momentum'),refs.get('momentum'),theme_roster,theme_clock,generated),
        'coverage':{'declared_inputs':3,'source_reads_planned':2,'structured_objects':objects,
            'whole_received':sum(len(v) for v in groups.values()),'distinct_received_payloads':len(groups),'independent_roots':None,'eligible_votes':0},
        'identical_payload_groups':[{'sha256':h,'sources':names} for h,names in groups.items() if len(names)>1],
        'quality':{'status':'research_only','observation_freshness':'unqualified'},'validation_rate':None,
        'evaluation':{'status':'not_a_prospective_backtest','predictive_validation':False,'forward_outcome_count':None,
            'missing_evidence':['immutable ex-ante forecasts and effective-dated universe/holdings',
                'post-forecast outcomes with corporate-action, currency and transaction-cost treatment',
                'predeclared hypothesis, benchmark, control population and evaluation horizons',
                'held-out or prospective evaluation with overlap and multiple-testing controls'],
            'same_snapshot_association_is_predictive_validation':False},
        'lift_metrics':{'primary_metric':None,'big_pumper_pct_in_top_10':None,'pumper_pct_in_top_10':None,
            'pct_in_top_10_lift_pp':None,'pct_in_top_20_lift_pp':None,'mean_theme_momentum_lift':None,
            'interpretation':'UNVALIDATED — contemporaneous selected-universe evidence cannot establish future predictive validity'},
        'n_momentum_leaders':None,'n_etf_themes':None,'top_10_etfs':[],'top_20_etfs':[],
        'big_pumpers_detail':[],'pumpers_detail':[],'laggards_hot_detail':[],
        'model_requests':0,'notifications_sent':0,
        'limitations':['The original inputs are contemporaneous selected-universe contexts, not frozen ex-ante forecasts or subsequent outcomes.',
            'Reported membership is not verified fund ownership or effective-dated historical membership; missing reports do not mean no holdings.',
            'Price changes are recomputed from the retained parent’s reported observations; original provider bodies are not re-read or independently revalidated here.',
            'Different endpoint dates remain in separate cohorts; duplicate/invalid parent issuers cannot inflate a sample.',
            'Five reported intervals do not establish five exchange sessions, total return, alpha, or a tradable strategy.',
            'The at-least-ten-percent cohort is a subset of at-least-five, not an independent sample. Genuine zero remains a measured value.',
            'No hot-theme rank, validation rate, laggard trade, forecast or position size is qualified. WAIT means abstain.']}
    for name in ('big_pumpers_5d_stats','pumpers_5d_stats','control_stats','laggards_hot_stats'):
        p[name]={'n':None,'n_with_theme':None,'pct_in_top_10':None,'pct_in_top_20':None,'pct_in_top_30':None,
            'mean_theme_momentum':None,'median_theme_momentum':None,'mean_theme_rs_rank':None,'median_theme_rs_rank':None}
    return p


def snapshot(parent,parent_ref,theme_roster,theme_clock,generated):
    """Replay descriptive derived-context prices, not unseen original provider data."""
    result={'status':'parent_observations_unavailable','parent_source_ref':parent_ref,'observations':[],
        'cohorts_by_window':[],'unavailable_observations':None,'root_source_revalidated':False,
        'historical_membership_verified':False,'prospective_validation':False,**FLAGS}
    parent_flags=('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified','independent_evidence_eligible','private_state_read_or_written')
    if (not isinstance(parent,dict) or parent.get('measurement_contract')!='leader-price-observations.v1' or
        parent.get('status')!='RESEARCH_ONLY' or parent.get('call') is not None or parent.get('ranking_eligible',False) is not False or any(parent.get(k) is not False for k in parent_flags)):
        return result
    stamp=clock(parent.get('generated_at'))
    if stamp is None or stamp>generated:return result
    members=parent.get('universe_membership',{}).get('selected') if isinstance(parent.get('universe_membership'),dict) else None
    records=parent.get('request_records')
    if not isinstance(members,list) or not isinstance(records,list) or len(members)!=len(records) or len(members)>500:return result
    seen=set()
    for i,(member,record) in enumerate(zip(members,records)):
        if not isinstance(member,dict) or not isinstance(record,dict):return result
        ticker=symbol(member.get('ticker'))
        if (ticker is None or ticker in seen or record.get('ticker')!=ticker or type(member.get('request_index')) is not int or member['request_index']!=i or
            type(record.get('request_index')) is not int or record['request_index']!=i):return result
        seen.add(ticker)
    reports=theme_roster.get('membership_occurrences',[]) if theme_clock is not None and theme_clock<=generated else []
    roster_usable=theme_roster.get('status')=='reported_roster_unqualified' and theme_clock is not None and theme_clock<=generated
    membership_index={}
    for report in reports:membership_index.setdefault(report.get('ticker'),[]).append(report)
    result.update(status='descriptive_current_context_only',unavailable_observations=0)
    by_window={}
    for i,record in enumerate(records):
        ticker=record['ticker'];ptr='/request_records/'+str(i)+'/observations'
        row={'ticker':ticker,'source_pointer':ptr,'source_index':i,'status':'price_change_unavailable',
            'value':None,'exact':None,'unit':'percent','start_date':None,'end_date':None,'intervals':5,
            'reported_memberships':[dict(r) for r in membership_index.get(ticker,[]) if r.get('etf') is not None],
            'membership_coverage':'reported_roster_with_unverified_effective_date' if roster_usable else 'unavailable',
            'no_record_does_not_mean_no_holdings':True,'total_return_verified':False,'root_source_revalidated':False,**FLAGS}
        obs=record.get('observations');selected=obs.get('selected_rows') if isinstance(obs,dict) else None
        if (isinstance(obs,dict) and obs.get('ticker')==ticker and obs.get('status')=='parsed_completed_observations' and
            all(obs.get(k) is False for k in parent_flags) and isinstance(selected,list) and 25<=len(selected)<=250):
            clean=[];dates=set();indices=set()
            for j,r in enumerate(selected):
                if not isinstance(r,dict):break
                d=day(r.get('date'));v=decimal(r.get('close'));index=r.get('index')
                if d is None or d>=stamp.date() or d in dates or type(index) is not int or index<0 or index in indices or v is None or v<=0 or r.get('issues')!=[]:break
                if clean and d<=clean[-1][0]:break
                dates.add(d);indices.add(index);clean.append((d,v,index,j))
            if len(clean)==len(selected):
                a,b=clean[-6],clean[-1]
                with localcontext() as ctx:
                    ctx.prec=40;value=(b[1]/a[1]-1)*100
                metric=obs.get('measurements',{}).get('price_change_5') if isinstance(obs.get('measurements'),dict) else None
                valid=(isinstance(metric,dict) and metric.get('status')=='descriptive_local_price_change' and metric.get('unit')=='percent' and
                    type(metric.get('intervals')) is int and metric['intervals']==5 and metric.get('total_return_verified') is False and
                    metric.get('start_date')==a[0].isoformat() and metric.get('end_date')==b[0].isoformat() and
                    type(metric.get('start_source_index')) is int and metric['start_source_index']==a[2] and
                    type(metric.get('end_source_index')) is int and metric['end_source_index']==b[2] and
                    decimal(metric.get('start_close'))==a[1] and decimal(metric.get('end_close'))==b[1] and
                    decimal(metric.get('exact'))==value and type(metric.get('value')) in (int,float) and math.isfinite(metric['value']) and metric['value']==float(value))
                if valid:
                    row.update(status='descriptive_parent_price_change',value=float(value),exact=str(value),start_date=a[0].isoformat(),end_date=b[0].isoformat(),
                        start_close=str(a[1]),end_close=str(b[1]),start_parent_pointer=ptr+'/selected_rows/'+str(a[3])+'/close',
                        end_parent_pointer=ptr+'/selected_rows/'+str(b[3])+'/close',start_original_source_index=a[2],end_original_source_index=b[2])
                    by_window.setdefault((row['start_date'],row['end_date']),[]).append((i,value))
        if row['value'] is None:result['unavailable_observations']+=1
        result['observations'].append(row)
    for (start,end),values in sorted(by_window.items()):
        groups=[]
        for name,predicate in (('up_at_least_ten_percent',lambda n:n>=10),('up_at_least_five_percent',lambda n:n>=5),
                               ('between_zero_and_five_percent',lambda n:0<n<5),('nonpositive_change',lambda n:n<=0)):
            entries=[(i,v) for i,v in values if predicate(v)];ordered=sorted(v for _,v in entries);n=len(ordered)
            with localcontext() as ctx:
                ctx.prec=40;median=(ordered[(n-1)//2]+ordered[n//2])/2 if n else None
            groups.append({'cohort':name,'distinct_reported_issuers':n,'observation_indices':[i for i,_ in entries],
                'median_price_change_percent':float(median) if median is not None else None,
                'median_price_change_exact':str(median) if median is not None else None,
                'with_reported_membership':sum(bool(result['observations'][i]['reported_memberships']) for i,_ in entries) if roster_usable else None,
                'membership_without_record_is_unknown':True,'forward_outcome_count':None})
        result['cohorts_by_window'].append({'start_date':start,'end_date':end,'intervals':5,'cohorts':groups,
            'at_least_ten_is_subset_of_at_least_five':True,'unverified_trading_session_count':True,'predictive_validation':False})
    return result


def day(value):
    try:return date.fromisoformat(value) if isinstance(value,str) and re.fullmatch(r'\d{4}-\d{2}-\d{2}',value) else None
    except ValueError:return None


def decimal(value):
    if not isinstance(value,str) or len(value)>100:return None
    try:n=Decimal(value)
    except ArithmeticError:return None
    return n if n.is_finite() and abs(n)<=Decimal('1e30') and (not n or abs(n)>=Decimal('1e-30')) else None
