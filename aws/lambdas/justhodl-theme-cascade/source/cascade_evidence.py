"""Reported theme-roster occurrences and complete context identities, not trades."""
import re,zlib
from context_evidence_store import clock,strict,validate_ref,sha
from provider_flow_research import covered

CONTRACT='theme-cascade-evidence.v1'
HEAD='data/theme-cascade.json'
PRIVATE='audit-private/20260909-originals/momentum-leaders-research/cascade-context/'
INPUTS={'velocity':'data/velocity-acceleration.json','theme_rotation':'data/theme-momentum.json',
        'themes':'data/momentum-themes.json','macro':'macro/regime.json',
        'momentum':'data/momentum-leaders.json','catalysts':'data/catalysts.json'}
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
    p={'engine':'theme-cascade','schema_version':'3.0','measurement_contract':CONTRACT,'generated_at':generated_at,
        'status':'research_only','call':'WAIT','call_semantics':'abstain',**FLAGS,'sources':records,
        'reported_theme_roster':roster(docs.get('theme_rotation'),refs.get('theme_rotation')),
        'coverage':{'declared_inputs':7,'source_reads_planned':6,'structured_objects':objects,'whole_received':sum(len(v) for v in groups.values()),
            'distinct_received_payloads':len(groups),'independent_roots':None,'eligible_votes':0},
        'identical_payload_groups':[{'sha256':h,'sources':names} for h,names in groups.items() if len(names)>1],
        'quality':{'status':'research_only','observation_freshness':'unqualified'},'macro_regime':None,
        'model_requests':0,'notifications_sent':0,'alert_state_reads':0,'alert_state_writes':0,
        'limitations':['Roster membership is an upstream report, not verified fund ownership, flows or causal theme exposure.',
            'A repeated symbol or payload remains a repeated occurrence, not an extra independent confirmation.',
            'Missing flow observations stay unknown. The existing provider-flow research exclusion remains before storage.',
            'Publication clocks do not establish observation freshness, dated holdings or historical membership.',
            'No composite multiplier, hot-theme rank, tier, laggard score or position percentage is qualified.',
            'WAIT means abstain. Original market contexts and prior/current publications remain private; notification state and history are untouched.']}
    for key in ('top_hot_themes','alert_tier','medium_tier','watch_tier','laggards_hot_themes','earnings_within_3d','all_ranked'):p[key]=[]
    for key in ('n_themes_tracked','n_tickers_mapped','n_total_ranked','n_alert_tier','n_medium_tier','n_watch_tier','n_laggards_hot_themes'):p[key]=None
    return p
