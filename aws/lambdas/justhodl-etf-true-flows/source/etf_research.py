"""Reproduce dated NAV-valued share changes; never infer investor identity or alpha."""
from collections import Counter
from datetime import timedelta
from decimal import Decimal,localcontext
import hashlib,json,re
import etf_native as n
import etf_issuers as issuers
from etf_universe import ETFS,PROSHARES,WRAPPER_COMPLEX

CONTRACT='etf-original-research.v1'
PREFIX='data/etf-research/'
CURRENT='data/etf-true-flows.json'
QUALIFICATION={'call':None,'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False}
METHOD='Sum each dated change in reported shares multiplied by that observation\u2019s issuer NAV. No cash distribution added; no market-price substitution. This estimates net issuance at NAV, not actual cash subscriptions or secondary-market purchases.'

def encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def digest(value):return hashlib.sha256(encoded(value)).hexdigest()
def number(value):return None if value is None else float(n.dec(value))
def ref_doc(document,kind='histories'):
    raw=encoded(document);sha=hashlib.sha256(raw).hexdigest()
    return {'key':PREFIX+kind+'/'+sha+'.json','sha256':sha,'bytes':len(raw),'rows':len(document.get('rows',[]))},raw

def retained_ledger(ref,read):
    """The previously retained ledger a content-addressed input reference names, verified byte-for-byte."""
    if not isinstance(ref,dict) or not ref.get('key','').startswith(PREFIX+'ledgers/') or not re.fullmatch('[a-f0-9]{64}',str(ref.get('sha256',''))):raise ValueError('ledger reference differs')
    body=read(ref['key'])
    if ref['key']!=PREFIX+'ledgers/'+ref['sha256']+'.json' or len(body)!=ref.get('bytes') or hashlib.sha256(body).hexdigest()!=ref['sha256']:raise ValueError('retained ledger differs')
    return json.loads(body)

def vendor_context(ticker,originals,read,at):
    out={'source_status':'unreviewed_vendor_context','flow_eligible':False,'reason':'Vendor update times do not establish a common NAV/share observation date; no fallback flow computed.'}
    for label,endpoint in (('info','etf/info'),('shares','shares-float'),('quote','quote')):
        ref=originals.get('vendor_'+ticker+'_'+label)
        if not ref:continue
        try:
            raw=n.original(ref,read,at,'https://financialmodelingprep.com/stable/'+endpoint+'?symbol='+ticker);doc=json.loads(raw)
            if not isinstance(doc,list) or len(doc)!=1 or doc[0].get('symbol')!=ticker:raise ValueError('vendor identity differs')
            record=doc[0];fields={'info':('name','isin','securityCusip','nav','navCurrency','assetsUnderManagement','updatedAt'),
                'shares':('date','outstandingShares','floatShares','source'),'quote':('price','timestamp','exchange')}
            out[label]={'reported_fields':{k:record.get(k) for k in fields[label]},'source':ref}
        except Exception as exc:out[label]={'status':'unavailable','failure_class':type(exc).__name__}
    return out

def aggregate(members,rows):
    selected=[rows[t] for t in members if t in rows and rows[t].get('net_flow_5d_usd') is not None]
    dates={(v['flow_windows']['5d']['start_date'],v['flow_windows']['5d']['end_date']) for v in selected}
    if len(dates)>1:raise ValueError('aggregate attempted across unequal source periods')
    with localcontext() as ctx:
        ctx.prec=50
        value=sum((n.dec(v['flow_windows']['5d']['value_decimal']) for v in selected),Decimal(0)) if selected else None
    return {'net_flow_5d_usd':number(value),'value_decimal':n.ds(value),'n_etfs':len(selected),'configured_members':list(members),
        'covered_members':[v['ticker'] for v in selected],'unavailable_members':[t for t in members if t not in {v['ticker'] for v in selected}],
        'period':{'start_date':next(iter(dates))[0],'end_date':next(iter(dates))[1]} if dates else None,
        'status':'coverage_subtotal' if selected else 'unavailable','whole_market_total':False,**QUALIFICATION}

def build(inputs,read,at):
    if inputs.get('contract')!='etf-original-inputs.v1':raise ValueError('ETF original input contract differs')
    n.clock(at);originals=inputs['originals'];universe=sorted({t for members in ETFS.values() for t in members});cat_of={}
    for category,members in ETFS.items():
        for t in members:cat_of.setdefault(t,category)
    identity_defects={}
    ic=n.ishares_catalog(n.original(originals['ishares_catalog'],read,at,n.ISHARES_URL),universe,identity_defects)
    sc=n.ssga_catalog(n.original(originals['ssga_catalog'],read,at,n.SSGA_URL),universe,identity_defects)
    if set(ic)&set(sc) or (set(ic)|set(sc))&set(PROSHARES):raise ValueError('issuer identity overlap')
    splits=n.proshares_splits(n.original(originals['proshares_splits'],read,at,n.SPLIT_URL),at)
    parsed={};identities={};source_refs={};checks={}
    # a fund whose issuer record is defective is reported as its own unreviewed identity (2026-11-11, after TLT's
    # percent-escaped page slug had failed every run since 2026-10-12); the other funds keep their histories
    source_errors={t:{'status':'issuer_identity_unreviewed',**d} for t,d in sorted(identity_defects.items()) if t not in ic and t not in sc and t not in PROSHARES}
    for ticker in universe:
        if ticker in ic:
            identity={**ic[ticker],'currency':'USD'};label='ishares_'+ticker;url=n.DOWNLOAD.format(pid=identity['portfolio_id']);parser=lambda raw:n.ishares_history(raw,identity,at)
        elif ticker in sc:
            identity=sc[ticker];label='ssga_'+ticker;url=identity['history_url'];parser=lambda raw:n.ssga_history(raw,identity,at)
        elif ticker in PROSHARES:
            identity={'issuer':'ProShares','ticker':ticker,'currency':'USD'};label='proshares_'+ticker;url=n.PRO_URL.format(ticker=ticker);parser=lambda raw:n.proshares_history(raw,ticker,at)
        else:continue
        identities[ticker]=identity
        try:
            source_refs[ticker]=originals[label];doc=parser(n.original(originals[label],read,at,url));parsed[ticker]=doc
            if ticker in ic or ticker in sc:
                obs=next((v for v in doc['rows'] if v['date']==identity['nav_date']),None)
                q=Decimal('0.01') if ticker in sc else Decimal('0.000001')
                residual=n.dec(obs['nav_decimal'])-n.dec(identity['nav_decimal']) if obs and obs['nav_decimal'] is not None else None
                checks[ticker]={'status':'within_published_precision' if residual is not None and abs(residual)<=q else 'conflict_or_missing',
                    'date':identity['nav_date'],'catalog_nav_decimal':identity['nav_decimal'],'native_nav_decimal':obs['nav_decimal'] if obs else None,
                    'residual_decimal':n.ds(residual),'display_precision_allowance_decimal':n.ds(q),
                    'independent_confirmation':False,'reason':'Catalogue and workbook come from the same issuer.'}
            else:checks[ticker]={'status':'native_history_with_aum_identity','independent_confirmation':False}
        except Exception as exc:source_errors[ticker]={'status':'source_unavailable_or_unreviewed','failure_class':type(exc).__name__}
    # 2026-11-12: issuers outside the three workbook families. Full daily histories where the issuer publishes one
    # (Amplify); otherwise the day's reviewed snapshot accrues into a retained ledger (etf_issuers.accrue).
    ledger_refs=inputs.get('ledgers') or {};snapshot_ledgers={};ledger_funds=set();histories={}
    for ticker in issuers.history_funds():
        if ticker not in universe:continue
        try:
            issuer,doc=issuers.parse_history(ticker,originals,read,at)
            identities[ticker]={'issuer':issuer,'ticker':ticker,'currency':'USD'};source_refs[ticker]=doc['rows'][-1]['source'][0];parsed[ticker]=doc
            checks[ticker]={'status':'native_history_without_independent_check','independent_confirmation':False}
        except Exception as exc:source_errors[ticker]={'status':'source_unavailable_or_unreviewed','failure_class':type(exc).__name__,'reason':str(exc)[:120]}
    for ticker in issuers.snapshot_funds():
        if ticker not in universe:continue
        issuer=issuers.issuer_of(ticker);previous=retained_ledger(ledger_refs[ticker],read) if ticker in ledger_refs else None
        try:issuer,snapshot=issuers.parse_snapshot(ticker,originals,read,at)
        except Exception as exc:
            snapshot=None;source_errors[ticker]={'status':'snapshot_unavailable_or_unreviewed','failure_class':type(exc).__name__,'reason':str(exc)[:120]}
        ledger=issuers.accrue(previous,snapshot,ticker,issuer,at)
        if not ledger['rows']:   # nothing observed yet: no ledger is retained until the first reviewed snapshot
            snapshot_ledgers[ticker]={'issuer':issuer,'rows':0,'latest_date':None,'observed_today':None,'status':'awaiting_first_reviewed_snapshot'};continue
        lref,lraw=ref_doc(ledger,'ledgers');histories[lref['key']]=lraw
        snapshot_ledgers[ticker]={**lref,'issuer':issuer,'latest_date':ledger['rows'][-1]['date'],'observed_today':snapshot['date'] if snapshot else None,
            'restatements':len(ledger['restatements']),'previous':ledger_refs.get(ticker),'status':'accruing'}
        ledger_funds.add(ticker);identities[ticker]={'issuer':issuer,'ticker':ticker,'currency':'USD'};last=ledger['rows'][-1]
        source_refs[ticker]={**last['source'][0],'ledger':lref,'observed_at':last.get('observed_at')}
        parsed[ticker]={'rows':ledger['rows'],'scope':'Issuer-published daily snapshots (NAV, shares outstanding, net assets where exact) accrued into a retained ledger on the day each was observed; no history before the first observation.','ledger':lref,'snapshot':snapshot}
        checks[ticker]={'status':'native_snapshot_ledger_without_independent_check','independent_confirmation':False}
    if 'IVV' not in parsed:raise ValueError('reference issuer valuation dates unavailable')
    dates=sorted({row['date'] for row in parsed['IVV']['rows']});latest=dates[-1];latest_clock=n.clock(latest+'T00:00:00+00:00')
    eligible_dates=[]
    for ticker,doc in parsed.items():
        age=(latest_clock-n.clock(doc['rows'][-1]['date']+'T00:00:00+00:00')).days
        # an accruing ledger follows the common alignment date; it never moves it
        if 0<=age<=4 and checks[ticker]['status']!='conflict_or_missing' and ticker not in ledger_funds:eligible_dates.append(doc['rows'][-1]['date'])
    end=min(eligible_dates) if eligible_dates else latest
    by_etf={};native_count=0;parsed_count=0;actions_count=0
    calendar={'contract':'etf-issuer-reference-dates.v1','rows':[{'date':d} for d in dates],
        'reference_fund':'IVV','source':source_refs['IVV'],'definition':'Observed valuation dates in the retained IVV workbook, not a claim of independent exchange-calendar completeness.'}
    calendar_ref,raw=ref_doc(calendar);histories[calendar_ref['key']]=raw
    for ticker in universe:
        vendor=vendor_context(ticker,originals,read,at);identity=identities.get(ticker,{'ticker':ticker,'issuer':None,'currency':None})
        entry={'ticker':ticker,'category':cat_of[ticker],'identity':identity,'vendor_context':vendor,
          'net_flow_1d_usd':None,'net_flow_5d_usd':None,'net_flow_20d_usd':None,'nav':None,'shares_outstanding':None,'tna':None,'price':None,
          'premium_discount_bps':None,'aum_est_b':None,'nav_source':None,'history':None,'flow_windows':{},'latest_native_windows':{},
          'source_status':'no_reviewed_native_history','quality':{'status':'unavailable'},'evidence_tier':'unqualified_context',**QUALIFICATION}
        if ticker in source_errors:entry['source_error']=source_errors[ticker]
        if ticker in issuers.REVIEWED_LIMITS:entry['reviewed_limit']=issuers.REVIEWED_LIMITS[ticker]
        if ticker in snapshot_ledgers:entry['ledger']=snapshot_ledgers[ticker]
        if ticker not in parsed:by_etf[ticker]=entry;continue
        doc=parsed[ticker];rows=n.flow_history(doc['rows'],dates,splits.get(ticker,[]));native_count+=len(rows);parsed_count+=1;last=rows[-1]
        common=[row for row in rows if row['date']<=end];windows={str(k)+'d':n.window(common,dates,k) for k in (1,5,20)}
        current={str(k)+'d':n.window(rows,dates,k) for k in (1,5,20)}
        observation_age=(n.clock(at).date()-n.clock(last['date']+'T00:00:00+00:00').date()).days
        acquisition_age=(n.clock(at)-n.clock(source_refs[ticker]['acquired_at'])).total_seconds()/3600
        quality='recent_source_check' if 0<=observation_age<=4 and 0<=acquisition_age<=48 and checks[ticker]['status']!='conflict_or_missing' else 'stale_or_conflicting'
        source_actions=splits.get(ticker,[]);actions_count+=len(source_actions)
        history={'contract':'etf-native-history.v1','ticker':ticker,'identity':identity,'source':source_refs[ticker],
           'rows':rows,'corporate_actions':source_actions,'scope':doc['scope'],'source_columns':doc.get('source_columns'),'ledger':doc.get('ledger'),
           'xml_bare_ampersands_escaped':doc.get('xml_bare_ampersands_escaped',0),'reference_dates':calendar_ref,
           'method':METHOD,'vintage_policy':'Current issuer historical vintage. Original acquisition time is not a historical publication date; not point-in-time backtest data.',
           'action_policy':'Listed ProShares split transition observations and possible inverse NAV/share discontinuities are excluded. No split ratio is applied a second time. Other corporate-action completeness is unproven.',
           'precision_policy':'One last displayed unit propagated through the arithmetic as a sensitivity, not a confidence interval or guaranteed source-error bound.',**QUALIFICATION}
        href,hraw=ref_doc(history);histories[href['key']]=hraw
        compatible={}
        for k,w in windows.items():
            aligned=w.get('end_date')==end and w.get('status')=='complete_descriptive_estimate' and quality=='recent_source_check'
            compatible['net_flow_'+k+'_usd']=number(w['value_decimal']) if aligned else None
        with localcontext() as ctx:
            ctx.prec=50
            tna=n.dec(last['net_assets_decimal']) if last['net_assets_decimal'] is not None else n.dec(last['nav_decimal'])*n.dec(last['shares_decimal']) if last['nav_decimal'] is not None and last['shares_decimal'] is not None else None
        entry.update(**compatible,nav=number(last['nav_decimal']),shares_outstanding=number(last['shares_decimal']),tna=number(tna),
          aum_est_b=number(tna/Decimal('1e9')) if tna is not None else None,net_assets_basis='reported' if last['net_assets_decimal'] is not None else 'NAV times reported shares estimate',
          nav_source=identity['issuer']+('_NATIVE_SNAPSHOT_LEDGER' if ticker in ledger_funds else '_NATIVE_HISTORY'),history=href,source=source_refs[ticker],observation_date=last['date'],
          source_status='retained_native_snapshot_ledger' if ticker in ledger_funds else 'retained_native_history',
          flow_windows=windows,latest_native_windows=current,quality={'status':quality,'observation_age_days':observation_age,'acquisition_age_hours':round(acquisition_age,6),
          'alignment_date':end,'latest_native_date':last['date'],'calendar_independently_complete':False},evidence_tier='issuer_nav_valued_share_change_estimate',
          source_comparison=checks[ticker],history_observations=len(rows),history_first_date=rows[0]['date'],
          excluded_observation_counts=dict(sorted(Counter(v['flow_status'] for v in rows).items())),corporate_actions=source_actions)
        by_etf[ticker]=entry
    groups=[]
    for category in ETFS:
        members=[t for t in universe if cat_of[t]==category]
        if members:groups.append({'category':category,**aggregate(members,by_etf)})
    complexes=[{'family':name,**aggregate(members,by_etf),'share_class_rotation':None,
          'read':'Partial configured wrapper subtotal; opposite signs do not identify investor migration or share-class rotation.'} for name,members in WRAPPER_COMPLEX.items()]
    candidates=[v for v in by_etf.values() if v['net_flow_5d_usd'] is not None];ranked=sorted(candidates,key=lambda v:(-v['net_flow_5d_usd'],v['ticker']))
    errors={**inputs.get('acquisition_errors',{}),**{k:v['failure_class'] for k,v in source_errors.items()}}
    output={'engine':'etf-true-flows','version':'3.1.0','contract':CONTRACT,'generated_at':at,'n_etfs':len(universe),'maturity':'RESEARCH_ONLY',
      'engine_class':'issuer_observation_research','method':METHOD,'evidence_tier':'dated_issuer_estimates_with_coverage',
      'quality':{'status':'partial' if len(candidates)<len(universe) or errors else 'fresh','configured_funds':len(universe),'native_histories':parsed_count,
         'aligned_five_observation_estimates':len(candidates),'native_observations':native_count,'all_funds_covered':len(candidates)==len(universe),**QUALIFICATION},
      'reference_calendar':{**calendar_ref,'latest_date':latest,'observations':len(dates),'source_fund':'IVV','exchange_calendar_verified':False},
      'source_clocks':{label:ref['acquired_at'] for label,ref in sorted(originals.items())},
      'source_generated_at':max(ref['acquired_at'] for ref in originals.values()),
      'aggregation_period':{'end_date':end,'selection':'Earliest latest issuer observation among recent reviewed histories; all subtotals require identical start/end dates. Not every configured fund has a qualifying estimate.'},
      'by_etf':by_etf,'inflows':[v for v in ranked if v['net_flow_5d_usd']>0][:25],'outflows':sorted([v for v in ranked if v['net_flow_5d_usd']<0],key=lambda v:v['net_flow_5d_usd'])[:20],
      'category_rotation':groups,'complexes':complexes,'nav_source_counts':dict(Counter(v['nav_source'] or 'NO_REVIEWED_HISTORY' for v in by_etf.values())),
      'n_price_fallback_degraded':0,'source_status_codes':errors,'gaps':[t for t in universe if by_etf[t]['net_flow_5d_usd'] is None],
      'snapshot_ledgers':snapshot_ledgers,'reviewed_limits':{t:issuers.REVIEWED_LIMITS[t] for t in universe if t in issuers.REVIEWED_LIMITS},
      'coverage':{'workbook_histories':sum(1 for t in parsed if t not in ledger_funds and t not in issuers.history_funds()),'issuer_histories':sum(1 for t in parsed if t in issuers.history_funds()),
         'snapshot_ledgers':len(ledger_funds),'ledgers_accruing':len(snapshot_ledgers),'reviewed_limits':sum(1 for t in universe if t in issuers.REVIEWED_LIMITS)},
      'ground_truth':{'status':'not_reconciled','source':'SEC fund-specific subscription/redemption filings','note':'Trust CIK or accession alone does not establish fund series/class, reporting month or a parsed flow comparison.'},
      'by_stock':{'inflows':[],'outflows':[],'n_etfs_joined':0,'status':'not_measured','method':'A fund issuance estimate times a holding weight does not prove an underlying stock trade or investor identity.'},
      'impact_map':{'status':'unavailable','beneficiaries':[],'sufferers':[],'reason':'Portfolio consequences require entered exposure and price/NAV assumptions; no inferred demand-to-return coefficient.'},
      'dependency_roots':sorted({'ISSUER:'+v['identity']['issuer'] for v in by_etf.values() if v['history']}),'additional_independent_calls_votes':0,
      'legacy_preservation':inputs['legacy'],'portfolio_impact':None,'vintage_policy':'Immutable current source vintage; no historical publication dates invented.',
      'paid_ai_calls':0,'notifications_sent':0,'private_account_reads':0,'portfolio_writes':0,**QUALIFICATION}
    return output,histories
