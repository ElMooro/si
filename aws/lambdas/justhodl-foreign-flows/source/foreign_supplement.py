"""Dated fiscal stocks and auction participation; neither identifies foreign buying."""
from datetime import date,timedelta
from decimal import Decimal,InvalidOperation
import re
import foreign_original as native

CLASSES={'Bills','Notes','Bonds','Federal Financing Bank','Floating Rate Notes','Inflation-Indexed Bonds','Inflation-Indexed Notes','Treasury Inflation-Protected Securities'}
LAYOUTS=[{'Bills','Notes','Bonds','Federal Financing Bank','Inflation-Indexed Bonds','Inflation-Indexed Notes'},
         {'Bills','Notes','Bonds','Federal Financing Bank','Treasury Inflation-Protected Securities'},
         {'Bills','Notes','Bonds','Federal Financing Bank','Treasury Inflation-Protected Securities','Floating Rate Notes'}]


def decimal(value):
    if value is None or isinstance(value,str) and value.strip().lower() in ('','null','n/a'):return None
    if isinstance(value,bool) or len(str(value))>60:raise ValueError('reported number shape')
    try:result=Decimal(str(value))
    except InvalidOperation:raise ValueError('reported number invalid')
    if not result.is_finite() or abs(result)>Decimal('1e20'):raise ValueError('reported number bound')
    return result


def txt(value):return str(value) if value is not None else None
def bn(value):return float(value/1000) if value is not None else None
def month_before(day,n):
    d=date.fromisoformat(day);year,month=divmod(d.year*12+d.month-1-n,12)
    return f'{year:04d}-{month+1:02d}-01'


def source_quality(ref,observation_end,at,max_age_days):
    age=(native.clock(at).date()-date.fromisoformat(observation_end)).days
    acquisition_age=(native.clock(at)-native.clock(ref['acquired_at'])).total_seconds()/3600
    if age<0 or acquisition_age<0:raise ValueError('future source clock')
    return {'status':'fresh' if age<=max_age_days and acquisition_age<=26 else 'stale',
        'observation_period_end':observation_end,'observation_age_days':age,'acquired_at':ref['acquired_at'],
        'max_observation_age_days':max_age_days,'max_acquisition_age_hours':26,'publication_time_verified':False}


def fiscal(ref,read,at,transactions,asof):
    doc=native.strict_json(native.original(ref,read,native.MSPD_URL,at));data=doc['data'];meta=doc['meta']
    if not isinstance(data,list) or not data or len(data)>10000:raise ValueError('MSPD response bound')
    if meta.get('total-count')!=len(data) or meta.get('count')!=len(data) or meta.get('total-pages')!=1 or (doc.get('links') or {}).get('next') is not None:raise ValueError('MSPD query incomplete')
    if (meta.get('dataFormats') or {}).get('debt_held_public_mil_amt')!='$1,000,000':raise ValueError('MSPD source units')
    periods={};records=[]
    for index,row in enumerate(data):
        day=date.fromisoformat(row['record_date']);category=row['security_class_desc']
        if day>native.clock(at).date() or (day+timedelta(days=1)).day!=1:raise ValueError('MSPD month end')
        if row['security_type_desc']!='Marketable' or category not in CLASSES:raise ValueError('MSPD source dimensions')
        period=periods.setdefault(day.isoformat(),{});
        if category in period:raise ValueError('duplicate MSPD dimensional row')
        value=decimal(row['debt_held_public_mil_amt'])
        if value is not None and value<0:raise ValueError('negative MSPD stock')
        period[category]={'value':value,'source_row_index':index}
        records.append({'record_date':day.isoformat(),'security_type_desc':'Marketable','security_class_desc':category,
            'debt_held_public_usd_million_decimal':txt(value),'source_row_index':index})
    monthly={};layouts=[]
    for day,components in sorted(periods.items()):
        complete=set(components) in LAYOUTS and all(v['value'] is not None for v in components.values())
        value=sum((v['value'] for v in components.values()),Decimal(0)) if complete else None
        key=day[:7]+'-01';monthly[key]={'date':key,'period_end':day,'value':value,'status':'complete' if complete else 'incomplete',
            'classes':sorted(components),'source_rows':[v['source_row_index'] for v in components.values()]}
        layouts.append({'period_end':day,'classes':sorted(components),'status':'complete' if complete else 'incomplete'})
    rows=[]
    for day,item in monthly.items():
        previous=monthly.get(month_before(day,1),{});a=item['value'];b=previous.get('value')
        change=a-b if a is not None and b is not None else None;foreign=decimal(transactions.get(day,{}).get('value_decimal'))
        ratio=100*foreign/change if foreign is not None and change is not None and change>0 else None
        rows.append({'month':day[:7],'period_end':item['period_end'],'stock_usd_million_decimal':txt(a),
            'stock_change_usd_million_decimal':txt(change),'foreign_transactions_usd_million_decimal':txt(foreign),
            'net_issuance_bn':bn(change),'foreign_bn':bn(foreign),'absorption_pct':None,'scale_comparison_pct':float(ratio) if ratio is not None else None,
            'source_rows':item['source_rows'],'prior_source_rows':previous.get('source_rows',[]),'transaction_source_row':transactions.get(day,{}).get('row_index'),
            'note':'Stock change is not cash net issuance; TIC includes all Treasury maturities and coverage differs. Ratio is a scale comparison, not foreign share of marginal supply.'})
    by_month={r['month']+'-01':r for r in rows};months=[month_before(asof,i) for i in range(12)];selected=[by_month.get(d) for d in months]
    complete=all(r and r['stock_change_usd_million_decimal'] is not None and r['foreign_transactions_usd_million_decimal'] is not None for r in selected)
    numerator=sum((decimal(r['foreign_transactions_usd_million_decimal']) for r in selected),Decimal(0)) if complete else None
    denominator=sum((decimal(r['stock_change_usd_million_decimal']) for r in selected),Decimal(0)) if complete else None
    ratio=100*numerator/denominator if complete and denominator>0 else None
    quality=source_quality(ref,max(periods),at,75)
    if not complete:quality['status']='partial'
    return {'status':'RESEARCH','quality':quality,'rows':rows,'raw_dimensional_rows':records,'layouts':layouts,'original':ref,
        'complete_query':True,'periods':len(periods),'aggregate_ending_month':asof,
        'incomplete_layout_periods':sum(r['status']!='complete' for r in layouts),
        'missing_class_policy':'An absent debt class is not assumed zero; affected monthly totals and changes remain unavailable.',
        'agg_12m':{'status':'complete' if complete else 'incomplete','foreign_bn':bn(numerator),'net_issuance_bn':bn(denominator),
            'stock_change_bn':bn(denominator),'pct':None,'scale_comparison_pct':float(ratio) if ratio is not None else None},
        'field_semantics':{'net_issuance_bn':'Compatibility field: change in marketable debt held by public, not cash issuance.','pct':'Unavailable: coverage and valuation conventions do not establish an absorption share.'},
        'doctrine':'Separate transaction amounts from debt-stock changes; positive purchases do not establish support, abandonment or investor intent.'}


def auctions(refs,read,at):
    records=[];seen=set();source_quality_rows={};today=native.clock(at).date();start=today-timedelta(days=60)
    for source,url in native.AUCTION_URLS.items():
        ref=refs[source];doc=native.strict_json(native.original(ref,read,url,at));kind='Note' if source=='auction_note' else 'Bond'
        if not isinstance(doc,list) or len(doc)>2000:raise ValueError('auction bounded response')
        newest=None
        for index,row in enumerate(doc):
            day=date.fromisoformat(row['auctionDate'][:10]);cusip=row['cusip'];identity=(cusip,day.isoformat(),row['securityType'])
            if not re.fullmatch('[A-Z0-9]{9}',cusip) or row['securityType']!=kind or identity in seen:raise ValueError('auction identity differs')
            seen.add(identity)
            if not start<=day<=today:raise ValueError('auction request-date range differs')
            newest=max(newest,day) if newest else day
            amounts={key:decimal(row.get(key)) for key in ('indirectBidderAccepted','directBidderAccepted','primaryDealerAccepted','competitiveAccepted','noncompetitiveAccepted','somaAccepted','fimaNoncompetitiveAccepted','totalAccepted')}
            if any(v is not None and v<0 for v in amounts.values()):raise ValueError('negative auction accepted amount')
            legs=[amounts[k] for k in ('indirectBidderAccepted','directBidderAccepted','primaryDealerAccepted')];competitive=amounts['competitiveAccepted']
            gap=sum(legs)-competitive if all(v is not None for v in legs) and competitive is not None else None
            valid=gap is not None and abs(gap)<=Decimal(1) and competitive>0
            share=amounts['indirectBidderAccepted']/competitive*100 if valid else None
            tips=row.get('tips');floating=row.get('floatingRate')
            if tips not in ('Yes','No') or floating not in ('Yes','No'):raise ValueError('auction instrument flags unknown')
            category='TIPS' if tips=='Yes' else 'FRN' if floating=='Yes' else 'Nominal '+kind
            records.append({'cusip':cusip,'date':day.isoformat(),'type':kind,'term':row.get('securityTerm'),'instrument_category':category,
                'issue_date':row.get('issueDate'),'maturity_date':row.get('maturityDate'),'indirect_pct':float(share) if share is not None else None,
                'bid_to_cover':txt(decimal(row.get('bidToCoverRatio'))),'high_yield':txt(decimal(row.get('highYield'))),
                'high_discount_margin':txt(decimal(row.get('highDiscountMargin'))),'yield_basis':'real_percent' if tips=='Yes' else 'discount_margin_bps' if floating=='Yes' else 'nominal_percent',
                'accepted_usd_decimal':{k:txt(v) for k,v in amounts.items()},'competitive_component_gap_usd_decimal':txt(gap),
                'competitive_reconciliation':'reconciled' if valid else 'unavailable_or_unreconciled','source':source,'source_row_index':index})
        source_quality_rows[source]=source_quality(ref,newest.isoformat(),at,75) if newest else {'status':'empty_returned_sample','acquired_at':ref['acquired_at']}
    records.sort(key=lambda r:(r['date'],r['cusip']),reverse=True);eligible=[r for r in records if r['competitive_reconciliation']=='reconciled']
    numerator=sum((decimal(r['accepted_usd_decimal']['indirectBidderAccepted']) for r in eligible),Decimal(0))
    denominator=sum((decimal(r['accepted_usd_decimal']['competitiveAccepted']) for r in eligible),Decimal(0))
    mean=sum((decimal(r['accepted_usd_decimal']['indirectBidderAccepted'])/decimal(r['accepted_usd_decimal']['competitiveAccepted'])*100 for r in eligible),Decimal(0))/len(eligible) if eligible else None
    return {'status':'RESEARCH','recent':records[:8],'records':records,'n_auctions_60d':len(records),'eligible_returned_auctions':len(eligible),
        'avg_indirect_pct_60d':float(mean) if mean is not None else None,'weighted_indirect_share_pct':float(100*numerator/denominator) if denominator>0 else None,
        'originals':{k:refs[k] for k in native.AUCTION_URLS},'quality_by_source':source_quality_rows,'complete_auction_universe_verified':False,
        'requested_window_start':start.isoformat(),'requested_window_end':today.isoformat(),
        'note':'Returned Note/Bond auction cohort, requested last 60 days. Indirect bidders include domestic and foreign customers; this is not a foreign-demand share. No when-issued yield is available, so no auction tail is inferred.'}
