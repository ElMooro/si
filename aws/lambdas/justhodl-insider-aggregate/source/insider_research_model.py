"""Deterministic diagnostics of a bounded vendor-reported insider filing sample.

P/S codes include open-market OR private transactions and do not identify a
discretionary common-share trade. Counts describe distinct vendor row
representations, not a reconciled SEC transaction population.
"""
from collections import Counter,defaultdict
from datetime import date,datetime,time,timedelta,timezone
from decimal import Decimal,localcontext,ROUND_HALF_EVEN
import hashlib,json,re
from urllib.parse import urlsplit

CONTRACT='insider-native-research.v1'
PREFIX='data/insider-research/'
PRIVATE='audit-private/20260909-originals/insider-research/'
CURRENT='data/insider-aggregate.json'
ENDPOINT='https://financialmodelingprep.com/stable/insider-trading/latest'
PERMISSIONS={k:False for k in ('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible')}
MAX_PAGES=40;PAGE_LIMIT=1000;MAX_BYTES=8*1024*1024

def encoded(doc):return json.dumps(doc,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def sha(raw):return hashlib.sha256(raw).hexdigest()
def clock(value):
    dt=datetime.fromisoformat(value.replace('Z','+00:00'))
    if dt.tzinfo is None:raise ValueError('Aware publication clock required')
    return dt.astimezone(timezone.utc)
def day(value):
    if not isinstance(value,str):return None
    try:
        if re.fullmatch(r'\d{4}-\d{2}-\d{2}',value):return date.fromisoformat(value)
        if re.fullmatch(r'\d{4}-\d{2}-\d{2}[T ][0-9:.+Z-]+',value):return datetime.fromisoformat(value.replace('Z','+00:00')).date()
    except ValueError:pass
    return None
def number(value):
    if type(value) not in (str,int,float,Decimal) or isinstance(value,bool):return None
    try:
        out=Decimal(str(value))
        return out if out.is_finite() and abs(out)<=Decimal('1e20') else None
    except ArithmeticError:return None
def cik(value):
    return str(value).zfill(10) if type(value) in (str,int) and re.fullmatch(r'[0-9]{1,10}',str(value)) and int(value)>0 else None
def filing_url(value):
    if not isinstance(value,str):return None
    try:
        p=urlsplit(value)
        if p.scheme=='https' and p.netloc in ('www.sec.gov','sec.gov') and not p.query and not p.fragment and re.fullmatch(r'/Archives/edgar/data/[0-9]+/[A-Za-z0-9/_.-]+',p.path):return value
    except ValueError:pass
    return None
def source_url(page):
    if type(page) is not int or not 0<=page<MAX_PAGES:raise ValueError('Bounded page identity required')
    return ENDPOINT+'?page='+str(page)+'&limit='+str(PAGE_LIMIT)
def due_at(generated):
    """Existing weekday 22:30 UTC schedule, plus a disclosed two-hour allowance."""
    dt=clock(generated)
    for offset in range(8):
        d=dt.date()+timedelta(days=offset);scheduled=datetime.combine(d,time(22,30),timezone.utc)
        if d.weekday()<5 and scheduled>dt:return (scheduled+timedelta(hours=2)).isoformat()
    raise ValueError('Next weekday schedule unavailable')
def decode(raw):
    if len(raw)>MAX_BYTES:raise ValueError('Provider page exceeds bound')
    def bad(_):raise ValueError('Nonfinite provider literal')
    result=json.loads(raw,parse_float=Decimal,parse_constant=bad)
    if not isinstance(result,list) or len(result)>PAGE_LIMIT:raise ValueError('Bounded transaction array required')
    return result
def row_digest(row):
    # Decimal tags preserve original numerical precision; original response bytes
    # remain the authority. This is representation dedupe, not SEC line identity.
    return sha(json.dumps(row,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False,
        default=lambda v:{'$decimal':str(v)} if isinstance(v,Decimal) else None).encode())

def classify(row,today):
    reasons=[];transaction_code=row.get('transactionType')
    code={'P':'buy','P-Purchase':'buy','S':'sell','S-Sale':'sell'}.get(transaction_code) if isinstance(transaction_code,str) else None
    form=row.get('formType');filing=day(row.get('filingDate'));transaction=day(row.get('transactionDate'))
    if code is None:reasons.append('not_exact_P_or_S')
    if form!='4':reasons.append('amendment_requires_reconciliation' if form=='4/A' else 'not_Form_4')
    if filing is None:reasons.append('missing_or_invalid_filing_date')
    elif filing>today:reasons.append('future_filing_date')
    if transaction is None:reasons.append('missing_or_invalid_transaction_date')
    elif transaction>today or (filing and transaction>filing):reasons.append('future_or_after_filing_transaction')
    issuer=cik(row.get('companyCik'));reporter=cik(row.get('reportingCik'))
    if issuer is None or reporter is None:reasons.append('missing_CIK_identity')
    if code and row.get('acquisitionOrDisposition')!=('A' if code=='buy' else 'D'):reasons.append('direction_conflict_or_missing')
    qty=number(row.get('securitiesTransacted'));price=number(row.get('price'))
    if qty is None or qty<=0:reasons.append('missing_or_nonpositive_transacted_quantity')
    price=price if price is not None and price>0 else None
    currency=row.get('currency')
    currency=currency if isinstance(currency,str) and re.fullmatch('[A-Z]{3}',currency) else None
    return {'side':code,'filing':filing,'transaction':transaction,'issuer_cik':issuer,'reporting_cik':reporter,
        'quantity':qty,'price':price,'currency':currency,'reported_amount':qty*price if qty is not None and price is not None else None,
        'security_label':str(row.get('securityName') or 'Unspecified')[:180],
        'symbol':row.get('symbol') if isinstance(row.get('symbol'),str) and re.fullmatch(r'[A-Za-z0-9.^_-]{1,25}',row['symbol']) else None,
        'filing_url':filing_url(row.get('url')),'reasons':reasons}

def window(rows,today,days,basis):
    cutoff=today-timedelta(days=days-1)
    selected=[r for r in rows if cutoff<=r[basis]<=today]
    counts=Counter(r['side'] for r in selected);buy=counts['buy'];sell=counts['sell']
    known=defaultdict(lambda:{'buy':Decimal(0),'sell':Decimal(0),'rows':0})
    for row in selected:
        if row['currency'] and row['reported_amount'] is not None:
            known[row['currency']][row['side']]+=row['reported_amount'];known[row['currency']]['rows']+=1
    all_usd=bool(selected) and all(r['currency']=='USD' and r['reported_amount'] is not None for r in selected)
    bd=known['USD']['buy'] if all_usd else None;sd=known['USD']['sell'] if all_usd else None
    identities=sorted(r['representation_sha256'] for r in selected)
    return {'days':days,'date_basis':basis+'_date','from_date':cutoff.isoformat(),'through_date':today.isoformat(),
        'population_complete':False,'representation_count':len(selected),'buy_count':buy,'sell_count':sell,
        'buy_sell_ratio_count':float(Decimal(buy)/sell) if sell else None,
        'ratio_count_unavailable_reason':'no_sale_rows_in_sample' if not sell else None,
        'buy_usd':float(bd) if bd is not None else None,'sell_usd':float(sd) if sd is not None else None,
        'buy_sell_ratio_dollar':float(bd/sd) if sd is not None and sd>0 else None,
        'dollar_amounts_complete':all_usd,'unpriced_rows':sum(r['price'] is None for r in selected),
        'unknown_currency_rows':sum(r['currency'] is None for r in selected),
        'amounts_by_explicit_currency':{c:{'buy_exact':str(v['buy']),'sell_exact':str(v['sell']),'valued_rows':v['rows']} for c,v in sorted(known.items())},
        'reporting_CIKs':len({r['reporting_cik'] for r in selected}),'issuer_CIKs':len({r['issuer_cik'] for r in selected}),
        'membership_sha256':sha(encoded(identities)),'plan_status':'unknown_not_excluded','cluster_buys':[]}

def compute(pages,generated,stop_reason):
    with localcontext() as ctx:
        ctx.prec=50;ctx.rounding=ROUND_HALF_EVEN
        return _compute(pages,generated,stop_reason)

def _compute(pages,generated,stop_reason):
    today=clock(generated).date();seen=set();valid=[];reject=Counter();codes=Counter();forms=Counter();securities=Counter()
    raw_count=duplicates=objects=0;filings=[];transactions=[];all_rows=[]
    for page in pages:
        rows=decode(page['raw']);raw_count+=len(rows)
        for index,row in enumerate(rows):
            if not isinstance(row,dict):reject['not_object']+=1;continue
            objects+=1;digest=row_digest(row)
            if digest in seen:duplicates+=1;continue
            seen.add(digest);codes[str(row.get('transactionType'))]+=1;forms[str(row.get('formType'))]+=1
            r=classify(row,today);r.update(representation_sha256=digest,page=page['page'],original_row=index)
            if r['filing'] and r['filing']<=today:filings.append(r['filing'])
            if r['transaction'] and r['transaction']<=today:transactions.append(r['transaction'])
            all_rows.append({'representation_sha256':digest,'page':page['page'],'original_row':index,'excluded_reasons':r['reasons']})
            if r['reasons']:
                reject.update(r['reasons']);continue
            valid.append(r);securities[r['security_label']]+=1
    if not pages:raise ValueError('No retained source page')
    windows={'last_'+str(d)+'d':window(valid,today,d,'transaction') for d in (7,30,90)}
    filed={'last_'+str(d)+'d':window(valid,today,d,'filing') for d in (7,30,90)}
    # Group only by official issuer/reporting CIK representations. No fallback
    # from a missing person to a job title, ticker string or anonymous buyer.
    grouped=defaultdict(list)
    for r in valid:
        if r['side']=='buy' and today-timedelta(days=29)<=r['transaction']<=today:grouped[r['issuer_cik']].append(r)
    clusters=[]
    for issuer,rows in grouped.items():
        buyers={r['reporting_cik'] for r in rows}
        if len(buyers)<2:continue
        symbols=sorted({r['symbol'] for r in rows if r['symbol']})
        clusters.append({'issuer_cik':issuer,'symbol':symbols[0] if len(symbols)==1 else None,'reported_symbols':symbols,
            'name':'Issuer CIK '+issuer,'n_buyers':len(buyers),'representation_count':len(rows),'total_usd':None,
            'filing_urls':sorted({r['filing_url'] for r in rows if r['filing_url']}),
            'first_transaction_date':min(r['transaction'] for r in rows).isoformat(),'last_transaction_date':max(r['transaction'] for r in rows).isoformat(),
            'membership_sha256':sha(encoded(sorted(r['representation_sha256'] for r in rows))),
            'interpretation':'P-coded vendor representations across reported securities; plans, amendments and SEC transaction-line identity unqualified.',**PERMISSIONS})
    clusters.sort(key=lambda r:(-r['n_buyers'],r['issuer_cik']));clusters=clusters[:15]
    windows['last_30d']['cluster_buys']=clusters
    latest=max(filings).isoformat() if filings else None
    status='partial' if latest and (today-date.fromisoformat(latest)).days<=7 and valid else 'unavailable'
    return {'contract':CONTRACT,'schema_version':'2.0','method':'bounded_vendor_insider_research','generated_at':generated,
        'as_of':latest,'elapsed_s':None,'n_transactions':None,'n_transactions_note':'A SEC transaction count is unavailable; use explicitly named vendor representation counts.',
        'data_coverage_days':(max(transactions)-min(transactions)).days if transactions else None,
        'coverage':{'requested_pages':len(pages),'rows_received':raw_count,'object_rows_received':objects,
            'distinct_representation_count':len(seen),'duplicate_representations':duplicates,'eligible_representations':len(valid),
            'excluded_representations':len(seen)-len(valid),'excluded_reason_counts':dict(sorted(reject.items())),
            'first_filing_date':min(filings).isoformat() if filings else None,'latest_filing_date':latest,
            'first_transaction_date':min(transactions).isoformat() if transactions else None,'latest_transaction_date':max(transactions).isoformat() if transactions else None,
            'stop_reason':stop_reason,'population_complete':False,'page_snapshot_consistent':False,
            'transaction_codes':dict(sorted(codes.items())),'form_types':dict(sorted(forms.items())),
            'note':'Date extrema are sample ranges, never proof of complete window coverage. Mutable vendor pages are not an atomic SEC census.'},
        'quality':{'status':status,'source_level':'vendor_reported','SEC_filings_independently_verified':False,
            'amendments_reconciled':False,'economic_transaction_deduplication_verified':False,'currency_verified':False,
            'plan_classification_available':False,'independent_investment_votes':0},
        'freshness':{'pipeline_check_due_at':due_at(generated),'sample_valid_until':min(clock(due_at(generated)),datetime.combine(date.fromisoformat(latest)+timedelta(days=8),time(),timezone.utc)).isoformat() if latest else generated,'basis':'Existing weekday 22:30 UTC schedule plus two hours; collection clock does not establish transaction freshness.','latest_filing_date':latest},
        'windows':windows,'filing_windows':filed,'security_labels':dict(sorted(securities.items())),
        'notable_cluster_buys':clusters,'headline_ratio_30d_dollar':windows['last_30d']['buy_sell_ratio_dollar'],
        'regime':None,'regime_read':'Descriptive filing sample only. No market-timing, net buying or insider-conviction inference.',
        'call':None,'portfolio_action':'WAIT',**PERMISSIONS,
        'methodology':'Exact P/P-Purchase and S/S-Sale codes; dated Form 4 vendor rows; matching A/D direction; positive securitiesTransacted only; issuer and reporting CIK identities. No securitiesOwned substitution or absolute-value repair. Separate inclusive transaction-date and filing-date windows. Planned trades cannot be identified or excluded from this endpoint.',
        'lineage':{'sample_membership_sha256':sha(encoded(all_rows)),'root_provider':'Financial Modeling Prep','root_endpoint':ENDPOINT,
            'sources':[{'page':p['page'],'acquired_at':p['acquired_at'],'evidence':p['evidence']} for p in pages],
            'membership_recipe':'Canonical whole-row representation SHA-256, first encountered page and zero-based row index, exclusion reasons; replay from protected originals.',
            'reference_definitions':['https://www.sec.gov/edgar/searchedgar/ownershipformcodes.html','https://www.sec.gov/files/form4.pdf']},
        'limitations':['No complete market population or SEC amendment reconciliation.','No verified currency or 10b5-1 plan flags.','P/S include private and derivative transactions.','No point-in-time backtest or independent investment vote.','A vendor row representation may duplicate an economic transaction or joint-owner filing.'],
        'history_reference':{'legacy_key':'data/insider-aggregate-history.json','used_in_calculations':False,'qualification':'legacy_unverified_snapshots_retained_unchanged'},
        'portfolio_consequence':{'automatic_position_change':False,'interpretation':'Insider sample counts do not identify your exposure, expected return or position size. Any portfolio scenario needs explicit holdings and a separately entered price shock.'}}
