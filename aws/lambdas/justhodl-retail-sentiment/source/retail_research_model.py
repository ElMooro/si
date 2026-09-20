"""Bounded public attention samples; no flows, population or return inference."""
from collections import Counter
from datetime import datetime,timedelta,timezone
from decimal import Decimal,localcontext,ROUND_HALF_EVEN
import hashlib,json,re
CONTRACT='retail-native-research.v1'
PREFIX='data/retail-research/'
PRIVATE='audit-private/20260909-originals/retail-research/'
CURRENT='data/retail-sentiment.json'
PERMISSIONS={k:False for k in ('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible')}
CATEGORIES={'all-stocks':2,'wallstreetbets':1,'stocks':1,'investing':1}
CONTEXT_KEYS=(CURRENT,'data/retail-sentiment-history.json','data/retail-attention-history.json','data/news-velocity.json',
 'data/ticker-trends.json','data/options-flow-scanner.json','data/finviz-news.json','data/short-interest.json',
 'data/13f-positions.json','data/estimate-revisions-latest.json','data/rotation-chains.json','data/finviz-short.json')
MAX_BYTES=8*1024*1024

def encoded(doc):return json.dumps(doc,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def sha(raw):return hashlib.sha256(raw).hexdigest()
def clock(value):
    d=datetime.fromisoformat(value.replace('Z','+00:00'))
    if d.tzinfo is None:raise ValueError('Aware acquisition clock required')
    return d.astimezone(timezone.utc)
def count(value):
    if type(value) is int and 0<=value<=9007199254740991:return value
    if isinstance(value,str) and re.fullmatch(r'0|[1-9][0-9]{0,15}',value) and int(value)<=9007199254740991:return int(value)
    return None
def symbol(value):return value if isinstance(value,str) and re.fullmatch('[A-Z][A-Z0-9.-]{0,12}',value) else None
def decode(raw):
    p=json.loads(raw,parse_constant=lambda x:(_ for _ in ()).throw(ValueError('Nonfinite provider response')))
    if not isinstance(p,dict):raise ValueError('Provider object required')
    return p
def source_url(kind,identity,page=1):
    if kind=='apewisdom' and identity in CATEGORIES and type(page) is int and 1<=page<=CATEGORIES[identity]:
        return 'https://apewisdom.io/api/v1.0/filter/'+identity+'/page/'+str(page)
    if kind=='stocktwits' and identity=='trending':return 'https://api.stocktwits.com/api/2/trending/symbols.json'
    if kind=='stocktwits' and symbol(identity):return 'https://api.stocktwits.com/api/2/streams/symbol/'+identity+'.json'
    raise ValueError('Reviewed provider identity required')
def due_at(stamp):
    at=clock(stamp);due=at.replace(hour=19,minute=10,second=0,microsecond=0)
    if due<=at:due+=timedelta(days=1)
    return (due+timedelta(hours=1)).isoformat()
def ratio(a,b):
    if a is None or b is None or b<=0:return None
    with localcontext() as ctx:
        ctx.prec=40;ctx.rounding=ROUND_HALF_EVEN
        return float(Decimal(a)/Decimal(b)*100)
def attention_row(raw,page,index):
    if not isinstance(raw,dict) or not symbol(raw.get('ticker')):return None,'invalid_symbol'
    mentions=count(raw.get('mentions'));rank=count(raw.get('rank'))
    if mentions is None or not rank:return None,'invalid_mentions_or_rank'
    prev=count(raw.get('mentions_24h_ago'));prior_rank=count(raw.get('rank_24h_ago'))
    status='missing_or_invalid' if prev is None else 'zero_baseline' if prev==0 else 'positive_baseline'
    return {'symbol':raw['ticker'],'provider_rank':rank,'mentions':mentions,'mentions_24h_ago':prev,
        'mention_change':mentions-prev if prev is not None else None,'mention_change_pct':ratio(mentions-prev,prev) if prev is not None else None,
        'baseline_status':status,'upvotes':count(raw.get('upvotes')),'rank_24h_ago':prior_rank if prior_rank else None,
        'rank_change':prior_rank-rank if prior_rank else None,'source_page':page,'source_row':index,
        'identity_scope':'Provider symbol only; issuer/share-class identity not reconciled.',**PERMISSIONS},None

def ape_category(category,pages):
    rows=[];reasons=Counter();seen={};conflicts=set();counts=[];totals=[];sources=[];received=0
    for item in pages:
        if item['kind']!='apewisdom' or item['identity']!=category:continue
        sources.append({k:v for k,v in item.items() if k!='raw'})
        if item.get('status')!='received' or item.get('http_status')!=200:continue
        try:
            p=decode(item['raw']);batch=p.get('results')
            if not isinstance(batch,list) or len(batch)>100 or count(p.get('current_page'))!=item['page']:raise ValueError('Page identity differs')
            total=count(p.get('count'));npages=count(p.get('pages'))
            if total is None or npages is None or npages!=(total+99)//100:raise ValueError('Provider pagination inconsistent')
        except (ValueError,TypeError):reasons['page_schema_invalid']+=1;continue
        counts.append(total);totals.append(npages);received+=len(batch)
        for index,raw in enumerate(batch):
            row,reason=attention_row(raw,item['page'],index)
            if reason:reasons[reason]+=1;continue
            ticker=row['symbol'];economic={k:v for k,v in row.items() if k not in ('source_page','source_row')}
            if ticker in seen:
                if seen[ticker]!=economic:conflicts.add(ticker);reasons['conflicting_symbol_representation']+=1
                else:reasons['duplicate_symbol_representation']+=1
                continue
            seen[ticker]=economic;rows.append(row)
    rows=[r for r in rows if r['symbol'] not in conflicts];rows.sort(key=lambda r:(r['provider_rank'],r['symbol']))
    n=len(rows);mentions=sum(r['mentions'] for r in rows) if n else None
    paired=[r for r in rows if r['mentions_24h_ago'] is not None];current=sum(r['mentions'] for r in paired);prior=sum(r['mentions_24h_ago'] for r in paired)
    status_counts=dict(sorted(Counter(r['baseline_status'] for r in rows).items()))
    reported=counts[0] if counts and len(set(counts))==1 else None
    return {'category':category,'available':bool(rows),'rows':rows,'rows_received':received,'eligible_symbols':n,
        'excluded_or_duplicate_reasons':dict(sorted(reasons.items())),'conflicting_symbols_removed':len(conflicts),
        'reported_symbol_count':reported,'reported_pages':totals[0] if totals and len(set(totals))==1 else None,
        'pagination_changed_between_requests':len(set(counts))>1 or len(set(totals))>1,
        'captured_symbol_fraction':n/reported if reported and n<=reported else None,'sample_mentions':mentions,
        'paired_sample':{'symbols':len(paired),'current_mentions':current if paired else None,'prior_mentions':prior if paired else None,
            'change_pct':ratio(current-prior,prior) if paired else None,'definition':'Same current-page symbols with a supplied comparison; a current-selection sample, not whole-market growth.'},
        'top10_share_of_sample_mentions_pct':ratio(sum(r['mentions'] for r in rows[:10]),mentions),
        'baseline_status_counts':status_counts,'population_complete':False,'time_window':'Vendor trailing 24-hour mention count; exact cutoff and prior-window alignment are not supplied.',
        'source_observed_at':None,'sources':sources,'unit':'vendor_mention_count',
        'note':'Ranked pages are not an atomic census. all-stocks overlaps the individual communities; never sum or treat them as independent votes.'}

def stock_stream(item):
    base={k:v for k,v in item.items() if k!='raw'};base.update(available=False,population_complete=False,messages_received=None,eligible_messages=None,
        bullish=None,bearish=None,unclassified=None,bullish_share_of_classified_pct=None,bull_bear_ratio=None,
        oldest_message_at=None,newest_message_at=None,unique_sample_user_count=None)
    if item['status']!='received' or item.get('http_status')!=200:return base
    try:
        p=decode(item['raw']);rows=p['messages']
        header=p.get('symbol')
        if not isinstance(rows,list) or len(rows)>30 or not isinstance(header,dict) or header.get('symbol')!=item['identity']:raise ValueError('Stream identity differs')
    except (KeyError,TypeError,ValueError):base['status']='stream_schema_invalid';return base
    seen={};conflicts=set();reasons=Counter();accepted=[];acquired=clock(item['acquired_at'])
    for index,r in enumerate(rows):
        if not isinstance(r,dict) or not count(r.get('id')):reasons['invalid_message_identity']+=1;continue
        mid=count(r['id']);digest=sha(encoded(r))
        if mid in seen:
            reasons['duplicate_message' if seen[mid]==digest else 'conflicting_message']+=1
            if seen[mid]!=digest:conflicts.add(mid)
            continue
        seen[mid]=digest
        try:at=clock(r['created_at'])
        except (KeyError,TypeError,ValueError,AttributeError):reasons['missing_message_clock']+=1;continue
        if at>acquired:reasons['future_message_clock']+=1;continue
        entities=r.get('entities');sentiment=entities.get('sentiment') if isinstance(entities,dict) else None
        tag=sentiment.get('basic') if isinstance(sentiment,dict) else None
        tag=tag if tag in ('Bullish','Bearish') else 'Unclassified'
        user=r.get('user');uid=count(user.get('id')) if isinstance(user,dict) else None
        accepted.append((mid,at,tag,uid))
    accepted=[r for r in accepted if r[0] not in conflicts];tags=Counter(r[2] for r in accepted);bull=tags['Bullish'];bear=tags['Bearish'];dates=[r[1] for r in accepted]
    return {**base,'available':bool(accepted),'messages_received':len(rows),'eligible_messages':len(accepted),'bullish':bull,'bearish':bear,
        'unclassified':tags['Unclassified'],'classified_messages':bull+bear,'bullish_share_of_classified_pct':ratio(bull,bull+bear),
        'bull_bear_ratio':bull/bear if bear else None,'oldest_message_at':min(dates).isoformat() if dates else None,'newest_message_at':max(dates).isoformat() if dates else None,
        'sample_span_seconds':(max(dates)-min(dates)).total_seconds() if dates else None,'unique_sample_user_count':len({r[3] for r in accepted if r[3] is not None}),
        'messages_without_user_id':sum(r[3] is None for r in accepted),'excluded_reasons':dict(sorted(reasons.items())),
        'definition':'Self-tagged messages in at most 30 returned representations, not StockTwits official sentiment, all users, trades, or a fixed-duration census.',**PERMISSIONS}

def compute(pages,contexts,generated_at):
    at=clock(generated_at);categories={k:ape_category(k,pages) for k in CATEGORIES}
    if not any(p['available'] for p in categories.values()):raise ValueError('No usable community sample; preserve previous publication')
    streams=[stock_stream(p) for p in pages if p['kind']=='stocktwits' and p['identity']!='trending']
    trending=next((p for p in pages if p['kind']=='stocktwits' and p['identity']=='trending'),None);trends=[]
    if trending and trending['status']=='received' and trending.get('http_status')==200:
        try:
            rows=decode(trending['raw'])['symbols']
            if not isinstance(rows,list) or len(rows)>100:raise ValueError('Trending page bound differs')
            for index,r in enumerate(rows):
                if isinstance(r,dict) and symbol(r.get('symbol')):trends.append({'symbol':r['symbol'],'provider_order':index+1,'watchlist_count':count(r.get('watchlist_count'))})
        except (KeyError,ValueError,TypeError):pass
    times=[clock(p['acquired_at']) for p in pages if p['status']=='received'];first=min(times)
    due=due_at(generated_at);valid=min(first+timedelta(hours=2),clock(due)).isoformat()
    return {'contract':CONTRACT,'version':'2.0.0','engine':'justhodl-retail-sentiment','generated_at':generated_at,'as_of':None,
        'freshness':{'pipeline_check_due_at':due,'sample_valid_until':valid,'collection_started_at':first.isoformat(),
            'source_observation_time_available':False,'schedule':'Existing live daily rule at 19:10 UTC; provider sample use expires two hours after acquisition.',
            'basis':'Acquisition freshness is not source publication freshness; vendor window endpoints are not supplied.'},
        'quality':{'status':'partial','community_samples_available':sum(p['available'] for p in categories.values()),'expected_community_samples':4,
            'source_observation_time_available':False,'population_complete':False,'point_in_time_forecast_validated':False},
        'communities':categories,'stocktwits':{'streams':streams,'trending':trends,'trending_source':{k:v for k,v in (trending or {}).items() if k!='raw'},
            'stream_selection':'First 25 unique valid symbols in the retained all-stocks first page; time/access limits can reduce coverage.',
            'note':'Symbols may include funds and other instruments. Community overlap and message duplication across symbols are not independent evidence.'},
        'context_evidence':contexts,'lineage':{'sources':[{k:v for k,v in p.items() if k!='raw'} for p in pages],
            'protected_originals':True,'provider_roots':['ApeWisdom','StockTwits'],'independent_investment_votes':0},
        'call':None,'portfolio_action':'WAIT','decision':{'verb':'WAIT','meaning':'Abstain from a new recommendation, not an instruction to retain existing exposure.','eligible_votes':0},**PERMISSIONS,
        'market_regime':None,'market_regime_signal':None,'market_regime_data':{},'top_30_by_mentions':[],'ranked':{},'stocktwits_trending':[],
        'subreddit_breakdown':{},'theme_rollup':[],'recent_alerts':[],'track_record':None,'signals_logged':0,'regime_changed_from_prior':False,'signal_persistence':None,
        'n_all_stocks':None,'n_wsb':None,'n_stocks':None,'n_investing':None,'n_with_stwt_data':None,'n_with_price':None,
        'interpretation':'Attention is descriptive research. No retail flow, squeeze probability, price confirmation, market-timing regime or expected return is inferred.',
        'portfolio_consequences':{'formula':'entered_signed_USD_exposure * entered_price_shock_pct / 100','model_implied_shock':None,'target_allocation':None,
            'limits':'First-order scenario only; no inferred probability, FX, dividends, financing, fees, convexity or hedge response.'}}
