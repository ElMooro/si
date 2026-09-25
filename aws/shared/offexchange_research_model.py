"""Compile dated FINRA source partitions into bounded public evidence records.

This module grants no directional or portfolio authority. Whole original-source
digests, literal reported identities and acquisition clocks remain inspectable.
"""
from datetime import datetime,timezone
from collections import defaultdict
import hashlib,json,re
import offexchange_measurements as measures

CONTRACT='offexchange-original-research.v1';PREFIX='data/offexchange-research/'
PRIVATE='audit-private/20260909-originals/offexchange-research/';CURRENT='data/dark-pool.json'
FLAGS=('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified')
PERMISSIONS=dict.fromkeys(FLAGS,False)
MAX=8*1024*1024

def sha(raw):return hashlib.sha256(raw).hexdigest()
def encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def digest(value):return sha(encoded(value))
def strict(raw):
    def pairs(items):
        out={}
        for key,value in items:
            if key in out:raise ValueError('Duplicate JSON key')
            out[key]=value
        return out
    def invalid(_):raise ValueError('Nonfinite JSON')
    return json.loads(raw,object_pairs_hook=pairs,parse_constant=invalid)
def clock(value):
    if not isinstance(value,str):raise ValueError('Explicit source clock required')
    stamp=datetime.fromisoformat(value.replace('Z','+00:00'))
    if stamp.tzinfo is None:raise ValueError('Timezone required')
    return stamp.astimezone(timezone.utc)
def original(ref,read):
    if not isinstance(ref,dict) or set(ref)!={'key','sha256','bytes'} or not re.fullmatch('[a-f0-9]{64}',ref.get('sha256','')) or ref['key']!=PRIVATE+ref['sha256']+'.bin' or type(ref['bytes']) is not int or not 0<=ref['bytes']<=MAX:raise ValueError('Complete protected original identity required')
    raw=read(ref['key'])
    if not isinstance(raw,bytes) or len(raw)!=ref['bytes'] or sha(raw)!=ref['sha256']:raise ValueError('Original source identity differs')
    return raw
def capture(row,read,generated):
    received=clock(row.get('received_at'));requested=clock(row.get('requested_at'))
    if not requested<=received<=clock(generated):raise ValueError('Acquisition chronology differs')
    if row.get('http_status')!=200 or row.get('status')!='response_retained':raise ValueError('Qualified complete response required')
    raw=original(row['original'],read);length=(row.get('headers') or {}).get('content-length')
    if length is not None and (not isinstance(length,str) or not length.isdigit() or int(length)!=len(raw)):raise ValueError('HTTP source length differs')
    return raw
def record_identity(value):
    raw=encoded(value)
    if len(raw)>MAX:raise ValueError('Public record shard exceeds bound')
    return {'key':PREFIX+'records/'+sha(raw)+'.json','sha256':sha(raw),'bytes':len(raw)}
def bucket(symbol):return format(int(sha(symbol.encode())[:2],16)%128,'02x')

def partition(partition,read,generated):
    part=partition['partition'];dataset=part.get('dataset');code=part.get('code');period=measures.day(part.get('period'));tier=part.get('tier')
    weekly=dataset=='weeklySummary'
    if not (weekly and code in measures.WEEKLY_CODES and tier in measures.TIERS or dataset=='monthlySummary' and code=='OTC_M_SMBL_FIRM' and tier in ('NMS','OTCE')):raise ValueError('Reviewed FINRA partition required')
    pages=partition.get('pages')
    if not isinstance(pages,list) or not 2<=len(pages)<=41:raise ValueError('Complete pages plus first-page recheck required')
    rows=[];seen=set();offset=0;total=None;clocks=[];field='weekStartDate' if weekly else 'monthStartDate'
    for index,page in enumerate(pages[:-1]):
        if page.get('url')!='https://api.finra.org/data/group/otcMarket/name/'+dataset or page.get('kind')!='probe':raise ValueError('FINRA source endpoint differs')
        body=page.get('body');expected={'summaryTypeCode':code,field:period,'tierIdentifier':tier}
        if not isinstance(body,dict) or set(body)!={'limit','offset','compareFilters','sortFields'} or type(body['limit']) is not int or not 1<=body['limit']<=5000 or body['offset']!=offset:raise ValueError('Partition request differs')
        filters=body['compareFilters']
        if not isinstance(filters,list) or len(filters)!=3 or any(set(v)!={'fieldName','compareType','fieldValue'} or v['compareType']!='EQUAL' for v in filters) or {v['fieldName']:v['fieldValue'] for v in filters}!=expected:raise ValueError('Exact partition filters required')
        allowed=[['issueSymbolIdentifier'],['issueSymbolIdentifier','issueName']] if weekly else [['issueSymbolIdentifier','firmCRDNumber'],['issueSymbolIdentifier','issueName','firmCRDNumber']]
        if body['sortFields'] not in allowed:raise ValueError('Reviewed partition sorting required')
        raw=capture(page,read,generated);boundary=measures.page(raw,page['headers'],offset,body['limit'])
        if clocks and clock(page['requested_at'])<clock(clocks[-1]):raise ValueError('Partition acquisition order differs')
        if total is None:total=boundary['reported_total']
        if boundary['reported_total']!=total:raise ValueError('Source total changed across pages')
        compiled=measures.weekly(raw,code,period,tier) if weekly else measures.monthly(raw,period,tier)
        for row in compiled:
            key=(row['symbol'],row['reported_issue_name'],row.get('firm_crd'))
            if key in seen:raise ValueError('Duplicate reported issue grain across pages')
            seen.add(key)
            if any(measures.day(row[k])>clock(page['received_at']).date().isoformat() for k in ('initialPublishedDate','lastUpdateDate','lastReportedDate')):raise ValueError('Source date is after acquisition')
            row.update(source_original=page['original'],source_page=index,source_received_at=page['received_at']);rows.append(row)
        offset=boundary['next_offset'];clocks.append(page['received_at'])
    if offset!=total or len(rows)!=total or total!=partition.get('rows') or total!=partition.get('reported_total'):raise ValueError('Whole partition count does not reconcile')
    check=pages[-1];capture(check,read,generated)
    if clock(check['requested_at'])<clock(clocks[-1]):raise ValueError('First-page recheck predates completion')
    if check['body']!=pages[0]['body'] or check['url']!=pages[0]['url'] or check['original']!=pages[0]['original'] or check['headers'].get('record-total')!=str(total):raise ValueError('First-page recheck differs')
    clocks.append(check['received_at'])
    summary={'dataset':dataset,'category':code,'period_start':period,'tier':tier,'rows':len(rows),'records_reconciled':True,'snapshot_atomic':False,
        'pages':len(pages)-1,'first_page_recheck_matched':True,'first_received_at':min(clocks,key=clock),'last_received_at':max(clocks,key=clock),
        'initial_publication_dates':sorted({row['initialPublishedDate'] for row in rows}),'last_update_dates':sorted({row['lastUpdateDate'] for row in rows}),
        'originals':[page['original'] for page in pages]}
    return rows,summary

def compile_output(inputs,read):
    if not isinstance(inputs,dict) or inputs.get('contract')!='offexchange-original-inputs.v1':raise ValueError('Original-input contract required')
    generated=inputs['generated_at'];clock(generated);partitions=inputs.get('partitions')
    if not isinstance(partitions,dict) or not 1<=len(partitions)<=32:raise ValueError('Bounded explicit partitions required')
    weekly=[];monthly=[];coverage=[];seen=set()
    for name,part in sorted(partitions.items()):
        identity=(part['partition']['dataset'],part['partition']['code'],part['partition']['period'],part['partition']['tier'])
        if identity in seen:raise ValueError('Duplicate partition')
        seen.add(identity);rows,summary=partition(part,read,generated);coverage.append(summary)
        if identity[0]=='weeklySummary':weekly.extend(rows)
        else:monthly.extend(rows)
    daily=inputs['daily'];date=measures.day(inputs['daily_date'])
    if daily.get('url')!='https://cdn.finra.org/equity/regsho/daily/CNMSshvol'+date.replace('-','')+'.txt' or daily.get('body') is not None or daily.get('kind')!='daily_file':raise ValueError('Exact CNMS source required')
    raw=capture(daily,read,generated)
    if date>clock(daily['received_at']).date().isoformat():raise ValueError('Daily observation after acquisition')
    daily_rows=measures.cnms(raw,date)
    grouped=defaultdict(lambda:{'daily':[],'weekly':[],'monthly':[]});names=defaultdict(set)
    for row in daily_rows:
        row.update(source_original=daily['original'],source_received_at=daily['received_at']);grouped[row['symbol']]['daily'].append(row)
    joined=measures.join_weekly(weekly);concentrations=measures.concentration(monthly,records_reconciled=True)
    for kind,rows in (('weekly',joined),('monthly',concentrations)):
        for row in rows:grouped[row['symbol']][kind].append(row);names[row['symbol']].add(row['reported_issue_name'])
    shards={};symbols=[]
    for name,rows in sorted(grouped.items()):
        shard=shards.setdefault(bucket(name),{'contract':'offexchange-record-shard.v1','records':{}})
        shard['records'][name]={'symbol':name,'reported_issue_names':sorted(names[name]),'security_master_identity_verified':False,
            'cross_period_identity_verified':False,'multiple_reported_issue_names':len(names[name])>1,**rows,**PERMISSIONS}
        symbols.append({'symbol':name,'reported_issue_names':sorted(names[name]),'bucket':bucket(name)})
    refs={key:record_identity(value) for key,value in sorted(shards.items())}
    packet={'contract':CONTRACT,'engine':'justhodl-dark-pool','version':'3.0.0','generated_at':generated,
        'quality':{'status':'dated_observations','market_coverage_complete':False,'source_originals_reconstructed':True,'security_master_identity_verified':False},
        'coverage':coverage,'daily':{'date':date,'rows':len(daily_rows),'source_received_at':daily['received_at'],'original':daily['original'],
            'scope':'FINRA disseminated NMS regular-session TRF/ADF volume; not all-market short volume or short interest'},
        'record_shards':refs,'symbols':symbols,'counts':{'daily_rows':len(daily_rows),'weekly_source_rows':len(weekly),'weekly_reported_issue_periods':len(joined),
            'monthly_source_rows':len(monthly),'monthly_reported_issue_periods':len(concentrations),'source_symbols':len(symbols)},
        'scope':'Explicit dated FINRA partitions. Reported issue names are not a verified security master. ATS activity is not beneficial-owner accumulation.',
        'evidence_dependencies':{'family':'finra_reported_equity_activity','daily_weekly_monthly_independence_verified':False,
            'note':'These reporting views can overlap in underlying transactions and are not independent investment votes.'},
        'call':None,'signal':None,'score':None,'state':None,'tickets':[],**PERMISSIONS,
        'decision':{'verb':'WAIT','abstain':True,'eligible_votes':0,'reason':'Descriptive execution-reporting data has no qualified forecast or sizing authority.'},
        'board':[],'top_picks':[],'top_accumulation':[],'top_distribution':[],'dark_map':{},'xray_map':{},'distribution':{},
        'dix':{'own_dix_pct':None,'read':'Daily short-sale reporting is not a directional buying-pressure measure.'}}
    if len(encoded(packet))>MAX:raise ValueError('Public research index exceeds byte bound')
    return {'packet':packet,'shards':shards}
