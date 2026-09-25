"""Exact FINRA measurement parsing. No ownership, direction or sizing inference.

CNMS ShortVolume already includes ShortExemptVolume. Decimal share quantities
must survive parsing. Weekly ATS and non-ATS legs join only on symbol/week/tier.
Pagination completeness describes returned records, not an atomic API snapshot.
"""
from datetime import date
from decimal import Decimal,localcontext,ROUND_HALF_EVEN
import json,re

CNMS_FIELDS=('Date','Symbol','ShortVolume','ShortExemptVolume','TotalVolume','Market')
WEEKLY_CODES={'ATS_W_SMBL':'ats','OTC_W_SMBL':'non_ats'}
TIERS=('T1','T2','OTCE')

def strict(raw):
    def pairs(items):
        out={}
        for key,value in items:
            if key in out:raise ValueError('Duplicate JSON field')
            out[key]=value
        return out
    def invalid(_):raise ValueError('Nonfinite JSON')
    return json.loads(raw,parse_float=Decimal,parse_int=Decimal,parse_constant=invalid,object_pairs_hook=pairs)

def scalar(value,integer=False):
    if isinstance(value,bool) or not isinstance(value,(str,int,Decimal)):raise ValueError('Exact numeric source required')
    text=str(value)
    if not re.fullmatch(r'\d{1,30}(?:\.\d{1,12})?',text):raise ValueError('Nonnegative bounded decimal required')
    number=Decimal(text)
    if integer and number!=number.to_integral_value():raise ValueError('Whole trade count required')
    return number

def number(value):return format(value,'f')
def ratio(numerator,denominator,multiplier=1):
    if denominator==0:return None
    with localcontext() as ctx:
        ctx.prec=64
        return number((numerator/denominator*multiplier).quantize(Decimal('0.000000000001'),rounding=ROUND_HALF_EVEN))

def symbol(value):
    if not isinstance(value,str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9.\-/$^_+]{0,31}',value):raise ValueError('Exact source symbol required')
    return value

def day(value):
    if not isinstance(value,str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}',value):raise ValueError('ISO observation date required')
    date.fromisoformat(value);return value

def cnms(raw,observation_date):
    stamp=day(observation_date).replace('-','');lines=raw.decode('utf-8-sig').splitlines()
    if len(lines)<2 or tuple(lines[0].split('|'))!=CNMS_FIELDS:raise ValueError('Complete CNMS header required')
    if not re.fullmatch(r'\d+',lines[-1]) or int(lines[-1])!=len(lines)-2:raise ValueError('CNMS trailer count differs')
    rows=[];seen=set()
    for index,line in enumerate(lines[1:-1],start=2):
        fields=line.split('|')
        if len(fields)!=6 or fields[0]!=stamp:raise ValueError('CNMS row date or width differs')
        name=symbol(fields[1]);short,exempt,total=(scalar(v) for v in fields[2:5])
        if not 0<=exempt<=short<=total:raise ValueError('CNMS inclusive short-volume reconciliation failed')
        markets=fields[5].split(',')
        if not markets or len(set(markets))!=len(markets) or not set(markets)<=set('BQND'):raise ValueError('CNMS facility scope differs')
        if name in seen:raise ValueError('Duplicate CNMS symbol/date')
        seen.add(name)
        rows.append({'symbol':name,'observation_date':observation_date,'source_line':index,'source_fields':dict(zip(CNMS_FIELDS,fields)),
            'short_volume_shares':number(short),'short_exempt_volume_shares':number(exempt),'total_volume_shares':number(total),
            'short_volume_pct':ratio(short,total,100),'short_exempt_pct_of_short':ratio(exempt,short,100),
            'short_includes_exempt':True,'scope':'FINRA disseminated NMS regular-session TRF/ADF volume',
            'missing_reason':'zero_reported_volume' if total==0 else None})
    return rows

def weekly(raw,code,week,tier):
    if code not in WEEKLY_CODES or tier not in TIERS or date.fromisoformat(day(week)).weekday()!=0:raise ValueError('Reviewed weekly partition required')
    doc=strict(raw)
    if not isinstance(doc,list):raise ValueError('Weekly rows required')
    rows=[];seen=set()
    for index,row in enumerate(doc):
        if not isinstance(row,dict):raise ValueError('Weekly object row required')
        if (row.get('summaryTypeCode'),row.get('weekStartDate'),row.get('summaryStartDate'),row.get('tierIdentifier'))!=(code,week,week,tier):raise ValueError('Weekly partition or grain differs')
        if row.get('MPID') is not None or row.get('firmCRDNumber') is not None:raise ValueError('Symbol summary cannot include a firm dimension')
        name=symbol(row.get('issueSymbolIdentifier'))
        if name in seen:raise ValueError('Duplicate weekly symbol/partition')
        seen.add(name);shares=scalar(row.get('totalWeeklyShareQuantity'));trades=scalar(row.get('totalWeeklyTradeCount'),integer=True)
        if trades==0 and shares!=0:raise ValueError('Shares without reported trades')
        clocks={key:day(row.get(key)) for key in ('initialPublishedDate','lastUpdateDate','lastReportedDate')}
        if clocks['initialPublishedDate']<week or clocks['lastUpdateDate']<clocks['initialPublishedDate']:raise ValueError('Weekly publication chronology differs')
        source_fields={key:number(value) if isinstance(value,Decimal) else value for key,value in row.items()}
        rows.append({'symbol':name,'week_start':week,'tier':tier,'leg':WEEKLY_CODES[code],'source_row':index,
            'shares':number(shares),'trades':number(trades),'average_shares_per_reported_trade':ratio(shares,trades),
            **clocks,'source_fields':source_fields})
    return rows

def page(raw,headers,request_offset,request_limit):
    """Prove a response's count boundary; never stop merely because rows < limit."""
    doc=strict(raw)
    if not isinstance(doc,list):raise ValueError('Array page required')
    values={}
    for field in ('record-total','record-offset','record-limit'):
        value=headers.get(field)
        if not isinstance(value,str) or not re.fullmatch(r'\d+',value):raise ValueError('Explicit pagination headers required')
        values[field]=int(value)
    total=values['record-total'];offset=values['record-offset'];limit=values['record-limit'];count=len(doc)
    # FINRA documents this optional header but the live weekly/monthly API omits it.
    declared=headers.get('total-records-on-page')
    if declared is not None and (not isinstance(declared,str) or not re.fullmatch(r'\d+',declared) or int(declared)!=count):raise ValueError('Declared page count differs')
    if type(request_offset) is not int or type(request_limit) is not int or request_offset<0 or request_limit<=0:raise ValueError('Bounded pagination request required')
    if offset!=request_offset or not 0<limit<=request_limit or count!=len(doc) or count>limit or offset+count>total:raise ValueError('Pagination count reconciliation failed')
    if count==0 and offset<total:raise ValueError('Empty page before reported end')
    return {'rows':len(doc),'reported_total':total,'offset':offset,'next_offset':offset+count,
        'reported_end_reached':offset+count==total,'row_count_basis':'parsed_complete_response',
        'page_count_header_present':declared is not None,'snapshot_atomic':False}

def join_weekly(legs):
    """Null missing legs; preserve both exact source references and clocks."""
    groups={}
    for row in legs:
        key=(row['symbol'],row['week_start'],row['tier']);group=groups.setdefault(key,{})
        if row['leg'] not in ('ats','non_ats') or row['leg'] in group:raise ValueError('Duplicate or invalid weekly leg')
        group[row['leg']]=row
    result=[]
    with localcontext() as ctx:
        ctx.prec=64
        for (name,week,tier),group in sorted(groups.items()):
            a=group.get('ats');b=group.get('non_ats');total=scalar(a['shares'])+scalar(b['shares']) if a and b else None
            result.append({'symbol':name,'week_start':week,'tier':tier,'ats':a,'non_ats':b,
                'reported_offexchange_shares':number(total) if total is not None else None,
                'ats_pct_of_reported_offexchange':ratio(scalar(a['shares']),total,100) if total is not None else None,
                'missing_reason':'missing_ats_leg' if not a else 'missing_non_ats_leg' if not b else 'zero_reported_volume' if total==0 else None,
                'scope':'FINRA weekly ATS plus non-ATS; excludes exchange executions',
                'market_share_pct':None,'owner_accumulation':None,'signal':None})
    return result

def monthly(raw,month,tier='NMS'):
    """Non-ATS reporting-firm activity, with CRD 0 preserved as an aggregate."""
    if date.fromisoformat(day(month)).day!=1 or tier not in ('NMS','OTCE'):raise ValueError('Reviewed monthly partition required')
    doc=strict(raw)
    if not isinstance(doc,list):raise ValueError('Monthly rows required')
    rows=[];seen=set()
    for index,row in enumerate(doc):
        if not isinstance(row,dict) or (row.get('summaryTypeCode'),row.get('monthStartDate'),row.get('summaryStartDate'),row.get('tierIdentifier'))!=('OTC_M_SMBL_FIRM',month,month,tier):raise ValueError('Monthly period or grain differs')
        name=symbol(row.get('issueSymbolIdentifier'));crd=number(scalar(row.get('firmCRDNumber'),integer=True));key=(name,crd)
        if key in seen:raise ValueError('Duplicate monthly symbol/firm')
        seen.add(key);shares=scalar(row.get('totalMonthlyShareQuantity'));trades=scalar(row.get('totalMonthlyTradeCount'),integer=True)
        if trades==0 and shares!=0:raise ValueError('Monthly shares without trades')
        firm=row.get('marketParticipantName')
        if not isinstance(firm,str) or not firm.strip():raise ValueError('Reporting-firm name required')
        if (crd=='0')!=('de minimis' in firm.lower()):raise ValueError('Undisclosed reporting-firm bucket identity differs')
        clocks={key:day(row.get(key)) for key in ('initialPublishedDate','lastUpdateDate','lastReportedDate')}
        if clocks['initialPublishedDate']<month or clocks['lastUpdateDate']<clocks['initialPublishedDate']:raise ValueError('Monthly publication chronology differs')
        rows.append({'symbol':name,'month_start':month,'tier':tier,'firm_crd':crd,'reporting_firm':firm,
            'firm_identity_kind':'aggregated_de_minimis_firms' if crd=='0' else 'named_reporting_firm',
            'shares':number(shares),'trades':number(trades),'average_shares_per_reported_trade':ratio(shares,trades),
            'source_row':index,**clocks,'source_fields':{key:number(value) if isinstance(value,Decimal) else value for key,value in row.items()}})
    return rows

def concentration(rows,*,records_reconciled):
    """HHI bounds over reporting activity, not investors or beneficial owners.

The undisclosed bucket contributes between zero and its squared share. Treating
it as one firm produces only an upper bound; it is never silently ranked as one.
"""
    if records_reconciled is not True:raise ValueError('Complete declared monthly partition required')
    groups={}
    for row in rows:
        key=(row['symbol'],row['month_start'],row['tier']);group=groups.setdefault(key,{})
        if row['firm_crd'] in group:raise ValueError('Duplicate monthly firm across pages')
        group[row['firm_crd']]=row
    result=[]
    with localcontext() as ctx:
        ctx.prec=64
        for (name,month,tier),firms in sorted(groups.items()):
            total=sum((scalar(row['shares']) for row in firms.values()),Decimal(0))
            named=[row for crd,row in firms.items() if crd!='0'];named.sort(key=lambda row:(-scalar(row['shares']),row['firm_crd']))
            unknown=scalar(firms['0']['shares']) if '0' in firms else Decimal(0)
            numerator=sum((scalar(row['shares'])**2 for row in named),Decimal(0));denominator=total**2
            result.append({'symbol':name,'month_start':month,'tier':tier,'reported_non_ats_shares':number(total),
                'named_reporting_firms':len(named),'de_minimis_bucket_present':'0' in firms,
                'de_minimis_shares':number(unknown),'de_minimis_pct':ratio(unknown,total,100),
                'reported_activity_hhi_lower_bound':ratio(numerator,denominator,10000),
                'reported_activity_hhi_upper_bound':ratio(numerator+unknown**2,denominator,10000),
                'top_named_firms':[{'firm_crd':row['firm_crd'],'name':row['reporting_firm'],'reported_share_pct':ratio(scalar(row['shares']),total,100),'source':row} for row in named[:5]],
                'source_rows':list(firms.values()),'scope':'Non-ATS reporting-firm activity within one complete published symbol/month/tier',
                'beneficial_owner_concentration':None,'ownership_flow':None,'signal':None,
                'missing_reason':'zero_reported_volume' if total==0 else None})
    return result
