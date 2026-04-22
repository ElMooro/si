import json,urllib.request,urllib.parse,traceback
from datetime import datetime,timedelta
FISCAL_BASE="https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
def lambda_handler(event,context):
    params=event.get('queryStringParameters') or {}
    action=params.get('action','recent')
    if action=='health': return resp(200,{'status':'ok'})
    try:
        if action=='recent': return get_recent(params)
        elif action=='search': return search_auctions(params)
        elif action=='upcoming': return get_upcoming(params)
        else: return resp(400,{'error':'Unknown action'})
    except Exception as e: return resp(500,{'error':str(e),'trace':traceback.format_exc()})
def get_recent(params):
    days=int(params.get('days','90'));ps=int(params.get('limit','200'));st=params.get('type','')
    sd=(datetime.now()-timedelta(days=days)).strftime('%Y-%m-%d')
    filt='auction_date:gte:'+sd
    if st: filt+=',security_type:eq:'+st
    url=FISCAL_BASE+'/v1/accounting/od/auctions_query?filter='+filt+'&sort=-auction_date&page%5Bnumber%5D=1&page%5Bsize%5D='+str(ps)+'&format=json'
    data=fetch_url(url)
    if not data: return resp(500,{'error':'Fetch failed','url':url})
    result=json.loads(data)
    raw=result.get('data',[])
    completed=[a for a in raw if a.get('bid_to_cover_ratio') not in (None,'null','')]
    announced=[a for a in raw if a.get('bid_to_cover_ratio') in (None,'null','')]
    auctions=[enrich(a) for a in completed]
    return resp(200,{'count':len(auctions),'auctions':auctions,'upcoming_count':len(announced),'upcoming':[{'cusip':a.get('cusip'),'security_type':a.get('security_type'),'security_term':a.get('security_term'),'auction_date':a.get('auction_date'),'announcemt_date':a.get('announcemt_date'),'offering_amt':a.get('offering_amt'),'est_pub_held_mat_by_type_amt':a.get('est_pub_held_mat_by_type_amt')} for a in announced]})
def search_auctions(params):
    st=params.get('type','Note');days=int(params.get('days','365'))
    sd=(datetime.now()-timedelta(days=days)).strftime('%Y-%m-%d')
    filt='auction_date:gte:'+sd
    if st: filt+=',security_type:eq:'+st
    url=FISCAL_BASE+'/v1/accounting/od/auctions_query?filter='+filt+'&sort=-auction_date&page%5Bnumber%5D=1&page%5Bsize%5D='+str(params.get('limit','50'))+'&format=json'
    data=fetch_url(url)
    if not data: return resp(500,{'error':'Failed'})
    result=json.loads(data)
    raw=result.get('data',[])
    completed=[a for a in raw if a.get('bid_to_cover_ratio') not in (None,'null','')]
    return resp(200,{'count':len(completed),'auctions':[enrich(a) for a in completed]})
def get_upcoming(params):
    today=datetime.now().strftime('%Y-%m-%d')
    url=FISCAL_BASE+'/v1/accounting/od/auctions_query?filter=auction_date:gte:'+today+'&sort=auction_date&page%5Bsize%5D=30&format=json'
    data=fetch_url(url)
    if not data: return resp(500,{'error':'Failed'})
    result=json.loads(data)
    raw=result.get('data',[])
    upcoming=[a for a in raw if a.get('bid_to_cover_ratio') in (None,'null','')]
    return resp(200,{'count':len(upcoming),'upcoming':upcoming})
def enrich(a):
    try:
        btc=sf(a.get('bid_to_cover_ratio'))
        ta=sf(a.get('total_accepted'))
        hy=sf(a.get('high_yield'))
        ay=sf(a.get('avg_med_yield'))
        hr=sf(a.get('high_discnt_rate'))
        ar=sf(a.get('avg_med_discnt_rate'))
        da=sf(a.get('direct_bidder_accepted'))
        ia=sf(a.get('indirect_bidder_accepted'))
        pa=sf(a.get('primary_dealer_accepted'))
        off=sf(a.get('offering_amt'))
        dp=(da/ta*100) if da and ta else None
        ip=(ia/ta*100) if ia and ta else None
        pp=(pa/ta*100) if pa and ta else None
        tail=None
        if hy is not None and ay is not None: tail=round((hy-ay)*100,1)
        elif hr is not None and ar is not None: tail=round((hr-ar)*100,1)
        grade,detail=grade_auction(btc,tail,ip,pp,a.get('security_type'))
        a['_btc']=btc
        a['_tail_bps']=tail
        a['_direct_pct']=round(dp,1) if dp else None
        a['_indirect_pct']=round(ip,1) if ip else None
        a['_dealer_pct']=round(pp,1) if pp else None
        a['_grade']=grade
        a['_grade_detail']=detail
        a['_offering_b']=round(off/1e9,2) if off else None
    except:
        a['_grade']='N/A'
        a['_grade_detail']='Insufficient data'
    return a
def grade_auction(btc,tail,ind,dlr,stype):
    score=50;d=[]
    if btc is not None:
        if stype=='Bill':
            if btc>=3.0: score+=15;d.append('Strong BTC')
            elif btc>=2.7: score+=8;d.append('Solid BTC')
            elif btc>=2.4: d.append('Avg BTC')
            elif btc>=2.0: score-=10;d.append('Weak BTC')
            else: score-=20;d.append('Very weak BTC')
        else:
            if btc>=2.8: score+=20;d.append('Exceptional demand')
            elif btc>=2.5: score+=12;d.append('Strong demand')
            elif btc>=2.3: score+=5;d.append('Solid demand')
            elif btc>=2.1: score-=5;d.append('Below-avg demand')
            else: score-=15;d.append('Weak demand')
    if tail is not None:
        if tail<=-1: score+=15;d.append('Stopped through')
        elif tail<=0: score+=8;d.append('On the screws')
        elif tail<=1: d.append('Tiny tail')
        elif tail<=3: score-=10;d.append('Notable tail')
        else: score-=20;d.append('Large tail')
    if ind is not None:
        if ind>=75: score+=12;d.append('Huge foreign demand')
        elif ind>=65: score+=6;d.append('Strong foreign bid')
        elif ind>=55: d.append('Avg foreign bid')
        else: score-=8;d.append('Weak foreign demand')
    if dlr is not None:
        if dlr<=10: score+=8;d.append('Low dealer takedown')
        elif dlr<=15: score+=3;d.append('Normal dealer')
        elif dlr<=25: score-=5;d.append('Elevated dealer')
        else: score-=12;d.append('High dealer forced buy')
    if score>=80: g='A+'
    elif score>=70: g='A'
    elif score>=60: g='A-'
    elif score>=55: g='B+'
    elif score>=50: g='B'
    elif score>=45: g='B-'
    elif score>=40: g='C+'
    elif score>=35: g='C'
    elif score>=30: g='C-'
    elif score>=25: g='D'
    else: g='F'
    return g,'; '.join(d)
def sf(val):
    if val is None or val=='' or val=='null': return None
    try: return float(val)
    except: return None
def fetch_url(url):
    try:
        req=urllib.request.Request(url,headers={'User-Agent':'Mozilla/5.0','Accept':'application/json'})
        with urllib.request.urlopen(req,timeout=25) as r: return r.read().decode('utf-8')
    except Exception as e: print(f"Fetch error: {e} URL: {url}");return None
def resp(code,body):
    return {'statusCode':code,'body':json.dumps(body,default=str)}
