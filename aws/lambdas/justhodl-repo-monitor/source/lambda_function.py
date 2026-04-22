import json,boto3,os,ssl,traceback
from datetime import datetime,timezone,timedelta
from urllib import request as urllib_request
s3=boto3.client('s3')
FRED_API_KEY=os.environ.get('FRED_API_KEY','2f057499936072679d8843d7fce99989')
S3_BUCKET=os.environ.get('S3_BUCKET','justhodl-dashboard-live')
ctx=ssl.create_default_context();ctx.check_hostname=False;ctx.verify_mode=ssl.CERT_NONE
def http_get(url,timeout=15):
    try:
        req=urllib_request.Request(url,headers={'User-Agent':'JustHodl/1.0','Accept':'application/json'})
        with urllib_request.urlopen(req,timeout=timeout,context=ctx) as r:return json.loads(r.read().decode('utf-8'))
    except Exception as e:print(f"HTTP_ERR[{url[:60]}]:{e}");return None
def get_fred(sid,n=30):
    d=http_get(f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}&api_key={FRED_API_KEY}&file_type=json&limit={n}&sort_order=desc")
    if not d or 'observations' not in d:return[]
    r=[]
    for o in d['observations']:
        try:r.append({'date':o['date'],'value':float(o['value'])})
        except:continue
    return r
def fl(sid):
    o=get_fred(sid,1);return o[0]['value'] if o else None
def fh(sid,n=90):return list(reversed(get_fred(sid,n)))
def zs(cur,hist):
    if not hist or len(hist)<5:return 0
    vals=[h['value'] for h in hist if h.get('value') is not None]
    if len(vals)<5:return 0
    m=sum(vals)/len(vals);s=(sum((v-m)**2 for v in vals)/len(vals))**.5
    return(cur-m)/s if s>0 else 0
def collect_repo_rates():
    M={}
    sofr_h=fh('SOFR',90);sofr=fl('SOFR')
    if sofr is not None:M['SOFR']={'value':sofr,'unit':'%','label':'Secured Overnight Financing Rate','history':sofr_h,'z_score':zs(sofr,sofr_h),'description':'Benchmark overnight Treasury repo rate','fred_id':'SOFR'}
    effr_h=fh('EFFR',90);effr=fl('EFFR')
    if effr is not None:M['EFFR']={'value':effr,'unit':'%','label':'Effective Federal Funds Rate','history':effr_h,'z_score':zs(effr,effr_h),'description':'Rate banks charge each other overnight','fred_id':'EFFR'}
    if sofr is not None and effr is not None:
        sp=round(sofr-effr,4);st='NORMAL' if abs(sp)<0.05 else 'WATCH' if abs(sp)<0.10 else 'STRESS'
        M['SOFR_EFFR_Spread']={'value':sp,'unit':'bps_x100','label':'SOFR - Fed Funds Spread','status':st,'description':'Repo tightness. Widening = collateral stress','threshold':{'normal':0.05,'watch':0.10,'stress':0.20}}
    obfr=fl('OBFR');obfr_h=fh('OBFR',90)
    if obfr is not None:M['OBFR']={'value':obfr,'unit':'%','label':'Overnight Bank Funding Rate','history':obfr_h,'z_score':zs(obfr,obfr_h),'description':'Fed funds + eurodollar','fred_id':'OBFR'}
    sv=fl('SOFRVOLUME');sv_h=fh('SOFRVOLUME',90)
    if sv is not None:M['SOFR_Volume']={'value':round(sv/1e9,1),'unit':'$B','label':'SOFR Transaction Volume','history':[{'date':h['date'],'value':round(h['value']/1e9,1)} for h in sv_h],'description':'Daily repo volume','fred_id':'SOFRVOLUME'}
    s75=fl('SOFR75');s25=fl('SOFR25')
    if s75 is not None and s25 is not None:
        disp=round(s75-s25,4);M['SOFR_Dispersion']={'value':disp,'unit':'bps_x100','label':'SOFR Dispersion (75th-25th)','status':'NORMAL' if disp<0.03 else 'WATCH' if disp<0.08 else 'STRESS','description':'Wide = fragmented repo market'}
    amb=fl('AMERIBOR')
    if amb is not None and sofr is not None:
        asp=round(amb-sofr,4);M['AMERIBOR_Spread']={'value':asp,'unit':'bps_x100','label':'AMERIBOR-SOFR Spread','status':'NORMAL' if asp<0.15 else 'WATCH' if asp<0.30 else 'STRESS','description':'Small bank funding stress'}
    return M
def collect_reverse_repo():
    M={}
    try:
        sd=(datetime.now(timezone.utc)-timedelta(days=5)).strftime('%Y-%m-%d');ed=datetime.now(timezone.utc).strftime('%Y-%m-%d')
        rrp=http_get(f"https://markets.newyorkfed.org/api/rp/reverserepo/propositions/search.json?startDate={sd}&endDate={ed}")
        if rrp and 'repo' in rrp and 'operations' in rrp['repo']:
            ops=rrp['repo']['operations']
            if ops:
                lt=ops[0];amt=float(lt.get('totalAmtAccepted',0))/1e9;rate=float(lt.get('operationCloseRate',lt.get('rate',0)));cp=int(lt.get('totalCounterpartiesAccepted',lt.get('numCounterparties',0)));od=lt.get('operationDate',lt.get('deliveryDate','N/A'))
                hist=[{'date':o.get('operationDate',''),'value':float(o.get('totalAmtAccepted',0))/1e9} for o in reversed(ops)]
                st='EXCESS' if amt>500 else 'NORMAL' if amt>100 else 'LOW' if amt>50 else 'CRITICAL'
                M['RRP_Volume']={'value':round(amt,1),'unit':'$B','label':'ON RRP Facility Usage','date':od,'status':st,'history':hist,'description':'Declining = reserves draining','threshold':{'excess':500,'normal':100,'low':50,'critical':20}}
                M['RRP_Rate']={'value':rate,'unit':'%','label':'ON RRP Rate','description':'Floor rate for money markets'}
                M['RRP_Counterparties']={'value':cp,'unit':'count','label':'RRP Counterparties','status':'DIVERSE' if cp>50 else 'CONCENTRATED','description':'Declining = fewer need parking'}
    except Exception as e:print(f"RRP_ERR:{e}")
    rf=fl('RRPONTSYD');rfh=fh('RRPONTSYD',90)
    if rf is not None:M['RRP_FRED']={'value':round(rf/1e3,1) if rf>1000 else round(rf,1),'unit':'$B','label':'ON RRP (FRED)','history':[{'date':h['date'],'value':round(h['value']/1e3,1) if h['value']>1000 else round(h['value'],1)} for h in rfh],'fred_id':'RRPONTSYD'}
    try:
        repo=http_get("https://markets.newyorkfed.org/api/rp/all/all/results/latest.json")
        if repo and 'repo' in repo:
            rps=repo['repo']
            if isinstance(rps,list) and rps:
                ra=float(rps[0].get('totalAmtAccepted',0))/1e9;M['Fed_Repo_Operations']={'value':round(ra,1),'unit':'$B','label':'Fed Repo Ops (Lending)','status':'ACTIVE' if ra>0 else 'DORMANT','description':'Fed lending cash. Active = market needs liquidity'}
    except Exception as e:print(f"REPO_OPS_ERR:{e}")
    try:
        srf=http_get("https://markets.newyorkfed.org/api/rp/srf/results/latest.json")
        if srf and 'repo' in srf:
            sr=srf['repo']
            if isinstance(sr,list) and sr:
                sa=float(sr[0].get('totalAmtAccepted',0))/1e9;M['SRF_Usage']={'value':round(sa,1),'unit':'$B','label':'Standing Repo Facility','status':'STRESS' if sa>10 else 'NORMAL','description':'Emergency backstop. Usage = stress'}
    except Exception as e:print(f"SRF_ERR:{e}")
    return M
def collect_fed_facilities():
    M={}
    dw=fl('WLCFLPCL');dw_h=fh('WLCFLPCL',90)
    if dw is not None:
        vb=round(dw/1e3,2) if dw>100 else round(dw,2);M['Discount_Window_Primary']={'value':vb,'unit':'$B','label':'Discount Window Primary Credit','history':[{'date':h['date'],'value':round(h['value']/1e3,2) if h['value']>100 else round(h['value'],2)} for h in dw_h],'z_score':zs(dw,dw_h),'status':'STRESS' if dw>10000 else 'ELEVATED' if dw>5000 else 'NORMAL','description':'Banks borrowing from Fed. Spikes = crisis','fred_id':'WLCFLPCL'}
    ot=fl('OTHL1690');ot_h=fh('OTHL1690',90)
    if ot is not None:
        vb=round(ot/1e3,2) if ot>100 else round(ot,2);M['Loans_16_90_Days']={'value':vb,'unit':'$B','label':'Liquidity Facilities: Loans 16-90 Days','history':[{'date':h['date'],'value':round(h['value']/1e3,2) if h['value']>100 else round(h['value'],2)} for h in ot_h],'z_score':zs(ot,ot_h),'fred_id':'OTHL1690','status':'STRESS' if ot>50000 else 'ELEVATED' if ot>20000 else 'NORMAL','description':'Fed term lending. Spikes = banks need funding'}
    for sid,lab,desc in [('WLCFLL','All Liquidity Facilities','Total Fed lending'),('H41RESPPALDKNWW','Loans to Depository Inst','Direct bank lending'),('WORAL','Other Fed Reserve Assets','Unusual Fed assets'),('SWPT','CB Liquidity Swaps','USD swap lines to foreign CBs')]:
        v=fl(sid);h=fh(sid,60)
        if v is not None:
            vd=round(v/1e6,2) if v>1e6 else round(v/1e3,2) if v>1000 else round(v,2);u='$T' if v>1e6 else '$B' if v>1000 else '$M'
            M[sid]={'value':vd,'unit':u,'label':lab,'history':h,'z_score':zs(v,h),'fred_id':sid,'description':desc}
    for sid,lab,desc,u,div in [('WALCL','Fed Total Assets','Fed balance sheet','$T',1e6),('TREAST','Fed Treasury Holdings','QT tracker','$T',1e6),('WSHOMCB','Fed MBS Holdings','MBS held','$T',1e6),('WDTGAL','Treasury General Account','Gov checking at Fed','$B',1e3),('TOTRESNS','Total Bank Reserves','Banking reserves','$T',1e6),('EXCSRESNS','Excess Reserves','Above requirements','$B',1e3)]:
        v=fl(sid);h=fh(sid,90)
        if v is not None:M[sid]={'value':round(v/div,2),'unit':u,'label':lab,'history':[{'date':x['date'],'value':round(x['value']/div,2)} for x in h],'z_score':zs(v,h),'fred_id':sid,'description':desc}
    tga=fl('WDTGAL')
    if tga is not None:tb=tga/1e3;M['TGA_Status']={'value':round(tb,1),'unit':'$B','status':'DRAINING' if tb>800 else 'NORMAL' if tb>200 else 'LOW','label':'TGA Liquidity Impact','description':'High=drains reserves. Low=debt ceiling'}
    return M
def collect_funding_spreads():
    M={}
    for sid,lab,desc,wl,sl,u in [('TEDRATE','TED Spread','Interbank stress',0.35,0.50,'%'),('BAMLH0A0HYM2','HY OAS','High yield stress',4.0,6.0,'bps'),('BAMLC0A0CM','IG OAS','IG credit stress',1.5,2.0,'bps'),('BAMLC0A4CBBB','BBB Spread','Widens before downgrades',2.0,3.0,'bps'),('BAMLH0A3HYC','CCC & Below','Distressed debt',8.0,12.0,'bps'),('T10Y2Y','10Y-2Y Spread','Recession signal',None,None,'%'),('T10Y3M','10Y-3M Spread','Best recession predictor',None,None,'%'),('T10YFF','10Y-FF Spread','Term premium',None,None,'%'),('AAA10Y','AAA-10Y','Top grade vs Treasury',0.8,1.5,'%'),('BAA10Y','BAA-10Y','Medium grade stress',2.5,3.5,'%')]:
        v=fl(sid);h=fh(sid,90)
        if v is not None:
            st='NORMAL'
            if wl is not None and sl is not None:st='STRESS' if v>sl else 'WATCH' if v>wl else 'NORMAL'
            elif sid in['T10Y2Y','T10Y3M']:st='INVERTED' if v<0 else 'FLAT' if v<0.25 else 'NORMAL'
            M[sid]={'value':round(v,4),'unit':u,'label':lab,'history':h,'z_score':zs(v,h),'status':st,'fred_id':sid,'description':desc}
    lib3m=fl('USD3MTD156N');sofr=fl('SOFR')
    if lib3m is not None and sofr is not None:frao=round(lib3m-sofr,4);M['FRA_OIS_Proxy']={'value':frao,'unit':'%','label':'FRA-OIS Proxy','status':'STRESS' if frao>0.50 else 'WATCH' if frao>0.25 else 'NORMAL','description':'Key bank funding stress gauge'}
    for sid,lab,desc in [('RIFSPPFAAD90NB','90D AA Financial CP','Corp funding cost'),('RIFSPPNAAD90NB','90D AA Nonfinancial CP','Real economy funding'),('DCPN3M','3M Dealer CP','Dealer costs')]:
        v=fl(sid);h=fh(sid,60)
        if v is not None:M[sid]={'value':round(v,3),'unit':'%','label':lab,'history':h,'fred_id':sid,'description':desc}
    return M
def collect_swaps():
    M={};swap_vals={};tsy_vals={}
    for sid,lab,desc in [('DEXUSEU','EUR/USD','Euro cross'),('DEXJPUS','JPY/USD','Yen cross'),('DEXUSUK','GBP/USD','Sterling cross'),('DEXCHUS','CNY/USD','Yuan cross')]:
        v=fl(sid);h=fh(sid,60)
        if v is not None:M[sid]={'value':round(v,4),'unit':'rate','label':lab,'history':h,'fred_id':sid,'description':desc}
    for sid,lab,desc in [('MSWP2','2Y Swap Rate','Short-end'),('MSWP5','5Y Swap Rate','Belly'),('MSWP10','10Y Swap Rate','Long-end'),('MSWP30','30Y Swap Rate','Ultra-long')]:
        v=fl(sid);h=fh(sid,60)
        if v is not None:t=sid.replace('MSWP','');swap_vals[t]=v;M[sid]={'value':round(v,3),'unit':'%','label':lab,'history':h,'fred_id':sid,'description':desc}
    for t,sid in{'2':'DGS2','5':'DGS5','10':'DGS10','30':'DGS30'}.items():
        v=fl(sid)
        if v is not None:tsy_vals[t]=v
    for t in['2','5','10','30']:
        if t in swap_vals and t in tsy_vals:sp=round((swap_vals[t]-tsy_vals[t])*100,1);M[f'Swap_Spread_{t}Y']={'value':sp,'unit':'bps','label':f'{t}Y Swap Spread','status':'NEGATIVE' if sp<0 else 'NORMAL','description':'Negative = basis trade stress'}
    sw=fl('SWPT');sw_h=fh('SWPT',60)
    if sw is not None:vb=round(sw/1e3,2) if sw>100 else round(sw,2);M['CB_Swap_Lines']={'value':vb,'unit':'$B','label':'CB USD Swap Lines','history':sw_h,'status':'STRESS' if sw>50000 else 'ELEVATED' if sw>10000 else 'NORMAL','description':'Fed lending USD globally. Active = dollar shortage'}
    return M
def collect_systemic():
    M={}
    for sid,lab,desc,wl,sl in [('STLFSI4','St Louis FSI','Above 0 = stress',0,1.0),('NFCI','Chicago NFCI','Above 0 = tight',0,0.5),('ANFCI','Adjusted NFCI','Risk-adjusted',0,0.5),('DRTSCILM','Bank Lending C&I','Tightening = crunch',20,40),('DRTSCLCC','Bank Lending Cards','Consumer tightening',15,30)]:
        v=fl(sid);h=fh(sid,60)
        if v is not None:st='STRESS' if v>sl else 'WATCH' if v>wl else 'NORMAL';M[sid]={'value':round(v,3),'unit':'index','label':lab,'history':h,'z_score':zs(v,h),'status':st,'fred_id':sid,'description':desc}
    for sid,lab,desc,wl,sl in [('VIXCLS','VIX','>30=fear >40=panic',20,30),('MOVE','MOVE Index','>120=stress >150=crisis',120,150)]:
        v=fl(sid);h=fh(sid,90)
        if v is not None:st='PANIC' if v>sl else 'FEAR' if v>wl else 'CALM';M[sid]={'value':round(v,2),'unit':'index','label':lab,'history':h,'z_score':zs(v,h),'status':st,'fred_id':sid,'description':desc}
    m2=fl('M2SL');m2h=fh('M2SL',60)
    if m2 is not None:M['M2']={'value':round(m2/1e3,2),'unit':'$T','label':'M2 Money Supply','history':[{'date':h['date'],'value':round(h['value']/1e3,2)} for h in m2h],'fred_id':'M2SL','description':'Contracting = deflationary'}
    return M
def collect_curve():
    curve={}
    for sid,t in{'DGS1MO':'1M','DGS3MO':'3M','DGS6MO':'6M','DGS1':'1Y','DGS2':'2Y','DGS3':'3Y','DGS5':'5Y','DGS7':'7Y','DGS10':'10Y','DGS20':'20Y','DGS30':'30Y'}.items():
        v=fl(sid)
        if v is not None:curve[t]=round(v,3)
    return{'yield_curve':{'curve':curve,'label':'Treasury Yield Curve'}}
def compute_stress(ad):
    score=0;flags=[];mx=0
    for cat,key,fld,thr,pts,ftxt in [('repo_rates','SOFR_EFFR_Spread','value',0.10,8,'SOFR-FF spread widening'),('repo_rates','SOFR_EFFR_Spread','value',0.20,15,'SOFR-FF CRITICAL'),('repo_rates','SOFR_Dispersion','value',0.05,5,'SOFR dispersion'),('repo_rates','AMERIBOR_Spread','value',0.20,8,'Small bank stress'),('reverse_repo','SRF_Usage','value',1,15,'SRF tapped'),('fed_facilities','Discount_Window_Primary','value',5,12,'Discount window elevated'),('fed_facilities','Loans_16_90_Days','z_score',2,10,'OTHL1690 spiking'),('funding_spreads','TEDRATE','value',0.35,8,'TED elevated'),('funding_spreads','BAMLH0A0HYM2','value',5.0,10,'HY blowout'),('funding_spreads','BAMLC0A4CBBB','value',2.5,8,'BBB widening'),('funding_spreads','FRA_OIS_Proxy','value',0.30,10,'FRA-OIS stress'),('systemic','STLFSI4','value',0.5,8,'FSI elevated'),('systemic','NFCI','value',0.3,8,'Conditions tight'),('systemic','VIXCLS','value',25,5,'Vol elevated'),('systemic','VIXCLS','value',35,10,'VIX panic'),('systemic','MOVE','value',130,8,'Bond vol up'),('systemic','MOVE','value',160,15,'MOVE crisis'),('swaps','CB_Swap_Lines','value',5,10,'Swap lines active')]:
        mx+=pts;d=ad.get(cat,{}).get(key)
        if d and fld in d:
            v=d[fld]
            if v is not None:
                if fld=='value' and v>thr:score+=pts;flags.append(f"\U0001f534 {ftxt}: {v}")
                elif fld=='z_score' and v>thr:score+=pts;flags.append(f"\U0001f534 {ftxt} (z={v:.1f})")
    rrp=ad.get('reverse_repo',{}).get('RRP_Volume')
    if rrp and rrp.get('value') is not None:
        rv=rrp['value']
        if rv<50:score+=10;flags.append(f"\U0001f534 RRP depleted: ${rv}B");mx+=10
        elif rv<100:score+=5;flags.append(f"\U0001f7e1 RRP low: ${rv}B");mx+=5
    for t in['2','5','10','30']:
        ss=ad.get('swaps',{}).get(f'Swap_Spread_{t}Y')
        if ss and ss.get('value') is not None and ss['value']<-10:score+=8;flags.append(f"\U0001f534 {t}Y swap negative: {ss['value']}bps");mx+=8
    for k in['T10Y2Y','T10Y3M']:
        d=ad.get('funding_spreads',{}).get(k)
        if d and d.get('value') is not None and d['value']<0:score+=5;flags.append(f"\U0001f7e1 Curve inverted ({k}): {d['value']:.2f}%");mx+=5
    norm=min(100,int((score/max(mx,1))*100)) if mx>0 else 0
    if norm>=60:st,act,col='CRITICAL','MAXIMUM DEFENSE','#ef4444'
    elif norm>=40:st,act,col='STRESS','REDUCE RISK','#f97316'
    elif norm>=25:st,act,col='ELEVATED','MONITOR CLOSELY','#f59e0b'
    elif norm>=10:st,act,col='WATCH','STAY ALERT','#3b82f6'
    else:st,act,col='NORMAL','ALL CLEAR','#10b981'
    return{'score':norm,'status':st,'action':act,'color':col,'flags':flags,'red_flags':len([f for f in flags if '\U0001f534' in f]),'yellow_flags':len([f for f in flags if '\U0001f7e1' in f]),'raw_score':score,'max_possible':mx}

# ═══════════════════════════════════════════════════════════════
#  MARKET INTELLIGENCE ENGINE
# ═══════════════════════════════════════════════════════════════
def generate_intelligence(ad, stress):
    """Generate comprehensive market intelligence report from all data"""
    ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M ET')
    
    # Extract key values
    def gv(cat, key, field='value'):
        d = ad.get(cat, {}).get(key)
        if d and isinstance(d, dict): return d.get(field)
        return None
    
    sofr = gv('repo_rates','SOFR')
    effr = gv('repo_rates','EFFR')
    sofr_ff = gv('repo_rates','SOFR_EFFR_Spread')
    sofr_disp = gv('repo_rates','SOFR_Dispersion')
    ameribor_sp = gv('repo_rates','AMERIBOR_Spread')
    rrp = gv('reverse_repo','RRP_Volume')
    rrp_cp = gv('reverse_repo','RRP_Counterparties')
    srf = gv('reverse_repo','SRF_Usage')
    dw = gv('fed_facilities','Discount_Window_Primary')
    othl = gv('fed_facilities','Loans_16_90_Days')
    fed_bs = gv('fed_facilities','WALCL')
    tsy_hold = gv('fed_facilities','TREAST')
    mbs_hold = gv('fed_facilities','WSHOMCB')
    tga = gv('fed_facilities','TGA_Status')
    reserves = gv('fed_facilities','TOTRESNS')
    vix = gv('systemic','VIXCLS')
    move = gv('systemic','MOVE')
    fsi = gv('systemic','STLFSI4')
    nfci = gv('systemic','NFCI')
    hy = gv('funding_spreads','BAMLH0A0HYM2')
    ig = gv('funding_spreads','BAMLC0A0CM')
    bbb = gv('funding_spreads','BAMLC0A4CBBB')
    ccc = gv('funding_spreads','BAMLH0A3HYC')
    ted = gv('funding_spreads','TEDRATE')
    fra_ois = gv('funding_spreads','FRA_OIS_Proxy')
    t10y2y = gv('funding_spreads','T10Y2Y')
    t10y3m = gv('funding_spreads','T10Y3M')
    cb_swaps = gv('swaps','CB_Swap_Lines')
    m2 = gv('systemic','M2')
    
    score = stress.get('score', 0)
    status = stress.get('status', 'UNKNOWN')
    
    # ═══ CRITICAL METRICS TABLE ═══
    metrics_table = []
    
    def add_metric(name, val, thresholds, actions):
        """thresholds: [(val, status, action), ...] sorted worst to best"""
        if val is None: return
        for thr_val, thr_status, thr_action in thresholds:
            if isinstance(thr_val, str):
                metrics_table.append({'metric': name, 'value': val, 'status': thr_status, 'action': thr_action})
                return
            if thr_val is not None and val <= thr_val:
                continue
            metrics_table.append({'metric': name, 'value': val, 'status': thr_status, 'action': thr_action})
            return
        if thresholds:
            metrics_table.append({'metric': name, 'value': val, 'status': thresholds[-1][1], 'action': thresholds[-1][2]})
    
    # RRP - most critical right now
    if rrp is not None:
        if rrp < 50: rrp_st, rrp_act = 'CRISIS LEVEL', 'EXIT RISK NOW'
        elif rrp < 100: rrp_st, rrp_act = 'Critical low', 'Reduce all risk'
        elif rrp < 200: rrp_st, rrp_act = 'Very low', 'Reduce leverage'
        elif rrp < 500: rrp_st, rrp_act = 'Low', 'Monitor closely'
        else: rrp_st, rrp_act = 'Normal', 'No action'
        metrics_table.append({'metric': 'RRP Balance', 'value': f"${rrp}B", 'status': rrp_st, 'action': rrp_act})
    
    if sofr is not None:
        metrics_table.append({'metric': 'SOFR', 'value': f"{sofr}%", 'status': 'Stable' if sofr < 5.0 else 'Elevated' if sofr < 5.5 else 'STRESS', 'action': 'Monitor' if sofr < 5.5 else 'Repo stress'})
    
    if sofr_ff is not None:
        st = 'Normal' if abs(sofr_ff) < 0.05 else 'Watch' if abs(sofr_ff) < 0.10 else 'STRESS'
        metrics_table.append({'metric': 'SOFR-FF Spread', 'value': f"{sofr_ff*100:.1f}bps", 'status': st, 'action': 'Normal' if st == 'Normal' else 'Collateral stress'})
    
    if vix is not None:
        if vix < 15: vst, va = 'Complacent', 'Buy protection'
        elif vix < 20: vst, va = 'Calm', 'Normal'
        elif vix < 30: vst, va = 'Elevated', 'Hedge'
        elif vix < 40: vst, va = 'Fear', 'Reduce risk'
        else: vst, va = 'PANIC', 'Max defense'
        metrics_table.append({'metric': 'VIX', 'value': f"{vix}", 'status': vst, 'action': va})
    
    if move is not None:
        if move < 100: mst, ma = 'Calm', 'Normal'
        elif move < 120: mst, ma = 'Elevated', 'Watch bonds'
        elif move < 150: mst, ma = 'Stress', 'Reduce duration'
        else: mst, ma = 'CRISIS', 'Cash only'
        metrics_table.append({'metric': 'MOVE Index', 'value': f"{move}", 'status': mst, 'action': ma})
    
    if hy is not None:
        if hy < 3.0: hst, ha = 'Too tight', 'Sell rallies'
        elif hy < 4.5: hst, ha = 'Normal', 'Hold'
        elif hy < 6.0: hst, ha = 'Widening', 'Reduce HY'
        else: hst, ha = 'Blowout', 'Exit HY'
        metrics_table.append({'metric': 'HY Spread', 'value': f"{hy}bps", 'status': hst, 'action': ha})
    
    if bbb is not None:
        metrics_table.append({'metric': 'BBB Spread', 'value': f"{bbb}bps", 'status': 'Tight' if bbb < 1.5 else 'Normal' if bbb < 2.5 else 'Wide' if bbb < 3.5 else 'STRESS', 'action': 'Watch downgrades' if bbb > 2.5 else 'Normal'})
    
    if fed_bs is not None:
        metrics_table.append({'metric': 'Fed Balance Sheet', 'value': f"${fed_bs}T", 'status': 'QT ongoing', 'action': 'Headwind'})
    
    if reserves is not None:
        if reserves < 2.5: rst = 'CRITICAL LOW'
        elif reserves < 3.0: rst = 'Low'
        elif reserves < 3.5: rst = 'Adequate'
        else: rst = 'Ample'
        metrics_table.append({'metric': 'Bank Reserves', 'value': f"${reserves}T", 'status': rst, 'action': 'Watch for squeeze' if reserves < 3.0 else 'Sufficient'})
    
    if tga is not None:
        metrics_table.append({'metric': 'TGA Balance', 'value': f"${tga}B", 'status': 'Draining' if tga > 800 else 'Normal' if tga > 200 else 'Debt ceiling', 'action': 'Reserves under pressure' if tga > 800 else 'Neutral' if tga > 200 else 'Watch debt limit'})
    
    if dw is not None:
        metrics_table.append({'metric': 'Discount Window', 'value': f"${dw}B", 'status': 'Normal' if dw < 5 else 'Elevated' if dw < 20 else 'STRESS', 'action': 'Monitor' if dw < 20 else 'Bank stress'})
    
    if othl is not None:
        metrics_table.append({'metric': 'OTHL1690 Loans', 'value': f"${othl}B", 'status': 'Normal' if othl < 10 else 'Elevated' if othl < 30 else 'STRESS', 'action': 'Banks need term funding' if othl > 10 else 'Normal'})
    
    if t10y2y is not None:
        metrics_table.append({'metric': '10Y-2Y Curve', 'value': f"{t10y2y:.2f}%", 'status': 'Inverted' if t10y2y < 0 else 'Flat' if t10y2y < 0.25 else 'Normal', 'action': 'Recession signal' if t10y2y < 0 else 'Watch' if t10y2y < 0.25 else 'Healthy'})
    
    if fsi is not None:
        metrics_table.append({'metric': 'Financial Stress', 'value': f"{fsi:.2f}", 'status': 'Normal' if fsi < 0 else 'Elevated' if fsi < 1 else 'STRESS', 'action': 'Tightening' if fsi > 0 else 'Accommodative'})
    
    if cb_swaps is not None:
        metrics_table.append({'metric': 'CB Swap Lines', 'value': f"${cb_swaps}B", 'status': 'Dormant' if cb_swaps < 1 else 'Active' if cb_swaps < 10 else 'STRESS', 'action': 'Dollar shortage' if cb_swaps > 5 else 'Normal'})
    
    if srf is not None:
        metrics_table.append({'metric': 'Standing Repo', 'value': f"${srf}B", 'status': 'Normal' if srf < 1 else 'ACTIVE' if srf < 10 else 'STRESS', 'action': 'Emergency liquidity' if srf > 1 else 'Dormant'})
    
    # ═══ PHASE DETECTION ═══
    phase = 'STABLE'
    phase_detail = ''
    forecast = ''
    comparison = ''
    action_required = ''
    
    crisis_signals = 0
    if rrp is not None and rrp < 100: crisis_signals += 3
    if rrp is not None and rrp < 50: crisis_signals += 2
    if vix is not None and vix > 30: crisis_signals += 2
    if move is not None and move > 130: crisis_signals += 2
    if hy is not None and hy > 5: crisis_signals += 2
    if dw is not None and dw > 20: crisis_signals += 2
    if srf is not None and srf > 1: crisis_signals += 3
    if fsi is not None and fsi > 1: crisis_signals += 2
    if t10y2y is not None and t10y2y < -0.5: crisis_signals += 1
    if sofr_ff is not None and abs(sofr_ff) > 0.15: crisis_signals += 2
    if cb_swaps is not None and cb_swaps > 10: crisis_signals += 2
    
    warning_signals = 0
    if rrp is not None and rrp < 200: warning_signals += 1
    if vix is not None and vix < 15: warning_signals += 1  # complacency
    if hy is not None and hy < 3: warning_signals += 1  # too tight
    if vix is not None and vix > 20: warning_signals += 1
    if move is not None and move > 110: warning_signals += 1
    if t10y2y is not None and t10y2y < 0: warning_signals += 1
    if fsi is not None and fsi > 0: warning_signals += 1
    
    if crisis_signals >= 6:
        phase = 'CRISIS'
        phase_detail = 'Multiple systemic stress indicators flashing red simultaneously.'
        if rrp is not None and rrp < 50:
            phase_detail = f'LIQUIDITY CRISIS - RRP at ${rrp}B near ZERO! System reserves critically depleted.'
        forecast = 'Market crash risk -20% to -40% within weeks. Historical precedent suggests severe dislocation.'
        comparison = 'Comparable to Sep 2019 repo spike, Mar 2020 COVID crash, or 2008 GFC early stages.'
        action_required = 'EXIT ALL RISK IMMEDIATELY. T-bills and cash only.'
    elif crisis_signals >= 3:
        phase = 'PRE-CRISIS'
        phase_detail = 'Critical warning signs emerging. Plumbing stress building across multiple indicators.'
        forecast = 'Correction risk -10% to -20%. Liquidity deteriorating. Credit markets showing cracks.'
        comparison = 'Similar to conditions before Sep 2019 repo squeeze or Q4 2018 selloff.'
        action_required = 'REDUCE ALL RISK. Raise cash to 40%+. Exit leveraged positions.'
    elif warning_signals >= 4:
        phase = 'DETERIORATING'
        phase_detail = 'Multiple warning signals active. Conditions shifting from stable to stressed.'
        forecast = 'Elevated correction risk -5% to -15%. Monitor daily for acceleration.'
        comparison = 'Reminiscent of mid-2018 or late 2021 conditions before vol events.'
        action_required = 'Reduce leverage. Hedge tail risk. Raise cash to 25%.'
    elif warning_signals >= 2:
        phase = 'CAUTIOUS'
        phase_detail = 'Some warning signs present but no systemic stress yet. Stay vigilant.'
        forecast = 'Normal volatility expected. Watch for spread widening or liquidity drawdown.'
        comparison = 'Typical mid-cycle conditions. Not yet concerning but worth monitoring.'
        action_required = 'Normal positioning. Maintain hedges. Monitor weekly.'
    else:
        phase = 'STABLE'
        phase_detail = 'Plumbing functioning normally. Liquidity adequate. No systemic stress detected.'
        forecast = 'Favorable conditions for risk assets. Normal market volatility expected.'
        comparison = 'Healthy market conditions similar to mid-2017 or H2 2024.'
        action_required = 'Stay invested. Normal operations. Review monthly.'
    
    # ═══ HEADLINE ═══
    if phase == 'CRISIS':
        headline = '\U0001f6a8 CRITICAL LIQUIDITY CRISIS \U0001f6a8'
        if rrp is not None and rrp < 50:
            headline_detail = f'RRP at ${rrp}B - NEAR ZERO! This is LOWER than March 2020 and Sept 2019 crises!'
        else:
            headline_detail = 'Multiple systemic stress indicators at crisis levels!'
    elif phase == 'PRE-CRISIS':
        headline = '\u26a0\ufe0f PRE-CRISIS WARNING'
        headline_detail = 'Plumbing stress building. Historical patterns suggest correction imminent.'
    elif phase == 'DETERIORATING':
        headline = '\U0001f7e1 CONDITIONS DETERIORATING'
        headline_detail = 'Warning signals increasing. Risk-reward shifting negative.'
    elif phase == 'CAUTIOUS':
        headline = '\U0001f535 CAUTION WARRANTED'
        headline_detail = 'Mixed signals. Some stress indicators emerging. Stay alert.'
    else:
        headline = '\u2705 PLUMBING CLEAR'
        headline_detail = 'All systems normal. Liquidity adequate. No stress detected.'
    
    # ═══ KEY RISKS ═══
    risks = []
    if rrp is not None and rrp < 200:
        risks.append(f"RRP DEPLETION: At ${rrp}B, reverse repo buffer is {'critically low' if rrp < 50 else 'dangerously low' if rrp < 100 else 'declining'}. When RRP hits zero, system must find new liquidity sources or face seizure.")
    if vix is not None and vix < 15:
        risks.append(f"COMPLACENCY: VIX at {vix} signals extreme complacency. Historically, VIX below 15 precedes volatility spikes. Protection is cheap - buy it.")
    if hy is not None and hy < 3:
        risks.append(f"CREDIT MISPRICING: HY spreads at {hy}bps are unsustainably tight. Credit risk is being ignored. When spreads normalize, expect -5% to -15% in HY bonds.")
    if t10y2y is not None and t10y2y < 0:
        risks.append(f"YIELD CURVE INVERTED: 10Y-2Y at {t10y2y:.2f}%. Every recession in 50 years was preceded by inversion. Recession risk is elevated.")
    if move is not None and move > 120:
        risks.append(f"BOND VOLATILITY: MOVE at {move} indicates Treasury market stress. This often precedes equity volatility. Reduce duration exposure.")
    if reserves is not None and reserves < 3.0:
        risks.append(f"LOW RESERVES: Bank reserves at ${reserves}T approaching critical threshold of $2.5T. Below this level, repo market seizes (see Sept 2019).")
    if dw is not None and dw > 5:
        risks.append(f"DISCOUNT WINDOW: Banks borrowing ${dw}B from Fed. Elevated DW usage signals bank funding stress not visible in markets.")
    if fsi is not None and fsi > 0:
        risks.append(f"FINANCIAL STRESS: St. Louis FSI at {fsi:.2f} (above zero = stress). Conditions tightening across credit markets.")
    
    if not risks:
        risks.append("No significant risks identified. Market plumbing functioning normally.")
    
    # ═══ HISTORICAL CONTEXT ═══
    hist_events = []
    if rrp is not None:
        if rrp < 50: hist_events.append({'event': 'Current RRP level', 'context': f'${rrp}B is lower than ANY modern crisis. Sept 2019 repo spike occurred with ~$200B. This is uncharted territory.', 'severity': 'EXTREME'})
        elif rrp < 200: hist_events.append({'event': 'RRP approaching crisis zone', 'context': 'Sept 2019: SOFR spiked to 10% when reserves fell too low. Current trajectory suggests similar stress ahead.', 'severity': 'HIGH'})
    if vix is not None and vix < 15:
        hist_events.append({'event': 'VIX complacency', 'context': 'Jan 2018: VIX at 9 preceded Volmageddon (-12% correction). Feb 2020: VIX at 15 preceded COVID crash.', 'severity': 'MEDIUM'})
    if move is not None and move > 130:
        hist_events.append({'event': 'MOVE elevated', 'context': 'Oct 2023: MOVE at 140 coincided with 10Y hitting 5% and bank stress. Similar conditions now.', 'severity': 'HIGH'})
    
    return {
        'timestamp': ts,
        'headline': headline,
        'headline_detail': headline_detail,
        'stress_score': score,
        'stress_status': status,
        'phase': phase,
        'phase_detail': phase_detail,
        'action_required': action_required,
        'forecast': forecast,
        'comparison': comparison,
        'metrics_table': metrics_table,
        'risks': risks,
        'historical_context': hist_events,
        'crisis_signals': crisis_signals,
        'warning_signals': warning_signals,
        'generated_at': datetime.now(timezone.utc).isoformat()
    }

def lambda_handler(event,context):
    try:
        print("=== REPO MONITOR V2 START ===");ts=datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
        print("Repo rates...");rr=collect_repo_rates();print(f"  {len(rr)}")
        print("Reverse repo...");rv=collect_reverse_repo();print(f"  {len(rv)}")
        print("Fed facilities...");ff=collect_fed_facilities();print(f"  {len(ff)}")
        print("Spreads...");fs=collect_funding_spreads();print(f"  {len(fs)}")
        print("Swaps...");sw=collect_swaps();print(f"  {len(sw)}")
        print("Systemic...");sy=collect_systemic();print(f"  {len(sy)}")
        print("Curve...");tc=collect_curve()
        ad={'repo_rates':rr,'reverse_repo':rv,'fed_facilities':ff,'funding_spreads':fs,'swaps':sw,'systemic':sy,'treasury':tc}
        print("Scoring...");stress=compute_stress(ad)
        print("Generating intelligence...");intel=generate_intelligence(ad,stress)
        total=sum(len(v) for v in ad.values() if isinstance(v,dict))
        out={'timestamp':ts,'generated_at':datetime.now(timezone.utc).isoformat(),'stress':stress,'intelligence':intel,'data':{},'summary':{'total_metrics':total,'score':stress['score'],'status':stress['status'],'red_flags':stress['red_flags'],'yellow_flags':stress['yellow_flags'],'phase':intel['phase']}}
        for cat,mets in ad.items():
            if isinstance(mets,dict):
                out['data'][cat]={}
                for k,v in mets.items():
                    if isinstance(v,dict):c=dict(v);(c.__setitem__('history',c['history'][-30:]) if 'history' in c and isinstance(c['history'],list) else None);out['data'][cat][k]=c
                    else:out['data'][cat][k]=v
        print(f"Publishing to {S3_BUCKET}/repo-data.json")
        s3.put_object(Bucket=S3_BUCKET,Key='repo-data.json',Body=json.dumps(out,default=str),ContentType='application/json',CacheControl='max-age=120')
        dk=datetime.now(timezone.utc).strftime('%Y/%m/%d/%H%M')
        s3.put_object(Bucket=S3_BUCKET,Key=f'archive/repo/{dk}.json',Body=json.dumps(out,default=str),ContentType='application/json')
        print(f"=== DONE === Score:{stress['score']}/100 ({stress['status']}) Phase:{intel['phase']} Metrics:{total}")
        return{'statusCode':200,'body':json.dumps({'status':'published','score':stress['score'],'plumbing_status':stress['status'],'phase':intel['phase'],'red_flags':stress['red_flags'],'yellow_flags':stress['yellow_flags'],'total_metrics':total,'action':stress['action'],'headline':intel['headline']})}
    except Exception as e:print(f"FATAL:{e}");traceback.print_exc();return{'statusCode':500,'body':json.dumps({'error':str(e)})}
