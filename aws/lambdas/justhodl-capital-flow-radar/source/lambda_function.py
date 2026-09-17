"""Dated ETF creation/redemption measurements, separated from directional comparisons."""
import math
import json
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

VERSION = "4.0.0"
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/capital-flow-radar.json"
STATE_KEY = "data/capital-flow-radar-state.json"
s3 = boto3.client("s3", region_name="us-east-1")

# Sector / theme COMPLEXES. core = unlevered; bull/bear = leveraged sentiment legs.
COMPLEXES = {
    "Semiconductors": {"core": ["SMH", "SOXX", "XSD"], "bull": ["SOXL", "USD"], "bear": ["SOXS", "SSG"], "primary": "SMH",
                       "stocks": ["NVDA", "AMD", "AVGO", "MU", "TSM", "LRCX", "AMAT", "KLAC", "MRVL", "ON", "ARM", "SMCI"]},
    "Technology": {"core": ["XLK", "QQQ", "VGT", "FTEC"], "bull": ["TQQQ", "TECL", "ROM", "QQQU"], "bear": ["SQQQ", "TECS", "REW"], "primary": "XLK",
                   "stocks": ["AAPL", "MSFT", "NVDA", "AVGO", "ORCL", "CRM", "ADBE", "AMD"]},
    "Software": {"core": ["IGV", "WCLD", "SKYY"], "bull": [], "bear": [], "primary": "IGV",
                 "stocks": ["MSFT", "CRM", "NOW", "ADBE", "SNOW", "PLTR", "DDOG", "NET"]},
    "Biotech": {"core": ["XBI", "IBB"], "bull": ["LABU", "BIB"], "bear": ["LABD", "BIS"], "primary": "XBI",
                "stocks": ["VRTX", "REGN", "GILD", "AMGN", "MRNA", "BIIB", "ALNY"]},
    "Energy": {"core": ["XLE", "XOP", "OIH"], "bull": ["ERX", "GUSH", "DIG"], "bear": ["ERY", "DRIP", "DUG"], "primary": "XLE",
               "stocks": ["XOM", "CVX", "COP", "SLB", "EOG", "OXY", "FANG", "PSX"]},
    "Oil": {"core": ["USO"], "bull": ["UCO"], "bear": ["SCO"], "primary": "USO", "stocks": []},
    "Natural Gas": {"core": ["UNG"], "bull": ["BOIL"], "bear": ["KOLD"], "primary": "UNG", "stocks": []},
    "Financials": {"core": ["XLF", "KRE", "KBE"], "bull": ["FAS", "DPST", "UYG"], "bear": ["FAZ", "SKF"], "primary": "XLF",
                   "stocks": ["JPM", "BAC", "WFC", "GS", "MS", "C", "SCHW"]},
    "Clean Energy": {"core": ["ICLN", "TAN"], "bull": [], "bear": [], "primary": "TAN",
                     "stocks": ["FSLR", "ENPH", "SEDG", "RUN", "NEE", "STEM"]},
    "China": {"core": ["KWEB", "FXI", "MCHI"], "bull": ["YINN", "CWEB", "CHAU", "XPP"], "bear": ["YANG", "FXP"], "primary": "KWEB",
              "stocks": ["BABA", "PDD", "JD", "BIDU", "NIO", "LI", "XPEV"]},
    "Innovation/ARK": {"core": ["ARKK", "ARKW", "ARKG"], "bull": [], "bear": [], "primary": "ARKK",
                       "stocks": ["TSLA", "COIN", "ROKU", "HOOD", "PLTR", "RBLX"]},
    "Crypto": {"core": ["IBIT", "FBTC", "BITO", "ETHA", "ARKB"], "bull": ["BITX", "ETHU", "BITU", "ETHT", "BTCL", "ETU"], "bear": ["BITI", "SBIT", "ETHD"], "primary": "IBIT",
               "stocks": ["COIN", "MSTR", "MARA", "RIOT", "CLSK", "HUT"]},
    "Gold": {"core": ["GLD", "IAU"], "bull": ["UGL"], "bear": ["GLL"], "primary": "GLD",
             "stocks": ["NEM", "GOLD", "AEM", "WPM", "FNV"]},
    "Gold Miners": {"core": ["GDX", "GDXJ"], "bull": ["NUGT", "JNUG", "GDXU"], "bear": ["DUST", "JDST", "GDXD"], "primary": "GDX",
                    "stocks": ["NEM", "GOLD", "AEM", "WPM", "AU"]},
    "Silver": {"core": ["SLV"], "bull": ["AGQ"], "bear": ["ZSL"], "primary": "SLV", "stocks": ["PAAS", "AG", "HL"]},
    "Copper/Mining": {"core": ["COPX", "XME", "CPER"], "bull": [], "bear": [], "primary": "XME",
                      "stocks": ["FCX", "SCCO", "TECK", "VALE", "RIO"]},
    "Uranium": {"core": ["URA", "URNM", "NLR"], "bull": [], "bear": [], "primary": "URA",
                "stocks": ["CCJ", "UEC", "DNN", "NXE", "UUUU"]},
    "Homebuilders": {"core": ["ITB", "XHB"], "bull": ["NAIL"], "bear": [], "primary": "ITB",
                     "stocks": ["DHI", "LEN", "PHM", "NVR", "TOL", "KBH"]},
    "Retail": {"core": ["XRT"], "bull": ["RETL"], "bear": [], "primary": "XRT",
               "stocks": ["AMZN", "WMT", "COST", "TGT", "HD", "LOW"]},
    "Small Caps": {"core": ["IWM"], "bull": ["TNA", "UWM", "SAA", "URTY"], "bear": ["TZA", "TWM", "RWM", "SRTY"], "primary": "IWM", "stocks": []},
    "S&P 500 Broad": {"core": ["SPY", "VOO", "IVV"], "bull": ["SPXL", "UPRO", "SSO", "SPUU"], "bear": ["SPXS", "SPXU", "SDS", "SH"],
                      "primary": "SPY", "stocks": []},
    "Nasdaq Broad": {"core": ["QQQ"], "bull": ["TQQQ", "QLD"], "bear": ["SQQQ", "QID", "PSQ"], "primary": "QQQ", "stocks": []},
    "Dow": {"core": ["DIA"], "bull": ["UDOW", "DDM"], "bear": ["SDOW", "DXD", "DOG"], "primary": "DIA", "stocks": []},
    "Industrials": {"core": ["XLI", "PAVE"], "bull": ["DUSL", "UXI"], "bear": ["SIJ"], "primary": "XLI",
                    "stocks": ["CAT", "DE", "GE", "HON", "UNP", "BA"]},
    "Aerospace/Defense": {"core": ["ITA", "PPA", "ARKX"], "bull": ["DFEN"], "bear": [], "primary": "ITA",
                          "stocks": ["RTX", "LMT", "NOC", "GD", "BA", "LHX"]},
    "Transports": {"core": ["IYT"], "bull": ["TPOR"], "bear": [], "primary": "IYT",
                   "stocks": ["UPS", "FDX", "UNP", "CSX", "NSC", "ODFL"]},
    "Airlines": {"core": ["JETS"], "bull": [], "bear": [], "primary": "JETS",
                 "stocks": ["DAL", "UAL", "AAL", "LUV", "ALK"]},
    "Materials": {"core": ["XLB"], "bull": ["UYM"], "bear": ["SMN"], "primary": "XLB",
                  "stocks": ["LIN", "FCX", "NEM", "SHW", "APD"]},
    "Healthcare": {"core": ["XLV"], "bull": ["CURE", "RXL"], "bear": ["RXD"], "primary": "XLV",
                   "stocks": ["UNH", "JNJ", "LLY", "ABBV", "MRK", "PFE"]},
    "Consumer Discretionary": {"core": ["XLY"], "bull": ["WANT", "UCC"], "bear": ["SCC"], "primary": "XLY",
                               "stocks": ["AMZN", "TSLA", "HD", "MCD", "NKE", "LOW"]},
    "Consumer Staples": {"core": ["XLP"], "bull": ["UGE"], "bear": ["SZK"], "primary": "XLP",
                         "stocks": ["PG", "KO", "PEP", "COST", "WMT"]},
    "Utilities": {"core": ["XLU", "PHO"], "bull": ["UTSL", "UPW"], "bear": ["SDP"], "primary": "XLU",
                  "stocks": ["NEE", "DUK", "SO", "D", "AEP"]},
    "Real Estate": {"core": ["XLRE"], "bull": ["DRN", "URE"], "bear": ["DRV", "SRS"], "primary": "XLRE",
                    "stocks": ["PLD", "AMT", "EQIX", "SPG", "O"]},
    "Communications/Internet": {"core": ["XLC"], "bull": ["WEBL"], "bear": ["WEBS"], "primary": "XLC",
                                "stocks": ["GOOGL", "META", "NFLX", "DIS", "TMUS"]},
    # ── EXPANSION v3 — new complexes (FANG+ mega-cap tech + global regions) ──
    "Mega-Cap Tech (FANG+)": {"core": ["QQQ"], "bull": ["FNGU", "BULZ"], "bear": ["FNGD", "BERZ"], "primary": "QQQ",
                              "stocks": ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "NFLX", "AVGO", "TSLA"]},
    "Europe": {"core": ["EFA", "VEA", "EWG", "EWU"], "bull": ["EURL"], "bear": [], "primary": "EFA", "stocks": []},
    "Emerging Markets": {"core": ["EEM", "VWO"], "bull": ["EDC"], "bear": ["EDZ"], "primary": "EEM", "stocks": []},
    "India": {"core": ["INDA"], "bull": ["INDL"], "bear": [], "primary": "INDA", "stocks": []},
    "Brazil": {"core": ["EWZ"], "bull": ["BRZU"], "bear": [], "primary": "EWZ", "stocks": ["VALE", "PBR", "ITUB", "NU"]},
    # ── EXPANSION (v3.0) — rate positioning, cyber, midcap, thematics ──
    "Long Treasuries (20Y)": {"core": ["TLT"], "bull": ["TMF", "UBT"], "bear": ["TMV", "TBT", "TTT", "TBF"], "primary": "TLT", "stocks": []},
    "Intermediate Treasuries (7-10Y)": {"core": ["IEF"], "bull": ["TYD", "UST"], "bear": ["TYO", "PST"], "primary": "IEF", "stocks": []},
    "Cybersecurity": {"core": ["CIBR", "HACK", "BUG"], "bull": [], "bear": [], "primary": "CIBR",
                      "stocks": ["PANW", "CRWD", "ZS", "FTNT", "NET", "S", "OKTA"]},
    "Mid Caps": {"core": ["IJH"], "bull": ["MIDU", "MVV", "UMDD"], "bear": ["MZZ", "SMDD"], "primary": "IJH", "stocks": []},
    "Cannabis": {"core": ["MSOS"], "bull": [], "bear": [], "primary": "MSOS",
                 "stocks": ["TLRY", "CGC", "CRON", "GTBIF", "CURLF"]},
    "Quantum Computing": {"core": ["QTUM"], "bull": [], "bear": [], "primary": "QTUM",
                          "stocks": ["IONQ", "RGTI", "QBTS", "QUBT", "IBM"]},
    "Robotics/AI": {"core": ["BOTZ", "ROBO"], "bull": [], "bear": [], "primary": "BOTZ",
                    "stocks": ["NVDA", "ISRG", "ABB", "ROK", "TER"]},
}

# Single-stock leveraged — direct leveraged-positioning read per mega-cap (board only).
SINGLE_STOCK_LEV = {
    "NVDA": {"bull": ["NVDL", "NVDX", "NVDU"], "bear": ["NVDS", "NVDD"]},
    "TSLA": {"bull": ["TSLL", "TSLR"], "bear": ["TSLQ", "TSLS", "TSLZ"]},
    "AAPL": {"bull": ["AAPU", "AAPB"], "bear": ["AAPD"]},
    "META": {"bull": ["METU"], "bear": ["METD"]},
    "AMZN": {"bull": ["AMZU", "AMUU"], "bear": ["AMZD"]},
    "MSFT": {"bull": ["MSFU"], "bear": ["MSFD"]},
    "AMD": {"bull": ["AMDL"], "bear": ["AMDD"]},
    "GOOGL": {"bull": ["GGLL"], "bear": ["GGLS"]},
    "MSTR": {"bull": ["MSTU", "MSTX"], "bear": ["MSTZ"]},
    "COIN": {"bull": ["CONL"], "bear": ["CONI", "COII"]},
    "PLTR": {"bull": ["PLTU"], "bear": ["PLTD"]},
    "SMCI": {"bull": ["SMCL", "SMCX"], "bear": []},
    "AVGO": {"bull": ["AVGX", "AVL"], "bear": []},
    "MU": {"bull": ["MUU"], "bear": ["MUD"]},
    "NFLX": {"bull": ["NFXL"], "bear": ["NFXS"]},
    "BABA": {"bull": ["BABX"], "bear": []},
    "TSM": {"bull": ["TSMX"], "bear": []},
    "BRK": {"bull": ["BRKU"], "bear": []},
    "CRWD": {"bull": ["CRWL"], "bear": []},
    "HOOD": {"bull": ["HOOX"], "bear": []},
}


def _read(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def finite(value):
    return isinstance(value,(int,float)) and not isinstance(value,bool) and math.isfinite(value)


def current_row(row,today):
    try:
        age=(today-datetime.strptime(row['observation_date'],'%Y-%m-%d').date()).days
        return (0<=age<=5 and row.get('quality',{}).get('status')=='fresh'
                and row.get('methodology_version')=='etf-dated-flows.v2' and row.get('date_basis')=='effective_date'
                and row.get('unit')=='USD' and row.get('measure')=='provider_fund_flow')
    except (KeyError,ValueError,TypeError):return False


def group_measurement(tickers,rows,today):
    requested=sorted(set(tickers));valid={t:rows[t] for t in requested if t in rows and current_row(rows[t],today)}
    dates={r['observation_date'] for r in valid.values()}
    missing=[t for t in requested if t not in valid]
    result={'required':requested,'present':list(valid),'missing':missing,'coverage_count':len(valid),'required_count':len(requested),
            'status':'not_applicable' if not requested else 'incomplete','observation_dates':sorted(dates),
            'daily_flow_usd':None,'flow_5obs_usd':None,'flow_21obs_usd':None,'windows':{},'aum_usd':None,'pct_aum_5obs':None}
    complete=bool(requested) and not missing and len(dates)==1
    if not complete:return result
    result['status']='fresh'
    values=[row.get('daily_flow_usd') for row in valid.values()]
    if all(finite(v) for v in values):result['daily_flow_usd']=sum(values)
    for n in (5,21):
        windows=[row.get('windows',{}).get(str(n),{}) for row in valid.values()]
        date_lists={tuple(w.get('dates') or []) for w in windows}
        eligible=(all(w.get('status')=='complete_observed_window' and w.get('n')==n and finite(w.get('sum_usd')) for w in windows)
                  and len(date_lists)==1 and len(next(iter(date_lists)))==n)
        result['windows'][str(n)]={'status':'matched' if eligible else 'incomplete_or_misaligned',
                                 'dates':list(next(iter(date_lists))) if eligible else []}
        if eligible:result[f'flow_{n}obs_usd']=sum(w['sum_usd'] for w in windows)
    aums=[r.get('aum_usd') for r in valid.values()]
    if all(finite(v) and v>0 for v in aums):result['aum_usd']=sum(aums)
    if result['aum_usd'] and result['flow_5obs_usd'] is not None:result['pct_aum_5obs']=round(result['flow_5obs_usd']/result['aum_usd']*100,4)
    if result['daily_flow_usd'] is None or result['flow_5obs_usd'] is None or result['flow_21obs_usd'] is None:result['status']='partial_windows'
    return result


def build_radar(donor,today=None):
    today=today or datetime.now(timezone.utc).date()
    rows={};duplicate=set()
    for row in donor.get('metrics') or []:
        ticker=row.get('ticker')
        if not ticker:continue
        if ticker in rows:duplicate.add(ticker)
        rows[ticker]=row
    for ticker in duplicate:rows.pop(ticker,None)
    if donor.get('methodology_version')!='etf-dated-flows.v2':rows={}
    complexes=[];boards=[]
    for name,members in COMPLEXES.items():
        core=group_measurement(members['core'],rows,today)
        f5,f21=core['flow_5obs_usd'],core['flow_21obs_usd']
        pace=None
        if finite(f5) and finite(f21) and core['windows']['5']['dates']==core['windows']['21']['dates'][:5]:
            pace=f5/5-(f21-f5)/16
        state='UNAVAILABLE' if f5 is None else 'NET_CREATIONS' if f5>0 else 'NET_REDEMPTIONS' if f5<0 else 'ZERO_NET_FLOW'
        complexes.append({'complex':name,'primary':members['primary'],'core':core,'regime':state,
                          'net_flow_daily_usd':core['daily_flow_usd'],'net_flow_5d_usd':None,'net_flow_21d_usd':None,
                          'flow_5obs_usd':f5,'flow_21obs_usd':f21,'pct_aum_5obs':core['pct_aum_5obs'],
                          'recent_minus_prior_pace_usd_per_observation':round(pace,2) if pace is not None else None,
                          'pump_probability':None,'persistence_days':None,'flow_zscore_90d':None,
                          'call':None,'execution_eligible':False,
                          'note':'Unlevered core fund creations/redemptions. Membership overlaps other complexes; do not sum rows.'})
    def board(name,kind,members):
        bull=group_measurement(members.get('bull',[]),rows,today);bear=group_measurement(members.get('bear',[]),rows,today)
        b5,s5=bull['flow_5obs_usd'],bear['flow_5obs_usd']
        aligned=(finite(b5) and finite(s5) and bull['windows']['5']['dates']==bear['windows']['5']['dates'])
        return {'name':name,'kind':kind,'bull':bull,'bear':bear,
                'bull_minus_bear_flow_5obs_usd':b5-s5 if aligned else None,
                'total_fund_flow_5obs_usd':b5+s5 if aligned else None,
                'status':'matched' if aligned else 'incomplete_or_not_applicable',
                'interpretation':'Unscaled fund-flow comparison, not net underlying exposure, investor identity or a forecast.'}
    for name,members in COMPLEXES.items():
        if members.get('bull') or members.get('bear'):boards.append(board(name,'sector',members))
    for name,members in SINGLE_STOCK_LEV.items():boards.append(board(name,'single_stock',members))
    # Membership overlap (e.g. TQQQ in Technology and Nasdaq) never duplicates dollars.
    universe=list(COMPLEXES.values())+list(SINGLE_STOCK_LEV.values())
    aggregate=board('Unique leveraged ETF universe','aggregate',{
        leg:sorted({ticker for group in universe for ticker in group.get(leg,[])}) for leg in ('bull','bear')})
    fresh=sum(row['core']['status']=='fresh' for row in complexes)
    return {'engine':'capital-flow-radar','version':'4.0.0','methodology_version':'dated-etf-radar.v2',
            'generated_at':datetime.now(timezone.utc).isoformat(),'donor_generated_at':donor.get('generated_at'),
            'quality':{'status':'fresh' if fresh==len(complexes) else 'partial' if fresh else 'unavailable',
                       'complete_complexes':fresh,'total_complexes':len(complexes),'duplicate_tickers_excluded':sorted(duplicate)},
            'n_complexes':len(complexes),'complexes':complexes,'leveraged_comparisons':boards,'unique_leveraged_aggregate':aggregate,
            'pump_setups':[],'party_over_alerts':[],'top_pick_cascade':[],
            'dollar_tide':{'status':'not_admitted','score_multiplier':None,'note':'No calibrated USD multiplier is applied to ETF flows.'},
            'call':None,'execution_eligible':False,'calibration_status':'MEASUREMENTS_ONLY',
            'sources':['etf-flows/measurements.json'],
            'methodology':'Effective-date provider fund flows; all group members and identical observation windows required. '
                          'Core, bull and inverse ETF flows remain separate. Actual total creations add bull and inverse flows; '
                          'bull minus inverse is separately labeled as a comparison. Unique ETF aggregates are deduplicated. '
                          'Recent five-observation pace is compared with the previous sixteen observations, not an overlapping average.',
            'caveats':'Creation/redemption flows are not trading volume, identified institutional purchases, underlying exposure or calibrated return forecasts. '
                      'Reported-date windows are not certified exchange-calendar windows. Overlapping sectors cannot be added together.'}


def lambda_handler(event,context):
    donor=_read('etf-flows/measurements.json') or {}
    out=build_radar(donor)
    s3.put_object(Bucket=S3_BUCKET,Key=OUT_KEY,Body=json.dumps(out,allow_nan=False).encode(),ContentType='application/json',CacheControl='public, max-age=3600')
    # This measurement engine emits no uncalibrated trade or pump alerts.
    return {'statusCode':200,'body':json.dumps({'quality':out['quality'],'n_complexes':out['n_complexes']})}
