import math
from concurrent.futures import ThreadPoolExecutor
import json
import boto3
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from decimal import Decimal
import _fred_shim  # noqa: F401  — cache-first FRED + 429 backoff (ops/1074)
from managed_secret import managed_secret  # audit 2026-09-08 INST-06: no literal credentials

def lambda_handler(event, context):
    """Manufacturing Agent - Global Manufacturing Intelligence"""
    
    fred_key = managed_secret(('fred_key', 'FRED_API_KEY', 'FRED_KEY'), ("/justhodl/fred/api-key",))
    
    # Complete manufacturing indicators
    manufacturing_indicators = {
        # ops 5112: the ISM (NAPM*) series were withdrawn from FRED years ago -- every id answered 400 on
        # every run; removed. ISM is licensed data and has no free official mirror; the regional Fed
        # surveys below are the live manufacturing-survey inputs.
        # ops 5111: ISM export/import (NAPMEI/NAPMII) are no longer on FRED (400) -- removed, not faked
        
        # Regional Fed Manufacturing Surveys
        # ops 5111: ids re-verified live on FRED (2026-08 prints); the dead ids answered 400 every run
        'EMPIRE_STATE': 'GACDISA066MSFRBNY',
        'PHILLY_FED': 'GACDFSA066MSFRBPHI',
        'DALLAS_FED': 'BACTSAMFRBDAL',
        # RICHMOND_FED (RMTSPL) and KANSAS_CITY_FED (KCLFEDFAB/KCFMCI) have no live FRED series -- removed
        
        # Industrial Production
        'INDUSTRIAL_PRODUCTION': 'INDPRO',
        'CAPACITY_UTILIZATION': 'TCU',
        'MANUFACTURING_PRODUCTION': 'IPMAN',
        'DURABLE_GOODS': 'IPDMAN',
        'NONDURABLE_GOODS': 'IPNMAN',
        'BUSINESS_EQUIPMENT': 'IPBUSEQ',
        'CONSUMER_GOODS': 'IPCONGD',
        'MATERIALS': 'IPMAT',
        
        # Global Manufacturing PMIs
        # ops 5111: CHEFMNM156N answers 400; the OECD MEI production mirrors (EA19/JPN/GBR/DEU PRMNTO01IXOBM)
        # stopped in 2023-10 / 2024-03 and are not PMIs -- removed rather than shown as current
        'FRANCE_MANUFACTURING_PRODUCTION_INDEX': 'FRAPRMNTO01IXOBM',
        
        # Manufacturing Employment
        'MANUFACTURING_EMPLOYMENT': 'MANEMP',
        'MANUFACTURING_HOURS': 'AWHMAN',
        'MANUFACTURING_EARNINGS': 'CES3000000003',
        'MANUFACTURING_OVERTIME': 'CES3000000004',
        
        # Orders and Inventories
        'DURABLE_GOODS_ORDERS': 'DGORDER',
        'NEW_ORDERS': 'NEWORDER',
        'UNFILLED_ORDERS': 'AMTUNO',
        'INVENTORIES_TO_SALES': 'ISRATIO',
        'MANUFACTURING_INVENTORIES': 'MNFCTRIMSA'
    }
    
    def fetch(item):
        name,series_id=item
        try:
            params={'series_id':series_id,'api_key':fred_key,'file_type':'json','limit':90,'sort_order':'desc'}
            url='https://api.stlouisfed.org/fred/series/observations?'+urllib.parse.urlencode(params)
            with urllib.request.urlopen(url,timeout=20) as response:
                data=json.loads(response.read())
            return name,measure_monthly(name,series_id,data.get('observations',[]))
        except Exception as exc:
            return name,{'current':None,'date':None,'changes':{},'signal':'UNAVAILABLE','source_id':series_id,
                         'quality':{'status':'unavailable','reason':type(exc).__name__}}
    with ThreadPoolExecutor(max_workers=5) as pool:
        results=dict(pool.map(fetch,manufacturing_indicators.items()))
    analysis=analyze_global_manufacturing(results)
    now=datetime.now(timezone.utc).isoformat()
    response_body={
        'timestamp':now,'generated_at':now,'engine':'manufacturing-global-agent','version':'2.0',
        'methodology_version':'manufacturing-measurement.v2','call':None,'execution_eligible':False,
        'us_manufacturing':{k:v for k,v in results.items() if k in ('EMPIRE_STATE','PHILLY_FED','DALLAS_FED')},
        'global_manufacturing':{k:v for k,v in results.items() if k.startswith('FRANCE_')},
        'industrial_production':{k:v for k,v in results.items() if 'PRODUCTION' in k or 'CAPACITY' in k},
        'orders_inventories':{k:v for k,v in results.items() if 'ORDER' in k or 'INVENTOR' in k},
        'employment':{k:v for k,v in results.items() if k in ('MANUFACTURING_EMPLOYMENT','MANUFACTURING_HOURS','MANUFACTURING_EARNINGS','MANUFACTURING_OVERTIME')},
        'ecb_data':{'note':'No ECB manufacturing feed in this engine','eurozone_sentiment':None},
        'analysis':analysis,'recommendations':[],
        'quality':{'status':'fresh' if any(v.get('quality',{}).get('status')=='fresh' for v in results.values()) else 'unavailable',
                   'fresh_series':sum(v.get('quality',{}).get('status')=='fresh' for v in results.values()),
                   'total_series':len(results),'global_pmi_status':'unavailable'},
        'metric_note':'Production indices are not PMI. Regional Fed survey balances use zero, not 50, as the reference. '
                      'Monthly changes use calendar months. No probability or investment recommendation is calibrated.'}
    boto3.client('s3',region_name='us-east-1').put_object(
        Bucket='justhodl-dashboard-live',Key='data/manufacturing.json',
        Body=json.dumps(response_body,allow_nan=False).encode(),ContentType='application/json',CacheControl='public, max-age=3600')

    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps(response_body, cls=DecimalEncoder)
    }

def measure_monthly(name,series_id,observations):
    values={}
    for row in observations:
        try:
            v=float(row['value']);d=row['date'];datetime.strptime(d,'%Y-%m-%d')
            if math.isfinite(v):values[d[:7]]=(d,v)
        except (ValueError,TypeError,KeyError):pass
    if not values:
        return {'current':None,'date':None,'changes':{},'signal':'UNAVAILABLE','source_id':series_id,'quality':{'status':'unavailable'}}
    ym=max(values);d,value=values[ym];year,month=map(int,ym.split('-'))
    age=(datetime.now(timezone.utc).date()-datetime.strptime(d,'%Y-%m-%d').date()).days
    status='invalid' if age<0 else 'stale' if age>100 else 'fresh'
    unit=('survey_balance_pct' if name in ('EMPIRE_STATE','PHILLY_FED','DALLAS_FED') else
          'percent' if name=='CAPACITY_UTILIZATION' else 'ratio' if name=='INVENTORIES_TO_SALES' else
          'index_source_base' if any(k in name for k in ('PRODUCTION','DURABLE_GOODS','BUSINESS_EQUIPMENT','CONSUMER_GOODS','MATERIALS')) and 'ORDERS' not in name else
          'USD_per_hour' if name=='MANUFACTURING_EARNINGS' else
          'hours' if name in ('MANUFACTURING_HOURS','MANUFACTURING_OVERTIME') else
          'thousands_of_employees' if name=='MANUFACTURING_EMPLOYMENT' else 'USD_millions')
    changes={}
    index=year*12+month-1
    for n in (1,3):
        ix=index-n;previous=values.get(f'{ix//12:04d}-{ix%12+1:02d}')
        changes[f'{n}M']=round(value-previous[1],4) if previous and status=='fresh' else None
    signal=interpret_manufacturing_signal(name,value) if status=='fresh' else 'UNAVAILABLE'
    return {'current':value if status=='fresh' else None,'last_observed_value':value,'date':d,
            'source_id':series_id,'unit':unit,'frequency':'monthly','changes':changes,
            'change_unit':unit,'signal':signal,'quality':{'status':status,'age_days':age,'max_age_days':100},
            'score_eligible':signal not in ('CHECK_DATA','OBSERVATION_ONLY','UNAVAILABLE')}


def interpret_manufacturing_signal(name,value):
    if not isinstance(value,(int,float)) or not math.isfinite(value):return 'CHECK_DATA'
    if 'PMI' in name or 'ISM' in name:
        if not 0<=value<=100:return 'CHECK_DATA'
        return 'EXPANSION' if value>50 else 'CONTRACTION' if value<50 else 'UNCHANGED'
    if name in ('EMPIRE_STATE','PHILLY_FED','DALLAS_FED'):
        if not -100<=value<=100:return 'CHECK_DATA'
        return 'POSITIVE_BALANCE' if value>0 else 'NEGATIVE_BALANCE' if value<0 else 'ZERO_BALANCE'
    return 'OBSERVATION_ONLY'


def analyze_global_manufacturing(data):
    usable={k:v for k,v in data.items() if v.get('quality',{}).get('status')=='fresh'
            and v.get('signal') not in ('CHECK_DATA','UNAVAILABLE','OBSERVATION_ONLY')}
    ism=usable.get('ISM_COMPOSITE',{}).get('current')
    return {'us_cycle':interpret_manufacturing_signal('ISM_COMPOSITE',ism) if ism is not None else 'UNKNOWN',
            'global_status':'UNAVAILABLE','ism_level':ism,'expansion_probability':None,'recession_risk':'UNKNOWN',
            'regional_surveys':{k:v['signal'] for k,v in usable.items() if k in ('EMPIRE_STATE','PHILLY_FED','DALLAS_FED')},
            'excluded_from_composites':[k for k in data if k not in usable],
            'note':'Regional surveys are separate observations. Production indices are not PMI and uncalibrated series do not vote.'}


def generate_manufacturing_recommendations(analysis):
    return []


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)
