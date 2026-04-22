import json
import urllib.request
import urllib.error
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# Cache configuration - 7 DAYS instead of 5 minutes
CACHE_DURATION = 604800  # 7 days in seconds (7 * 24 * 60 * 60)
cache = {}

def get_cached_or_fetch(series_id, api_key):
    """Get data from cache or fetch from FRED API - ONLY ONCE PER WEEK"""
    now = time.time()
    
    # Check cache
    if series_id in cache:
        cached_data, timestamp = cache[series_id]
        # Only fetch new data if cache is older than 7 days
        if now - timestamp < CACHE_DURATION:
            print(f"Using cached data for {series_id} (age: {(now-timestamp)/3600:.1f} hours)")
            return cached_data
    
    # If we get here, cache is expired or doesn't exist - fetch new data
    print(f"Fetching fresh data for {series_id}")
    
    url = f"https://api.stlouisfed.org/fred/series/observations"
    params = f"?series_id={series_id}&api_key={api_key}&file_type=json&limit=1&sort_order=desc"
    
    try:
        with urllib.request.urlopen(url + params, timeout=10) as response:
            data = json.loads(response.read())
            observations = data.get('observations', [])
            
            if observations and observations[0].get('value', '.') != '.':
                try:
                    value = float(observations[0]['value'])
                    cache[series_id] = (value, now)
                    return value
                except ValueError:
                    return None
    except Exception as e:
        print(f"Error fetching {series_id}: {str(e)}")
        return None
    
    return None

def fetch_indicator(series_id, api_key):
    """Fetch a single indicator"""
    value = get_cached_or_fetch(series_id, api_key)
    return {
        "symbol": series_id,
        "value": value,
        "timestamp": datetime.now().isoformat()
    }

def lambda_handler(event, context):
    """Main Lambda handler with WEEKLY data updates"""
    
    api_key = "2f057499936072679d8843d7fce99989"
    
    # Parse the path from the event
    path = event.get('path', '')
    
    # Define indicator groups (179 total defined, ~143 active)
    DXY_INDICATORS = [
        "DTWEXBGS", "DEXUSEU", "DEXJPUS", "DEXUSUK", "DEXCHUS", 
        "DEXCAUS", "DEXSZUS", "DEXUSAL", "DEXMXUS", "DEXUSNZ",
        "DEXKOUS", "DEXINUS", "DEXBZUS", "DEXSFUS", "DEXSIUS",
        "DEXNOUS", "DEXSDUS", "DEXDNUS", "DEXTAUS", "DEXMAUS",
        "DEXTHUS", "DEXHKUS", "DEXSLUS", "DEXVZUS", "DEXUSEU"
    ]
    
    ICE_BOFA_INDICATORS = [
        "BAMLC0A1CAAA", "BAMLC0A2CAA", "BAMLC0A3CA", "BAMLC0A4CBBB",
        "BAMLH0A0HYM2", "BAMLH0A1HYBB", "BAMLH0A2HYB", "BAMLH0A3HYC",
        "BAMLHYH0", "BAMLC0A0CM", "BAMLC1A0C13Y", "BAMLC2A0C35Y",
        "BAMLC3A0C57Y", "BAMLC4A0C710Y", "BAMLC7A0C1015Y", "BAMLC8A0C15PY",
        "BAMLEMPBUBCRPITRIV", "BAMLEMPGUBCRPITRIV", "BAMLEMPUPUBSLCRPITRIV",
        "BAMLEMPTOTLCRPITRIV", "BAMLEMREARNPUBSLCRPITRIV", "BAMLEMCBPITRIV",
        "BAMLEMHBHYCRPITRIV", "BAMLEMIBHYCRPITRIV"
    ]
    
    FED_INDICATORS = [
        "WALCL", "WSHOSHO", "SWPT", "WLRRAL", "WLODLL", "WDTGAL",
        "WDFOL", "WORAL", "WFCDA", "WIMFSL", "RRPONTSYD", "WTREGEN",
        "RESPPLLOPNWW", "TERMT", "TREAST", "MBST", "WACBS", "OTHLT",
        "RPONTSYD", "WLCFLPCL", "WLCFLL", "WDPSACBW027SBOG", "TOTRESNS",
        "REQRESNS", "EXCSRESNS", "BOGMBASE", "M1SL", "M2SL",
        "WCURRNS", "WIMFAL", "WRESBAL", "H41RESPPALDKNWW", "WSHOMCB"
    ]
    
    BLACK_SWAN_INDICATORS = [
        "VIXCLS", "VXVCLS", "EVZCLS", "VXGDXCLS", "OVXCLS",
        "GVZCLS", "VXSLVCLS", "VXGOGCLS", "VXEWZCLS", "T10Y2Y",
        "T10Y3M", "DFII10", "DFII5", "T5YIE", "T10YIE",
        "TEDRATE", "AAAFF", "BAMLH0A0HYM2", "BAMLH0A3HYC", "DCOILWTICO",
        "GOLDAMGBD228NLBM", "DEXJPUS", "DEXSZUS", "DEXUSEU", "DGS10",
        "DGS2", "DGS5", "DGS30", "MORTGAGE30US", "DPRIME",
        "DTWEXBGS", "DTWEXAFEGS", "DTWEXEMEGS", "STLFSI3", "ANFCI",
        "NFCI", "GVZCLS", "MOVE", "VXEEMCLS", "VXFXICLS",
        "SKEW", "CCC", "WILL5000INDFC", "DSPIC96", "PCE"
    ]
    
    LIQUIDITY_INDICATORS = [
        "SOFR", "EFFR", "OBFR", "AMERIBOR", "TGCR",
        "BGCR", "IORB", "IOER", "DFEDTARU", "DFF",
        "RIFSPFFNB", "RIFSPFFNA", "RIFSPPFAAD90NB", "DTB3", "DTB6",
        "DTB1YR", "DGS1", "DGS3", "DGS5", "DGS7",
        "AAA", "AA", "A", "BBB", "FF",
        "CPFF", "CP3M", "DCPN3M", "DCPF3M", "CPF1M",
        "WRESBAL", "TOTRESNS", "EXCSRESNS", "STDMAACBW027SBOG", "STDSL",
        "ASTLL", "BSTNSL"
    ]
    
    CRISIS_INDICATORS = [
        "DBAA", "DAAA", "DGS10", "T10Y2Y", "TEDRATE",
        "BAMLH0A0HYM2", "VIXCLS", "DEXUSEU", "DTWEXBGS", "STLFSI3",
        "NFCI", "ANFCI", "GFDEBTN", "GFDEGDQ188S", "DDDM01USA156NWDB",
        "DDDI06USA156NWDB", "DODFFSWCMI", "DPSACBW027SBOG"
    ]
    
    STANDARD_INDICATORS = [
        "UNRATE", "CPIAUCSL", "GDP", "VIXCLS", "SP500",
        "DCOILWTICO", "DGS10", "DTWEXBGS", "GOLDAMGBD228NLBM"
    ]
    
    # Route based on path
    if '/fed' in path:
        indicators_to_fetch = FED_INDICATORS
        endpoint_name = "fed"
    elif '/blackswan' in path:
        indicators_to_fetch = BLACK_SWAN_INDICATORS
        endpoint_name = "blackswan"
    elif '/dxy' in path:
        indicators_to_fetch = DXY_INDICATORS
        endpoint_name = "dxy"
    elif '/ice_bofa' in path:
        indicators_to_fetch = ICE_BOFA_INDICATORS
        endpoint_name = "ice_bofa"
    elif '/liquidity' in path:
        indicators_to_fetch = LIQUIDITY_INDICATORS
        endpoint_name = "liquidity"
    elif '/crisis' in path:
        indicators_to_fetch = CRISIS_INDICATORS
        endpoint_name = "crisis"
    elif '/dashboard/overview' in path:
        indicators_to_fetch = STANDARD_INDICATORS
        endpoint_name = "overview"
    elif '/dashboard/mega' in path:
        all_indicators = list(set(
            DXY_INDICATORS + ICE_BOFA_INDICATORS + FED_INDICATORS + 
            BLACK_SWAN_INDICATORS + LIQUIDITY_INDICATORS + 
            CRISIS_INDICATORS + STANDARD_INDICATORS
        ))
        
        # Fetch all with concurrency
        indicators = {}
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(fetch_indicator, symbol, api_key): symbol 
                      for symbol in all_indicators}
            
            for future in as_completed(futures):
                result = future.result()
                if result and result['value'] is not None:
                    indicators[result['symbol']] = result
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'timestamp': datetime.now().isoformat(),
                'total_indicators_available': len(indicators),
                'total_indicators_requested': len(all_indicators),
                'indicators': indicators,
                'cache_info': f"Data updates weekly. Cache age: {len(cache)} items cached"
            })
        }
    elif '/health' in path:
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'status': 'healthy', 
                'timestamp': datetime.now().isoformat(),
                'cache_duration': 'Weekly (7 days)',
                'cached_items': len(cache)
            })
        }
    else:
        return {
            'statusCode': 404,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({'error': 'Endpoint not found'})
        }
    
    # Fetch indicators with concurrency
    indicators = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_indicator, symbol, api_key): symbol 
                  for symbol in indicators_to_fetch}
        
        for future in as_completed(futures):
            result = future.result()
            if result and result['value'] is not None:
                indicators[result['symbol']] = result
    
    # Build response based on endpoint
    response_body = {
        'timestamp': datetime.now().isoformat(),
        'count': len(indicators),
        'indicators': indicators,
        'cache_info': 'Data updates weekly'
    }
    
    # Add analysis for specific endpoints with fixed None handling
    if endpoint_name == "fed":
        fed_assets = indicators.get('WALCL', {}).get('value', None)
        response_body['analysis'] = {
            'balance_sheet_size': fed_assets if fed_assets else 0,
            'qt_progress': "Active" if (fed_assets and fed_assets < 7000000) else "Unknown"
        }
    
    elif endpoint_name == "blackswan":
        # Safely get values with None handling
        vix = indicators.get('VIXCLS', {}).get('value', None)
        ted = indicators.get('TEDRATE', {}).get('value', None)
        stress = indicators.get('STLFSI3', {}).get('value', None)
        
        # Calculate risk with None checks
        high_risk = 0
        medium_risk = 0
        
        if vix is not None:
            if vix > 30:
                high_risk += 1
            elif vix > 20:
                medium_risk += 1
        
        if ted is not None:
            if ted > 1.0:
                high_risk += 1
            elif ted > 0.5:
                medium_risk += 1
        
        if stress is not None:
            if stress > 2:
                high_risk += 1
            elif stress > 0:
                medium_risk += 1
        
        # Determine risk level
        if (vix and vix > 40) or (ted and ted > 1.0) or (stress and stress > 2):
            risk_level = "HIGH"
        elif (vix and vix > 25) or (ted and ted > 0.5) or (stress and stress > 1):
            risk_level = "MEDIUM"
        else:
            risk_level = "LOW"
        
        risk_score = high_risk * 3 + medium_risk * 1
        
        response_body['risk_assessment'] = {
            'level': risk_level,
            'risk_score': risk_score,
            'high_risk_count': high_risk,
            'medium_risk_count': medium_risk
        }
    
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps(response_body)
    }
