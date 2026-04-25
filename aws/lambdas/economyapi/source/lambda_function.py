import json
import urllib.request
import urllib.parse
from datetime import datetime, timedelta

def lambda_handler(event, context):
    print(f"Event keys: {list(event.keys())}")
    
    try:
        # Extract path and query parameters from API Gateway event
        path = event.get('rawPath', '/')
        query_params = event.get('queryStringParameters') or {}
        
        print(f"Path: {path}, Params: {query_params}")
        
        response_body = handle_request(path, query_params)
        
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Content-Type': 'application/json'
            },
            'body': json.dumps(response_body, default=str)
        }
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Content-Type': 'application/json'
            },
            'body': json.dumps({
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            })
        }

def handle_request(path, query_params):
    FRED_API_KEY = "2f057499936072679d8843d7fce99989"
    
    # Complete list of all economic indicators from your document
    ECONOMIC_INDICATORS = {
        # Oil & Energy
        'DCOILWTICO': 'WTI Crude Oil Price',
        # Volatility Indices
        'VXVCLS': 'VIX of VIX',
        'VIXCLS': 'VIX Index',
        # Employment
        'UNRATE': 'Unemployment Rate',
        # Exchange Rates
        'DEXSFUS': 'USD/South African Rand',
        'DEXMXUS': 'USD/Mexican Peso', 
        'DEXKOUS': 'USD/Korean Won',
        'DEXJPUS': 'USD/JPY Exchange Rate',
        'DEXINUS': 'USD/Indian Rupee',
        'DEXUSUK': 'USD/GBP Exchange Rate',
        'DEXUSEU': 'USD/EUR Exchange Rate',
        'DEXCHUS': 'USD/CNY Exchange Rate',
        'DEXSZUS': 'USD/CHF Exchange Rate',
        'DEXCAUS': 'USD/CAD Exchange Rate',
        'DEXBZUS': 'USD/Brazilian Real',
        'DEXUSAL': 'USD/AUD Exchange Rate',
        # Treasury Securities (from email list)
        'TREAST': 'Treasury Securities Held Outright - All',
        'SWPT': 'Central Bank Liquidity Swaps',
        'TREAS10Y': 'Treasury Securities - Over 10 Years',
        'TREAS1T5': 'Treasury Securities - 1 to 5 Years',
        'TREAS1590': 'Treasury Securities - 16 to 90 Days',
        'TREAS5T10': 'Treasury Securities - 5 to 10 Years',
        'MBS10Y': 'Mortgage-Backed Securities - Over 10 Years',
        'TREAS911Y': 'Treasury Securities - 91 Days to 1 Year',
        'TREAS15': 'Treasury Securities - Within 15 Days',
        'RESPPALGUONNWW': 'Treasury Notes and Bonds',
        'TERMT': 'Term Deposits by Depository Institutions',
        'RESPPALGUOXCH1NWW': 'Treasury Securities - Weekly Change',
        'SWP1690': 'Central Bank Liquidity Swaps - 16 to 90 Days',
        'FEDD5T10': 'Federal Agency Debt - 5 to 10 Years',
        'H41RESPPLLDENWW': 'Deposits by Depository Institutions',
        'OTHL1690': 'Liquidity Facilities Loans - 16 to 90 Days',
        'SWP15': 'Central Bank Liquidity Swaps - Within 15 Days',
        'OTHL15': 'Liquidity Facilities Loans - Within 15 Days',
        'OTHL1T5': 'Liquidity Facilities Loans - 1 to 5 Years',
        'RESPPALGUOXAWXCH52NWW': 'Treasury Securities - Year Over Year Change',
        'REP1690': 'Repurchase Agreements - 16 to 90 Days',
        'RESPPALGUMD16T90XCH1NWW': 'Treasury Securities 16-90 Days - Weekly Change',
        'OTHL91T1Y': 'Liquidity Facilities Loans - 91 Days to 1 Year',
        'RESPPALGUMXCH1NWW': 'Treasury Securities All - Weekly Change',
        'RESH4FXAWXCH52NWW': 'Foreign Custody Holdings - Year Over Year',
        # Industrial Production
        'INDPRO': 'Industrial Production - Total',
        'IPMAN': 'Manufacturing Output Index',
        'IPDMAN': 'Durable Goods Output Index',
        'IPG339S': 'Durable Goods - Miscellaneous',
        'MANEMP': 'Manufacturing Employment',
        # OECD Leading Indicators  
        'USALOLITOAASTSAM': 'OECD Leading Indicator - US',
        'G7LOLITOAASTSAM': 'OECD Leading Indicator - G7',
        'CHNLOLITOAASTSAM': 'OECD Leading Indicator - China',
        'GBRLOLITOAASTSAM': 'OECD Leading Indicator - UK',
        # Manufacturing & Capacity
        'TCU': 'Total Capacity Utilization',
        'IPB50001N': 'Total Industrial Production (NSA)',
        'CAPB50001SQ': 'Industrial Capacity (Quarterly)',
        # Unemployment Demographics
        'LNS14000006': 'Black/African American Unemployment (SA)',
        'LNU04000006': 'Black/African American Unemployment (NSA)',
        'LNS14000031': 'Black/African American Men 20+ Unemployment (SA)',
        'LNU04000031': 'Black/African American Men 20+ Unemployment (NSA)',
        'LNS14000009': 'Hispanic/Latino Unemployment (SA)',
        'LNU04000009': 'Hispanic/Latino Unemployment (NSA)',
        'LNU04000034': 'Hispanic/Latino Men 20+ Unemployment (NSA)',
        'UNEMPLOY': 'Unemployment Level (Count)',
        'LNS14000024': 'Unemployment Rate 20+ Years',
        'U5RATE': 'U-5 Unemployment Rate',
        'U6RATE': 'U-6 Unemployment Rate',
        'UEMPMEAN': 'Average Weeks Unemployed',
        # OECD International Unemployment
        'LRHUTTTTUSM156S': 'OECD US Unemployment Rate',
        'LRUN64TTUSQ156S': 'OECD US Unemployment 15-64 (Quarterly)',
        'LRUN24TTUSM156S': 'OECD US Unemployment 15-24',
        'LRHUTTTTGBM156S': 'OECD UK Unemployment Rate',
        'LRUNTTTTCAQ156S': 'OECD Canada Unemployment (Quarterly)',
        'LRHUTTTTGRM156S': 'OECD Greece Unemployment Rate',
        'LRUN64TTGRA156N': 'OECD Greece Unemployment 15-64 (Annual)'
    }
    
    if path == '/' or path == '/health':
        return {
            'status': 'healthy',
            'service': 'economyapi',
            'indicators_available': len(ECONOMIC_INDICATORS),
            'timestamp': datetime.utcnow().isoformat()
        }
    
    elif path == '/indicators':
        return {
            'indicators': ECONOMIC_INDICATORS,
            'total_count': len(ECONOMIC_INDICATORS),
            'timestamp': datetime.utcnow().isoformat()
        }
    
    elif path == '/economy':
        series_id = query_params.get('series', 'summary')
        
        if series_id == 'summary':
            # Get key summary indicators
            summary_indicators = [
                'DCOILWTICO',  # Oil
                'VIXCLS',      # VIX
                'UNRATE',      # Unemployment
                'DEXUSEU',     # EUR/USD
                'DEXJPUS',     # USD/JPY
                'TREAST',      # Treasury Holdings
                'INDPRO',      # Industrial Production
                'IPMAN'        # Manufacturing
            ]
            
            result_data = {}
            for fred_id in summary_indicators:
                if fred_id in ECONOMIC_INDICATORS:
                    try:
                        data = fetch_fred_data_with_changes(fred_id, FRED_API_KEY, 10)
                        if data:
                            result_data[fred_id] = {
                                'description': ECONOMIC_INDICATORS[fred_id],
                                'data': data
                            }
                    except Exception as e:
                        result_data[fred_id] = {'error': str(e)}
            
            return {
                'series_count': len(result_data),
                'data': result_data,
                'timestamp': datetime.utcnow().isoformat()
            }
        
        elif series_id == 'all':
            # Get all indicators (limit to prevent timeout)
            result_data = {}
            count = 0
            for fred_id, description in ECONOMIC_INDICATORS.items():
                if count >= 20:  # Limit to prevent timeout
                    break
                try:
                    data = fetch_fred_data_with_changes(fred_id, FRED_API_KEY, 5)
                    if data:
                        result_data[fred_id] = {
                            'description': description,
                            'data': data
                        }
                        count += 1
                except Exception as e:
                    continue
            
            return {
                'series_count': len(result_data),
                'data': result_data,
                'timestamp': datetime.utcnow().isoformat(),
                'note': 'Showing first 20 indicators to prevent timeout'
            }
        
        elif series_id in ECONOMIC_INDICATORS:
            # Get specific indicator
            data = fetch_fred_data_with_changes(series_id, FRED_API_KEY, 50)
            return {
                'series_id': series_id,
                'description': ECONOMIC_INDICATORS[series_id],
                'data': data,
                'timestamp': datetime.utcnow().isoformat()
            }
        
        else:
            return {
                'error': 'Invalid series_id',
                'available_series': list(ECONOMIC_INDICATORS.keys())[:10],
                'total_available': len(ECONOMIC_INDICATORS)
            }
    
    return {
        'error': 'Path not found',
        'path': path,
        'available_endpoints': ['/', '/health', '/indicators', '/economy']
    }

def fetch_fred_data_with_changes(series_id, api_key, limit=50):
    try:
        url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={api_key}&file_type=json&limit={limit}&sort_order=desc"
        
        with urllib.request.urlopen(url, timeout=30) as response:
            data = json.loads(response.read().decode())
        
        if 'observations' not in data:
            return None
        
        # Filter valid observations
        valid_obs = []
        for obs in data['observations']:
            if obs['value'] != '.' and obs['value'] is not None:
                try:
                    valid_obs.append({
                        'date': obs['date'],
                        'value': float(obs['value'])
                    })
                except:
                    continue
        
        if not valid_obs:
            return None
        
        # Sort by date (newest first)
        valid_obs.sort(key=lambda x: x['date'], reverse=True)
        
        # Calculate percentage changes for recent data
        result = []
        for i, obs in enumerate(valid_obs):
            current_value = obs['value']
            pct_changes = {}
            
            # Week change (approximate - 5 data points back)
            if i + 5 < len(valid_obs):
                week_value = valid_obs[i + 5]['value']
                pct_changes['week_change'] = round(((current_value - week_value) / week_value) * 100, 2) if week_value else None
            
            # Month change (approximate - 20 data points back)
            if i + 20 < len(valid_obs):
                month_value = valid_obs[i + 20]['value']
                pct_changes['month_change'] = round(((current_value - month_value) / month_value) * 100, 2) if month_value else None
            
            # Quarter change (approximate - 60 data points back)
            if i + 60 < len(valid_obs):
                quarter_value = valid_obs[i + 60]['value']
                pct_changes['quarter_change'] = round(((current_value - quarter_value) / quarter_value) * 100, 2) if quarter_value else None
            
            # Year change (approximate - 250 data points back)
            if i + 250 < len(valid_obs):
                year_value = valid_obs[i + 250]['value']
                pct_changes['year_change'] = round(((current_value - year_value) / year_value) * 100, 2) if year_value else None
            
            result.append({
                'date': obs['date'],
                'value': current_value,
                'percentage_changes': pct_changes
            })
        
        return result[:limit]  # Return only requested number of points
        
    except Exception as e:
        print(f"Error fetching {series_id}: {str(e)}")
        return None
