import json
import urllib.request
import urllib.parse
from datetime import datetime, timedelta
import boto3
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# Initialize S3 client
s3 = boto3.client('s3')

# Configuration
FRED_API_KEY = os.environ.get('FRED_API_KEY', '2f057499936072679d8843d7fce99989')
S3_BUCKET = os.environ.get('S3_BUCKET', 'macro-data-lake')
CONFIG_FILE = os.environ.get('CONFIG_FILE', 'config/complete_indicators_config.json')

def load_config():
    """Load indicator configuration from S3"""
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=CONFIG_FILE)
        config = json.loads(response['Body'].read())
        print(f"Loaded config from {CONFIG_FILE}")
        return config
    except Exception as e:
        print(f"Error loading config: {str(e)}")
        # Return a minimal config if loading fails
        return {
            'fred': {},
            'ecb': {},
            'oecd': {},
            'nyfed': {},
            'treasury': {}
        }

def safe_request(url, headers=None, timeout=30):
    """Make HTTP request with error handling"""
    try:
        req = urllib.request.Request(url)
        if headers:
            for key, value in headers.items():
                req.add_header(key, value)
        
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return json.loads(response.read().decode('utf-8'))
    except Exception as e:
        print(f"Error fetching {url}: {str(e)}")
        return None

def collect_fred_data(indicators):
    """Collect FRED data"""
    all_data = {}
    base_url = "https://api.stlouisfed.org/fred/series/observations"
    
    for category, series_dict in indicators.items():
        if isinstance(series_dict, dict):
            for series_id, description in series_dict.items():
                url = f"{base_url}?series_id={series_id}&api_key={FRED_API_KEY}&file_type=json&limit=5000&sort_order=desc"
                data = safe_request(url)
                
                if data and 'observations' in data:
                    all_data[series_id] = {
                        'description': description,
                        'source': 'fred',
                        'category': category,
                        'observations': data['observations']
                    }
                    print(f"✓ FRED {series_id}: {len(data['observations'])} observations")
                else:
                    print(f"✗ FRED {series_id}: No data")
                
                time.sleep(0.1)  # Rate limiting
    
    return all_data

def collect_ecb_data(indicators):
    """Collect ECB data with improved parsing"""
    all_data = {}
    base_url = "https://sdw-wsrest.ecb.europa.eu/service/data"
    
    for category, series_dict in indicators.items():
        if isinstance(series_dict, dict):
            for series_id, description in series_dict.items():
                # Parse the series ID to build the correct URL
                # ECB format: DATASET.FREQ.REF_AREA.etc
                parts = series_id.split('.')
                if len(parts) >= 2:
                    dataset = parts[0]
                    key = '.'.join(parts[1:])
                    url = f"{base_url}/{dataset}/{key}?lastNObservations=1000&format=jsondata"
                else:
                    url = f"{base_url}/{series_id}?lastNObservations=1000&format=jsondata"
                
                data = safe_request(url)
                
                if data and 'dataSets' in data and len(data['dataSets']) > 0:
                    try:
                        # Parse ECB's complex JSON structure
                        dataset = data['dataSets'][0]
                        if 'series' in dataset:
                            observations = []
                            
                            # Get time periods from structure
                            time_periods = []
                            if 'structure' in data and 'dimensions' in data['structure']:
                                for dim in data['structure']['dimensions']['observation']:
                                    if dim['id'] == 'TIME_PERIOD':
                                        time_periods = dim['values']
                                        break
                            
                            # Extract observations
                            for series_key, series_data in dataset['series'].items():
                                if 'observations' in series_data:
                                    for time_idx, values in series_data['observations'].items():
                                        if time_periods and int(time_idx) < len(time_periods):
                                            date = time_periods[int(time_idx)]['id']
                                        else:
                                            date = time_idx
                                        
                                        observations.append({
                                            'date': date,
                                            'value': values[0] if isinstance(values, list) else values
                                        })
                            
                            if observations:
                                all_data[series_id] = {
                                    'description': description,
                                    'source': 'ecb',
                                    'category': category,
                                    'observations': sorted(observations, key=lambda x: x['date'], reverse=True)
                                }
                                print(f"✓ ECB {series_id}: {len(observations)} observations")
                            else:
                                print(f"✗ ECB {series_id}: No observations found")
                        else:
                            print(f"✗ ECB {series_id}: No series in dataset")
                    except Exception as e:
                        print(f"✗ ECB {series_id}: Parse error - {str(e)}")
                else:
                    print(f"✗ ECB {series_id}: No data")
                
                time.sleep(0.2)  # Rate limiting for ECB
    
    return all_data

def collect_oecd_data(indicators):
    """Collect OECD data"""
    all_data = {}
    base_url = "https://stats.oecd.org/SDMX-JSON/data"
    
    for category, series_dict in indicators.items():
        if isinstance(series_dict, dict):
            for series_id, description in series_dict.items():
                # Parse OECD series ID (e.g., USA.CLI)
                parts = series_id.split('.')
                if len(parts) >= 2:
                    country = parts[0]
                    indicator = parts[1]
                    
                    # Build OECD URL
                    dataset = "KEI"  # Key Economic Indicators
                    filter_expr = f"{country}.{indicator}.AMPLITUD.LTRENDIDX.M"
                    url = f"{base_url}/{dataset}/{filter_expr}/all?startTime=2020"
                    
                    data = safe_request(url)
                    
                    if data and 'dataSets' in data:
                        try:
                            observations = []
                            dataset = data['dataSets'][0]
                            
                            if 'observations' in dataset:
                                # Get time periods
                                time_dim = data['structure']['dimensions']['observation'][0]['values']
                                
                                for time_idx, values in dataset['observations'].items():
                                    idx = int(time_idx.split(':')[0])
                                    if idx < len(time_dim):
                                        date = time_dim[idx]['id']
                                        observations.append({
                                            'date': date,
                                            'value': values[0]
                                        })
                            
                            if observations:
                                all_data[series_id] = {
                                    'description': description,
                                    'source': 'oecd',
                                    'category': category,
                                    'observations': sorted(observations, key=lambda x: x['date'], reverse=True)
                                }
                                print(f"✓ OECD {series_id}: {len(observations)} observations")
                        except Exception as e:
                            print(f"✗ OECD {series_id}: Parse error - {str(e)}")
                    else:
                        print(f"✗ OECD {series_id}: No data")
                
                time.sleep(0.2)
    
    return all_data

def collect_nyfed_data(indicators):
    """Collect NY Fed data"""
    all_data = {}
    base_url = "https://markets.newyorkfed.org/api"
    
    # Define endpoint mappings
    endpoint_mappings = {
        'SOFR': '/rates/secured/sofr/last/100',
        'EFFR': '/rates/unsecured/effr/last/100',
        'OBFR': '/rates/unsecured/obfr/last/100',
        'REPO_VOLUME': '/repo/all/all/last/100',
        'FAILS_TREASURY': '/pd/fails/get/TREASURIES/latest',
        'FAILS_AGENCY': '/pd/fails/get/AGENCIES/latest',
        'FAILS_MBS': '/pd/fails/get/MBS/latest',
        'SOMA_HOLDINGS': '/soma/summary',
        'REPO_OPERATIONS': '/soma/operations/repo/results/last/100',
        'REVERSE_REPO': '/soma/operations/reverse_repo/results/last/100'
    }
    
    for category, series_dict in indicators.items():
        if isinstance(series_dict, dict):
            for series_id, description in series_dict.items():
                if series_id in endpoint_mappings:
                    url = base_url + endpoint_mappings[series_id]
                    
                    # NY Fed requires specific headers
                    headers = {
                        'Accept': 'application/json',
                        'User-Agent': 'Mozilla/5.0 (compatible; FinancialDataCollector/1.0)'
                    }
                    
                    data = safe_request(url, headers=headers)
                    
                    if data:
                        try:
                            observations = []
                            
                            # Parse different response formats
                            if 'repo' in data:
                                for item in data['repo']:
                                    observations.append({
                                        'date': item.get('effectiveDate', ''),
                                        'value': item.get('totalAmtSubmitted', 0)
                                    })
                            elif 'rates' in data:
                                for item in data['rates']:
                                    observations.append({
                                        'date': item.get('effectiveDate', ''),
                                        'value': item.get('percentRate', 0)
                                    })
                            elif 'fails' in data:
                                for item in data['fails']:
                                    observations.append({
                                        'date': item.get('asOfDate', ''),
                                        'value': item.get('totalFails', 0)
                                    })
                            
                            if observations:
                                all_data[series_id] = {
                                    'description': description,
                                    'source': 'nyfed',
                                    'category': category,
                                    'observations': observations[:100]  # Limit to recent data
                                }
                                print(f"✓ NY Fed {series_id}: {len(observations)} observations")
                        except Exception as e:
                            print(f"✗ NY Fed {series_id}: Parse error - {str(e)}")
                    else:
                        print(f"✗ NY Fed {series_id}: No data")
                
                time.sleep(0.3)  # NY Fed rate limiting
    
    return all_data

def collect_treasury_data(indicators):
    """Collect Treasury data"""
    all_data = {}
    base_url = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
    
    # Treasury API endpoints
    endpoints = {
        'auction_results': '/v1/accounting/od/auctions_query',
        'debt_outstanding': '/v2/accounting/od/debt_to_penny',
        'interest_rates': '/v1/accounting/od/avg_interest_rates'
    }
    
    for category, series_dict in indicators.items():
        if isinstance(series_dict, dict):
            # Collect by category rather than individual series
            if 'auction' in category.lower():
                url = f"{base_url}{endpoints['auction_results']}?format=json&page[size]=100&sort=-auction_date"
                data = safe_request(url)
                
                if data and 'data' in data:
                    # Group by security type
                    security_types = {}
                    for auction in data['data']:
                        sec_type = auction.get('security_type', 'Unknown')
                        if sec_type not in security_types:
                            security_types[sec_type] = []
                        
                        security_types[sec_type].append({
                            'date': auction.get('auction_date', ''),
                            'high_yield': auction.get('high_yield', 0),
                            'bid_to_cover': auction.get('bid_to_cover_ratio', 0)
                        })
                    
                    for sec_type, observations in security_types.items():
                        series_id = f"TREASURY_{sec_type.replace(' ', '_').upper()}"
                        all_data[series_id] = {
                            'description': f"Treasury {sec_type} Auction Results",
                            'source': 'treasury',
                            'category': category,
                            'observations': observations[:50]
                        }
                        print(f"✓ Treasury {series_id}: {len(observations)} observations")
    
    return all_data

def lambda_handler(event, context):
    """Main Lambda handler"""
    print(f"Starting collection at {datetime.utcnow().isoformat()}")
    
    # Load configuration
    config = load_config()
    
    # Initialize results
    all_results = {}
    stats = {
        'total_attempted': 0,
        'total_successful': 0,
        'fred_attempted': 0,
        'fred_successful': 0,
        'ecb_attempted': 0,
        'ecb_successful': 0,
        'oecd_attempted': 0,
        'oecd_successful': 0,
        'nyfed_attempted': 0,
        'nyfed_successful': 0,
        'treasury_attempted': 0,
        'treasury_successful': 0
    }
    
    # Collect from each source
    if 'fred' in config and config['fred']:
        print("\n=== Collecting FRED data ===")
        stats['fred_attempted'] = sum(len(v) for v in config['fred'].values() if isinstance(v, dict))
        fred_data = collect_fred_data(config['fred'])
        stats['fred_successful'] = len(fred_data)
        all_results.update(fred_data)
    
    if 'ecb' in config and config['ecb']:
        print("\n=== Collecting ECB data ===")
        stats['ecb_attempted'] = sum(len(v) for v in config['ecb'].values() if isinstance(v, dict))
        ecb_data = collect_ecb_data(config['ecb'])
        stats['ecb_successful'] = len(ecb_data)
        all_results.update(ecb_data)
    
    if 'oecd' in config and config['oecd']:
        print("\n=== Collecting OECD data ===")
        stats['oecd_attempted'] = sum(len(v) for v in config['oecd'].values() if isinstance(v, dict))
        oecd_data = collect_oecd_data(config['oecd'])
        stats['oecd_successful'] = len(oecd_data)
        all_results.update(oecd_data)
    
    if 'nyfed' in config and config['nyfed']:
        print("\n=== Collecting NY Fed data ===")
        stats['nyfed_attempted'] = sum(len(v) for v in config['nyfed'].values() if isinstance(v, dict))
        nyfed_data = collect_nyfed_data(config['nyfed'])
        stats['nyfed_successful'] = len(nyfed_data)
        all_results.update(nyfed_data)
    
    if 'treasury' in config and config['treasury']:
        print("\n=== Collecting Treasury data ===")
        stats['treasury_attempted'] = sum(len(v) for v in config['treasury'].values() if isinstance(v, dict))
        treasury_data = collect_treasury_data(config['treasury'])
        stats['treasury_successful'] = len(treasury_data)
        all_results.update(treasury_data)
    
    # Calculate totals
    stats['total_attempted'] = sum(v for k, v in stats.items() if k.endswith('_attempted'))
    stats['total_successful'] = sum(v for k, v in stats.items() if k.endswith('_successful'))
    
    # Save to S3
    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    path = f"massive_indicators/{timestamp}/"
    
    # Save data files by source
    for source in ['fred', 'ecb', 'oecd', 'nyfed', 'treasury']:
        source_data = {k: v for k, v in all_results.items() if v.get('source') == source}
        if source_data:
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=f"{path}{source}_data.json",
                Body=json.dumps(source_data),
                ContentType='application/json'
            )
    
    # Save combined data
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=f"{path}all_data.json",
        Body=json.dumps(all_results),
        ContentType='application/json'
    )
    
    # Create and save manifest
    manifest = {
        'timestamp': timestamp,
        'path': path,
        'stats': stats,
        'config_file': CONFIG_FILE,
        'indicators': list(all_results.keys())
    }
    
    manifest_key = f"{path}manifest.json"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=manifest_key,
        Body=json.dumps(manifest, indent=2),
        ContentType='application/json'
    )
    
    print(f"\n=== Collection Summary ===")
    print(f"Total: {stats['total_successful']}/{stats['total_attempted']} successful")
    print(f"FRED: {stats['fred_successful']}/{stats['fred_attempted']}")
    print(f"ECB: {stats['ecb_successful']}/{stats['ecb_attempted']}")
    print(f"OECD: {stats['oecd_successful']}/{stats['oecd_attempted']}")
    print(f"NY Fed: {stats['nyfed_successful']}/{stats['nyfed_attempted']}")
    print(f"Treasury: {stats['treasury_successful']}/{stats['treasury_attempted']}")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Collection completed successfully',
            'manifest_key': manifest_key,
            'stats': stats
        })
    }
