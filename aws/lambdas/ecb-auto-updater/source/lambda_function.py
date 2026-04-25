import json
import boto3
import urllib.request
import urllib.error
from datetime import datetime
import time
import ssl

s3 = boto3.client('s3')
BUCKET = 'openbb-lambda-data'
KEY = 'ecb_data.json'

def lambda_handler(event, context):
    """Complete ECB CISS updater with proper ECB SDW API calls"""
    
    print(f"Starting CISS subindices update at {datetime.now().isoformat()}")
    
    # Load existing data
    try:
        response = s3.get_object(Bucket=BUCKET, Key=KEY)
        existing_data = json.loads(response['Body'].read())
        print(f"Loaded {len(existing_data)} existing indicators")
    except Exception as e:
        print(f"Could not load existing data: {e}")
        existing_data = {}
    
    updated_data = existing_data.copy()
    update_count = 0
    
    # Create SSL context to handle certificates
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    # Euro Area aggregate CISS components (U2 = Euro Area changing composition)
    # These are the actual series keys from ECB
    ciss_components = [
        {
            'key': 'CISS.D.U2.Z0Z.4F.EC.BON_CI.IDX',
            'symbol': 'ECB.CISS.D.U2.BON_CI',
            'name': 'CISS Bond Market Subindex (Euro Area Daily)'
        },
        {
            'key': 'CISS.D.U2.Z0Z.4F.EC.FX_CI.IDX',
            'symbol': 'ECB.CISS.D.U2.FX_CI',
            'name': 'CISS Foreign Exchange Market Subindex (Euro Area Daily)'
        },
        {
            'key': 'CISS.D.U2.Z0Z.4F.EC.MMS_CI.IDX',
            'symbol': 'ECB.CISS.D.U2.MMS_CI',
            'name': 'CISS Money Market Subindex (Euro Area Daily)'
        },
        {
            'key': 'CISS.D.U2.Z0Z.4F.EC.EQU_CI.IDX',
            'symbol': 'ECB.CISS.D.U2.EQU_CI',
            'name': 'CISS Equity Market Subindex (Euro Area Daily)'
        },
        {
            'key': 'CISS.D.U2.Z0Z.4F.EC.FII_CI.IDX',
            'symbol': 'ECB.CISS.D.U2.FII_CI',
            'name': 'CISS Financial Intermediaries Subindex (Euro Area Daily)'
        },
        {
            'key': 'CISS.D.U2.Z0Z.4F.EC.COR_CI.IDX',
            'symbol': 'ECB.CISS.D.U2.COR_CI',
            'name': 'CISS Cross-Subindex Correlations (Euro Area Daily)'
        },
        {
            'key': 'CISS.D.U2.Z0Z.4F.EC.CISS_CI.IDX',
            'symbol': 'ECB.CISS.D.U2.MAIN',
            'name': 'Composite Indicator of Systemic Stress Main (Euro Area Daily)'
        }
    ]
    
    # Fetch each component
    for component in ciss_components:
        try:
            # Use the ECB SDW API with correct path
            series_key = component['key'].replace('.', '/')
            url = f"https://sdw-wsrest.ecb.europa.eu/service/data/{series_key}"
            params = "?format=jsondata&detail=dataonly&lastNObservations=250"
            full_url = url + params
            
            print(f"Fetching: {component['name']}")
            print(f"URL: {full_url}")
            
            headers = {
                'Accept': 'application/json',
                'User-Agent': 'Mozilla/5.0'
            }
            
            req = urllib.request.Request(full_url, headers=headers)
            
            try:
                with urllib.request.urlopen(req, timeout=30, context=ssl_context) as response:
                    data = json.loads(response.read())
                    
                    # Parse the response
                    if 'dataSets' in data and data['dataSets']:
                        dataset = data['dataSets'][0]
                        if 'series' in dataset and dataset['series']:
                            # Get first series
                            series_key_in_data = list(dataset['series'].keys())[0]
                            series_data = dataset['series'][series_key_in_data]
                            observations = series_data.get('observations', {})
                            
                            # Get dates from structure
                            dates = []
                            if 'structure' in data:
                                structure = data['structure']
                                if 'dimensions' in structure:
                                    obs_dim = structure['dimensions'].get('observation', [])
                                    if obs_dim and 'values' in obs_dim[0]:
                                        dates = [v['id'] for v in obs_dim[0]['values']]
                            
                            # Build historical data
                            historical = []
                            for idx_str, values in observations.items():
                                try:
                                    idx = int(idx_str)
                                    if idx < len(dates) and values:
                                        historical.append({
                                            'date': dates[idx],
                                            'value': float(values[0])
                                        })
                                except (ValueError, IndexError):
                                    continue
                            
                            # Sort by date
                            historical.sort(key=lambda x: x['date'])
                            
                            if historical:
                                latest = historical[-1]
                                
                                # Store the data
                                updated_data[component['symbol']] = {
                                    'symbol': component['symbol'],
                                    'name': component['name'],
                                    'value': latest['value'],
                                    'date': latest['date'],
                                    'frequency': 'Daily',
                                    'dataset': 'CISS',
                                    'observations': len(historical),
                                    'historical_data': historical[-250:],  # Keep last 250 days
                                    'last_updated': datetime.now().isoformat()
                                }
                                update_count += 1
                                print(f"✅ Updated {component['name']}: {latest['value']:.6f}")
                            else:
                                print(f"⚠️ No observations found for {component['name']}")
                    else:
                        print(f"⚠️ No datasets in response for {component['name']}")
                        
            except urllib.error.HTTPError as e:
                print(f"HTTP Error for {component['name']}: {e.code} - {e.reason}")
            except urllib.error.URLError as e:
                print(f"URL Error for {component['name']}: {e.reason}")
                
        except Exception as e:
            print(f"Error updating {component['name']}: {str(e)}")
    
    # Try to fetch SovCISS (Sovereign Stress Indicator)
    try:
        print("Fetching SovCISS...")
        url = "https://sdw-wsrest.ecb.europa.eu/service/data/CISS/M.U2.Z0Z.4F.EC.SOVCISS_CI.IDX"
        params = "?format=jsondata&detail=dataonly&lastNObservations=120"
        
        req = urllib.request.Request(url + params, headers={'Accept': 'application/json', 'User-Agent': 'Mozilla/5.0'})
        
        with urllib.request.urlopen(req, timeout=30, context=ssl_context) as response:
            data = json.loads(response.read())
            
            if 'dataSets' in data and data['dataSets']:
                dataset = data['dataSets'][0]
                if 'series' in dataset and dataset['series']:
                    series_key = list(dataset['series'].keys())[0]
                    series_data = dataset['series'][series_key]
                    observations = series_data.get('observations', {})
                    
                    # Get dates
                    dates = []
                    if 'structure' in data and 'dimensions' in data['structure']:
                        obs_dim = data['structure']['dimensions'].get('observation', [])
                        if obs_dim and 'values' in obs_dim[0]:
                            dates = [v['id'] for v in obs_dim[0]['values']]
                    
                    historical = []
                    for idx_str, values in observations.items():
                        try:
                            idx = int(idx_str)
                            if idx < len(dates) and values:
                                historical.append({
                                    'date': dates[idx],
                                    'value': float(values[0])
                                })
                        except (ValueError, IndexError):
                            continue
                    
                    historical.sort(key=lambda x: x['date'])
                    
                    if historical:
                        latest = historical[-1]
                        updated_data['ECB.SOVCISS'] = {
                            'symbol': 'ECB.SOVCISS',
                            'name': 'Composite Indicator of Sovereign Stress (Euro Area Monthly)',
                            'value': latest['value'],
                            'date': latest['date'],
                            'frequency': 'Monthly',
                            'dataset': 'SOVCISS',
                            'observations': len(historical),
                            'historical_data': historical[-120:],
                            'last_updated': datetime.now().isoformat()
                        }
                        update_count += 1
                        print(f"✅ Updated SovCISS: {latest['value']:.6f}")
                        
    except Exception as e:
        print(f"Error updating SovCISS: {str(e)}")
    
    # Keep all existing indicators (country CISS, unemployment, etc.)
    print(f"Preserving existing indicators...")
    
    # Save to S3
    try:
        s3.put_object(
            Bucket=BUCKET,
            Key=KEY,
            Body=json.dumps(updated_data),
            ContentType='application/json'
        )
        print(f"✅ Successfully saved {len(updated_data)} indicators to S3 ({update_count} updated)")
    except Exception as e:
        print(f"Error saving to S3: {e}")
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'CISS subindices update completed',
            'indicators_updated': update_count,
            'total_indicators': len(updated_data),
            'timestamp': datetime.now().isoformat()
        })
    }
