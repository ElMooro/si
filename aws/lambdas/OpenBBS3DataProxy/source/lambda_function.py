import json
import boto3
from datetime import datetime

s3 = boto3.client('s3')
BUCKET_NAME = 'macro-data-lake'

def lambda_handler(event, context):
    """Enhanced S3 proxy with proper data formatting"""
    
    # CORS headers
    headers = {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'GET,POST,OPTIONS'
    }
    
    # Handle OPTIONS
    if event.get('httpMethod') == 'OPTIONS' or event.get('requestContext', {}).get('http', {}).get('method') == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': headers,
            'body': json.dumps('OK')
        }
    
    try:
        # Parse request
        body = event.get('body', '{}')
        if isinstance(body, str):
            request_data = json.loads(body) if body else {}
        else:
            request_data = body
        
        action = request_data.get('action', 'list')
        indicator = request_data.get('indicator', '')
        
        response_data = {}
        
        if action == 'list':
            # List all available indicators with proper structure
            response = s3.list_objects_v2(
                Bucket=BUCKET_NAME,
                Prefix='scraped_data/',
                MaxKeys=1000
            )
            
            files = []
            indicators = {}
            
            if 'Contents' in response:
                for obj in response['Contents']:
                    key = obj['Key']
                    if key == 'scraped_data/':  # Skip directory itself
                        continue
                        
                    files.append({
                        'key': key,
                        'size': obj['Size'],
                        'lastModified': obj['LastModified'].isoformat()
                    })
                    
                    # Extract indicator name from filename
                    parts = key.split('/')
                    if len(parts) > 1:
                        filename = parts[-1]
                        # Handle different filename patterns
                        if filename.endswith('.json'):
                            filename = filename[:-5]  # Remove .json
                        
                        # Extract indicator name
                        if '_' in filename:
                            ind_name = filename.split('_')[0]
                        else:
                            ind_name = filename
                        
                        # Ensure indicator name is valid
                        if ind_name and ind_name != 'scraped':
                            if ind_name not in indicators:
                                indicators[ind_name] = {
                                    'name': ind_name,
                                    'files': [],
                                    'lastUpdate': obj['LastModified'].isoformat()
                                }
                            indicators[ind_name]['files'].append(key)
            
            response_data = {
                'status': 'success',
                'totalFiles': len(files),
                'files': files,
                'indicators': list(indicators.keys()),
                'indicatorDetails': indicators,
                'bucket': BUCKET_NAME
            }
            
        elif action == 'getIndicators':
            # Get formatted indicator list for the dashboard
            response = s3.list_objects_v2(
                Bucket=BUCKET_NAME,
                Prefix='scraped_data/',
                MaxKeys=1000
            )
            
            indicator_map = {}
            
            if 'Contents' in response:
                for obj in response['Contents']:
                    key = obj['Key']
                    if key == 'scraped_data/':
                        continue
                    
                    parts = key.split('/')
                    if len(parts) > 1:
                        filename = parts[-1]
                        if filename.endswith('.json'):
                            filename = filename[:-5]
                        
                        # Extract clean indicator name
                        if filename.startswith('macro_'):
                            continue  # Skip macro files
                        
                        ind_name = filename.split('_')[0] if '_' in filename else filename
                        
                        if ind_name and ind_name not in ['scraped', 'data', '']:
                            if ind_name not in indicator_map:
                                indicator_map[ind_name] = {
                                    'id': ind_name,
                                    'name': ind_name.upper(),
                                    'description': f'{ind_name} indicator data',
                                    'source': 'scraped',
                                    'category': detectCategory(ind_name),
                                    'lastUpdate': obj['LastModified'].isoformat()
                                }
            
            # Add any missing common indicators
            common_indicators = [
                'DGS10', 'DGS2', 'T10Y2Y', 'DXY', 'VIX', 'VIXCLS', 'MOVE',
                'SP500', 'GDP', 'CPIAUCSL', 'UNRATE', 'DFF', 'SOFR',
                'DEXUSEU', 'DEXJPUS', 'DEXUSUK', 'WALCL', 'WRESBAL'
            ]
            
            for ind in common_indicators:
                if ind not in indicator_map:
                    indicator_map[ind] = {
                        'id': ind,
                        'name': ind,
                        'description': f'{ind} indicator',
                        'source': 'pending',
                        'category': detectCategory(ind),
                        'lastUpdate': datetime.now().isoformat()
                    }
            
            response_data = {
                'status': 'success',
                'indicators': list(indicator_map.values()),
                'total': len(indicator_map)
            }
            
        elif action == 'get' and indicator:
            # Get specific indicator data
            response = s3.list_objects_v2(
                Bucket=BUCKET_NAME,
                Prefix=f'scraped_data/{indicator}',
                MaxKeys=10
            )
            
            if 'Contents' in response and response['Contents']:
                latest_file = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)[0]
                
                obj_response = s3.get_object(
                    Bucket=BUCKET_NAME,
                    Key=latest_file['Key']
                )
                
                file_content = obj_response['Body'].read().decode('utf-8')
                
                try:
                    data = json.loads(file_content)
                except:
                    data = {'raw': file_content}
                
                response_data = {
                    'status': 'success',
                    'indicator': indicator,
                    'key': latest_file['Key'],
                    'lastModified': latest_file['LastModified'].isoformat(),
                    'data': data
                }
            else:
                response_data = {
                    'status': 'error',
                    'message': f'No data found for indicator: {indicator}'
                }
                
        else:
            response_data = {
                'status': 'error',
                'message': 'Invalid action. Use: list, getIndicators, or get'
            }
        
        return {
            'statusCode': 200,
            'headers': headers,
            'body': json.dumps(response_data)
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': headers,
            'body': json.dumps({
                'status': 'error',
                'message': str(e),
                'trace': str(e.__class__.__name__)
            })
        }

def detectCategory(indicator):
    """Detect category based on indicator name"""
    if not indicator:
        return 'Other'
    
    ind = indicator.upper()
    
    if any(x in ind for x in ['VIX', 'MOVE', 'SKEW']):
        return 'Volatility'
    elif any(x in ind for x in ['DGS', 'T10Y', 'T2Y', 'SOFR', 'DFF']):
        return 'Rates'
    elif any(x in ind for x in ['DXY', 'DEX', 'USD', 'EUR', 'JPY', 'GBP']):
        return 'Currency'
    elif any(x in ind for x in ['SP', 'SPX', 'NDX', 'DJI']):
        return 'Equity'
    elif any(x in ind for x in ['GDP', 'CPI', 'PCE', 'UNRATE', 'PAYEMS']):
        return 'Economic'
    elif any(x in ind for x in ['WALCL', 'WRESBAL', 'M2']):
        return 'Liquidity'
    elif any(x in ind for x in ['HYG', 'LQD', 'BAML']):
        return 'Credit'
    else:
        return 'Other'
