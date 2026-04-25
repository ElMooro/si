import json
import boto3
from datetime import datetime

def lambda_handler(event, context):
    """Global Liquidity API with CORS headers"""
    
    # CORS headers for ALL responses
    headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
        'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
        'Content-Type': 'application/json'
    }
    
    # Handle OPTIONS preflight
    if event.get('httpMethod') == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': headers,
            'body': json.dumps({'message': 'CORS preflight OK'})
        }
    
    path = event.get('path', '')
    
    try:
        # /liquidity/percentage endpoint
        if 'percentage' in path:
            return {
                'statusCode': 200,
                'headers': headers,
                'body': json.dumps({
                    'percentage': 65,
                    'status': 'NORMAL',
                    'timestamp': datetime.utcnow().isoformat(),
                    'metrics': 272
                })
            }
        
        # Default response
        return {
            'statusCode': 200,
            'headers': headers,
            'body': json.dumps({
                'message': 'Global Liquidity API',
                'endpoints': ['/liquidity/percentage'],
                'status': 'online'
            })
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': headers,
            'body': json.dumps({'error': str(e)})
        }
