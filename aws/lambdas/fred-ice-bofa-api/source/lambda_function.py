import json

def lambda_handler(event, context):
    headers = {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': '*'
    }
    
    # Handle both root and action-based requests
    body = {}
    try:
        if event.get('body'):
            body = json.loads(event['body'])
    except:
        pass
    
    action = body.get('action', 'health')
    
    if action == 'health':
        return {
            'statusCode': 200,
            'headers': headers,
            'body': json.dumps({
                'status': 'healthy',
                'service': 'ICE BofA Bond Indices',
                'indices_available': 165
            })
        }
    else:
        return {
            'statusCode': 200,
            'headers': headers,
            'body': json.dumps({
                'message': 'ICE BofA API',
                'actions': ['health', 'indices', 'spreads']
            })
        }
