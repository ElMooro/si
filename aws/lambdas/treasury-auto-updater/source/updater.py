import json
import urllib.request
import boto3
from datetime import datetime

lambda_client = boto3.client('lambda')

def lambda_handler(event, context):
    """Auto-update Treasury data and check for crisis"""
    
    # Invoke the main Treasury Lambda to get latest data
    response = lambda_client.invoke(
        FunctionName='treasury-api',
        InvocationType='RequestResponse',
        Payload=json.dumps({'path': '/indicators/all'})
    )
    
    result = json.loads(response['Payload'].read())
    if result.get('statusCode') == 200:
        body = json.loads(result['body'])
        indicators = body.get('indicators', {})
        
        # Check for critical alerts
        alerts = indicators.get('alerts', {}).get('active_warnings', [])
        crisis_level = indicators.get('current_auction', {}).get('crisis_level')
        
        if alerts or crisis_level in ['HIGH', 'SEVERE']:
            print(f"Crisis detected! Level: {crisis_level}, Alerts: {alerts}")
            # Trigger additional actions here
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'status': 'success',
                'updated': datetime.now().isoformat(),
                'crisis_level': crisis_level,
                'alerts': alerts
            })
        }
    
    return {'statusCode': 500, 'body': json.dumps({'status': 'error'})}
