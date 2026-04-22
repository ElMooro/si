import json
import boto3
from datetime import datetime

def lambda_handler(event, context):
    """Fixed handler that properly routes EventBridge events"""
    
    # CRITICAL: EventBridge sends detail-type, not 'source'
    if 'detail-type' in event or 'resources' in event:
        # This is from EventBridge - send the report
        return send_report_now()
    
    # Handle direct test invocations
    if event.get('test') or event.get('action') == 'send_daily_report':
        return send_report_now()
    
    # Default API response
    return {'statusCode': 200, 'body': json.dumps('Handler ready')}

def send_report_now():
    """Actually send the email immediately"""
    ses = boto3.client('ses', region_name='us-east-1')
    now = datetime.now()
    
    html = f"""<!DOCTYPE html>
<html>
<body style="background:white;color:black;font-family:Arial;">
<h1>Daily Report - {now.strftime('%Y-%m-%d %H:%M:%S')}</h1>
<p>✅ EventBridge trigger working correctly!</p>
<p>✅ Email sent immediately - no 24hr delay!</p>
<p>Timestamp: {now.isoformat()}</p>
</body>
</html>"""
    
    response = ses.send_email(
        Source='raafouis@gmail.com',
        Destination={'ToAddresses': ['raafouis@gmail.com', 'khalidbernoussi@yahoo.com']},
        Message={
            'Subject': {'Data': f'Daily Report {now.strftime("%m/%d %H:%M")} - IMMEDIATE'},
            'Body': {'Html': {'Data': html}}
        }
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'sent': True,
            'messageId': response['MessageId'],
            'timestamp': now.isoformat()
        })
    }
