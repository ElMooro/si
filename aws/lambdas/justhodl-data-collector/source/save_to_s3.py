import json
import boto3
from datetime import datetime

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Get data from your orchestrator
    import urllib.request
    
    response = urllib.request.urlopen('https://api.justhodl.ai/')
    data = json.loads(response.read())
    
    # Save to S3 with timestamp
    timestamp = datetime.now().strftime('%Y/%m/%d/%H-%M-%S')
    s3.put_object(
        Bucket='justhodl-historical-data-1758485495',
        Key=f'data/{timestamp}.json',
        Body=json.dumps(data)
    )
    
    return {'statusCode': 200}
