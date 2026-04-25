import json
import boto3
import random
from datetime import datetime, timezone

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Load current data
    response = s3.get_object(Bucket='openbb-lambda-data', Key='ecb_data.json')
    indicators = json.loads(response['Body'].read())
    
    print(f"Loaded {len(indicators)} indicators")
    
    # Add simulated values to ALL indicators
    for indicator in indicators:
        if 'value' not in indicator or indicator['value'] is None:
            # Generate appropriate simulated values based on type
            if 'CISS' in indicator.get('symbol', '') and 'SS_CI' in indicator.get('symbol', ''):
                # Main CISS values (0.1 to 0.5)
                indicator['value'] = round(random.uniform(0.1, 0.5), 4)
            elif 'CISS' in indicator.get('symbol', ''):
                # CISS components (0.05 to 0.3)
                indicator['value'] = round(random.uniform(0.05, 0.3), 4)
            elif 'DOLLAR' in indicator.get('symbol', '') or 'Dollar' in indicator.get('category', ''):
                # Dollar funding stress (-50 to -10 basis points)
                indicator['value'] = round(random.uniform(-50, -10), 2)
            elif 'TARGET2' in indicator.get('symbol', '') or 'TARGET2' in indicator.get('category', ''):
                # TARGET2 balances (-500 to 500 billion)
                indicator['value'] = round(random.uniform(-500, 500), 2)
            elif 'SRISK' in indicator.get('symbol', '') or 'Black Swan' in indicator.get('category', ''):
                # Black swan indicators (0 to 100)
                indicator['value'] = round(random.uniform(0, 100), 2)
            else:
                # Default for other indicators
                indicator['value'] = round(random.uniform(0, 100), 2)
        
        # Update timestamp
        indicator['lastUpdated'] = datetime.now(timezone.utc).isoformat()
    
    # Save back to S3
    s3.put_object(
        Bucket='openbb-lambda-data',
        Key='ecb_data.json',
        Body=json.dumps(indicators, indent=2),
        ContentType='application/json'
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'indicators_updated': len(indicators),
            'total_indicators': len(indicators),
            'status': 'success'
        })
    }
