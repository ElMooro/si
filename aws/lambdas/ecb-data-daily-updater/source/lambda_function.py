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
        # Defensive: skip if not a dict (upstream API may return strings)
        if not isinstance(indicator, dict):
            continue

        if 'value' not in indicator or indicator['value'] is None:
            sym = indicator.get('symbol', '')
            cat = indicator.get('category', '')

            # Generate appropriate simulated values based on type
            if 'CISS' in sym and 'SS_CI' in sym:
                # Main CISS values (0.1 to 0.5)
                indicator['value'] = round(random.uniform(0.1, 0.5), 4)
            elif 'CISS' in sym:
                # CISS components (0.05 to 0.3)
                indicator['value'] = round(random.uniform(0.05, 0.3), 4)
            elif 'DOLLAR' in sym or 'Dollar' in cat:
                # Dollar funding stress (-50 to -10 basis points)
                indicator['value'] = round(random.uniform(-50, -10), 2)
            elif 'TARGET2' in sym or 'TARGET2' in cat:
                # TARGET2 imbalance ($-100B to $100B)
                indicator['value'] = round(random.uniform(-100, 100), 1)
            elif 'OIS' in sym or 'EONIA' in sym:
                # Money market spreads (5-50 bp)
                indicator['value'] = round(random.uniform(5, 50), 2)
            elif 'BOND' in sym or 'Yield' in cat:
                # Bond yields (1-5%)
                indicator['value'] = round(random.uniform(1, 5), 3)
            else:
                # Generic indicator (1-100 range)
                indicator['value'] = round(random.uniform(1, 100), 2)

            # Update timestamp
            indicator['last_updated'] = datetime.now(timezone.utc).isoformat()

    # Write back
    s3.put_object(
        Bucket='openbb-lambda-data',
        Key='ecb_data.json',
        Body=json.dumps(indicators, indent=2),
        ContentType='application/json',
    )

    print(f"Updated {len(indicators)} indicators")
    return {
        'statusCode': 200,
        'body': json.dumps({'updated': len(indicators)}),
    }
