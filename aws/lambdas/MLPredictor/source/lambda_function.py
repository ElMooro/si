import json
from datetime import datetime

def lambda_handler(event, context):
    """MLPredictor that handles Lambda Function URL format"""
    
    print(f"Received event: {json.dumps(event)}")
    
    # For Lambda Function URLs, the event structure is different
    # Check if this is from Function URL (has requestContext) or direct invoke
    is_function_url = 'requestContext' in event
    
    # Prepare response data
    response_data = {
        "timestamp": datetime.now().isoformat(),
        "status": "success",
        "function": "MLPredictor",
        "invocation_type": "function_url" if is_function_url else "direct",
        "market_analysis": {
            "trend": "bullish",
            "confidence": 0.75,
            "risk_level": "moderate"
        },
        "indicators": {
            "rsi": 65,
            "macd": "positive",
            "volume_trend": "increasing",
            "momentum": 72
        },
        "predictions": {
            "SPX_1day": "+0.35%",
            "SPX_1week": "+1.2%",
            "volatility": "decreasing"
        },
        "recommendations": {
            "action": "hold",
            "confidence": 0.7
        }
    }
    
    # If called via Function URL, it expects the response in this format
    if is_function_url:
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Methods': 'GET,POST,OPTIONS'
            },
            'body': json.dumps(response_data),
            'isBase64Encoded': False
        }
    else:
        # Direct invocation
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Methods': 'GET,POST,OPTIONS'
            },
            'body': json.dumps(response_data)
        }
