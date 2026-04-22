def lambda_handler(event, context):
    import json
    return {
        'statusCode': 200,
        'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
        'body': json.dumps({'status': 'healthy', 'service': 'News Sentiment', 'sentiment': 'neutral'})
    }
