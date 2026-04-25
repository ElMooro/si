import boto3
import json

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('WebSocketConnections')
api_gateway = boto3.client('apigatewaymanagementapi')

def lambda_handler(event, context):
    connection_id = event['requestContext']['connectionId']
    event_type = event['requestContext']['eventType']
    
    if event_type == 'CONNECT':
        table.put_item(Item={'connectionId': connection_id})
        return {'statusCode': 200}
    
    elif event_type == 'DISCONNECT':
        table.delete_item(Key={'connectionId': connection_id})
        return {'statusCode': 200}
    
    elif event_type == 'MESSAGE':
        body = json.loads(event['body'])
        response = process_query(body['query'])
        
        api_gateway.post_to_connection(
            ConnectionId=connection_id,
            Data=json.dumps(response)
        )
        return {'statusCode': 200}

def process_query(query):
    # Process the query and return results
    pass
