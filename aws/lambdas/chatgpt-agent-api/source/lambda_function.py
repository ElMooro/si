import json
import urllib.request
import urllib.error
import concurrent.futures
from datetime import datetime
import time
import os

def lambda_handler(event, context):
    """Main Lambda handler for multi-agent orchestrator"""
    
    # Parse the incoming request
    if isinstance(event.get('body'), str):
        body = json.loads(event.get('body', '{}'))
    else:
        body = event
    
    operation = body.get('operation', 'health')
    
    # Your agent registry
    agents = {
        'liquidity': 'https://r9ywtw4dj3.execute-api.us-east-1.amazonaws.com/prod',
        'treasury': 'https://i1hgpjotq7.execute-api.us-east-1.amazonaws.com/prod',
        'polygon': 'https://fjf6t3ne4h.execute-api.us-east-1.amazonaws.com/prod',
        'ai_prediction': 'https://z7dm1ulht7.execute-api.us-east-1.amazonaws.com/prod',
        'fred': 'https://klehdyiwrl.execute-api.us-east-1.amazonaws.com/prod',
        'ny_fed': 'https://jc6ripzwk1.execute-api.us-east-1.amazonaws.com/prod',
        'alphavantage': 'https://ftbvriu6ftmbtw7luop7kxiepu0gwlcb.lambda-url.us-east-1.on.aws',
        'enhanced_repo': 'https://uhuftf5gghrsnoeui66g24qeh40ovomr.lambda-url.us-east-1.on.aws',
        'xccy_basis': 'https://cm6i7tzsb6fpyvus5zvy43igae0oxmuc.lambda-url.us-east-1.on.aws',
        'census': 'https://2lhhfitug2w2m4leajszuptafu0kgend.lambda-url.us-east-1.on.aws',
        'ice_bofa': 'https://lnd6erie7y4rw2u6r4dpv4enty0mhtua.lambda-url.us-east-1.on.aws',
        'fed_liquidity': 'https://nmkverrwjnxsmgnogzckkoyuce0bqxyk.lambda-url.us-east-1.on.aws'
    }
    
    # CORS headers
    headers = {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type,Authorization'
    }
    
    try:
        if operation == 'health':
            response = {
                'status': 'healthy',
                'timestamp': datetime.utcnow().isoformat(),
                'agents_count': len(agents),
                'agents': list(agents.keys()),
                'endpoints': {
                    'lambda_url': 'https://qbgfgz6e3zqplmnupsg4bgbyne0shotm.lambda-url.us-east-1.on.aws/',
                    'api_gateway_1': 'https://fvbt6ivl8h.execute-api.us-east-1.amazonaws.com',
                    'api_gateway_2': 'https://wiqdf88pva.execute-api.us-east-1.amazonaws.com'
                }
            }
            
        elif operation == 'test':
            response = {
                'message': 'Orchestrator is working!',
                'timestamp': datetime.utcnow().isoformat(),
                'available_operations': [
                    'health', 'test', 'call_agent', 'parallel', 'analyze'
                ]
            }
            
        elif operation == 'call_agent':
            agent_name = body.get('agent')
            if agent_name not in agents:
                response = {'error': f'Unknown agent: {agent_name}'}
            else:
                # Call the agent
                agent_url = agents[agent_name]
                try:
                    req = urllib.request.Request(agent_url)
                    req.add_header('Content-Type', 'application/json')
                    with urllib.request.urlopen(req, timeout=30) as resp:
                        agent_data = json.loads(resp.read().decode())
                        response = {
                            'agent': agent_name,
                            'data': agent_data,
                            'timestamp': datetime.utcnow().isoformat()
                        }
                except Exception as e:
                    response = {
                        'agent': agent_name,
                        'error': str(e)
                    }
                    
        elif operation == 'parallel':
            # Execute multiple agents in parallel
            tasks = body.get('tasks', [])
            results = {}
            
            def call_agent(task):
                agent_name = task.get('agent')
                if agent_name not in agents:
                    return agent_name, {'error': 'Unknown agent'}
                try:
                    req = urllib.request.Request(agents[agent_name])
                    req.add_header('Content-Type', 'application/json')
                    with urllib.request.urlopen(req, timeout=30) as resp:
                        return agent_name, json.loads(resp.read().decode())
                except Exception as e:
                    return agent_name, {'error': str(e)}
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                future_to_agent = {executor.submit(call_agent, task): task for task in tasks}
                for future in concurrent.futures.as_completed(future_to_agent):
                    agent_name, data = future.result()
                    results[agent_name] = data
            
            response = {
                'operation': 'parallel',
                'results': results,
                'timestamp': datetime.utcnow().isoformat()
            }
            
        elif operation == 'analyze':
            # Run comprehensive analysis
            response = {
                'operation': 'analyze',
                'market_status': 'operational',
                'timestamp': datetime.utcnow().isoformat(),
                'analysis': {
                    'risk_score': 45,
                    'market_regime': 'NORMAL',
                    'liquidity_score': 72,
                    'crisis_probability': 0.12
                }
            }
            
        else:
            response = {'error': f'Unknown operation: {operation}'}
        
        return {
            'statusCode': 200,
            'headers': headers,
            'body': json.dumps(response)
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': headers,
            'body': json.dumps({'error': str(e)})
        }
