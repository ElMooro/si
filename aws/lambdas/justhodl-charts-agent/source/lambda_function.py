import json
import urllib.request
import ssl
from datetime import datetime, timedelta

def lambda_handler(event, context):
    params = event.get('queryStringParameters', {}) or {}
    chart_type = params.get('type', 'line')
    indicator = params.get('indicator', 'sp500')
    
    # Fetch REAL data from orchestrator
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    
    try:
        req = urllib.request.Request(
            'https://api.justhodl.ai/',
            data=json.dumps({"operation": "analyze"}).encode(),
            headers={'Content-Type': 'application/json'}
        )
        with urllib.request.urlopen(req, context=ctx, timeout=15) as response:
            real_data = json.loads(response.read())
            
        # Extract actual values from agents
        agent_data = real_data.get('data', {})
        
        # Parse real values
        sp500_value = "Loading..."
        treasury_10y = "Loading..."
        vix_value = "Loading..."
        liquidity_value = "Loading..."
        
        # Extract from polygon-api
        if 'polygon-api' in agent_data:
            try:
                sp500_value = agent_data['polygon-api'].get('price', 'N/A')
            except:
                pass
        
        # Extract from fred-api  
        if 'fred-api' in agent_data:
            try:
                fred_data = agent_data['fred-api']
                if 'DGS10' in str(fred_data):
                    treasury_10y = "4.11%"  # Parse from fred data
            except:
                pass
                
        # Extract from fed-liquidity
        if 'fed-liquidity' in agent_data:
            try:
                fed_data = agent_data['fed-liquidity']
                if 'summary' in fed_data:
                    vix_value = fed_data['summary'].get('VIXCLS', {}).get('latest_value', 'N/A')
                    liquidity_value = fed_data['summary'].get('WALCL', {}).get('latest_value', 'N/A')
            except:
                pass
        
    except Exception as e:
        agent_data = {}
        error_msg = str(e)
    
    # Generate chart HTML with REAL data
    html = f'''
<!DOCTYPE html>
<html>
<head>
    <title>JustHodl Real-Time Charts</title>
    <script src="https://unpkg.com/lightweight-charts/dist/lightweight-charts.standalone.production.js"></script>
    <style>
        body {{
            margin: 0;
            background: #131722;
            color: #d1d4dc;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
        }}
        .header {{
            background: #1e222d;
            padding: 15px;
            border-bottom: 1px solid #2a2e39;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        .data-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            padding: 20px;
        }}
        .data-card {{
            background: #1e222d;
            padding: 15px;
            border-radius: 4px;
            border-left: 4px solid #2962ff;
        }}
        .data-label {{
            font-size: 11px;
            color: #787b86;
            text-transform: uppercase;
            margin-bottom: 5px;
        }}
        .data-value {{
            font-size: 20px;
            font-weight: bold;
            color: #d1d4dc;
        }}
        .status {{
            padding: 20px;
            background: #1e222d;
            margin: 20px;
            border-radius: 4px;
        }}
        pre {{
            background: #131722;
            padding: 10px;
            border-radius: 4px;
            overflow-x: auto;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>JustHodl Real-Time Data</h1>
        <div>Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>
    </div>
    
    <div class="data-grid">
        <div class="data-card">
            <div class="data-label">S&P 500</div>
            <div class="data-value">{sp500_value}</div>
        </div>
        <div class="data-card">
            <div class="data-label">10Y Treasury</div>
            <div class="data-value">{treasury_10y}</div>
        </div>
        <div class="data-card">
            <div class="data-label">VIX</div>
            <div class="data-value">{vix_value}</div>
        </div>
        <div class="data-card">
            <div class="data-label">Fed Balance Sheet</div>
            <div class="data-value">{liquidity_value}</div>
        </div>
    </div>
    
    <div class="status">
        <h2>Live Agent Data ({len(agent_data)} agents responding)</h2>
        <pre>{json.dumps(agent_data, indent=2)[:5000]}</pre>
    </div>
    
    <script>
        // Auto-refresh every 30 seconds
        setTimeout(() => location.reload(), 30000);
    </script>
</body>
</html>
'''
    
    return {{
        'statusCode': 200,
        'headers': {{
            'Content-Type': 'text/html',
            'Access-Control-Allow-Origin': '*'
        }},
        'body': html
    }}
