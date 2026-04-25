
import json
import boto3
import urllib.request
from datetime import datetime

s3 = boto3.client('s3')

def lambda_handler(event, context):
    indicators = get_fred_data()
    api_status = check_apis()
    html = generate_report(indicators, api_status)
    
    s3.put_object(
        Bucket='daily-liquidity-reports',
        Key='latest_report.html',
        Body=html,
        ContentType='text/html',
        ACL='public-read'
    )
    
    return {'statusCode': 200, 'body': json.dumps({'message': 'Report generated'})}

def get_fred_data():
    indicators = {}
    fred_key = '2f057499936072679d8843d7fce99989'
    series = {'SOFR': 'SOFR', 'VIXCLS': 'VIX', 'DGS10': '10Y', 'DGS2': '2Y', 'DFF': 'Fed Funds'}
    
    for sid, name in series.items():
        try:
            url = f'https://api.stlouisfed.org/fred/series/observations?series_id={sid}&api_key={fred_key}&file_type=json&limit=1&sort_order=desc'
            with urllib.request.urlopen(url, timeout=3) as r:
                data = json.loads(r.read())
                if data.get('observations'):
                    indicators[name] = float(data['observations'][0]['value'])
        except:
            indicators[name] = 0
    return indicators

def check_apis():
    apis = {
        'Global Liquidity': 'https://r9ywtw4dj3.execute-api.us-east-1.amazonaws.com/prod',
        'Treasury': 'https://i1hgpjotq7.execute-api.us-east-1.amazonaws.com/prod',
        'FRED': 'https://klehdyiwrl.execute-api.us-east-1.amazonaws.com/prod'
    }
    
    status = []
    for name, url in apis.items():
        try:
            req = urllib.request.Request(url + '/health')
            urllib.request.urlopen(req, timeout=2)
            status.append({'name': name, 'status': 'ACTIVE', 'url': url})
        except:
            status.append({'name': name, 'status': 'OFFLINE', 'url': url})
    return status

def generate_report(indicators, apis):
    vix = indicators.get('VIX', 0)
    stress = 30 if vix < 20 else 60 if vix < 30 else 90
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Daily Liquidity Report</title>
        <style>
            body {{ font-family: Arial; background: #1a1a2e; color: white; margin: 0; padding: 20px; }}
            .container {{ max-width: 1400px; margin: auto; background: rgba(20,20,30,0.95); padding: 40px; border-radius: 20px; }}
            h1 {{ color: #4fbdba; font-size: 3em; text-align: center; }}
            .timestamp {{ text-align: center; color: #888; font-size: 1.2em; margin: 20px 0; }}
            .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; margin: 30px 0; }}
            .card {{ background: rgba(79,189,186,0.1); border: 1px solid rgba(79,189,186,0.3); border-radius: 15px; padding: 20px; }}
            .card:hover {{ transform: translateY(-5px); box-shadow: 0 15px 40px rgba(79,189,186,0.3); transition: 0.3s; }}
            .label {{ color: #888; font-size: 0.9em; text-transform: uppercase; }}
            .value {{ color: #4fbdba; font-size: 2.2em; font-weight: bold; margin: 10px 0; }}
            .status {{ padding: 5px 12px; border-radius: 20px; display: inline-block; margin: 5px 0; }}
            .active {{ background: rgba(74,222,128,0.2); color: #4ade80; }}
            .offline {{ background: rgba(251,191,36,0.2); color: #fbbf24; }}
            table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
            th {{ background: rgba(79,189,186,0.1); padding: 15px; text-align: left; color: #4fbdba; }}
            td {{ padding: 12px 15px; border-bottom: 1px solid rgba(255,255,255,0.1); }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Daily Liquidity Report</h1>
            <div class="timestamp">{datetime.now().strftime('%B %d, %Y at %I:%M %p ET')}</div>
            
            <h2 style="color: #4fbdba;">Market Indicators</h2>
            <div class="grid">
    """
    
    for name, value in indicators.items():
        unit = '%' if name in ['SOFR', '10Y', '2Y', 'Fed Funds'] else ''
        html += f'<div class="card"><div class="label">{name}</div><div class="value">{value:.2f}{unit}</div></div>'
    
    html += '</div><h2 style="color: #4fbdba;">System Status</h2><table><tr><th>API</th><th>Status</th><th>Endpoint</th></tr>'
    
    for api in apis:
        sc = 'active' if api['status'] == 'ACTIVE' else 'offline'
        html += f'<tr><td>{api["name"]}</td><td><span class="status {sc}">{api["status"]}</span></td><td>{api["url"]}</td></tr>'
    
    html += '</table></div></body></html>'
    return html
