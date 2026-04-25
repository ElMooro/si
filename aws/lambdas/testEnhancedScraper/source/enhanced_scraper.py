import json
import boto3
import urllib.request
import urllib.parse
from datetime import datetime
import os
import time

s3 = boto3.client('s3')
BUCKET = os.environ.get('S3_BUCKET', 'macro-data-lake')

# SAFE MODE - saves to test directory
SAFE_PREFIX = "test/enhanced_data"

def safe_request(url, timeout=30):
    """Make safe HTTP request"""
    try:
        req = urllib.request.Request(url)
        req.add_header('User-Agent', 'Mozilla/5.0')
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return json.loads(response.read().decode('utf-8'))
    except Exception as e:
        print(f"Error fetching {url}: {str(e)[:100]}")
        return None

def collect_new_data_sources():
    """Collect NEW data sources not in your current scraper"""
    
    new_data = {
        'timestamp': datetime.utcnow().isoformat(),
        'new_sources': {}
    }
    
    # 1. IMF - International Monetary Fund Data
    print("Collecting IMF data...")
    imf_indicators = {
        'USD_EUR': 'http://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/IFS/M.US.ENDA_EUR_USD_RATE',
        'reserves': 'http://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/IFS/M.US.RAFA_USD',
        'cpi': 'http://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/IFS/M.US.PCPI_IX'
    }
    
    imf_data = {}
    for name, url in imf_indicators.items():
        data = safe_request(url)
        if data:
            imf_data[name] = data
    new_data['new_sources']['imf'] = imf_data
    
    # 2. OECD Data
    print("Collecting OECD data...")
    oecd_urls = {
        'leading_indicators': 'https://stats.oecd.org/SDMX-JSON/data/MEI_CLI/USA.CLI.AMPLITUD.M/all?startTime=2020',
        'business_confidence': 'https://stats.oecd.org/SDMX-JSON/data/MEI_BTS_COS/USA.BSCI.BLSA.M/all?startTime=2020',
        'consumer_confidence': 'https://stats.oecd.org/SDMX-JSON/data/MEI_BTS_COS/USA.CSCI.BLSA.M/all?startTime=2020'
    }
    
    oecd_data = {}
    for name, url in oecd_urls.items():
        data = safe_request(url)
        if data:
            oecd_data[name] = data
    new_data['new_sources']['oecd'] = oecd_data
    
    # 3. Chicago Fed National Activity Index
    print("Collecting Chicago Fed CFNAI components...")
    cfnai_url = 'https://www.chicagofed.org/api/cfnai/getData'
    cfnai_data = safe_request(cfnai_url)
    if cfnai_data:
        new_data['new_sources']['chicago_fed_cfnai'] = cfnai_data
    
    # 4. Atlanta Fed GDPNow
    print("Collecting Atlanta Fed GDPNow...")
    gdpnow_url = 'https://www.atlantafed.org/cqer/research/gdpnow/gdpnow_data.json'
    gdpnow_data = safe_request(gdpnow_url)
    if gdpnow_data:
        new_data['new_sources']['atlanta_fed_gdpnow'] = gdpnow_data
    
    # 5. Cleveland Fed Inflation Nowcasting
    print("Collecting Cleveland Fed inflation expectations...")
    cleveland_urls = {
        'inflation_expectations': 'https://www.clevelandfed.org/api/inflation/expectations',
        'yield_curve': 'https://www.clevelandfed.org/api/yieldcurve'
    }
    
    cleveland_data = {}
    for name, url in cleveland_urls.items():
        data = safe_request(url)
        if data:
            cleveland_data[name] = data
    new_data['new_sources']['cleveland_fed'] = cleveland_data
    
    # 6. Bank of Canada
    print("Collecting Bank of Canada data...")
    boc_urls = {
        'exchange_rates': 'https://www.bankofcanada.ca/valet/observations/group/FX_RATES_DAILY/json',
        'interest_rates': 'https://www.bankofcanada.ca/valet/observations/group/INTEREST_RATES/json'
    }
    
    boc_data = {}
    for name, url in boc_urls.items():
        data = safe_request(url)
        if data:
            boc_data[name] = data
    new_data['new_sources']['bank_of_canada'] = boc_data
    
    # 7. CBOE Options Data
    print("Collecting CBOE volatility indices...")
    cboe_indices = {
        'VIX9D': 'https://cdn.cboe.com/api/global/delayed_quotes/indices/VIX9D.json',
        'VIX': 'https://cdn.cboe.com/api/global/delayed_quotes/indices/VIX.json',
        'VIX3M': 'https://cdn.cboe.com/api/global/delayed_quotes/indices/VIX3M.json',
        'VVIX': 'https://cdn.cboe.com/api/global/delayed_quotes/indices/VVIX.json',
        'SKEW': 'https://cdn.cboe.com/api/global/delayed_quotes/indices/SKEW.json',
        'PUT': 'https://cdn.cboe.com/api/global/delayed_quotes/indices/PUT.json',
        'CALL': 'https://cdn.cboe.com/api/global/delayed_quotes/indices/CALL.json'
    }
    
    cboe_data = {}
    for name, url in cboe_indices.items():
        data = safe_request(url)
        if data:
            cboe_data[name] = data
    new_data['new_sources']['cboe'] = cboe_data
    
    # 8. US Census Economic Indicators
    print("Collecting US Census data...")
    census_base = 'https://api.census.gov/data/timeseries/eits'
    census_indicators = {
        'retail_sales': f'{census_base}/retail',
        'manufacturing': f'{census_base}/m3',
        'construction': f'{census_base}/vip'
    }
    
    census_data = {}
    for name, url in census_indicators.items():
        # Note: Census API might need an API key
        data = safe_request(url + '?get=cell_value,time_slot_id&for=us:*&time=from+2020')
        if data:
            census_data[name] = data
    new_data['new_sources']['census'] = census_data
    
    # 9. Philadelphia Fed
    print("Collecting Philadelphia Fed surveys...")
    philly_fed = {
        'aruoba_index': 'https://www.philadelphiafed.org/api/ads/aruoba_data.json',
        'state_indexes': 'https://www.philadelphiafed.org/api/state-indexes'
    }
    
    philly_data = {}
    for name, url in philly_fed.items():
        data = safe_request(url)
        if data:
            philly_data[name] = data
    new_data['new_sources']['philadelphia_fed'] = philly_data
    
    # 10. CFTC Commitment of Traders
    print("Collecting CFTC COT data...")
    cftc_url = 'https://publicreporting.cftc.gov/api/COTReport'
    cftc_data = safe_request(cftc_url)
    if cftc_data:
        new_data['new_sources']['cftc_cot'] = cftc_data
    
    return new_data

def lambda_handler(event, context):
    """Handler that collects NEW data sources safely"""
    
    headers = {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*'
    }
    
    try:
        print("Starting NEW data source collection (safe mode)...")
        
        # Collect new data
        new_data = collect_new_data_sources()
        
        # Count what we collected
        source_count = len(new_data['new_sources'])
        total_items = sum(len(v) if isinstance(v, dict) else 1 for v in new_data['new_sources'].values())
        
        # Save to SAFE location
        timestamp = datetime.utcnow()
        safe_key = f"{SAFE_PREFIX}/{timestamp.strftime('%Y/%m/%d')}/new_sources_{timestamp.strftime('%H%M%S')}.json"
        
        s3.put_object(
            Bucket=BUCKET,
            Key=safe_key,
            Body=json.dumps(new_data),
            ContentType='application/json'
        )
        
        summary = {
            'timestamp': new_data['timestamp'],
            'sources_tested': source_count,
            'total_datasets': total_items,
            'location': f's3://{BUCKET}/{safe_key}',
            'sources': list(new_data['new_sources'].keys())
        }
        
        # Save summary
        summary_key = f"{SAFE_PREFIX}/latest_test.json"
        s3.put_object(
            Bucket=BUCKET,
            Key=summary_key,
            Body=json.dumps(summary),
            ContentType='application/json'
        )
        
        return {
            'statusCode': 200,
            'headers': headers,
            'body': json.dumps(summary)
        }
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'headers': headers,
            'body': json.dumps({
                'error': str(e),
                'message': 'Test failed but your production data is safe!'
            })
        }
