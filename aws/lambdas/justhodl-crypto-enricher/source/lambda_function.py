import json, urllib.request, ssl, boto3, traceback, os
from datetime import datetime, timezone

s3 = boto3.client('s3', region_name='us-east-1')
BUCKET = os.environ.get('S3_BUCKET', 'justhodl-dashboard-live')
REPORT_KEY = 'data/report.json'
CMC_KEY = os.environ.get('CMC_API_KEY', '')

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

def fetch(url, headers=None, timeout=10):
    try:
        hdrs = {'User-Agent': 'JustHodl/2.0', 'Accept': 'application/json'}
        if headers:
            hdrs.update(headers)
        req = urllib.request.Request(url, headers=hdrs)
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f"FETCH_ERR[{url[:60]}]: {e}")
        return None

def get_defi_tvl():
    """DeFi Llama - Total TVL + top protocols"""
    result = {'total_tvl': 0, 'chains': {}, 'top_protocols': [], 'source': 'defillama'}
    try:
        # Total TVL
        data = fetch('https://api.llama.fi/v2/historicalChainTvl')
        if data and isinstance(data, list) and len(data) > 0:
            result['total_tvl'] = data[-1].get('tvl', 0)
            if len(data) > 1:
                prev = data[-2].get('tvl', 0)
                if prev > 0:
                    result['tvl_24h_change'] = round((result['total_tvl'] - prev) / prev * 100, 2)
        
        # Chain breakdown
        chains = fetch('https://api.llama.fi/v2/chains')
        if chains and isinstance(chains, list):
            for chain in chains[:15]:
                if isinstance(chain, dict):
                    name = chain.get('name', '')
                    tvl = chain.get('tvl', 0)
                    if name and tvl:
                        result['chains'][name] = round(tvl, 0)
        
        # Top protocols
        protocols = fetch('https://api.llama.fi/protocols')
        if protocols and isinstance(protocols, list):
            for p in protocols[:20]:
                if isinstance(p, dict):
                    result['top_protocols'].append({
                        'name': p.get('name', ''),
                        'tvl': round(p.get('tvl', 0), 0),
                        'chain': p.get('chain', ''),
                        'change_1d': p.get('change_1d', 0),
                        'change_7d': p.get('change_7d', 0)
                    })
    except Exception as e:
        print(f"DeFi TVL error: {e}")
    return result

def get_eth_gas():
    """Etherscan-free gas estimate via public APIs"""
    result = {'low': 0, 'average': 0, 'high': 0, 'source': 'blocknative'}
    try:
        # Try Blocknative (free, no key needed)
        data = fetch('https://api.blocknative.com/gasprices/blockprices')
        if data and 'blockPrices' in data:
            bp = data['blockPrices'][0]
            prices = bp.get('estimatedPrices', [])
            if len(prices) >= 3:
                result['low'] = prices[2].get('price', 0)
                result['average'] = prices[1].get('price', 0)
                result['high'] = prices[0].get('price', 0)
                result['base_fee'] = bp.get('baseFeePerGas', 0)
                return result
        
        # Fallback: ethgasstation equivalent
        data = fetch('https://api.gasprice.io/v1/estimates')
        if data and isinstance(data, dict):
            result['low'] = data.get('slow', {}).get('gasPrice', 0)
            result['average'] = data.get('standard', {}).get('gasPrice', 0)
            result['high'] = data.get('fast', {}).get('gasPrice', 0)
    except Exception as e:
        print(f"ETH gas error: {e}")
    return result

def get_funding_rates():
    """Crypto funding rates from public APIs"""
    result = {'rates': {}, 'source': 'coinglass_public'}
    try:
        # Try CoinGlass public endpoint
        data = fetch('https://open-api.coinglass.com/public/v2/funding', 
                     headers={'accept': 'application/json'})
        if data and isinstance(data, dict) and 'data' in data:
            for item in data['data'][:20]:
                if isinstance(item, dict):
                    symbol = item.get('symbol', '')
                    rate = item.get('uMarginList', [{}])[0].get('rate', 0) if item.get('uMarginList') else 0
                    if symbol:
                        result['rates'][symbol] = round(float(rate) * 100, 4) if rate else 0
        
        if not result['rates']:
            # Fallback: manual from Binance
            for sym in ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'XRPUSDT']:
                data = fetch(f'https://fapi.binance.com/fapi/v1/fundingRate?symbol={sym}&limit=1')
                if data and isinstance(data, list) and len(data) > 0:
                    rate = float(data[0].get('fundingRate', 0))
                    result['rates'][sym.replace('USDT', '')] = round(rate * 100, 4)
    except Exception as e:
        print(f"Funding rates error: {e}")
    return result

def get_leverage_sentiment():
    """Long/short ratios from Binance"""
    result = {}
    try:
        for sym in ['BTCUSDT', 'ETHUSDT', 'SOLUSDT']:
            data = fetch(f'https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol={sym}&period=1h&limit=1')
            if data and isinstance(data, list) and len(data) > 0:
                ratio = data[0]
                result[sym.replace('USDT', '')] = {
                    'long_ratio': round(float(ratio.get('longAccount', 0.5)) * 100, 1),
                    'short_ratio': round(float(ratio.get('shortAccount', 0.5)) * 100, 1),
                    'long_short_ratio': round(float(ratio.get('longShortRatio', 1)), 3)
                }
    except Exception as e:
        print(f"Leverage error: {e}")
    return result

def compute_market_intelligence(report):
    """Compute missing market intelligence fields from existing data"""
    intel = {}
    
    # ML Regime - derive from khalid_index
    ki = report.get('khalid_index', {})
    ki_val = ki.get('value', ki.get('score', 50)) if isinstance(ki, dict) else ki
    if isinstance(ki_val, (int, float)):
        if ki_val >= 75: intel['ml_regime'] = 'RISK-ON'
        elif ki_val >= 55: intel['ml_regime'] = 'NEUTRAL-BULLISH'
        elif ki_val >= 45: intel['ml_regime'] = 'NEUTRAL'
        elif ki_val >= 25: intel['ml_regime'] = 'NEUTRAL-BEARISH'
        else: intel['ml_regime'] = 'RISK-OFF'
    
    # Risk Level - from risk_dashboard
    rd = report.get('risk_dashboard', {})
    if isinstance(rd, dict):
        risk_score = rd.get('composite_score', rd.get('overall_risk', 50))
        if isinstance(risk_score, (int, float)):
            if risk_score >= 80: intel['risk_level'] = 'EXTREME'
            elif risk_score >= 60: intel['risk_level'] = 'HIGH'
            elif risk_score >= 40: intel['risk_level'] = 'MODERATE'
            elif risk_score >= 20: intel['risk_level'] = 'LOW'
            else: intel['risk_level'] = 'MINIMAL'
            intel['risk_score'] = risk_score
    
    # Liquidity - from net_liquidity
    nl = report.get('net_liquidity', {})
    if isinstance(nl, dict):
        trend = nl.get('trend', nl.get('direction', ''))
        if isinstance(trend, str):
            intel['liquidity'] = trend.upper() if trend else 'NEUTRAL'
        nl_val = nl.get('value', nl.get('net_liquidity', 0))
        if isinstance(nl_val, (int, float)):
            intel['net_liquidity_value'] = nl_val
    
    # Carry Risk - from market_flow
    mf = report.get('market_flow', {})
    if isinstance(mf, dict):
        carry = mf.get('carry_risk', mf.get('carry_trade', ''))
        if isinstance(carry, str) and carry:
            intel['carry_risk'] = carry.upper()
        else:
            intel['carry_risk'] = 'MODERATE'
    
    # Sector Regime - from sectors
    sectors = report.get('sectors', {})
    if isinstance(sectors, dict) and len(sectors) > 0:
        # Find strongest sector
        best_sector = ''
        best_val = -999
        for k, v in sectors.items():
            if isinstance(v, dict):
                perf = v.get('performance', v.get('change', 0))
                if isinstance(perf, (int, float)) and perf > best_val:
                    best_val = perf
                    best_sector = k
            elif isinstance(v, (int, float)) and v > best_val:
                best_val = v
                best_sector = k
        intel['sector_regime'] = best_sector.upper() if best_sector else 'MIXED'
    
    # US Outlook - derive from multiple signals
    signals = report.get('signals', {})
    if isinstance(signals, dict):
        outlook = signals.get('us_outlook', signals.get('outlook', ''))
        if isinstance(outlook, str) and outlook:
            intel['us_outlook'] = outlook.upper()
    
    return intel

def lambda_handler(event, context):
    headers = {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'GET,POST,OPTIONS'
    }
    
    try:
        print("Loading current report.json from S3...")
        obj = s3.get_object(Bucket=BUCKET, Key=REPORT_KEY)
        report = json.loads(obj['Body'].read())
        print(f"Report loaded: {len(report)} keys")
        
        enrichments = {}
        
        # 1. DeFi TVL
        print("Fetching DeFi TVL...")
        defi = get_defi_tvl()
        if defi.get('total_tvl', 0) > 0:
            enrichments['defi_tvl'] = defi
            print(f"  TVL: ${defi['total_tvl']:,.0f}, {len(defi.get('chains',{}))} chains, {len(defi.get('top_protocols',[]))} protocols")
        
        # 2. ETH Gas
        print("Fetching ETH gas...")
        gas = get_eth_gas()
        if gas.get('average', 0) > 0:
            enrichments['eth_gas'] = gas
            print(f"  Gas: low={gas['low']} avg={gas['average']} high={gas['high']}")
        
        # 3. Funding Rates
        print("Fetching funding rates...")
        funding = get_funding_rates()
        if funding.get('rates'):
            enrichments['funding_rates'] = funding
            print(f"  Rates: {len(funding['rates'])} pairs")
        
        # 4. Leverage Sentiment
        print("Fetching leverage sentiment...")
        leverage = get_leverage_sentiment()
        if leverage:
            enrichments['leverage_sentiment'] = leverage
            print(f"  Leverage: {len(leverage)} pairs")
        
        # 5. Computed Market Intelligence
        print("Computing market intelligence...")
        intel = compute_market_intelligence(report)
        enrichments['market_intelligence'] = intel
        print(f"  Intel fields: {list(intel.keys())}")
        
        # Merge into report
        report.update(enrichments)
        report['enriched_at'] = datetime.now(timezone.utc).isoformat()
        report['enrichment_fields'] = list(enrichments.keys())
        
        # Save back to S3
        print("Saving enriched report to S3...")
        s3.put_object(
            Bucket=BUCKET,
            Key=REPORT_KEY,
            Body=json.dumps(report, default=str),
            ContentType='application/json'
        )
        
        summary = {
            'status': 'enriched',
            'fields_added': list(enrichments.keys()),
            'defi_tvl': enrichments.get('defi_tvl', {}).get('total_tvl', 0),
            'eth_gas_avg': enrichments.get('eth_gas', {}).get('average', 0),
            'funding_pairs': len(enrichments.get('funding_rates', {}).get('rates', {})),
            'leverage_pairs': len(enrichments.get('leverage_sentiment', {})),
            'intel_fields': len(enrichments.get('market_intelligence', {})),
            'enriched_at': report['enriched_at']
        }
        
        return {'statusCode': 200, 'headers': headers, 'body': json.dumps(summary)}
        
    except Exception as e:
        print(f"Error: {traceback.format_exc()}")
        return {'statusCode': 500, 'headers': headers, 'body': json.dumps({'error': str(e)})}
