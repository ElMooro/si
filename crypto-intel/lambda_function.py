"""
JustHodl.AI Crypto Intelligence Engine v3.0
Real-time crypto market intelligence from 9 free API sources
Deployed to: justhodl-crypto-intel Lambda
Output: s3://justhodl-dashboard-live/crypto-intel.json + crypto.html
"""
import json, os, ssl, traceback, time, hashlib
from datetime import datetime, timezone, timedelta
from urllib import request as urllib_request
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

# === CONFIG ===
S3_BUCKET = os.environ.get('S3_BUCKET', 'justhodl-dashboard-live')
CMC_API_KEY = os.environ.get('CMC_API_KEY', '17ba8e87-53f0-46f4-abe5-014d9cd99597')
s3 = boto3.client('s3')
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

def http_get(url, headers=None, timeout=12):
    try:
        hdrs = {'User-Agent': 'JustHodl/3.0', 'Accept': 'application/json'}
        if headers:
            hdrs.update(headers)
        req = urllib_request.Request(url, headers=hdrs)
        with urllib_request.urlopen(req, timeout=timeout, context=ctx) as r:
            return json.loads(r.read().decode('utf-8'))
    except Exception as e:
        print(f"HTTP_ERR[{url[:80]}]: {e}")
        return None

# ══════════════════════════════════════════════════════════════
# SOURCE 1: DeFiLlama — Stablecoins, TVL, DEX Volume, Yields
# ══════════════════════════════════════════════════════════════
def fetch_defillama_stablecoins():
    """Track stablecoin supply changes (mint/burn signals)"""
    data = http_get("https://stablecoins.llama.fi/stablecoins?includePrices=true")
    if not data or 'peggedAssets' not in data:
        return {'status': 'error', 'stablecoins': []}
    
    stables = []
    total_mcap = 0
    for s in data['peggedAssets'][:20]:
        try:
            name = s.get('name', '?')
            symbol = s.get('symbol', '?')
            mcap = s.get('circulating', {}).get('peggedUSD', 0) or 0
            mcap_prev = 0
            chains = s.get('chainCirculating', {})
            
            # Get 7d change from price history
            peg_type = s.get('pegType', 'peggedUSD')
            peg_mech = s.get('pegMechanism', 'unknown')
            
            total_mcap += mcap
            change_7d = 0
            if mcap > 0:
                # Try to get historical for change calc
                sid = s.get('id', '')
                hist = http_get(f"https://stablecoins.llama.fi/stablecoin/{sid}")
                if hist and 'tokens' in hist:
                    tokens = hist['tokens']
                    if len(tokens) >= 2:
                        current = tokens[-1].get('circulating', {}).get('peggedUSD', 0) or 0
                        prev_7d = tokens[-8].get('circulating', {}).get('peggedUSD', 0) if len(tokens) > 8 else tokens[0].get('circulating', {}).get('peggedUSD', 0)
                        if prev_7d and prev_7d > 0:
                            change_7d = round((current - prev_7d) / prev_7d * 100, 2)
            
            signal = 'MINTING' if change_7d > 1 else 'BURNING' if change_7d < -1 else 'STABLE'
            
            stables.append({
                'name': name,
                'symbol': symbol,
                'mcap': round(mcap, 0),
                'mcap_fmt': fmt_num(mcap),
                'change_7d': change_7d,
                'signal': signal,
                'peg_type': peg_type,
                'mechanism': peg_mech
            })
        except Exception as e:
            print(f"Stable parse err: {e}")
            continue
    
    # Only get top 10 detailed, rest summary
    stables = sorted(stables, key=lambda x: x['mcap'], reverse=True)[:15]
    
    minting = sum(1 for s in stables if s['signal'] == 'MINTING')
    burning = sum(1 for s in stables if s['signal'] == 'BURNING')
    
    return {
        'status': 'ok',
        'total_mcap': round(total_mcap, 0),
        'total_mcap_fmt': fmt_num(total_mcap),
        'stablecoins': stables[:10],
        'minting_count': minting,
        'burning_count': burning,
        'net_signal': 'INFLOW' if minting > burning else 'OUTFLOW' if burning > minting else 'NEUTRAL'
    }

def fetch_defillama_tvl():
    """Total DeFi TVL and chain breakdown"""
    protocols = http_get("https://api.llama.fi/protocols")
    chains_data = http_get("https://api.llama.fi/v2/chains")
    
    result = {'status': 'ok', 'total_tvl': 0, 'chains': [], 'top_protocols': []}
    
    if chains_data:
        chains_sorted = sorted(chains_data, key=lambda x: x.get('tvl', 0), reverse=True)[:15]
        total = sum(c.get('tvl', 0) for c in chains_data)
        result['total_tvl'] = round(total, 0)
        result['total_tvl_fmt'] = fmt_num(total)
        result['chains'] = [{
            'name': c.get('name', '?'),
            'tvl': round(c.get('tvl', 0), 0),
            'tvl_fmt': fmt_num(c.get('tvl', 0)),
            'share': round(c.get('tvl', 0) / total * 100, 1) if total > 0 else 0
        } for c in chains_sorted]
    
    if protocols:
        top = sorted(protocols, key=lambda x: x.get('tvl', 0), reverse=True)[:10]
        result['top_protocols'] = [{
            'name': p.get('name', '?'),
            'tvl': round(p.get('tvl', 0), 0),
            'tvl_fmt': fmt_num(p.get('tvl', 0)),
            'chain': p.get('chain', '?'),
            'category': p.get('category', '?'),
            'change_1d': round(p.get('change_1d', 0) or 0, 2),
            'change_7d': round(p.get('change_7d', 0) or 0, 2)
        } for p in top]
    
    return result

def fetch_defillama_dex():
    """DEX trading volume"""
    data = http_get("https://api.llama.fi/overview/dexs?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyVolume")
    if not data:
        return {'status': 'error'}
    
    total_24h = data.get('totalDataChart', [])
    protocols = data.get('protocols', [])
    
    top_dexes = sorted(protocols, key=lambda x: x.get('total24h', 0) or 0, reverse=True)[:10]
    total_vol = sum(d.get('total24h', 0) or 0 for d in protocols)
    
    return {
        'status': 'ok',
        'total_24h_volume': round(total_vol, 0),
        'total_24h_fmt': fmt_num(total_vol),
        'top_dexes': [{
            'name': d.get('name', '?'),
            'volume_24h': round(d.get('total24h', 0) or 0, 0),
            'volume_fmt': fmt_num(d.get('total24h', 0) or 0),
            'change_1d': round(d.get('change_1d', 0) or 0, 2)
        } for d in top_dexes]
    }

def fetch_defillama_yields():
    """Top DeFi yields"""
    data = http_get("https://yields.llama.fi/pools")
    if not data or 'data' not in data:
        return {'status': 'error'}
    
    pools = data['data']
    # Filter: TVL > $1M, stablecoin or major asset
    good_pools = [p for p in pools if (p.get('tvlUsd', 0) or 0) > 1000000]
    top = sorted(good_pools, key=lambda x: x.get('apy', 0) or 0, reverse=True)[:15]
    
    return {
        'status': 'ok',
        'top_yields': [{
            'pool': p.get('pool', '?')[:20],
            'project': p.get('project', '?'),
            'chain': p.get('chain', '?'),
            'symbol': p.get('symbol', '?'),
            'apy': round(p.get('apy', 0) or 0, 2),
            'tvl': round(p.get('tvlUsd', 0) or 0, 0),
            'tvl_fmt': fmt_num(p.get('tvlUsd', 0) or 0),
            'il_risk': p.get('ilRisk', 'unknown'),
            'stablecoin': p.get('stablecoin', False)
        } for p in top]
    }

# ══════════════════════════════════════════════════════════════
# SOURCE 2: Binance — Derivatives, Funding Rates, Open Interest
# ══════════════════════════════════════════════════════════════
def fetch_binance_funding():
    """Perpetual futures funding rates"""
    data = http_get("https://fapi.binance.com/fapi/v1/premiumIndex")
    if not data:
        return {'status': 'error'}
    
    pairs = []
    for p in data:
        symbol = p.get('symbol', '')
        if not symbol.endswith('USDT'):
            continue
        rate = float(p.get('lastFundingRate', 0))
        mark = float(p.get('markPrice', 0))
        idx = float(p.get('indexPrice', 0))
        
        annualized = rate * 3 * 365 * 100  # 8hr rate * 3 * 365
        
        pairs.append({
            'symbol': symbol.replace('USDT', ''),
            'funding_rate': round(rate * 100, 4),
            'annualized': round(annualized, 1),
            'mark_price': round(mark, 2),
            'index_price': round(idx, 2),
            'basis_bps': round((mark - idx) / idx * 10000, 1) if idx > 0 else 0
        })
    
    pairs = sorted(pairs, key=lambda x: abs(x['funding_rate']), reverse=True)
    
    # Market sentiment from funding
    positive = sum(1 for p in pairs if p['funding_rate'] > 0)
    negative = sum(1 for p in pairs if p['funding_rate'] < 0)
    avg_rate = sum(p['funding_rate'] for p in pairs[:20]) / min(20, len(pairs)) if pairs else 0
    
    sentiment = 'EXTREME_GREED' if avg_rate > 0.05 else 'GREEDY' if avg_rate > 0.01 else 'NEUTRAL' if avg_rate > -0.01 else 'FEARFUL' if avg_rate > -0.05 else 'EXTREME_FEAR'
    
    return {
        'status': 'ok',
        'top_funding': pairs[:20],
        'most_shorted': sorted(pairs, key=lambda x: x['funding_rate'])[:5],
        'most_longed': sorted(pairs, key=lambda x: x['funding_rate'], reverse=True)[:5],
        'avg_funding': round(avg_rate, 4),
        'positive_count': positive,
        'negative_count': negative,
        'leverage_sentiment': sentiment
    }

def fetch_binance_oi():
    """Open interest for top futures"""
    symbols = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'XRPUSDT', 'DOGEUSDT', 'ADAUSDT', 'AVAXUSDT', 'LINKUSDT', 'DOTUSDT']
    oi_data = []
    
    for sym in symbols:
        data = http_get(f"https://fapi.binance.com/fapi/v1/openInterest?symbol={sym}")
        if data:
            oi_data.append({
                'symbol': sym.replace('USDT', ''),
                'open_interest': round(float(data.get('openInterest', 0)), 2),
                'time': data.get('time', 0)
            })
    
    return {'status': 'ok', 'open_interest': oi_data}

# ══════════════════════════════════════════════════════════════
# SOURCE 3: CoinGecko — Global Market Data
# ══════════════════════════════════════════════════════════════
def fetch_coingecko_global():
    """Global crypto market overview"""
    data = http_get("https://api.coingecko.com/api/v3/global")
    if not data or 'data' not in data:
        return {'status': 'error'}
    
    g = data['data']
    return {
        'status': 'ok',
        'total_mcap': round(g.get('total_market_cap', {}).get('usd', 0), 0),
        'total_mcap_fmt': fmt_num(g.get('total_market_cap', {}).get('usd', 0)),
        'total_volume': round(g.get('total_volume', {}).get('usd', 0), 0),
        'total_volume_fmt': fmt_num(g.get('total_volume', {}).get('usd', 0)),
        'btc_dominance': round(g.get('market_cap_percentage', {}).get('btc', 0), 1),
        'eth_dominance': round(g.get('market_cap_percentage', {}).get('eth', 0), 1),
        'active_cryptos': g.get('active_cryptocurrencies', 0),
        'markets': g.get('markets', 0),
        'mcap_change_24h': round(g.get('market_cap_change_percentage_24h_usd', 0), 2),
        'defi_vol_24h': round(g.get('total_volume', {}).get('usd', 0) * 0.15, 0),  # Estimate DeFi %
    }

def fetch_coingecko_top():
    """Top coins by market cap"""
    data = http_get("https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=25&page=1&sparkline=false&price_change_percentage=1h,24h,7d,30d")
    if not data:
        return {'status': 'error', 'coins': []}
    
    coins = []
    for c in data:
        coins.append({
            'rank': c.get('market_cap_rank', 0),
            'name': c.get('name', '?'),
            'symbol': (c.get('symbol', '?') or '?').upper(),
            'price': round(c.get('current_price', 0) or 0, 6),
            'price_fmt': fmt_price(c.get('current_price', 0) or 0),
            'mcap': round(c.get('market_cap', 0) or 0, 0),
            'mcap_fmt': fmt_num(c.get('market_cap', 0) or 0),
            'volume_24h': round(c.get('total_volume', 0) or 0, 0),
            'change_1h': round(c.get('price_change_percentage_1h_in_currency', 0) or 0, 2),
            'change_24h': round(c.get('price_change_percentage_24h', 0) or 0, 2),
            'change_7d': round(c.get('price_change_percentage_7d_in_currency', 0) or 0, 2),
            'change_30d': round(c.get('price_change_percentage_30d_in_currency', 0) or 0, 2),
            'ath': round(c.get('ath', 0) or 0, 2),
            'ath_change': round(c.get('ath_change_percentage', 0) or 0, 1),
        })
    
    return {'status': 'ok', 'coins': coins}

# ══════════════════════════════════════════════════════════════
# SOURCE 4: Blockchain.com — BTC On-Chain
# ══════════════════════════════════════════════════════════════
def fetch_btc_onchain():
    """Bitcoin on-chain metrics"""
    metrics = {}
    
    endpoints = {
        'hash_rate': 'https://api.blockchain.info/charts/hash-rate?timespan=30days&format=json',
        'difficulty': 'https://api.blockchain.info/charts/difficulty?timespan=90days&format=json',
        'n_transactions': 'https://api.blockchain.info/charts/n-transactions?timespan=30days&format=json',
        'mempool_size': 'https://api.blockchain.info/charts/mempool-size?timespan=7days&format=json',
        'avg_block_size': 'https://api.blockchain.info/charts/avg-block-size?timespan=30days&format=json',
    }
    
    for key, url in endpoints.items():
        data = http_get(url)
        if data and 'values' in data:
            vals = data['values']
            if vals:
                current = vals[-1].get('y', 0)
                prev = vals[-8].get('y', 0) if len(vals) > 8 else vals[0].get('y', 0)
                change = round((current - prev) / prev * 100, 2) if prev > 0 else 0
                metrics[key] = {
                    'value': round(current, 2),
                    'change_7d': change,
                    'unit': data.get('unit', ''),
                    'name': data.get('name', key)
                }
    
    # BTC supply
    supply_data = http_get("https://blockchain.info/q/totalbc")
    if supply_data:
        metrics['total_supply'] = {'value': round(int(supply_data) / 1e8, 0), 'unit': 'BTC'}
        metrics['pct_mined'] = {'value': round(int(supply_data) / 1e8 / 21000000 * 100, 2), 'unit': '%'}
    
    return {'status': 'ok', 'metrics': metrics}

# ══════════════════════════════════════════════════════════════
# SOURCE 5: Alternative.me — Fear & Greed
# ══════════════════════════════════════════════════════════════
def fetch_fear_greed():
    """Crypto Fear & Greed Index"""
    data = http_get("https://api.alternative.me/fng/?limit=30&format=json")
    if not data or 'data' not in data:
        return {'status': 'error'}
    
    entries = data['data']
    current = entries[0] if entries else {}
    history = [{
        'value': int(e.get('value', 50)),
        'label': e.get('value_classification', '?'),
        'date': datetime.fromtimestamp(int(e.get('timestamp', 0))).strftime('%Y-%m-%d')
    } for e in entries[:30]]
    
    return {
        'status': 'ok',
        'current': int(current.get('value', 50)),
        'label': current.get('value_classification', '?'),
        'history': history,
        'avg_7d': round(sum(h['value'] for h in history[:7]) / min(7, len(history)), 0) if history else 50,
        'avg_30d': round(sum(h['value'] for h in history) / len(history), 0) if history else 50
    }

# ══════════════════════════════════════════════════════════════
# SOURCE 6: CoinMarketCap — Enhanced Market Data
# ══════════════════════════════════════════════════════════════
def fetch_cmc_data():
    """CoinMarketCap top movers and market data"""
    data = http_get(
        "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest?limit=50&sort=market_cap&convert=USD",
        headers={'X-CMC_PRO_API_KEY': CMC_API_KEY}
    )
    if not data or 'data' not in data:
        return {'status': 'error'}
    
    coins = data['data']
    top_gainers = sorted(coins, key=lambda x: x.get('quote', {}).get('USD', {}).get('percent_change_24h', 0) or 0, reverse=True)[:5]
    top_losers = sorted(coins, key=lambda x: x.get('quote', {}).get('USD', {}).get('percent_change_24h', 0) or 0)[:5]
    
    return {
        'status': 'ok',
        'top_gainers': [{
            'symbol': c.get('symbol', '?'),
            'name': c.get('name', '?'),
            'change_24h': round(c.get('quote', {}).get('USD', {}).get('percent_change_24h', 0) or 0, 2),
            'price': round(c.get('quote', {}).get('USD', {}).get('price', 0) or 0, 4),
            'volume_24h': fmt_num(c.get('quote', {}).get('USD', {}).get('volume_24h', 0) or 0)
        } for c in top_gainers],
        'top_losers': [{
            'symbol': c.get('symbol', '?'),
            'name': c.get('name', '?'),
            'change_24h': round(c.get('quote', {}).get('USD', {}).get('percent_change_24h', 0) or 0, 2),
            'price': round(c.get('quote', {}).get('USD', {}).get('price', 0) or 0, 4),
            'volume_24h': fmt_num(c.get('quote', {}).get('USD', {}).get('volume_24h', 0) or 0)
        } for c in top_losers]
    }

# ══════════════════════════════════════════════════════════════
# COMPOSITE RISK SCORING
# ══════════════════════════════════════════════════════════════
def compute_risk_score(fg, funding, stables, global_data):
    """Composite crypto risk score 0-100"""
    scores = []
    signals = []
    
    # Fear & Greed (weight: 25%)
    if fg.get('status') == 'ok':
        fg_val = fg['current']
        if fg_val < 20:
            scores.append(('Fear & Greed: Extreme Fear', 90, 25))
            signals.append('🔴 EXTREME FEAR — Potential capitulation')
        elif fg_val < 35:
            scores.append(('Fear & Greed: Fear', 70, 25))
            signals.append('🟡 FEAR — Caution warranted')
        elif fg_val < 55:
            scores.append(('Fear & Greed: Neutral', 50, 25))
        elif fg_val < 75:
            scores.append(('Fear & Greed: Greed', 35, 25))
            signals.append('🟡 GREED — Consider taking profits')
        else:
            scores.append(('Fear & Greed: Extreme Greed', 15, 25))
            signals.append('🔴 EXTREME GREED — Distribution likely')
    
    # Funding Rates (weight: 25%)
    if funding.get('status') == 'ok':
        avg = funding.get('avg_funding', 0)
        if avg > 0.05:
            scores.append(('Funding: Extreme Long', 85, 25))
            signals.append('🔴 Extreme long bias — Correction risk HIGH')
        elif avg > 0.02:
            scores.append(('Funding: Long Bias', 65, 25))
            signals.append('🟡 Long bias building')
        elif avg > -0.02:
            scores.append(('Funding: Neutral', 45, 25))
        elif avg > -0.05:
            scores.append(('Funding: Short Bias', 30, 25))
            signals.append('🟢 Short squeeze potential')
        else:
            scores.append(('Funding: Extreme Short', 15, 25))
            signals.append('🟢 Extreme shorts — Squeeze imminent')
    
    # Stablecoin Flow (weight: 25%)
    if stables.get('status') == 'ok':
        net = stables.get('net_signal', 'NEUTRAL')
        mint = stables.get('minting_count', 0)
        burn = stables.get('burning_count', 0)
        if net == 'INFLOW' and mint >= 3:
            scores.append(('Stablecoins: Strong Inflow', 25, 25))
            signals.append('🟢 Stablecoin minting — Capital entering crypto')
        elif net == 'INFLOW':
            scores.append(('Stablecoins: Mild Inflow', 35, 25))
        elif net == 'OUTFLOW' and burn >= 3:
            scores.append(('Stablecoins: Strong Outflow', 80, 25))
            signals.append('🔴 Stablecoin burning — Capital exiting crypto')
        elif net == 'OUTFLOW':
            scores.append(('Stablecoins: Mild Outflow', 65, 25))
        else:
            scores.append(('Stablecoins: Neutral', 50, 25))
    
    # Market Momentum (weight: 25%)
    if global_data.get('status') == 'ok':
        change = global_data.get('mcap_change_24h', 0)
        btc_dom = global_data.get('btc_dominance', 50)
        if change < -5:
            scores.append(('Market: Crash', 90, 25))
            signals.append('🔴 Market crash — Risk OFF')
        elif change < -2:
            scores.append(('Market: Selling', 70, 25))
            signals.append('🟡 Selling pressure')
        elif change < 2:
            scores.append(('Market: Stable', 45, 25))
        elif change < 5:
            scores.append(('Market: Buying', 30, 25))
            signals.append('🟢 Buying momentum')
        else:
            scores.append(('Market: Euphoria', 20, 25))
            signals.append('🟡 Euphoria — Caution at tops')
    
    # Weighted average
    if scores:
        total_weight = sum(s[2] for s in scores)
        composite = round(sum(s[1] * s[2] for s in scores) / total_weight) if total_weight > 0 else 50
    else:
        composite = 50
    
    # Determine regime
    if composite >= 75:
        regime = 'HIGH RISK'
        action = 'Reduce exposure, raise stops'
    elif composite >= 60:
        regime = 'ELEVATED'
        action = 'Defensive positioning'
    elif composite >= 40:
        regime = 'NEUTRAL'
        action = 'Normal allocation'
    elif composite >= 25:
        regime = 'LOW RISK'
        action = 'Accumulation zone'
    else:
        regime = 'OPPORTUNITY'
        action = 'Maximum accumulation'
    
    return {
        'score': composite,
        'regime': regime,
        'action': action,
        'components': [{'label': s[0], 'score': s[1], 'weight': s[2]} for s in scores],
        'signals': signals
    }

# ══════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════
def fmt_num(n):
    if n is None:
        return '$0'
    n = float(n)
    if abs(n) >= 1e12:
        return f"${n/1e12:.2f}T"
    elif abs(n) >= 1e9:
        return f"${n/1e9:.2f}B"
    elif abs(n) >= 1e6:
        return f"${n/1e6:.1f}M"
    elif abs(n) >= 1e3:
        return f"${n/1e3:.1f}K"
    else:
        return f"${n:.2f}"

def fmt_price(n):
    if n is None:
        return '$0'
    n = float(n)
    if n >= 1000:
        return f"${n:,.0f}"
    elif n >= 1:
        return f"${n:,.2f}"
    elif n >= 0.01:
        return f"${n:.4f}"
    else:
        return f"${n:.6f}"

# ══════════════════════════════════════════════════════════════
# LAMBDA HANDLER
# ══════════════════════════════════════════════════════════════
def lambda_handler(event, context):
    start = time.time()
    print("═══ CRYPTO INTELLIGENCE ENGINE v3.0 ═══")
    
    results = {}
    
    # Parallel fetch all sources
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {
            executor.submit(fetch_defillama_stablecoins): 'stablecoins',
            executor.submit(fetch_defillama_tvl): 'tvl',
            executor.submit(fetch_defillama_dex): 'dex',
            executor.submit(fetch_defillama_yields): 'yields',
            executor.submit(fetch_binance_funding): 'funding',
            executor.submit(fetch_binance_oi): 'open_interest',
            executor.submit(fetch_coingecko_global): 'global_market',
            executor.submit(fetch_coingecko_top): 'top_coins',
            executor.submit(fetch_btc_onchain): 'btc_onchain',
            executor.submit(fetch_fear_greed): 'fear_greed',
            executor.submit(fetch_cmc_data): 'cmc_movers',
        }
        
        for future in as_completed(futures):
            key = futures[future]
            try:
                results[key] = future.result()
                print(f"  ✅ {key}")
            except Exception as e:
                print(f"  ❌ {key}: {e}")
                results[key] = {'status': 'error', 'error': str(e)}
    
    # Compute composite risk score
    risk = compute_risk_score(
        results.get('fear_greed', {}),
        results.get('funding', {}),
        results.get('stablecoins', {}),
        results.get('global_market', {})
    )
    
    # Build output
    output = {
        'generated_at': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'fetch_time': round(time.time() - start, 1),
        'version': '3.0',
        'risk_score': risk,
        **results
    }
    
    # Publish to S3
    try:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key='crypto-intel.json',
            Body=json.dumps(output, default=str),
            ContentType='application/json',
            CacheControl='max-age=60'
        )
        print(f"  ✅ Published crypto-intel.json ({round(time.time()-start,1)}s)")
    except Exception as e:
        print(f"  ❌ S3 publish failed: {e}")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'status': 'published',
            'risk_score': risk['score'],
            'regime': risk['regime'],
            'sources': len([k for k, v in results.items() if isinstance(v, dict) and v.get('status') == 'ok']),
            'total_sources': len(results),
            'fetch_time': round(time.time() - start, 1)
        })
    }
