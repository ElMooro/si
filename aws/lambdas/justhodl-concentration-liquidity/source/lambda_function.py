"""
justhodl-concentration-liquidity — Position Concentration, Sector/Factor
Exposure, and Liquidity Risk Engine
========================================================================

What it does
============
For every position in data/portfolio.json:
  1. % of NAV (concentration)
  2. Sector exposure (per GICS-style sector ETF mapping)
  3. Factor exposure (value/growth/momentum/quality buckets)
  4. Liquidity: days-to-exit at 20% of average daily volume (ADV)
     - Pulls 30d avg volume from FMP
     - Days-to-exit = position_shares / (0.20 × ADV)

Alerts (Telegram):
  - Any position >10% NAV (concentration risk)
  - Any single sector >30% NAV (sector concentration)
  - Any position with >5d to exit at 20% ADV (liquidity risk)
  - Factor exposure imbalance (e.g., >80% growth, no quality)

Output: data/concentration-liquidity.json
Schedule: cron(0 14 * * ? *) — daily 14 UTC (after market close + portfolio settle)
"""
import os, json, time, urllib.request, urllib.parse
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3

VERSION = "1.0.0"
REGION = os.environ.get('AWS_REGION', 'us-east-1')
BUCKET = os.environ.get('S3_BUCKET', 'justhodl-dashboard-live')
OUT_KEY = "data/concentration-liquidity.json"
FMP_KEY = os.environ.get('FMP_KEY', '')
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '')

CONCENTRATION_THRESHOLD = float(os.environ.get('POSITION_CONCENTRATION_PCT', '10'))
SECTOR_THRESHOLD = float(os.environ.get('SECTOR_CONCENTRATION_PCT', '30'))
LIQUIDITY_DAYS_THRESHOLD = float(os.environ.get('LIQUIDITY_DAYS_THRESHOLD', '5'))

s3 = boto3.client('s3', region_name=REGION)


# Symbol → (sector, factor_tilt) — manually curated for common tickers
# In production, would be augmented from FMP's profile endpoint
SECTOR_MAP = {
    # Tech / Growth
    'AAPL': ('Technology', 'quality'), 'MSFT': ('Technology', 'quality'),
    'GOOGL': ('Technology', 'quality'), 'GOOG': ('Technology', 'quality'),
    'AMZN': ('Consumer Discretionary', 'growth'), 'META': ('Technology', 'quality'),
    'NVDA': ('Technology', 'momentum'), 'TSLA': ('Consumer Discretionary', 'momentum'),
    'AMD': ('Technology', 'momentum'), 'INTC': ('Technology', 'value'),
    'CRM': ('Technology', 'growth'), 'ORCL': ('Technology', 'quality'),
    'IBM': ('Technology', 'value'), 'ADBE': ('Technology', 'growth'),
    'AVGO': ('Technology', 'quality'), 'CSCO': ('Technology', 'value'),
    'NFLX': ('Communication', 'growth'),
    # Financials
    'JPM': ('Financials', 'quality'), 'BAC': ('Financials', 'value'),
    'WFC': ('Financials', 'value'), 'GS': ('Financials', 'quality'),
    'MS': ('Financials', 'quality'), 'V': ('Financials', 'quality'),
    'MA': ('Financials', 'quality'), 'BRK.B': ('Financials', 'value'),
    # Energy
    'XOM': ('Energy', 'value'), 'CVX': ('Energy', 'value'),
    'COP': ('Energy', 'value'), 'OXY': ('Energy', 'value'),
    # Healthcare
    'JNJ': ('Healthcare', 'quality'), 'UNH': ('Healthcare', 'quality'),
    'PFE': ('Healthcare', 'value'), 'LLY': ('Healthcare', 'growth'),
    'ABBV': ('Healthcare', 'value'), 'MRK': ('Healthcare', 'quality'),
    # Industrials / Defense
    'BA': ('Industrials', 'value'), 'CAT': ('Industrials', 'value'),
    'LMT': ('Industrials', 'quality'), 'RTX': ('Industrials', 'value'),
    # Consumer
    'WMT': ('Consumer Staples', 'quality'), 'COST': ('Consumer Staples', 'quality'),
    'PG': ('Consumer Staples', 'quality'), 'KO': ('Consumer Staples', 'quality'),
    'PEP': ('Consumer Staples', 'quality'),
    # ETFs
    'SPY': ('ETF/Index', 'mixed'), 'VOO': ('ETF/Index', 'mixed'),
    'IVV': ('ETF/Index', 'mixed'), 'QQQ': ('ETF/Index_Tech', 'growth'),
    'IWM': ('ETF/Index_Small', 'value'), 'DIA': ('ETF/Index', 'mixed'),
    'XLK': ('Technology', 'growth'), 'XLF': ('Financials', 'value'),
    'XLE': ('Energy', 'value'), 'XLV': ('Healthcare', 'quality'),
    'XLY': ('Consumer Discretionary', 'growth'),
    'XLP': ('Consumer Staples', 'quality'), 'XLI': ('Industrials', 'value'),
    'XLU': ('Utilities', 'quality'), 'XLB': ('Materials', 'value'),
    'XLRE': ('Real Estate', 'quality'), 'XLC': ('Communication', 'growth'),
    'SMH': ('Technology', 'momentum'), 'SOXX': ('Technology', 'momentum'),
    # Bonds
    'TLT': ('Treasury Long', 'defensive'), 'IEF': ('Treasury Mid', 'defensive'),
    'SHY': ('Treasury Short', 'defensive'), 'BIL': ('Cash', 'defensive'),
    'HYG': ('Credit High Yield', 'value'), 'LQD': ('Credit Investment Grade', 'quality'),
    'JNK': ('Credit High Yield', 'value'),
    # Commodities / FX / Crypto
    'GLD': ('Gold', 'defensive'), 'IAU': ('Gold', 'defensive'),
    'SLV': ('Silver', 'defensive'), 'USO': ('Oil', 'value'),
    'UNG': ('Natural Gas', 'value'), 'UUP': ('USD', 'defensive'),
    'GBTC': ('Crypto', 'momentum'), 'IBIT': ('Crypto', 'momentum'),
    'BITO': ('Crypto', 'momentum'),
    # Vol
    'VXX': ('Volatility', 'defensive'), 'UVXY': ('Volatility', 'defensive'),
}


def http_get_json(url, timeout=12):
    req = urllib.request.Request(url, headers={'User-Agent': 'JustHodl/ConcLiq'})
    return json.loads(urllib.request.urlopen(req, timeout=timeout).read())


def send_telegram(msg):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = urllib.parse.urlencode({
            'chat_id': TELEGRAM_CHAT_ID, 'text': msg[:4000],
            'parse_mode': 'Markdown',
        }).encode()
        urllib.request.urlopen(urllib.request.Request(url, data=data, method='POST'), timeout=10)
        return True
    except Exception:
        return False


def fetch_adv_for_symbol(symbol):
    """30d average daily volume via FMP."""
    if not FMP_KEY:
        return None
    try:
        url = f"https://financialmodelingprep.com/stable/historical-price-eod/light?symbol={symbol}&apikey={FMP_KEY}"
        data = http_get_json(url, timeout=8)
        if isinstance(data, dict):
            data = data.get('historical', [])
        if not isinstance(data, list) or len(data) < 5:
            return None
        vols = [int(d.get('volume', 0) or 0) for d in data[:30] if d.get('volume')]
        if not vols:
            return None
        return sum(vols) / len(vols)
    except Exception:
        return None


def load_portfolio():
    try:
        obj = s3.get_object(Bucket=BUCKET, Key='data/portfolio.json')
        raw = json.loads(obj['Body'].read())
    except Exception as e:
        print(f"[portfolio] read err: {e}")
        return None
    
    positions = raw if isinstance(raw, list) else raw.get('positions', [])
    out = []
    for p in positions:
        if not isinstance(p, dict):
            continue
        sym = (p.get('symbol') or p.get('ticker') or '').upper()
        if not sym:
            continue
        mv = float(p.get('market_value') or p.get('mv') or p.get('value') or 0)
        shares = float(p.get('shares') or p.get('quantity') or p.get('qty') or 0)
        price = float(p.get('price') or p.get('last_price') or 0)
        if mv == 0:
            continue
        # Estimate shares if missing
        if shares == 0 and price > 0:
            shares = mv / price
        out.append({
            'symbol': sym, 'market_value': mv,
            'shares': shares, 'price': price,
        })
    return out


def lambda_handler(event=None, context=None):
    started = time.time()
    print(f"[concentration-liquidity] v{VERSION} starting")
    
    positions = load_portfolio()
    if not positions:
        result = {
            'version': VERSION,
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'no_action': 'no_portfolio_or_empty',
            'elapsed_s': round(time.time() - started, 1),
        }
        s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                      Body=json.dumps(result, default=str, indent=2).encode(),
                      ContentType='application/json')
        return {'statusCode': 200, 'body': json.dumps(result)}
    
    total_nav = sum(p['market_value'] for p in positions)
    
    # Fetch ADV in parallel
    syms = list({p['symbol'] for p in positions})
    adv_map = {}
    with ThreadPoolExecutor(max_workers=10) as ex:
        future_to_sym = {ex.submit(fetch_adv_for_symbol, s): s for s in syms}
        for f in as_completed(future_to_sym):
            adv_map[future_to_sym[f]] = f.result()
    
    # Enrich + compute
    enriched = []
    sector_exposure = {}
    factor_exposure = {}
    
    for p in positions:
        sym = p['symbol']
        sector, factor = SECTOR_MAP.get(sym, ('Unknown', 'unknown'))
        pct_nav = p['market_value'] / total_nav * 100 if total_nav else 0
        adv = adv_map.get(sym)
        days_to_exit = None
        if adv and adv > 0 and p['shares'] > 0:
            days_to_exit = p['shares'] / (adv * 0.20)
        
        enriched.append({
            'symbol': sym,
            'market_value': round(p['market_value'], 2),
            'pct_of_nav': round(pct_nav, 2),
            'shares': round(p['shares'], 2),
            'price': round(p['price'], 2) if p['price'] else None,
            'sector': sector,
            'factor': factor,
            'adv_30d': int(adv) if adv else None,
            'days_to_exit_20pct_adv': round(days_to_exit, 2) if days_to_exit else None,
        })
        
        sector_exposure[sector] = sector_exposure.get(sector, 0) + p['market_value']
        factor_exposure[factor] = factor_exposure.get(factor, 0) + p['market_value']
    
    enriched.sort(key=lambda p: p['pct_of_nav'], reverse=True)
    
    sector_pct = {k: round(v / total_nav * 100, 2) for k, v in sector_exposure.items()}
    factor_pct = {k: round(v / total_nav * 100, 2) for k, v in factor_exposure.items()}
    
    # Alerts
    concentration_alerts = [p for p in enriched if p['pct_of_nav'] > CONCENTRATION_THRESHOLD]
    sector_alerts = [(k, v) for k, v in sector_pct.items() if v > SECTOR_THRESHOLD]
    liquidity_alerts = [p for p in enriched
                        if p.get('days_to_exit_20pct_adv') is not None
                        and p['days_to_exit_20pct_adv'] > LIQUIDITY_DAYS_THRESHOLD]
    
    # Factor imbalance: any factor >70% or quality <10%
    factor_alerts = []
    for f, pct in factor_pct.items():
        if pct > 70:
            factor_alerts.append(f"{f} concentration {pct}%")
    quality_pct = factor_pct.get('quality', 0)
    if total_nav > 10000 and quality_pct < 10:
        factor_alerts.append(f"low quality factor {quality_pct}% (target >10%)")
    
    payload = {
        'version': VERSION,
        'generated_at': datetime.now(timezone.utc).isoformat(),
        'n_positions': len(positions),
        'total_nav': round(total_nav, 2),
        'positions': enriched,
        'sector_exposure_pct': sector_pct,
        'factor_exposure_pct': factor_pct,
        'alerts': {
            'concentration': [{'symbol': p['symbol'], 'pct_of_nav': p['pct_of_nav']}
                              for p in concentration_alerts],
            'sector_concentration': [{'sector': s, 'pct_of_nav': v}
                                     for s, v in sector_alerts],
            'liquidity': [{'symbol': p['symbol'],
                           'days_to_exit': p['days_to_exit_20pct_adv'],
                           'pct_of_nav': p['pct_of_nav']}
                          for p in liquidity_alerts],
            'factor_imbalance': factor_alerts,
        },
        'thresholds': {
            'position_pct_nav': CONCENTRATION_THRESHOLD,
            'sector_pct_nav': SECTOR_THRESHOLD,
            'liquidity_days': LIQUIDITY_DAYS_THRESHOLD,
        },
        'elapsed_s': round(time.time() - started, 1),
    }
    
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(payload, default=str, indent=2).encode(),
                  ContentType='application/json', CacheControl='max-age=3600')
    
    n_alerts = (len(concentration_alerts) + len(sector_alerts)
                + len(liquidity_alerts) + len(factor_alerts))
    if n_alerts:
        lines = [f"*⚠️ CONCENTRATION + LIQUIDITY ALERTS*"]
        lines.append(f"NAV: ${total_nav:,.0f}  ({len(positions)} positions)")
        if concentration_alerts:
            lines.append(f"\n*Position concentration >{CONCENTRATION_THRESHOLD}% NAV ({len(concentration_alerts)}):*")
            for p in concentration_alerts[:5]:
                lines.append(f"  • `{p['symbol']}` — {p['pct_of_nav']}%")
        if sector_alerts:
            lines.append(f"\n*Sector concentration >{SECTOR_THRESHOLD}% NAV ({len(sector_alerts)}):*")
            for s, v in sector_alerts[:5]:
                lines.append(f"  • {s} — {v}%")
        if liquidity_alerts:
            lines.append(f"\n*Liquidity risk >{LIQUIDITY_DAYS_THRESHOLD}d to exit at 20% ADV ({len(liquidity_alerts)}):*")
            for p in liquidity_alerts[:5]:
                lines.append(f"  • `{p['symbol']}` — {p['days_to_exit_20pct_adv']:.1f}d ({p['pct_of_nav']}% NAV)")
        if factor_alerts:
            lines.append(f"\n*Factor imbalance:*")
            for f in factor_alerts:
                lines.append(f"  • {f}")
        send_telegram("\n".join(lines))
    
    print(f"[concentration-liquidity] done · n={len(positions)} · alerts={n_alerts} · "
          f"elapsed={payload['elapsed_s']}s")
    return {
        'statusCode': 200,
        'body': json.dumps({'ok': True, 'n_positions': len(positions),
                            'n_alerts': n_alerts,
                            'elapsed_s': payload['elapsed_s']}),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2))
