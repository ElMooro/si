"""
JustHodl.AI Crypto Intelligence Engine v4.0
=============================================
TIER 1: Technical Indicators (RSI, MACD, EMA, BB, Supertrend, StochRSI, ATR)
TIER 2: Chart Pattern Detection (double top/bottom, H&S, triangles, flags, wedges)
TIER 3: Additional Data Sources (whale txns, ETH gas, on-chain ratios)
TIER 4: Multi-Timeframe Analysis (4h, 1d, 1w for BTC, ETH, PEPE, DOGE, POL)
+ AI Intelligence via Claude API
+ All existing sources (DeFiLlama, Binance, CoinGecko, Blockchain, F&G, CMC)
"""
import json, os, time, math, urllib.request, urllib.error, ssl, statistics
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

S3_BUCKET = os.environ.get('S3_BUCKET', 'justhodl-dashboard-live')
CMC_API_KEY = os.environ.get('CMC_API_KEY', '')
ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY', '')

import boto3
s3 = boto3.client('s3')
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

TARGET_COINS = ['BTC', 'ETH', 'PEPE', 'DOGE', 'POL']
TIMEFRAMES = {'4h': '4h', '1d': '1d', '1w': '1w'}
BINANCE_SYMBOLS = {'BTC': 'BTCUSDT', 'ETH': 'ETHUSDT', 'PEPE': 'PEPEUSDT', 'DOGE': 'DOGEUSDT', 'POL': 'POLUSDT'}

def http_get(url, headers=None, timeout=15):
    try:
        req = urllib.request.Request(url, headers=headers or {'User-Agent': 'JustHodl/4.0'})
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        print(f"  HTTP ERR {url[:80]}: {e}")
        return None

def http_post(url, data, headers, timeout=45):
    try:
        req = urllib.request.Request(url, data=json.dumps(data).encode(), headers=headers, method='POST')
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        print(f"  POST ERR {url[:60]}: {e}")
        return None

def fmt_num(n):
    if n is None: return '-'
    n = float(n)
    if abs(n) >= 1e12: return f"${n/1e12:.2f}T"
    if abs(n) >= 1e9: return f"${n/1e9:.2f}B"
    if abs(n) >= 1e6: return f"${n/1e6:.1f}M"
    if abs(n) >= 1e3: return f"${n/1e3:.1f}K"
    return f"${n:.2f}"

def sr(v, d=2):
    try: return round(float(v or 0), d)
    except: return 0

# ════════════════════════════════════════════════════════════════
# TIER 1: TECHNICAL INDICATORS (pure Python, from Binance klines)
# ════════════════════════════════════════════════════════════════
def calc_ema(closes, period):
    if len(closes) < period: return [None] * len(closes)
    k = 2 / (period + 1)
    ema = [None] * (period - 1)
    ema.append(sum(closes[:period]) / period)
    for i in range(period, len(closes)):
        ema.append(closes[i] * k + ema[-1] * (1 - k))
    return ema

def calc_rsi(closes, period=14):
    if len(closes) < period + 1: return None, []
    deltas = [closes[i] - closes[i-1] for i in range(1, len(closes))]
    gains = [max(d, 0) for d in deltas]
    losses = [abs(min(d, 0)) for d in deltas]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    rsi_vals = []
    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        rs = avg_gain / avg_loss if avg_loss > 0 else 100
        rsi_vals.append(round(100 - 100 / (1 + rs), 2))
    current = rsi_vals[-1] if rsi_vals else None
    return current, rsi_vals

def calc_macd(closes, fast=12, slow=26, signal=9):
    ema_fast = calc_ema(closes, fast)
    ema_slow = calc_ema(closes, slow)
    macd_line = []
    for i in range(len(closes)):
        if ema_fast[i] is not None and ema_slow[i] is not None:
            macd_line.append(ema_fast[i] - ema_slow[i])
        else:
            macd_line.append(None)
    valid = [v for v in macd_line if v is not None]
    if len(valid) < signal: return None, None, None, 'NONE'
    sig_line = calc_ema(valid, signal)
    cur_macd = valid[-1]
    cur_sig = sig_line[-1] if sig_line else None
    histogram = round(cur_macd - cur_sig, 6) if cur_sig is not None else None
    cross = 'BULLISH' if len(valid) >= 2 and cur_sig and valid[-2] < sig_line[-2] and cur_macd > cur_sig else \
            'BEARISH' if len(valid) >= 2 and cur_sig and valid[-2] > sig_line[-2] and cur_macd < cur_sig else None
    return round(cur_macd, 6), round(cur_sig, 6) if cur_sig else None, histogram, cross if cross else 'NONE'

def calc_bollinger(closes, period=20, std_mult=2):
    if len(closes) < period: return None, None, None, None, None
    sma = sum(closes[-period:]) / period
    std = statistics.stdev(closes[-period:])
    upper = sma + std_mult * std
    lower = sma - std_mult * std
    width = (upper - lower) / sma * 100
    pos = (closes[-1] - lower) / (upper - lower) * 100 if upper != lower else 50
    return round(upper, 6), round(sma, 6), round(lower, 6), round(width, 2), round(pos, 1)

def calc_atr(highs, lows, closes, period=14):
    if len(closes) < period + 1: return None
    trs = []
    for i in range(1, len(closes)):
        tr = max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1]))
        trs.append(tr)
    atr = sum(trs[:period]) / period
    for i in range(period, len(trs)):
        atr = (atr * (period - 1) + trs[i]) / period
    return round(atr, 6)

def calc_stochrsi(closes, rsi_period=14, stoch_period=14, k_period=3, d_period=3):
    _, rsi_vals = calc_rsi(closes, rsi_period)
    if len(rsi_vals) < stoch_period: return None, None
    stoch_vals = []
    for i in range(stoch_period - 1, len(rsi_vals)):
        window = rsi_vals[i - stoch_period + 1:i + 1]
        low = min(window)
        high = max(window)
        stoch = (rsi_vals[i] - low) / (high - low) * 100 if high != low else 50
        stoch_vals.append(stoch)
    if len(stoch_vals) < k_period: return None, None
    k_vals = [sum(stoch_vals[i:i+k_period]) / k_period for i in range(len(stoch_vals) - k_period + 1)]
    d_vals = [sum(k_vals[i:i+d_period]) / d_period for i in range(len(k_vals) - d_period + 1)] if len(k_vals) >= d_period else []
    return round(k_vals[-1], 2) if k_vals else None, round(d_vals[-1], 2) if d_vals else None

def calc_supertrend(highs, lows, closes, period=10, multiplier=3):
    atr_val = calc_atr(highs, lows, closes, period)
    if atr_val is None: return None, None
    hl2 = (highs[-1] + lows[-1]) / 2
    upper = hl2 + multiplier * atr_val
    lower = hl2 - multiplier * atr_val
    trend = 'BULLISH' if closes[-1] > upper else 'BEARISH' if closes[-1] < lower else 'BULLISH' if closes[-1] > hl2 else 'BEARISH'
    return round(lower if trend == 'BULLISH' else upper, 6), trend

def calc_volume_profile(volumes, closes, n_bins=10):
    if not volumes or not closes: return []
    min_p, max_p = min(closes), max(closes)
    if min_p == max_p: return []
    step = (max_p - min_p) / n_bins
    bins = [{'low': min_p + i * step, 'high': min_p + (i+1) * step, 'volume': 0} for i in range(n_bins)]
    for i, c in enumerate(closes):
        idx = min(int((c - min_p) / step), n_bins - 1)
        bins[idx]['volume'] += volumes[i] if i < len(volumes) else 0
    max_vol = max(b['volume'] for b in bins) if bins else 1
    poc_idx = max(range(len(bins)), key=lambda i: bins[i]['volume'])
    return {
        'poc_price': round((bins[poc_idx]['low'] + bins[poc_idx]['high']) / 2, 2),
        'poc_volume_pct': round(bins[poc_idx]['volume'] / sum(b['volume'] for b in bins) * 100, 1) if sum(b['volume'] for b in bins) > 0 else 0,
        'high_volume_zones': [round((b['low'] + b['high']) / 2, 2) for b in sorted(bins, key=lambda x: x['volume'], reverse=True)[:3]]
    }

def fetch_binance_klines(symbol, interval, limit=100):
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
    data = http_get(url)
    if not data: return None
    candles = []
    for k in data:
        candles.append({
            'time': k[0], 'open': float(k[1]), 'high': float(k[2]),
            'low': float(k[3]), 'close': float(k[4]), 'volume': float(k[5])
        })
    return candles

def analyze_coin_timeframe(coin, tf):
    """TIER 1 + TIER 2 analysis for one coin on one timeframe"""
    symbol = BINANCE_SYMBOLS.get(coin)
    if not symbol: return {'status': 'error', 'error': f'Unknown symbol {coin}'}
    
    candles = fetch_binance_klines(symbol, tf, 200)
    if not candles or len(candles) < 30:
        return {'status': 'error', 'error': 'Insufficient data'}
    
    closes = [c['close'] for c in candles]
    highs = [c['high'] for c in candles]
    lows = [c['low'] for c in candles]
    volumes = [c['volume'] for c in candles]
    
    # TIER 1: Technical Indicators
    rsi, _ = calc_rsi(closes)
    macd_val, macd_sig, macd_hist, macd_cross = calc_macd(closes)
    ema20 = calc_ema(closes, 20)
    ema50 = calc_ema(closes, 50)
    ema200 = calc_ema(closes, 200)
    bb_upper, bb_mid, bb_lower, bb_width, bb_pos = calc_bollinger(closes)
    atr = calc_atr(highs, lows, closes)
    stoch_k, stoch_d = calc_stochrsi(closes)
    st_val, st_trend = calc_supertrend(highs, lows, closes)
    vol_profile = calc_volume_profile(volumes, closes)
    
    cur = closes[-1]
    ema20_cur = ema20[-1] if ema20 and ema20[-1] else None
    ema50_cur = ema50[-1] if ema50 and ema50[-1] else None
    ema200_cur = ema200[-1] if ema200 and ema200[-1] else None
    
    # EMA alignment
    ema_trend = 'STRONG_BULL' if ema20_cur and ema50_cur and ema200_cur and ema20_cur > ema50_cur > ema200_cur else \
                'BULL' if ema20_cur and ema50_cur and ema20_cur > ema50_cur else \
                'STRONG_BEAR' if ema20_cur and ema50_cur and ema200_cur and ema20_cur < ema50_cur < ema200_cur else \
                'BEAR' if ema20_cur and ema50_cur and ema20_cur < ema50_cur else 'NEUTRAL'
    
    # Golden/Death cross detection
    cross_signal = None
    if ema50 and ema200 and len(ema50) >= 2 and len(ema200) >= 2:
        if ema50[-1] and ema200[-1] and ema50[-2] and ema200[-2]:
            if ema50[-2] < ema200[-2] and ema50[-1] > ema200[-1]: cross_signal = 'GOLDEN_CROSS'
            elif ema50[-2] > ema200[-2] and ema50[-1] < ema200[-1]: cross_signal = 'DEATH_CROSS'
    
    # TIER 2: Chart Pattern Detection
    patterns = detect_patterns(closes, highs, lows, volumes)
    
    # Composite signal
    signals = []
    bull = bear = 0
    if rsi and rsi < 30: signals.append('RSI oversold'); bull += 2
    elif rsi and rsi > 70: signals.append('RSI overbought'); bear += 2
    if macd_cross == 'BULLISH': signals.append('MACD bullish cross'); bull += 2
    elif macd_cross == 'BEARISH': signals.append('MACD bearish cross'); bear += 2
    if st_trend == 'BULLISH': bull += 1
    else: bear += 1
    if ema_trend in ('STRONG_BULL', 'BULL'): bull += 1
    elif ema_trend in ('STRONG_BEAR', 'BEAR'): bear += 1
    if stoch_k and stoch_k < 20: bull += 1
    elif stoch_k and stoch_k > 80: bear += 1
    if bb_pos and bb_pos < 10: bull += 1; signals.append('Near BB lower')
    elif bb_pos and bb_pos > 90: bear += 1; signals.append('Near BB upper')
    if cross_signal == 'GOLDEN_CROSS': bull += 3; signals.append('GOLDEN CROSS!')
    elif cross_signal == 'DEATH_CROSS': bear += 3; signals.append('DEATH CROSS!')
    
    total = bull + bear
    score = round((bull / total * 100) if total > 0 else 50)
    bias = 'STRONG_BUY' if score >= 80 else 'BUY' if score >= 60 else 'NEUTRAL' if score >= 40 else 'SELL' if score >= 20 else 'STRONG_SELL'
    
    return {
        'status': 'ok', 'coin': coin, 'timeframe': tf,
        'price': round(cur, 8), 'change_pct': round((cur - closes[0]) / closes[0] * 100, 2),
        'indicators': {
            'rsi': rsi,
            'macd': {'value': macd_val, 'signal': macd_sig, 'histogram': macd_hist, 'cross': macd_cross},
            'ema': {'ema20': round(ema20_cur, 8) if ema20_cur else None, 'ema50': round(ema50_cur, 8) if ema50_cur else None, 'ema200': round(ema200_cur, 8) if ema200_cur else None, 'trend': ema_trend, 'cross_signal': cross_signal},
            'bollinger': {'upper': bb_upper, 'middle': bb_mid, 'lower': bb_lower, 'width': bb_width, 'position': bb_pos},
            'atr': atr, 'atr_pct': round(atr / cur * 100, 2) if atr and cur > 0 else None,
            'stochrsi': {'k': stoch_k, 'd': stoch_d},
            'supertrend': {'value': st_val, 'trend': st_trend},
            'volume_profile': vol_profile
        },
        'patterns': patterns,
        'score': score, 'bias': bias, 'signals': signals
    }

# ════════════════════════════════════════════════════════════════
# TIER 2: CHART PATTERN DETECTION
# ════════════════════════════════════════════════════════════════
def find_pivots(data, left=5, right=5):
    highs, lows = [], []
    for i in range(left, len(data) - right):
        if all(data[i] >= data[i-j] for j in range(1, left+1)) and all(data[i] >= data[i+j] for j in range(1, right+1)):
            highs.append((i, data[i]))
        if all(data[i] <= data[i-j] for j in range(1, left+1)) and all(data[i] <= data[i+j] for j in range(1, right+1)):
            lows.append((i, data[i]))
    return highs, lows

def detect_patterns(closes, highs_arr, lows_arr, volumes):
    patterns = []
    pivot_highs, pivot_lows = find_pivots(closes, 5, 5)
    
    # Double Top
    if len(pivot_highs) >= 2:
        h1, h2 = pivot_highs[-2], pivot_highs[-1]
        if abs(h1[1] - h2[1]) / h1[1] < 0.03 and h2[0] - h1[0] >= 10:
            neckline = min(closes[h1[0]:h2[0]+1])
            if closes[-1] < neckline:
                patterns.append({'type': 'DOUBLE_TOP', 'confidence': 'HIGH', 'bias': 'BEARISH', 'detail': f'Peaks at {h1[1]:.2f} & {h2[1]:.2f}, neckline {neckline:.2f}'})
            else:
                patterns.append({'type': 'DOUBLE_TOP_FORMING', 'confidence': 'MEDIUM', 'bias': 'BEARISH', 'detail': f'Watch neckline {neckline:.2f}'})
    
    # Double Bottom
    if len(pivot_lows) >= 2:
        l1, l2 = pivot_lows[-2], pivot_lows[-1]
        if abs(l1[1] - l2[1]) / l1[1] < 0.03 and l2[0] - l1[0] >= 10:
            neckline = max(closes[l1[0]:l2[0]+1])
            if closes[-1] > neckline:
                patterns.append({'type': 'DOUBLE_BOTTOM', 'confidence': 'HIGH', 'bias': 'BULLISH', 'detail': f'Troughs at {l1[1]:.2f} & {l2[1]:.2f}, neckline {neckline:.2f}'})
            else:
                patterns.append({'type': 'DOUBLE_BOTTOM_FORMING', 'confidence': 'MEDIUM', 'bias': 'BULLISH', 'detail': f'Watch neckline {neckline:.2f}'})
    
    # Head & Shoulders
    if len(pivot_highs) >= 3:
        s1, head, s2 = pivot_highs[-3], pivot_highs[-2], pivot_highs[-1]
        if head[1] > s1[1] and head[1] > s2[1] and abs(s1[1] - s2[1]) / s1[1] < 0.05:
            neckline = (min(closes[s1[0]:head[0]+1]) + min(closes[head[0]:s2[0]+1])) / 2
            if closes[-1] < neckline:
                patterns.append({'type': 'HEAD_SHOULDERS', 'confidence': 'HIGH', 'bias': 'BEARISH', 'detail': f'Head {head[1]:.2f}, neckline {neckline:.2f}'})
            else:
                patterns.append({'type': 'HEAD_SHOULDERS_FORMING', 'confidence': 'MEDIUM', 'bias': 'BEARISH', 'detail': f'Neckline at {neckline:.2f}'})
    
    # Inverse Head & Shoulders
    if len(pivot_lows) >= 3:
        s1, head, s2 = pivot_lows[-3], pivot_lows[-2], pivot_lows[-1]
        if head[1] < s1[1] and head[1] < s2[1] and abs(s1[1] - s2[1]) / max(s1[1], 0.0001) < 0.05:
            neckline = (max(closes[s1[0]:head[0]+1]) + max(closes[head[0]:s2[0]+1])) / 2
            if closes[-1] > neckline:
                patterns.append({'type': 'INV_HEAD_SHOULDERS', 'confidence': 'HIGH', 'bias': 'BULLISH', 'detail': f'Head {head[1]:.2f}, breakout above {neckline:.2f}'})
    
    # Ascending/Descending Triangle
    if len(pivot_highs) >= 2 and len(pivot_lows) >= 2:
        h_slope = (pivot_highs[-1][1] - pivot_highs[-2][1]) / max(pivot_highs[-1][0] - pivot_highs[-2][0], 1)
        l_slope = (pivot_lows[-1][1] - pivot_lows[-2][1]) / max(pivot_lows[-1][0] - pivot_lows[-2][0], 1)
        
        if abs(h_slope) < 0.001 * closes[-1] and l_slope > 0:
            patterns.append({'type': 'ASCENDING_TRIANGLE', 'confidence': 'MEDIUM', 'bias': 'BULLISH', 'detail': f'Flat top ~{pivot_highs[-1][1]:.2f}, rising lows'})
        elif abs(l_slope) < 0.001 * closes[-1] and h_slope < 0:
            patterns.append({'type': 'DESCENDING_TRIANGLE', 'confidence': 'MEDIUM', 'bias': 'BEARISH', 'detail': f'Flat bottom ~{pivot_lows[-1][1]:.2f}, falling highs'})
        elif h_slope < 0 and l_slope > 0:
            patterns.append({'type': 'SYMMETRICAL_TRIANGLE', 'confidence': 'LOW', 'bias': 'NEUTRAL', 'detail': 'Converging trendlines - breakout imminent'})
    
    # Bull/Bear Flag
    if len(closes) >= 20:
        recent = closes[-20:]
        prior = closes[-40:-20] if len(closes) >= 40 else closes[:len(closes)//2]
        if prior:
            prior_move = (prior[-1] - prior[0]) / prior[0] * 100
            recent_range = (max(recent) - min(recent)) / min(recent) * 100
            if prior_move > 10 and recent_range < 5:
                patterns.append({'type': 'BULL_FLAG', 'confidence': 'MEDIUM', 'bias': 'BULLISH', 'detail': f'Strong rally ({prior_move:.1f}%) + tight consolidation ({recent_range:.1f}%)'})
            elif prior_move < -10 and recent_range < 5:
                patterns.append({'type': 'BEAR_FLAG', 'confidence': 'MEDIUM', 'bias': 'BEARISH', 'detail': f'Sharp decline ({prior_move:.1f}%) + consolidation ({recent_range:.1f}%)'})
    
    return patterns

# ════════════════════════════════════════════════════════════════
# MULTI-TIMEFRAME ANALYSIS (TIER 4)
# ════════════════════════════════════════════════════════════════
def fetch_all_technicals():
    """Run TIER 1+2+4 for all target coins across all timeframes"""
    print("  📊 TIER 1+2+4: Multi-timeframe technical analysis...")
    results = {}
    
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {}
        for coin in TARGET_COINS:
            for tf_key, tf_val in TIMEFRAMES.items():
                key = f"{coin}_{tf_key}"
                futures[executor.submit(analyze_coin_timeframe, coin, tf_val)] = key
        
        for future in as_completed(futures):
            key = futures[future]
            try:
                results[key] = future.result()
            except Exception as e:
                results[key] = {'status': 'error', 'error': str(e)}
    
    # Build per-coin summary
    summaries = {}
    for coin in TARGET_COINS:
        coin_data = {}
        for tf_key in TIMEFRAMES:
            k = f"{coin}_{tf_key}"
            coin_data[tf_key] = results.get(k, {'status': 'error'})
        
        # Multi-TF consensus
        bull = bear = 0
        all_signals = []
        for tf in coin_data.values():
            if tf.get('status') == 'ok':
                if tf.get('score', 50) > 60: bull += 1
                elif tf.get('score', 50) < 40: bear += 1
                all_signals.extend(tf.get('signals', []))
        
        consensus = 'STRONG_BUY' if bull == 3 else 'BUY' if bull >= 2 else 'STRONG_SELL' if bear == 3 else 'SELL' if bear >= 2 else 'MIXED'
        
        summaries[coin] = {
            'timeframes': coin_data,
            'consensus': consensus,
            'bull_timeframes': bull,
            'bear_timeframes': bear,
            'key_signals': list(set(all_signals))[:8],
            'price': coin_data.get('1d', {}).get('price', 0)
        }
    
    return {'status': 'ok', 'coins': summaries}

# ════════════════════════════════════════════════════════════════
# EXISTING SOURCES (DeFiLlama, Binance, CoinGecko, etc.)
# ════════════════════════════════════════════════════════════════
def fetch_defillama_stablecoins():
    data = http_get("https://stablecoins.llama.fi/stablecoins?includePrices=true")
    if not data or 'peggedAssets' not in data:
        return {'status': 'error', 'error': 'No stablecoin data'}
    stables = []
    total_mcap = 0
    minting = burning = stable_c = 0
    for s in sorted(data['peggedAssets'], key=lambda x: x.get('circulating', {}).get('peggedUSD', 0) or 0, reverse=True)[:25]:
        circ = s.get('circulating', {}).get('peggedUSD', 0) or 0
        pd_ = s.get('circulatingPrevDay', {}).get('peggedUSD', 0) or 0
        pw = s.get('circulatingPrevWeek', {}).get('peggedUSD', 0) or 0
        pm = s.get('circulatingPrevMonth', {}).get('peggedUSD', 0) or 0
        c1d = ((circ - pd_) / pd_ * 100) if pd_ > 0 else 0
        c7d = ((circ - pw) / pw * 100) if pw > 0 else 0
        c30d = ((circ - pm) / pm * 100) if pm > 0 else 0
        sig = 'MINTING' if c7d > 0.5 else 'BURNING' if c7d < -0.5 else 'STABLE'
        if sig == 'MINTING': minting += 1
        elif sig == 'BURNING': burning += 1
        else: stable_c += 1
        total_mcap += circ
        if circ > 50_000_000:
            stables.append({'name': s.get('name', '?'), 'symbol': s.get('symbol', '?'), 'mcap': round(circ), 'mcap_fmt': fmt_num(circ), 'change_1d': sr(c1d), 'change_7d': sr(c7d), 'change_30d': sr(c30d), 'signal': sig, 'mechanism': s.get('pegMechanism', 'unknown')})
    return {'status': 'ok', 'stablecoins': stables, 'total_mcap': round(total_mcap), 'total_mcap_fmt': fmt_num(total_mcap), 'minting_count': minting, 'burning_count': burning, 'stable_count': stable_c, 'net_signal': 'INFLOW' if minting > burning + 2 else 'OUTFLOW' if burning > minting + 2 else 'NEUTRAL'}

def fetch_defillama_tvl():
    protocols = http_get("https://api.llama.fi/protocols")
    chains_data = http_get("https://api.llama.fi/v2/chains")
    result = {'status': 'ok', 'total_tvl': 0, 'chains': [], 'top_protocols': []}
    if chains_data:
        chains_sorted = sorted(chains_data, key=lambda x: x.get('tvl', 0), reverse=True)[:15]
        total = sum(c.get('tvl', 0) for c in chains_data)
        result['total_tvl'] = round(total)
        result['total_tvl_fmt'] = fmt_num(total)
        result['chains'] = [{'name': c.get('name', '?'), 'tvl': round(c.get('tvl', 0)), 'tvl_fmt': fmt_num(c.get('tvl', 0)), 'share': sr(c.get('tvl', 0) / total * 100 if total else 0, 1)} for c in chains_sorted]
    if protocols:
        top = sorted(protocols, key=lambda x: x.get('tvl', 0), reverse=True)[:10]
        result['top_protocols'] = [{'name': p.get('name', '?'), 'tvl': round(p.get('tvl', 0)), 'tvl_fmt': fmt_num(p.get('tvl', 0)), 'chain': p.get('chain', '?'), 'category': p.get('category', '?'), 'change_1d': sr(p.get('change_1d', 0) or 0), 'change_7d': sr(p.get('change_7d', 0) or 0)} for p in top]
    return result

def fetch_defillama_dex():
    data = http_get("https://api.llama.fi/overview/dexs?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyVolume")
    if not data: return {'status': 'error', 'error': 'No DEX data'}
    protocols = data.get('protocols', [])
    top_dexes = sorted(protocols, key=lambda x: x.get('total24h', 0) or 0, reverse=True)[:10]
    total_vol = sum(d.get('total24h', 0) or 0 for d in protocols)
    return {'status': 'ok', 'total_24h_volume': round(total_vol), 'total_24h_fmt': fmt_num(total_vol), 'top_dexes': [{'name': d.get('name', '?'), 'volume_24h': round(d.get('total24h', 0) or 0), 'volume_fmt': fmt_num(d.get('total24h', 0) or 0), 'change_1d': sr(d.get('change_1d', 0) or 0)} for d in top_dexes]}

def fetch_defillama_yields():
    data = http_get("https://yields.llama.fi/pools")
    if not data or 'data' not in data: return {'status': 'error', 'error': 'No yield data'}
    pools = data['data']
    good = [p for p in pools if (p.get('tvlUsd', 0) or 0) > 1_000_000]
    top = sorted(good, key=lambda x: x.get('apy', 0) or 0, reverse=True)[:15]
    return {'status': 'ok', 'top_yields': [{'project': p.get('project', '?'), 'chain': p.get('chain', '?'), 'symbol': p.get('symbol', '?'), 'apy': sr(p.get('apy', 0) or 0), 'tvl': round(p.get('tvlUsd', 0) or 0), 'tvl_fmt': fmt_num(p.get('tvlUsd', 0) or 0), 'stablecoin': p.get('stablecoin', False)} for p in top]}

def fetch_binance_funding():
    data = http_get("https://fapi.binance.com/fapi/v1/premiumIndex")
    if not data: return {'status': 'error', 'error': 'No funding data'}
    pairs = []
    for p in data:
        sym = p.get('symbol', '')
        if not sym.endswith('USDT'): continue
        rate = float(p.get('lastFundingRate', 0))
        mark = float(p.get('markPrice', 0))
        idx = float(p.get('indexPrice', 0))
        ann = rate * 3 * 365 * 100
        pairs.append({'symbol': sym.replace('USDT', ''), 'funding_rate': round(rate * 100, 4), 'annualized': round(ann, 1), 'mark_price': round(mark, 2), 'index_price': round(idx, 2), 'basis_bps': round((mark - idx) / idx * 10000, 1) if idx > 0 else 0})
    pairs = sorted(pairs, key=lambda x: abs(x['funding_rate']), reverse=True)
    pos = sum(1 for p in pairs if p['funding_rate'] > 0)
    neg = sum(1 for p in pairs if p['funding_rate'] < 0)
    avg = sum(p['funding_rate'] for p in pairs[:20]) / min(20, len(pairs)) if pairs else 0
    sent = 'EXTREME_GREED' if avg > 0.05 else 'GREEDY' if avg > 0.01 else 'NEUTRAL' if avg > -0.01 else 'FEARFUL' if avg > -0.05 else 'EXTREME_FEAR'
    return {'status': 'ok', 'top_funding': pairs[:20], 'most_shorted': sorted(pairs, key=lambda x: x['funding_rate'])[:5], 'most_longed': sorted(pairs, key=lambda x: x['funding_rate'], reverse=True)[:5], 'avg_funding': round(avg, 4), 'positive_count': pos, 'negative_count': neg, 'leverage_sentiment': sent}

def fetch_binance_oi():
    symbols = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'XRPUSDT', 'DOGEUSDT', 'ADAUSDT', 'AVAXUSDT', 'LINKUSDT', 'DOTUSDT']
    oi_data = []
    for sym in symbols:
        data = http_get(f"https://fapi.binance.com/fapi/v1/openInterest?symbol={sym}")
        if data:
            oi_data.append({'symbol': sym.replace('USDT', ''), 'open_interest': round(float(data.get('openInterest', 0)), 2)})
    return {'status': 'ok', 'open_interest': oi_data}

def fetch_coingecko_global():
    data = http_get("https://api.coingecko.com/api/v3/global")
    if not data or 'data' not in data: return {'status': 'error', 'error': 'No global data'}
    g = data['data']
    mcap = g.get('total_market_cap', {}).get('usd', 0) or 0
    vol = g.get('total_volume', {}).get('usd', 0) or 0
    return {'status': 'ok', 'total_mcap': round(mcap), 'total_mcap_fmt': fmt_num(mcap), 'total_volume': round(vol), 'total_volume_fmt': fmt_num(vol), 'btc_dominance': sr(g.get('market_cap_percentage', {}).get('btc', 0), 1), 'eth_dominance': sr(g.get('market_cap_percentage', {}).get('eth', 0), 1), 'mcap_change_24h': sr(g.get('market_cap_change_percentage_24h_usd', 0)), 'active_coins': g.get('active_cryptocurrencies', 0)}

def fetch_coingecko_top():
    data = http_get("https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=25&sparkline=false&price_change_percentage=1h,24h,7d,30d")
    if not data: return {'status': 'error', 'error': 'No top coins data'}
    coins = []
    for c in data:
        coins.append({'rank': c.get('market_cap_rank', 0), 'name': c.get('name', '?'), 'symbol': (c.get('symbol', '?') or '?').upper(), 'price': c.get('current_price', 0), 'price_fmt': f"${c.get('current_price', 0):,.2f}" if c.get('current_price', 0) >= 1 else f"${c.get('current_price', 0):.6f}", 'change_1h': sr(c.get('price_change_percentage_1h_in_currency', 0)), 'change_24h': sr(c.get('price_change_percentage_24h_in_currency', 0)), 'change_7d': sr(c.get('price_change_percentage_7d_in_currency', 0)), 'change_30d': sr(c.get('price_change_percentage_30d_in_currency', 0)), 'mcap': c.get('market_cap', 0), 'mcap_fmt': fmt_num(c.get('market_cap', 0)), 'ath': c.get('ath', 0), 'ath_change': sr(c.get('ath_change_percentage', 0))})
    return {'status': 'ok', 'coins': coins}

def fetch_btc_onchain():
    endpoints = {'hash_rate': 'https://api.blockchain.info/charts/hash-rate?timespan=30days&format=json', 'difficulty': 'https://api.blockchain.info/charts/difficulty?timespan=30days&format=json', 'mempool_size': 'https://api.blockchain.info/charts/mempool-size?timespan=30days&format=json', 'avg_block_size': 'https://api.blockchain.info/charts/avg-block-size?timespan=30days&format=json', 'n_transactions': 'https://api.blockchain.info/charts/n-transactions?timespan=30days&format=json', 'miners_revenue': 'https://api.blockchain.info/charts/miners-revenue?timespan=30days&format=json'}
    metrics = {}
    for key, url in endpoints.items():
        data = http_get(url, timeout=10)
        if data and 'values' in data and len(data['values']) > 0:
            current = data['values'][-1].get('y', 0)
            metrics[key] = {'value': round(current, 2), 'unit': data.get('unit', ''), 'name': data.get('name', key)}
    supply = http_get("https://blockchain.info/q/totalbc", timeout=10)
    if supply:
        try:
            metrics['total_supply'] = {'value': round(int(supply) / 1e8, 0), 'unit': 'BTC'}
            metrics['pct_mined'] = {'value': round(int(supply) / 1e8 / 21000000 * 100, 2), 'unit': '%'}
        except: pass
    return {'status': 'ok', 'metrics': metrics}

def fetch_fear_greed():
    data = http_get("https://api.alternative.me/fng/?limit=31")
    if not data or 'data' not in data: return {'status': 'error', 'error': 'No F&G data'}
    entries = data['data']
    current = entries[0] if entries else {}
    history = [{'value': int(e.get('value', 50)), 'label': e.get('value_classification', '?'), 'date': datetime.fromtimestamp(int(e.get('timestamp', 0))).strftime('%Y-%m-%d')} for e in entries[:30]]
    vals = [h['value'] for h in history]
    return {'status': 'ok', 'current': int(current.get('value', 50)), 'label': current.get('value_classification', '?'), 'history': history, 'avg_7d': round(sum(vals[:7]) / min(7, len(vals))) if vals else 50, 'avg_30d': round(sum(vals) / len(vals)) if vals else 50}

def fetch_cmc_data():
    if not CMC_API_KEY: return {'status': 'error', 'error': 'No CMC key'}
    headers = {'User-Agent': 'JustHodl/4.0', 'X-CMC_PRO_API_KEY': CMC_API_KEY, 'Accept': 'application/json'}
    gainers = http_get("https://pro-api.coinmarketcap.com/v1/cryptocurrency/trending/gainers-losers?limit=5&time_period=24h", headers)
    result = {'status': 'ok', 'top_gainers': [], 'top_losers': []}
    if gainers and 'data' in gainers:
        for c in gainers['data'][:5]:
            result['top_gainers'].append({'symbol': c.get('symbol', '?'), 'name': c.get('name', '?'), 'price': round(c.get('quote', {}).get('USD', {}).get('price', 0) or 0, 4), 'change_24h': sr(c.get('quote', {}).get('USD', {}).get('percent_change_24h', 0))})
        losers = sorted(gainers['data'], key=lambda x: x.get('quote', {}).get('USD', {}).get('percent_change_24h', 0) or 0)
        for c in losers[:5]:
            result['top_losers'].append({'symbol': c.get('symbol', '?'), 'name': c.get('name', '?'), 'price': round(c.get('quote', {}).get('USD', {}).get('price', 0) or 0, 4), 'change_24h': sr(c.get('quote', {}).get('USD', {}).get('percent_change_24h', 0))})
    return result

# ════════════════════════════════════════════════════════════════
# TIER 3: ADDITIONAL DATA SOURCES
# ════════════════════════════════════════════════════════════════
def fetch_eth_gas():
    data = http_get("https://api.etherscan.io/api?module=gastracker&action=gasoracle")
    if not data or data.get('status') != '1': return {'status': 'error'}
    r = data.get('result', {})
    return {'status': 'ok', 'low': sr(r.get('SafeGasPrice', 0), 0), 'standard': sr(r.get('ProposeGasPrice', 0), 0), 'fast': sr(r.get('FastGasPrice', 0), 0), 'base_fee': sr(r.get('suggestBaseFee', 0), 1)}

def fetch_whale_transactions():
    """Large BTC transactions from blockchain.info"""
    data = http_get("https://blockchain.info/unconfirmed-transactions?format=json", timeout=10)
    if not data: return {'status': 'error'}
    txs = data.get('txs', [])
    large = []
    for tx in txs[:200]:
        total = sum(o.get('value', 0) for o in tx.get('out', [])) / 1e8
        if total >= 100:
            large.append({'hash': tx.get('hash', '?')[:16] + '...', 'btc_amount': round(total, 2), 'usd_est': fmt_num(total * 95000), 'inputs': len(tx.get('inputs', [])), 'outputs': len(tx.get('out', []))})
    large.sort(key=lambda x: x['btc_amount'], reverse=True)
    return {'status': 'ok', 'large_txs': large[:10], 'whale_count': len(large), 'total_whale_btc': round(sum(t['btc_amount'] for t in large), 2)}

def fetch_onchain_ratios():
    """Bitcoin market cap vs realized value approximation"""
    mcap_data = http_get("https://api.blockchain.info/charts/market-cap?timespan=365days&format=json")
    if not mcap_data or 'values' not in mcap_data: return {'status': 'error'}
    vals = mcap_data['values']
    if len(vals) < 30: return {'status': 'error'}
    current = vals[-1]['y']
    avg_365 = sum(v['y'] for v in vals) / len(vals)
    avg_30 = sum(v['y'] for v in vals[-30:]) / 30
    mvrv_approx = current / avg_365 if avg_365 > 0 else 1
    momentum = (current - avg_30) / avg_30 * 100 if avg_30 > 0 else 0
    signal = 'OVERVALUED' if mvrv_approx > 3 else 'EXPENSIVE' if mvrv_approx > 2 else 'FAIR' if mvrv_approx > 0.8 else 'UNDERVALUED'
    return {'status': 'ok', 'mvrv_approx': round(mvrv_approx, 2), 'signal': signal, 'market_cap': round(current), 'market_cap_fmt': fmt_num(current), 'avg_365d_cap': round(avg_365), 'momentum_30d': round(momentum, 2)}

# ════════════════════════════════════════════════════════════════
# AI INTELLIGENCE (Claude API)
# ════════════════════════════════════════════════════════════════
def generate_ai_intelligence(results, technicals):
    if not ANTHROPIC_API_KEY:
        return {'status': 'error', 'error': 'No Anthropic key'}
    
    print("  🤖 Generating AI Intelligence via Claude...")
    
    # Pull from ENTIRE system — main terminal + repo + predictions
    system_data = {}
    for key, filename in [('terminal', 'data.json'), ('repo', 'repo-data.json'), ('predictions', 'predictions.json')]:
        try:
            obj = s3.get_object(Bucket=S3_BUCKET, Key=filename)
            system_data[key] = json.loads(obj['Body'].read().decode())
            print(f"    ✅ Loaded {filename}")
        except:
            print(f"    ⚠️ Could not load {filename}")
    
    # Extract key terminal data
    term = system_data.get('terminal', {})
    ki = term.get('khalid_index', term.get('ki', '?'))
    regime = term.get('regime', '?')
    dxy = term.get('dxy', '?')
    vix = term.get('vix', term.get('stats', {}).get('vix', '?'))
    hy_spread = term.get('hy_spread', '?')
    risk_composite = term.get('risk_composite', '?')
    
    # Extract stock signals
    stocks = term.get('stocks', {})
    stock_summary = ""
    if isinstance(stocks, dict):
        buys = [s for s in stocks.values() if isinstance(s, dict) and s.get('signal') == 'BUY']
        sells = [s for s in stocks.values() if isinstance(s, dict) and s.get('signal') == 'SELL']
        stock_summary = f"Stock signals: {len(buys)} BUYs, {len(sells)} SELLs out of {len(stocks)} tracked"
    
    # Extract repo/plumbing data
    repo = system_data.get('repo', {})
    sofr = repo.get('metrics', {}).get('SOFR', {}).get('value', '?')
    rrp = repo.get('metrics', {}).get('RRP', {}).get('value', '?')
    plumbing_stress = repo.get('plumbing_score', repo.get('score', '?'))
    
    # Extract ML predictions
    preds = system_data.get('predictions', {})
    ml_regime = preds.get('macro_regime', preds.get('regime', '?'))
    ml_risk = preds.get('risk_score', '?')
    
    # Build technical summary
    tech_summary = []
    coins_data = technicals.get('coins', {})
    for coin, data in coins_data.items():
        line = f"\n=== {coin} (${data.get('price', 0):,.6f}) | Consensus: {data.get('consensus', '?')} ==="
        for tf_key, tf_data in data.get('timeframes', {}).items():
            if tf_data.get('status') != 'ok': continue
            ind = tf_data.get('indicators', {})
            line += f"\n  [{tf_key}] RSI:{ind.get('rsi','?')} MACD:{ind.get('macd',{}).get('cross','?')} EMA:{ind.get('ema',{}).get('trend','?')} SuperT:{ind.get('supertrend',{}).get('trend','?')} BB%:{ind.get('bollinger',{}).get('position','?')} Score:{tf_data.get('score','?')}/100"
            if ind.get('ema',{}).get('cross_signal'): line += f" ⚡{ind['ema']['cross_signal']}"
            patterns = tf_data.get('patterns', [])
            if patterns:
                line += f"\n  PATTERNS: {', '.join(p['type']+'('+p['bias']+','+p.get('confidence','?')+')' for p in patterns)}"
        tech_summary.append(line)
    
    risk = results.get('risk_score', {})
    fg = results.get('fear_greed', {})
    funding = results.get('funding', {})
    stables = results.get('stablecoins', {})
    tvl = results.get('tvl', {})
    global_m = results.get('global_market', {})
    onchain = results.get('onchain_ratios', {})
    whales = results.get('whale_txs', {})
    eth_gas = results.get('eth_gas', {})
    
    prompt = f"""You are an elite crypto quantitative analyst at a $10B hedge fund with access to the COMPLETE JustHodl.AI terminal data. Analyze ALL data and produce an institutional-grade intelligence report with specific predictions.

═══ MACRO SYSTEM (from main terminal) ═══
Khalid Index: {ki}/100 | Regime: {regime}
DXY (Dollar): {dxy} | VIX: {vix} | HY Spread: {hy_spread}
Risk Composite: {risk_composite}
{stock_summary}
SOFR: {sofr}% | RRP: {rrp} | Plumbing Stress: {plumbing_stress}
ML Regime: {ml_regime} | ML Risk: {ml_risk}

═══ CRYPTO MARKET STATE ═══
Crypto Risk Score: {risk.get('score', '?')}/100 ({risk.get('regime', '?')})
Fear & Greed: {fg.get('current', '?')} ({fg.get('label', '?')}) | 7D: {fg.get('avg_7d', '?')} | 30D: {fg.get('avg_30d', '?')}
Total Crypto MCap: {global_m.get('total_mcap_fmt', '?')} ({global_m.get('mcap_change_24h', '?')}% 24h)
BTC Dominance: {global_m.get('btc_dominance', '?')}% | ETH: {global_m.get('eth_dominance', '?')}%
DeFi TVL: {tvl.get('total_tvl_fmt', '?')}
DeFi TVL 24h change: check protocol changes

═══ DERIVATIVES ═══
Avg Funding: {funding.get('avg_funding', '?')}% | Sentiment: {funding.get('leverage_sentiment', '?')}
Long/Short: {funding.get('positive_count', '?')}/{funding.get('negative_count', '?')}

═══ STABLECOINS ═══
Total: {stables.get('total_mcap_fmt', '?')} | Flow: {stables.get('net_signal', '?')}
Minting: {stables.get('minting_count', 0)} | Burning: {stables.get('burning_count', 0)}

═══ ON-CHAIN ═══
MVRV: {onchain.get('mvrv_approx', '?')} ({onchain.get('signal', '?')}) | 30D Momentum: {onchain.get('momentum_30d', '?')}%
Whale TXs (100+ BTC): {whales.get('whale_count', '?')} totaling {whales.get('total_whale_btc', '?')} BTC
ETH Gas: {eth_gas.get('low', '?')}/{eth_gas.get('standard', '?')}/{eth_gas.get('fast', '?')} gwei

═══ TECHNICAL ANALYSIS ═══
{''.join(tech_summary)}

═══ INSTRUCTIONS ═══
Produce this EXACT structure:

**1. WYCKOFF PHASE ANALYSIS**
For EACH coin (BTC, ETH, PEPE, DOGE, POL), identify current Wyckoff phase:
- ACCUMULATION (smart money buying, price flat/bottoming)
- MARKUP/EXPANSION (breakout, trending up)
- DISTRIBUTION (smart money selling, price topping)
- MARKDOWN/DECLINE (breakdown, trending down)
State which phase with confidence % and what evidence supports it.

**2. MACRO → CRYPTO IMPACT**
How does the Khalid Index at {ki}, DXY at {dxy}, VIX at {vix}, and current liquidity conditions specifically affect crypto? Give direction and magnitude.

**3. PUMP OR DUMP PREDICTIONS**
For each target coin, predict:
- Direction: PUMP ↑ or DUMP ↓
- Magnitude: expected % move in 24h, 7d, 30d
- Probability: confidence level
- Key levels: support and resistance

**4. ACCUMULATION ZONES**
Where should you BUY? Give exact price ranges for each coin.

**5. RISK WARNINGS**
Top 3 risks that could crash crypto in next 72h. Include probability.

**6. HIGHEST CONVICTION TRADES**
3 specific trades with EXACT entry, stop-loss, take-profit, R:R ratio.

**7. PORTFOLIO ALLOCATION**
Optimal allocation across BTC/ETH/PEPE/DOGE/POL/CASH right now with % weights.

Be extremely specific. Use numbers. No hedging. This is a $10B fund — act like it."""

    resp = http_post("https://api.anthropic.com/v1/messages", {
        'model': 'claude-sonnet-4-20250514',
        'max_tokens': 3000,
        'messages': [{'role': 'user', 'content': prompt}]
    }, {
        'Content-Type': 'application/json',
        'x-api-key': ANTHROPIC_API_KEY,
        'anthropic-version': '2023-06-01'
    }, timeout=60)
    
    if resp and 'content' in resp:
        text = resp['content'][0].get('text', '')
        return {'status': 'ok', 'analysis': text, 'model': 'claude-sonnet-4-20250514', 'generated_at': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'), 'system_sources': list(system_data.keys())}
    return {'status': 'error', 'error': 'Claude API failed'}

# ════════════════════════════════════════════════════════════════
# RISK SCORING
# ════════════════════════════════════════════════════════════════
def compute_risk(fear_greed, funding, stablecoins, global_market, technicals):
    score = 50
    signals = []
    
    fg = fear_greed.get('current', 50) if isinstance(fear_greed, dict) else 50
    if fg < 20: score += 15; signals.append('🔴 Extreme Fear - high volatility risk')
    elif fg < 35: score += 8; signals.append('🟡 Market fearful')
    elif fg > 80: score += 12; signals.append('🔴 Extreme Greed - correction risk')
    elif fg > 65: score += 5; signals.append('🟡 Greedy sentiment')
    
    avg_f = funding.get('avg_funding', 0) if isinstance(funding, dict) else 0
    if abs(avg_f) > 0.05: score += 10; signals.append('🔴 Extreme funding rate divergence')
    elif abs(avg_f) > 0.02: score += 5; signals.append('🟡 Elevated funding rates')
    
    net = stablecoins.get('net_signal', 'NEUTRAL') if isinstance(stablecoins, dict) else 'NEUTRAL'
    if net == 'OUTFLOW': score += 8; signals.append('🔴 Stablecoin outflows - liquidity draining')
    elif net == 'INFLOW': score -= 5; signals.append('🟢 Stablecoin inflows - fresh capital')
    
    mcap_chg = global_market.get('mcap_change_24h', 0) if isinstance(global_market, dict) else 0
    if mcap_chg < -5: score += 10; signals.append('🔴 Market cap down >5% in 24h')
    elif mcap_chg < -2: score += 5; signals.append('🟡 Market declining')
    elif mcap_chg > 5: score -= 3; signals.append('🟢 Strong market rally')
    
    # Check technical consensus
    coins = technicals.get('coins', {}) if isinstance(technicals, dict) else {}
    bearish_coins = sum(1 for c in coins.values() if c.get('consensus', '').startswith('S'))
    if bearish_coins >= 4: score += 8; signals.append('🔴 4+ coins bearish across timeframes')
    elif bearish_coins >= 3: score += 4; signals.append('🟡 Multiple coins showing weakness')
    
    score = max(0, min(100, score))
    regime = 'CRITICAL' if score >= 80 else 'HIGH' if score >= 65 else 'ELEVATED' if score >= 50 else 'MODERATE' if score >= 35 else 'LOW'
    action = 'REDUCE EXPOSURE IMMEDIATELY' if score >= 80 else 'HEDGE & REDUCE RISK' if score >= 65 else 'PROCEED WITH CAUTION' if score >= 50 else 'NORMAL OPERATIONS' if score >= 35 else 'RISK ON - ACCUMULATE'
    
    return {'score': score, 'regime': regime, 'action': action, 'signals': signals}

# ════════════════════════════════════════════════════════════════
# MAIN HANDLER
# ════════════════════════════════════════════════════════════════
def lambda_handler(event, context):
    start = time.time()
    print("═══ CRYPTO INTELLIGENCE ENGINE v4.0 ═══")
    print("  TIER 1: Technical Indicators | TIER 2: Pattern Detection")
    print("  TIER 3: On-Chain + Whale + Gas | TIER 4: Multi-Timeframe")
    
    results = {}
    
    # Phase 1: Fetch all data sources in parallel
    print("  Phase 1: Fetching 11 data sources + TIER 3...")
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
            executor.submit(fetch_eth_gas): 'eth_gas',
            executor.submit(fetch_whale_transactions): 'whale_txs',
            executor.submit(fetch_onchain_ratios): 'onchain_ratios',
        }
        for future in as_completed(futures):
            key = futures[future]
            try:
                results[key] = future.result()
                status = results[key].get('status', '?')
                print(f"    ✅ {key}: {status}")
            except Exception as e:
                results[key] = {'status': 'error', 'error': str(e)}
                print(f"    ❌ {key}: {e}")
    
    # Phase 2: TIER 1+2+4 Technical Analysis
    print(f"  Phase 2: Multi-timeframe analysis ({len(TARGET_COINS)} coins × {len(TIMEFRAMES)} TFs)...")
    technicals = fetch_all_technicals()
    results['technicals'] = technicals
    print(f"    ✅ Technicals complete")
    
    # Phase 3: Risk scoring
    risk = compute_risk(
        results.get('fear_greed', {}),
        results.get('funding', {}),
        results.get('stablecoins', {}),
        results.get('global_market', {}),
        technicals
    )
    
    # Phase 4: AI Intelligence
    print("  Phase 4: AI Intelligence (Claude)...")
    ai_intel = generate_ai_intelligence(results, technicals)
    results['ai_intelligence'] = ai_intel
    print(f"    {'✅' if ai_intel.get('status') == 'ok' else '❌'} AI: {ai_intel.get('status')}")
    
    # Build output
    output = {
        'generated_at': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
        'fetch_time': round(time.time() - start, 1),
        'version': '4.0',
        'risk_score': risk,
        **results
    }
    
    # Publish to S3
    try:
        s3.put_object(Bucket=S3_BUCKET, Key='crypto-intel.json', Body=json.dumps(output, default=str), ContentType='application/json', CacheControl='max-age=60')
        print(f"  ✅ Published to s3://{S3_BUCKET}/crypto-intel.json")
    except Exception as e:
        print(f"  ❌ S3 publish failed: {e}")
    
    ok_sources = len([k for k, v in results.items() if isinstance(v, dict) and v.get('status') == 'ok'])
    print(f"═══ COMPLETE: {ok_sources}/{len(results)} sources | Risk {risk['score']} ({risk['regime']}) | {round(time.time()-start,1)}s ═══")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'status': 'published',
            'risk_score': risk['score'],
            'regime': risk['regime'],
            'sources': ok_sources,
            'total_sources': len(results),
            'ai_intel': ai_intel.get('status') == 'ok',
            'technicals': technicals.get('status') == 'ok',
            'fetch_time': round(time.time() - start, 1)
        })
    }
