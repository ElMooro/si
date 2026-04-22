import json, boto3, urllib.request, time, concurrent.futures
from datetime import datetime, timezone

FRED_KEY    = '2f057499936072679d8843d7fce99989'
POLYGON_KEY = 'zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d'
S3_BUCKET   = 'justhodl-dashboard-live'

def hget(url, timeout=10):
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'JustHodl/2.0'})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode('utf-8'))
    except Exception as e:
        print('hget error ' + url[:50] + ': ' + str(e))
        return None

def fred(sid):
    d = hget('https://api.stlouisfed.org/fred/series/observations?series_id='
             + sid + '&api_key=' + FRED_KEY + '&file_type=json&sort_order=desc&limit=3')
    if d and d.get('observations'):
        obs = [o for o in d['observations'] if o.get('value') not in ['.','',None]]
        return float(obs[0]['value']) if obs else None
    return None

def poly(ticker):
    d = hget('https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/'
             + ticker + '?apiKey=' + POLYGON_KEY)
    return d.get('ticker') if d else None

def chg(snap):
    if not snap: return 0
    day  = snap.get('day', {})
    prev = snap.get('prevDay', {})
    p    = day.get('c') or prev.get('c', 0)
    pp   = prev.get('c', 1)
    return round(((p - pp) / pp * 100), 2) if pp else 0

def engine_options():
    vix   = fred('VIXCLS')
    vix3m = fred('VXVCLS')
    term  = round(vix3m - vix, 2) if vix and vix3m else None
    score = 50
    if vix:
        if vix > 30: score -= 20
        elif vix > 20: score -= 10
        elif vix < 15: score += 15
    if term and term < -2: score -= 10
    return {'vix': vix, 'vix_3m': vix3m, 'term_structure': term,
            'regime': 'BACKWARDATION' if (term and term < 0) else 'CONTANGO',
            'score': max(0, min(100, score)),
            'signal': 'BEARISH' if score < 40 else ('BULLISH' if score > 60 else 'NEUTRAL')}

def engine_sentiment():
    hy  = fred('BAMLH0A0HYM2')
    ig  = fred('BAMLC0A0CM')
    ted = fred('TEDRATE')
    fg_val, fg_label = None, None
    fg = hget('https://api.alternative.me/fng/?limit=1')
    if fg and fg.get('data'):
        fg_val   = int(fg['data'][0]['value'])
        fg_label = fg['data'][0]['value_classification']
    score = 50
    if hy:
        if hy > 6: score -= 20
        elif hy > 4: score -= 10
        elif hy < 3: score += 10
    if fg_val:
        if fg_val < 25: score -= 15
        elif fg_val > 75: score += 15
    return {'hy_spread': hy, 'ig_spread': ig, 'ted_spread': ted,
            'fear_greed': fg_val, 'fear_greed_label': fg_label,
            'score': max(0, min(100, score)),
            'signal': 'RISK_OFF' if score < 40 else ('RISK_ON' if score > 60 else 'NEUTRAL')}

def engine_earnings():
    t10 = fred('GS10')
    t2  = fred('GS2')
    yc  = round(t10 - t2, 3) if t10 and t2 else None
    bells = {}
    for tk in ['AAPL', 'MSFT', 'NVDA', 'GOOGL', 'META']:
        snap = poly(tk)
        if snap: bells[tk] = {'price': snap.get('day',{}).get('c'), 'change_pct': chg(snap)}
    avg = sum(v['change_pct'] for v in bells.values()) / len(bells) if bells else 0
    score = 50
    if avg > 1: score += 15
    elif avg < -1: score -= 15
    if yc and yc > 0: score += 5
    elif yc and yc and yc < -0.5: score -= 10
    return {'yield_curve': yc, 't10y': t10, 't2y': t2, 'bellwethers': bells,
            'avg_change': round(avg, 2), 'score': max(0, min(100, score)),
            'signal': 'IMPROVING' if score > 60 else ('DETERIORATING' if score < 40 else 'STABLE')}

def engine_liquidity():
    fed  = fred('WALCL')
    m2   = fred('M2SL')
    rrp  = fred('RRPONTSYD')
    sofr = fred('SOFR')
    ff   = fred('FEDFUNDS')
    net  = round(fed / 1000 - rrp, 1) if fed and rrp else None
    score = 50
    if net:
        if net > 6000: score += 15
        elif net < 4000: score -= 15
    return {'fed_assets_b': round(fed/1000, 1) if fed else None, 'm2_b': m2,
            'rrp_b': rrp, 'net_liquidity_b': net, 'sofr': sofr, 'ff_rate': ff,
            'score': max(0, min(100, score)),
            'signal': 'EXPANSIVE' if score > 60 else ('CONTRACTING' if score < 40 else 'NEUTRAL')}

def engine_correlation():
    tickers = {'SPY': None, 'TLT': None, 'GLD': None, 'UUP': None, 'USO': None}
    for t in list(tickers.keys()):
        snap = poly(t)
        if snap: tickers[t] = chg(snap)
    spy = tickers.get('SPY') or 0
    tlt = tickers.get('TLT') or 0
    gld = tickers.get('GLD') or 0
    uup = tickers.get('UUP') or 0
    alerts = []
    if (spy > 0.5 and tlt > 0.5) or (spy < -0.5 and tlt < -0.5):
        alerts.append('BREAKDOWN: Stocks+Bonds correlated (SPY{:+.1f}%/TLT{:+.1f}%)'.format(spy, tlt))
    if gld > 0.5 and uup > 0.5:
        alerts.append('SIGNAL: Gold+Dollar both rising - safe-haven demand')
    if spy < -1 and gld > 0.5:
        alerts.append('RISK-OFF: Equity selloff + Gold bid')
    score = max(0, min(100, 50 - len(alerts)*10 + (5 if spy > 0.5 else -5 if spy < -0.5 else 0)))
    return {'changes': {k: v for k, v in tickers.items() if v is not None},
            'alerts': alerts, 'score': score,
            'signal': 'BREAKDOWN' if alerts else 'NORMAL'}

def lambda_handler(event, context):
    cors = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
        'Content-Type': 'application/json'
    }
    if event.get('requestContext', {}).get('http', {}).get('method') == 'OPTIONS':
        return {'statusCode': 200, 'headers': cors, 'body': ''}
    try:
        t0 = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
            f1 = ex.submit(engine_options)
            f2 = ex.submit(engine_sentiment)
            f3 = ex.submit(engine_earnings)
            f4 = ex.submit(engine_liquidity)
            f5 = ex.submit(engine_correlation)
            e1 = f1.result(timeout=25)
            e2 = f2.result(timeout=25)
            e3 = f3.result(timeout=25)
            e4 = f4.result(timeout=25)
            e5 = f5.result(timeout=25)
        scores = [e.get('score', 50) for e in [e1, e2, e3, e4, e5]]
        composite = round(sum(scores) / len(scores))
        regime = 'RISK_ON' if composite >= 65 else ('RISK_OFF' if composite <= 35 else 'NEUTRAL')
        alerts = list(e5.get('alerts', []))
        if e1.get('vix') and e1['vix'] > 25:
            alerts.append('VIX elevated: ' + str(e1['vix']))
        if e4.get('signal') == 'CONTRACTING':
            alerts.append('Liquidity contracting: $' + str(e4.get('net_liquidity_b')) + 'B')
        output = {
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'composite_score': composite,
            'regime': regime,
            'engine_scores': {
                'options_flow': e1.get('score'),
                'fund_sentiment': e2.get('score'),
                'earnings': e3.get('score'),
                'liquidity': e4.get('score'),
                'correlation': e5.get('score')
            },
            'options_flow': e1,
            'fund_flow': e2,
            'earnings_momentum': e3,
            'global_liquidity': e4,
            'correlation': e5,
            'alerts': alerts,
            'fetch_time_s': round(time.time() - t0, 1)
        }
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.put_object(Bucket=S3_BUCKET, Key='edge-data.json',
                      Body=json.dumps(output).encode('utf-8'),
                      ContentType='application/json', CacheControl='no-cache')
        print('Done score=' + str(composite) + ' time=' + str(output['fetch_time_s']) + 's')
        return {'statusCode': 200, 'headers': cors, 'body': json.dumps(output)}
    except Exception as e:
        import traceback
        err = traceback.format_exc()
        print('ERROR: ' + str(e))
        return {'statusCode': 500, 'headers': cors,
                'body': json.dumps({'error': str(e), 'detail': err[:500]})}