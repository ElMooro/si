
import json
import boto3
import os
import urllib.request
import urllib.error
import re
from datetime import datetime, timezone

# ── AUTH MODULE (token from SSM + origin allowlist) ──────────────────
_AUTH_TOKEN_CACHE = None
def _get_auth_token():
    global _AUTH_TOKEN_CACHE
    if _AUTH_TOKEN_CACHE is None:
        try:
            import boto3
            _AUTH_TOKEN_CACHE = boto3.client("ssm", region_name="us-east-1").get_parameter(
                Name="/justhodl/ai-chat/auth-token", WithDecryption=True
            )["Parameter"]["Value"]
        except Exception as _e:
            print(f"[AUTH] SSM fetch failed: {_e}")
            _AUTH_TOKEN_CACHE = ""
    return _AUTH_TOKEN_CACHE

_ALLOWED_ORIGINS = ("https://justhodl.ai", "https://www.justhodl.ai")
# ── END AUTH MODULE ─────────────────────────────────────────────────


POLYGON_KEY  = 'zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d'
CMC_KEY      = '17ba8e87-53f0-46f4-abe5-014d9cd99597'
ANTHROPIC_KEY = os.environ.get('ANTHROPIC_API_KEY', '')
S3_BUCKET    = 'justhodl-dashboard-live'

CRYPTO_IDS = {
    'BTC':'bitcoin','ETH':'ethereum','SOL':'solana','XRP':'ripple',
    'DOGE':'dogecoin','ADA':'cardano','PEPE':'pepe','AVAX':'avalanche-2',
    'DOT':'polkadot','LINK':'chainlink','UNI':'uniswap','BNB':'binancecoin',
    'POL':'polygon-ecosystem-token','LTC':'litecoin','MATIC':'matic-network',
    'ATOM':'cosmos','NEAR':'near','ARB':'arbitrum','OP':'optimism',
    'SUI':'sui','APT':'aptos','INJ':'injective-protocol',
}

STOCK_UNIVERSE = {
    'AAPL','MSFT','GOOGL','GOOG','AMZN','NVDA','TSLA','META','JPM',
    'GS','BAC','WFC','C','MS','V','MA','PYPL','SPY','QQQ','IWM',
    'DIA','GLD','TLT','AGG','AMD','INTC','NFLX','UBER','COIN','PLTR',
    'SOFI','HOOD','NIO','BABA','SHOP','SQ','RBLX','DKNG',
    'MU','QCOM','AVGO','TXN','AMAT','ASML','ORCL','CRM','NOW',
    'SNOW','DDOG','NET','ZS','CRWD','ABNB','LYFT','DASH','MELI',
    'XOM','CVX','COP','SLB','GE','HON','CAT','DE','BA','LMT','RTX',
    'JNJ','PFE','MRK','ABBV','UNH','VTI','VOO','XLF','XLE','XLK','XLV',
    'VIX','UVXY','SQQQ','SH','TBT','UUP','GDX','SLV','USO','BITO',
}

def http_get(url, headers=None, timeout=8):
    try:
        req = urllib.request.Request(url, headers=headers or {'User-Agent': 'JustHodl/2.0'})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode('utf-8'))
    except Exception:
        return None

def fetch_stock(ticker):
    t = ticker.upper().replace('.', '-')
    snap = http_get(f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{t}?apiKey={POLYGON_KEY}")
    if snap and snap.get('ticker'):
        tk = snap['ticker']
        day  = tk.get('day', {})
        prev = tk.get('prevDay', {})
        last = tk.get('lastTrade', {})
        close   = day.get('c') or prev.get('c') or last.get('p', 0)
        prev_c  = prev.get('c', 0)
        chg     = ((close - prev_c) / prev_c * 100) if prev_c else 0
        return {
            'ticker': t, 'price': close, 'prev_close': prev_c,
            'open': day.get('o') or prev.get('o'),
            'high': day.get('h') or prev.get('h'),
            'low':  day.get('l') or prev.get('l'),
            'volume': day.get('v') or prev.get('v'),
            'change_pct': chg
        }
    agg = http_get(f"https://api.polygon.io/v2/aggs/ticker/{t}/prev?adjusted=true&apiKey={POLYGON_KEY}")
    if agg and agg.get('results'):
        bar = agg['results'][0]
        chg = ((bar['c'] - bar['o']) / bar['o'] * 100) if bar.get('o') else 0
        return {'ticker': t, 'price': bar['c'], 'open': bar['o'],
                'high': bar['h'], 'low': bar['l'],
                'volume': bar.get('v'), 'change_pct': chg}
    return None

def fetch_cryptos(sym_id_pairs):
    ids = ','.join([cid for _, cid in sym_id_pairs])
    data = http_get(
        f"https://api.coingecko.com/api/v3/simple/price?ids={ids}&vs_currencies=usd"
        f"&include_24hr_change=true&include_market_cap=true"
    )
    return data or {}

def get_s3(key):
    try:
        s3 = boto3.client('s3', region_name='us-east-1')
        r = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(r['Body'].read().decode('utf-8'))
    except Exception:
        return None

def detect_entities(message):
    msg_up = message.upper()
    words  = re.findall(r'\b([A-Z]{1,5})\b', msg_up)
    stocks  = list(dict.fromkeys([w for w in words if w in STOCK_UNIVERSE]))
    cryptos = list(dict.fromkeys([(w, CRYPTO_IDS[w]) for w in words if w in CRYPTO_IDS]))
    for sym, cid in CRYPTO_IDS.items():
        name = cid.replace('-', ' ')
        if name in message.lower() and (sym, cid) not in cryptos:
            cryptos.append((sym, cid))
    return stocks[:4], cryptos[:4]

def build_context(message):
    stocks, cryptos = detect_entities(message)
    lines = [f"Date/Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"]

    for ticker in stocks:
        d = fetch_stock(ticker)
        if d:
            lines.append(
                f"[LIVE STOCK] {d['ticker']}: ${d['price']:.2f} ({d['change_pct']:+.2f}%) | "
                f"O:${d.get('open') or 0:.2f} H:${d.get('high') or 0:.2f} L:${d.get('low') or 0:.2f} | "
                f"Vol:{d.get('volume') or 0:,.0f}"
            )
        else:
            lines.append(f"[STOCK] {ticker}: Could not fetch from Polygon")

    if cryptos:
        prices = fetch_cryptos(cryptos)
        for sym, cid in cryptos:
            if cid in prices:
                d   = prices[cid]
                p   = d.get('usd', 0)
                chg = d.get('usd_24h_change', 0)
                mc  = d.get('usd_market_cap', 0)
                lines.append(f"[LIVE CRYPTO] {sym}: ${p:,.4f} ({chg:+.2f}% 24h) | MCap:${mc/1e9:.2f}B")

    report = get_s3('data.json')
    if report:
        ki = report.get('khalid_index', report.get('khalidIndex', {}))
        if isinstance(ki, dict):
            score  = ki.get('score',  ki.get('value', 'N/A'))
            regime = ki.get('regime', ki.get('label', 'N/A'))
        else:
            score, regime = ki, 'N/A'
        ts = report.get('generated_at', report.get('timestamp', 'unknown'))
        lines.append(f"[KHALID INDEX] Score:{score}/100  Regime:{regime}  (data as of {ts})")
        mr = report.get('market_regime', report.get('regime', {}))
        if isinstance(mr, dict) and mr:
            lines.append(f"[REGIME] {', '.join(f'{k}:{v}' for k,v in list(mr.items())[:6])}")
        macro = report.get('macro', {})
        if isinstance(macro, dict):
            m_items = []
            for k, v in list(macro.items())[:6]:
                if isinstance(v, (int, float)):
                    m_items.append(f"{k}:{v}")
                elif isinstance(v, dict):
                    val = v.get('value', v.get('current', ''))
                    if val:
                        m_items.append(f"{k}:{val}")
            if m_items:
                lines.append(f"[MACRO] {' | '.join(m_items)}")

    msg_up = message.upper()
    if cryptos or any(w in msg_up for w in ['CRYPTO','BITCOIN','DEFI','ETHEREUM','ALTCOIN']):
        cd = get_s3('crypto-intel.json')
        if cd:
            fg = cd.get('fear_greed', {})
            if isinstance(fg, dict):
                lines.append(f"[FEAR&GREED] {fg.get('value','N/A')}  {fg.get('value_classification','')}")
            dom = cd.get('dominance', {})
            if isinstance(dom, dict):
                lines.append(f"[BTC DOMINANCE] {dom.get('btc', dom.get('BTC','N/A'))}%")

    if any(w in msg_up for w in ['MARKET','RISK','REGIME','SIGNAL','PORTFOLIO','INTEL','PHASE']):
        intel = get_s3('intelligence-report.json')
        if intel:
            phase = intel.get('market_phase', intel.get('phase', 'N/A'))
            score = intel.get('composite_score', intel.get('score', 'N/A'))
            lines.append(f"[INTELLIGENCE] Phase:{phase}  Score:{score}/100")
            sigs = intel.get('stock_signals', [])
            if isinstance(sigs, list) and sigs:
                lines.append(f"[TOP SIGNALS] {', '.join(s.get('ticker','?')+':'+s.get('signal','?') for s in (sigs if isinstance(sigs, list) else [])[:3] if isinstance(s, dict))}")

    return '\n'.join(lines)

def call_claude(message, context, history=None):
    system = (
        "You are JustHodl AI  institutional-grade financial intelligence for JustHodl.AI "
        "(AWS-hosted Bloomberg Terminal, Khalid's personal platform).\n\n"
        "REAL-TIME DATA (fetched live moments ago  USE THESE EXACT NUMBERS):\n"
        + context +
        "\n\nINSTRUCTIONS:\n"
        "- Cite exact prices from [LIVE STOCK] and [LIVE CRYPTO] tags\n"
        "- NEVER say you lack real-time data  you have it above\n"
        "- If a ticker has no [LIVE] tag, say: ask specifically about [TICKER] for live data\n"
        "- Reference Khalid Index regime in your analysis\n"
        "- Be concise, institutional-quality, actionable\n"
        "- Format: prices $1,234.56  |  pct +1.23%  |  market caps $1.2B"
    )
    msgs = list((history or [])[-6:]) + [{"role": "user", "content": message}]
    payload = json.dumps({
        "model": "claude-haiku-4-5-20251001", "max_tokens": 1024,
        "system": system, "messages": msgs
    }).encode()
    req = urllib.request.Request(
        'https://api.anthropic.com/v1/messages', data=payload,
        headers={'Content-Type': 'application/json',
                 'x-api-key': ANTHROPIC_KEY,
                 'anthropic-version': '2023-06-01'},
        method='POST'
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        data = json.loads(r.read().decode())
    return data['content'][0]['text'] if data.get('content') else 'Error: empty response'

def lambda_handler(event, context):

    # ── AUTH GUARD ──────────────────────────────────────────────────
    _m = (event.get("requestContext", {}).get("http", {}).get("method")
          or event.get("httpMethod") or "").upper()
    if _m != "OPTIONS":
        _h = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
        _tok = _h.get("x-justhodl-token", "")
        _org = _h.get("origin", "") or _h.get("referer", "")
        _exp = _get_auth_token()
        _tok_ok = bool(_exp) and _tok == _exp
        _org_ok = any(_org.startswith(o) for o in _ALLOWED_ORIGINS)
        if not (_tok_ok and _org_ok):
            print(f"[AUTH] DENY tok_ok={_tok_ok} origin={_org!r}")
            return {
                "statusCode": 403,
                "headers": {
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Headers": "Content-Type, x-justhodl-token",
                    "Content-Type": "application/json"
                },
                "body": '{"error":"Unauthorized"}'
            }
    # ── END AUTH GUARD ──────────────────────────────────────────────
    cors = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type,Authorization',
        'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
        'Content-Type': 'application/json'
    }
    if event.get('requestContext', {}).get('http', {}).get('method') == 'OPTIONS':
        return {'statusCode': 200, 'headers': cors, 'body': ''}
    try:
        message, history = '', []
        if event.get('body'):
            body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
            message = body.get('message', body.get('query', body.get('prompt', '')))
            history = body.get('history', body.get('conversation', []))
        if not message:
            qs = event.get('queryStringParameters') or {}
            message = qs.get('message', qs.get('q', qs.get('query', '')))
        if not message or not message.strip():
            return {'statusCode': 400, 'headers': cors,
                    'body': json.dumps({'error': 'No message provided',
                                        'usage': 'POST {"message": "price AAPL"}'})}
        realtime_context = build_context(message.strip())
        response_text    = call_claude(message.strip(), realtime_context, history)
        return {'statusCode': 200, 'headers': cors,
                'body': json.dumps({'response': response_text,
                                    'timestamp': datetime.now(timezone.utc).isoformat()})}
    except urllib.error.HTTPError as e:
        err = e.read().decode() if hasattr(e, 'read') else str(e)
        return {'statusCode': 500, 'headers': cors,
                'body': json.dumps({'error': f'HTTP {e.code}', 'details': err[:500]})}
    except Exception as e:
        return {'statusCode': 500, 'headers': cors,
                'body': json.dumps({'error': str(e), 'type': type(e).__name__})}
