import json, urllib.request, ssl, os, time, traceback

ANTHROPIC_KEY = os.environ.get('ANTHROPIC_API_KEY','')
S3_BUCKET = os.environ.get('S3_BUCKET','justhodl-dashboard-live')

def get_report_data():
    """Fetch latest report.json from S3 for context"""
    try:
        import boto3
        s3 = boto3.client('s3', region_name='us-east-1')
        obj = s3.get_object(Bucket=S3_BUCKET, Key='data/report.json')
        return json.loads(obj['Body'].read())
    except Exception as e:
        print(f"[CHAT] S3 error: {e}")
        return None

def build_system_prompt(report, mode='chat'):
    """Build context-rich system prompt from live market data"""
    if not report:
        return "You are JustHodl AI, a professional financial analyst. No live data currently available."

    ki = report.get('khalid_index', {})
    risk = report.get('risk_dashboard', {})
    nl = report.get('net_liquidity', {})
    stats = report.get('stats', {})
    signals = report.get('signals', {})
    sectors = report.get('sectors', {})
    ai = report.get('ai_analysis', {})
    stocks = report.get('stocks', {})
    crypto = report.get('crypto', {})
    news = report.get('news', [])
    fred = report.get('fred', {})

    # Top movers
    stock_list = [(t, s) for t, s in stocks.items() if s.get('price')]
    top_gainers = sorted(stock_list, key=lambda x: x[1].get('day_pct', 0), reverse=True)[:10]
    top_losers = sorted(stock_list, key=lambda x: x[1].get('day_pct', 0))[:10]
    strong_buys = [(t, s) for t, s in stock_list if s.get('grade') == 'STRONG_BUY']
    strong_sells = [(t, s) for t, s in stock_list if s.get('grade') == 'STRONG_SELL']
    golden_crosses = [(t, s) for t, s in stock_list if s.get('cross') == 'GOLDEN_NEW']
    death_crosses = [(t, s) for t, s in stock_list if s.get('cross') == 'DEATH_NEW']

    # Key FRED values
    def gv(cat, sid):
        return fred.get(cat, {}).get(sid, {}).get('current')

    # Breaking news
    critical_news = [n for n in news if n.get('importance') == 'critical'][:5]
    high_news = [n for n in news if n.get('importance') == 'high'][:5]
    news_text = '\n'.join([f"  [{n['importance'].upper()}] {n['title']} ({n.get('source','')})" for n in (critical_news + high_news)[:10]])

    # Sector performance
    sector_text = '\n'.join([f"  {s['name']}: Day {s.get('day_pct',0):+.1f}% | Week {s.get('week_pct',0):+.1f}% | Month {s.get('month_pct',0):+.1f}%" for _, s in sorted(sectors.items(), key=lambda x: x[1].get('day_pct', 0), reverse=True)])

    # AI sections summary
    ai_text = ''
    for key, sec in ai.get('sections', {}).items():
        outlook = sec.get('outlook', '')
        sigs = sec.get('signals', [])[:3]
        ai_text += f"\n  {sec.get('title', key)} [{outlook}]: {'; '.join(sigs)}"

    # Best plays
    bp = ai.get('best_plays', {})
    bp_text = ''
    for cat in ['top_stocks', 'top_etfs', 'top_bonds', 'top_commodities', 'top_crypto']:
        items = bp.get(cat, [])[:5]
        if items:
            bp_text += f"\n  {cat.replace('_',' ').title()}: " + ', '.join([f"{i['ticker']} (Score:{i.get('score','?')} {i.get('grade','')})" for i in items])

    # Crypto summary
    btc = crypto.get('BTC', {})
    eth = crypto.get('ETH', {})
    sol = crypto.get('SOL', {})
    crypto_text = f"BTC ${btc.get('price',0):,.0f} ({btc.get('change_24h',0):+.1f}% 24h) | ETH ${eth.get('price',0):,.0f} ({eth.get('change_24h',0):+.1f}%) | SOL ${sol.get('price',0):,.0f} ({sol.get('change_24h',0):+.1f}%)"

    # Market flow summary
    flow = report.get('market_flow', {})
    flow_text = ''
    if flow:
        bought = flow.get('most_bought', [])[:10]
        sold = flow.get('most_sold', [])[:10]
        sec_buy = flow.get('sectors_buying', [])
        sec_sell = flow.get('sectors_selling', [])
        flow_text += f"\nBuying pressure: {flow.get('total_buying',0)} stocks | Selling pressure: {flow.get('total_selling',0)} stocks"
        flow_text += f"\nSectors with inflows: {', '.join(sec_buy[:8])}"
        flow_text += f"\nSectors with outflows: {', '.join(sec_sell[:8])}"
        if bought: flow_text += "\nTop bought: " + ', '.join([f"{b['ticker']}(+{b['flow_score']})" for b in bought])
        if sold: flow_text += "\nTop sold: " + ', '.join([f"{s['ticker']}({s['flow_score']})" for s in sold])

    ts = report.get('generated_at', 'unknown')

    base = f"""You are JustHodl AI, Khalid's elite financial intelligence assistant embedded in his Bloomberg Terminal V10.3.
You have access to LIVE market data updated at {ts}.

=== KHALID INDEX ===
Score: {ki.get('score','?')}/100 | Regime: {ki.get('regime','?')}
Components: {', '.join([f"{s[0]}: {s[1]:+d} ({s[2]})" for s in ki.get('signals',[])])}

=== RISK DASHBOARD ===
Composite: {risk.get('composite','?')}/100 | Credit: {risk.get('credit','?')} | Liquidity: {risk.get('liquidity','?')} | Market: {risk.get('market','?')} | Recession: {risk.get('recession','?')} | Systemic: {risk.get('systemic','?')} | Inflation: {risk.get('inflation','?')}

=== NET LIQUIDITY ===
Net: ${nl.get('net',0)/1e6:.2f}T | Fed: ${nl.get('fed',0)/1e6:.2f}T | TGA: ${nl.get('tga',0)/1e6:.2f}T | RRP: ${nl.get('rrp',0):.0f}B

=== KEY RATES ===
VIX: {gv('risk','VIXCLS')} | 10Y: {gv('treasury','DGS10')} | 2Y: {gv('treasury','DGS2')} | FFR: {gv('treasury','DFF')} | DXY: {gv('dxy','DTWEXBGS')} | HY Spread: {gv('ice_bofa','BAMLH0A0HYM2')}
Curve 10Y-2Y: {gv('treasury','T10Y2Y')} | 10Y-3M: {gv('treasury','T10Y3M')} | 30Y Mort: {gv('risk','MORTGAGE30US')}
CPI: {gv('inflation','CPALTT01USM657N')} | Unemp: {gv('macro','UNRATE')} | GDP: {gv('macro','A191RL1Q225SBEA')}

=== SECTORS ===
{sector_text}

=== SIGNALS ===
Bullish ({len(signals.get('buys',[]))}): {', '.join(signals.get('buys',[])[:15])}
Bearish ({len(signals.get('sells',[]))}): {', '.join(signals.get('sells',[])[:15])}
Warnings: {', '.join(signals.get('warnings',[])[:10])}

=== TOP GAINERS ===
{', '.join([f"{t} {s.get('day_pct',0):+.1f}%" for t,s in top_gainers])}

=== TOP LOSERS ===
{', '.join([f"{t} {s.get('day_pct',0):+.1f}%" for t,s in top_losers])}

=== STRONG BUY SIGNALS ({len(strong_buys)}) ===
{', '.join([f"{t}(Score:{s.get('score','?')})" for t,s in strong_buys[:15]])}

=== STRONG SELL SIGNALS ({len(strong_sells)}) ===
{', '.join([f"{t}(Score:{s.get('score','?')})" for t,s in strong_sells[:15]])}

=== NEW GOLDEN CROSSES ===
{', '.join([t for t,_ in golden_crosses]) or 'None today'}

=== NEW DEATH CROSSES ===
{', '.join([t for t,_ in death_crosses]) or 'None today'}

=== BEST PLAYS ==={bp_text}

=== CRYPTO ===
{crypto_text}

=== BREAKING NEWS ===
{news_text}

=== AI ANALYSIS SUMMARY ==={ai_text}

=== MARKET FLOW ==={flow_text}

=== DATA COVERAGE ===
{stats.get('fred',0)} FRED series | {stats.get('stocks',0)} stocks | {stats.get('crypto',0)} crypto | {stats.get('ecb_ciss',0)} ECB CISS | {len(news)} news articles
"""

    if mode == 'chat':
        base += """
INSTRUCTIONS:
- Answer questions about markets using the LIVE data above
- Be direct, data-driven, cite specific numbers
- Give actionable insights when asked
- If asked about a specific ticker, provide its full stats
- Format with markdown for readability
- You ARE the terminal's AI brain — speak with authority"""
    elif mode == 'agent':
        base += """
INSTRUCTIONS — AGENT MODE:
You are an autonomous financial agent. When given a task, execute it thoroughly using all data above.
Available tasks:
1. SCAN: Deep scan for opportunities across all 188 stocks, bonds, commodities, crypto
2. ANALYZE: Deep analysis of any ticker, sector, or macro theme
3. REPORT: Generate comprehensive market intelligence report
4. ALERT: Identify critical market conditions, anomalies, or risks
5. STRATEGY: Build investment strategies based on current conditions
6. COMPARE: Cross-asset or cross-sector comparison
7. FORECAST: Probability-weighted scenario analysis
8. PORTFOLIO: Analyze portfolio positioning vs current macro

For each task, be exhaustive and professional. Include specific data points.
Format with clear sections using markdown headers and tables where helpful.
Always conclude with actionable next steps."""

    return base

def call_claude(system, messages, max_tokens=4000):
    """Call Anthropic API with retry on 429/529"""
    data = json.dumps({
        "model": "claude-sonnet-4-20250514",
        "max_tokens": max_tokens,
        "system": system,
        "messages": messages
    }).encode()
    
    for attempt in range(3):
        try:
            req = urllib.request.Request(
                'https://api.anthropic.com/v1/messages',
                data=data,
                headers={
                    'Content-Type': 'application/json',
                    'x-api-key': ANTHROPIC_KEY,
                    'anthropic-version': '2023-06-01'
                }
            )
            ctx = ssl.create_default_context()
            with urllib.request.urlopen(req, timeout=80, context=ctx) as r:
                resp = json.loads(r.read())
            return resp.get('content', [{}])[0].get('text', 'No response')
        except urllib.error.HTTPError as e:
            code = e.code
            print(f"[CHAT] Anthropic HTTP {code} attempt {attempt+1}/3")
            if code in (429, 529) and attempt < 2:
                wait = (attempt + 1) * 5
                print(f"[CHAT] Retrying in {wait}s...")
                time.sleep(wait)
                continue
            body = e.read().decode() if hasattr(e, 'read') else ''
            raise Exception(f"Anthropic API error {code}: {body[:200]}")
    raise Exception("Anthropic API failed after 3 retries")

def lambda_handler(event, context):
    headers = {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'POST,OPTIONS'
    }

    # Handle CORS
    method = event.get('httpMethod') or event.get('requestContext', {}).get('http', {}).get('method', '')
    if method == 'OPTIONS':
        return {'statusCode': 200, 'headers': headers, 'body': '{}'}

    try:
        body = json.loads(event.get('body', '{}')) if isinstance(event.get('body'), str) else event
        messages = body.get('messages', [])
        mode = body.get('mode', 'chat')  # 'chat' or 'agent'
        task = body.get('task', '')  # For agent mode preset tasks

        if not messages and not task:
            return {'statusCode': 400, 'headers': headers, 'body': json.dumps({'error': 'No message provided'})}

        # Get live market data for context
        report = get_report_data()
        system = build_system_prompt(report, mode)

        # For agent mode with preset task
        if mode == 'agent' and task and not messages:
            messages = [{'role': 'user', 'content': task}]

        reply = call_claude(system, messages)
        return {'statusCode': 200, 'headers': headers, 'body': json.dumps({'reply': reply, 'mode': mode, 'data_available': report is not None})}

    except Exception as e:
        print(f"[CHAT] Error: {traceback.format_exc()}")
        return {'statusCode': 500, 'headers': headers, 'body': json.dumps({'error': str(e)})}
