import json, urllib.request, urllib.error, ssl, os, time, traceback

ANTHROPIC_KEY = os.environ.get('ANTHROPIC_API_KEY','')
S3_BUCKET = os.environ.get('S3_BUCKET','justhodl-dashboard-live')

def get_report_data():
    try:
        import boto3
        s3 = boto3.client('s3', region_name='us-east-1')
        obj = s3.get_object(Bucket=S3_BUCKET, Key='data/report.json')
        return json.loads(obj['Body'].read())
    except Exception as e:
        print(f"[CHAT] S3 error: {e}")
        return None

def build_system_prompt(report, mode='chat'):
    """Build COMPACT system prompt — keep under 15K chars to avoid 529"""
    if not report:
        return "You are JustHodl AI, Khalid's financial intelligence assistant. No live data loaded."

    ki = report.get('khalid_index', {})
    risk = report.get('risk_dashboard', {})
    nl = report.get('net_liquidity', {})
    stocks = report.get('stocks', {})
    crypto = report.get('crypto', {})
    signals = report.get('signals', {})
    news = report.get('news', [])
    ai = report.get('ai_analysis', {})
    flow = report.get('market_flow', {})
    ts = report.get('generated_at', 'unknown')
    stats = report.get('stats', {})

    # COMPACT STOCKS: top/bottom scored + day movers
    scored = [(t, s) for t, s in stocks.items() if s.get('score')]
    scored.sort(key=lambda x: x[1].get('score', 50), reverse=True)
    top10 = scored[:10]
    bot10 = scored[-10:] if len(scored) > 10 else []
    day_sorted = sorted(stocks.items(), key=lambda x: x[1].get('day_pct', 0), reverse=True)
    top_gain = day_sorted[:8]
    top_loss = day_sorted[-8:] if len(day_sorted) > 8 else []

    def sl(t, s):
        return f"{t} ${s.get('price',0):.2f} d:{s.get('day_pct',0):+.1f}% w:{s.get('week_pct',0):+.1f}% RSI:{s.get('rsi14','-')} MACD:{s.get('macd_cross','-')} Score:{s.get('score','-')}{s.get('grade','')}"

    stock_text = "TOP:\n" + "\n".join([sl(t,s) for t,s in top10])
    stock_text += "\nBOTTOM:\n" + "\n".join([sl(t,s) for t,s in bot10])
    stock_text += "\nGAINERS:\n" + "\n".join([sl(t,s) for t,s in top_gain])
    stock_text += "\nLOSERS:\n" + "\n".join([sl(t,s) for t,s in top_loss])

    # All stocks compact reference
    all_ref = ",".join([f"{t}:{s.get('price',0):.0f}/{s.get('day_pct',0):+.1f}/{s.get('score','')}" for t,s in sorted(stocks.items())])

    # Crypto
    crypto_text = "|".join([f"{t} ${c.get('price',0):,.0f} {c.get('change_24h',0):+.1f}%" for t,c in list(crypto.items())[:10]])

    # News (compact)
    imp_news = [n for n in news if n.get('importance') in ('critical','high')][:10]
    news_text = "\n".join([f"[{n['importance'][:4].upper()}] {n['title'][:80]}" for n in imp_news])

    # AI analysis (compact)
    ai_text = ''
    for key, sec in (ai.get('sections', {}) or {}).items():
        ai_text += f"\n{key}: {sec.get('outlook','')} — {'; '.join(sec.get('signals',[])[:2])}"

    # Flow
    flow_text = ''
    if flow:
        flow_text = f"\nBuying:{flow.get('total_buying',0)} Selling:{flow.get('total_selling',0)}"
        flow_text += f"\nIn:{','.join(flow.get('sectors_buying',[])[:6])}"
        flow_text += f"\nOut:{','.join(flow.get('sectors_selling',[])[:6])}"

    # ATH breakouts
    ath = report.get('ath_breakouts', {})
    ath_text = ''
    if ath:
        bkouts = ath.get('breakouts', [])
        near = ath.get('near_ath', [])
        if bkouts:
            ath_text = f"\nNEW ALL-TIME HIGHS ({len(bkouts)}): " + ", ".join([f"{b['ticker']} ${b['new_ath']}(+{b['pct_above']}% above prev ATH)" for b in bkouts[:10]])
        if near:
            ath_text += f"\nNEAR ATH ({len(near)}): " + ", ".join([f"{n['ticker']}({n['pct_from_ath']:.1f}% away)" for n in near[:10]])

    # Sectors
    sectors = report.get('sectors', {})
    sec_text = "|".join([f"{v.get('name',k)} d:{v.get('day_pct',0):+.1f}% m:{v.get('month_pct',0):+.1f}%" for k,v in sectors.items()])

    # Best plays
    bp = ai.get('best_plays', {})
    bp_text = ''
    for cat, items in (bp or {}).items():
        if items: bp_text += f"\n{cat}: " + ",".join([f"{i['ticker']}({i.get('score','?')})" for i in items[:5]])

    base = f"""You are JustHodl AI in Khalid's Bloomberg Terminal V10.4.
LIVE DATA {ts}. {stats.get('stocks',0)} stocks, {stats.get('fred',0)} FRED, {stats.get('crypto',0)} crypto.

KI:{ki.get('score','?')}/100 Regime:{ki.get('regime','')}
Risk:{risk.get('composite',{}).get('score','?')}/100 {risk.get('composite',{}).get('level','')}
NetLiq:${nl.get('net_liquidity',0)/1e12:.2f}T

SECTORS: {sec_text}
SIGNALS: Buys:{','.join(signals.get('buys',[])[:10])} Sells:{','.join(signals.get('sells',[])[:10])}

{stock_text}

ALL({len(stocks)}):{all_ref}

CRYPTO:{crypto_text}

NEWS:
{news_text}

AI:{ai_text}
PLAYS:{bp_text}
FLOW:{flow_text}
ATH:{ath_text}
"""

    if mode == 'agent':
        base += "\nAGENT: Execute thoroughly. Cite exact numbers. Use markdown tables. Be exhaustive."
    else:
        base += "\nBe direct, cite numbers, use markdown. You ARE the terminal AI."

    return base


def call_claude(system, messages, max_tokens=4000):
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
            body = ''
            try: body = e.read().decode()[:300]
            except: pass
            print(f"[CHAT] HTTP {code} attempt {attempt+1}/3: {body}")
            if code in (429, 529) and attempt < 2:
                time.sleep((attempt + 1) * 8)
                continue
            raise Exception(f"Anthropic {code}: {body[:200]}")
    raise Exception("Anthropic failed after 3 retries")


def lambda_handler(event, context):
    headers = {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Methods': 'POST,OPTIONS'
    }

    method = event.get('httpMethod') or event.get('requestContext', {}).get('http', {}).get('method', '')
    if method == 'OPTIONS':
        return {'statusCode': 200, 'headers': headers, 'body': '{}'}

    try:
        body = json.loads(event.get('body', '{}')) if isinstance(event.get('body'), str) else event
        messages = body.get('messages', [])
        mode = body.get('mode', 'chat')
        task = body.get('task', '')

        if not messages and not task:
            return {'statusCode': 400, 'headers': headers, 'body': json.dumps({'error': 'No message'})}

        t0 = time.time()
        report = get_report_data()
        load_time = round(time.time() - t0, 1)

        system = build_system_prompt(report, mode)
        print(f"[CHAT] Prompt: {len(system)} chars, report: {'yes' if report else 'no'}")

        if mode == 'agent' and task and not messages:
            messages = [{'role': 'user', 'content': task}]

        reply = call_claude(system, messages)
        return {'statusCode': 200, 'headers': headers, 'body': json.dumps({
            'reply': reply, 'mode': mode,
            'data_available': report is not None,
            'prompt_chars': len(system),
            'load_time': load_time
        })}

    except Exception as e:
        print(f"[CHAT] Error: {traceback.format_exc()}")
        return {'statusCode': 500, 'headers': headers, 'body': json.dumps({'error': str(e)})}
