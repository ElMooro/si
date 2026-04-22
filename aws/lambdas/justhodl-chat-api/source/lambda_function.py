import os
import json,os,urllib.request,ssl

ANTHROPIC_KEY=os.environ.get('ANTHROPIC_API_KEY',os.environ.get('ANTHROPIC_API_KEY', ''))
BUCKET='justhodl-dashboard-live'

def lambda_handler(event, context):
    headers={'Access-Control-Allow-Origin':'*','Access-Control-Allow-Headers':'Content-Type','Access-Control-Allow-Methods':'POST,OPTIONS'}
    if event.get('httpMethod')=='OPTIONS':
        return{'statusCode':200,'headers':headers,'body':''}
    try:
        body=json.loads(event.get('body','{}'))
        messages=body.get('messages',[])
        # Load latest market data for context
        import boto3
        s3=boto3.client('s3',region_name='us-east-1')
        try:
            obj=s3.get_object(Bucket=BUCKET,Key='data/report.json')
            market_data=json.loads(obj['Body'].read())
        except:
            market_data={}
        system_prompt=f"""You are Khalid's personal AI financial advisor built into the JustHodl.AI dashboard. You have access to his live market data updated daily.

CURRENT MARKET DATA:
- Khalid Index: {market_data.get('khalid_index','N/A')}/100 ({market_data.get('regime','N/A')})
- Signals: {json.dumps(market_data.get('signals',[]))}

FRED Economic Data:
{json.dumps({k:v for k,v in market_data.get('fred',{}).items()},indent=1)}

Stock Prices:
{json.dumps({k:v for k,v in market_data.get('stocks',{}).items()},indent=1)}

Top Gainers: {json.dumps(market_data.get('gainers',[])[:5])}
Top Losers: {json.dumps(market_data.get('losers',[])[:5])}
Stocks Near ATH: {json.dumps(market_data.get('ath',[])[:10])}
Risk-Reward Plays: {json.dumps(market_data.get('plays',[]))}

Generated: {market_data.get('generated','Unknown')}

You are an expert financial analyst. Answer questions about the market data, provide analysis, suggest trades, explain the Khalid Index, and help with any financial questions. Be direct, data-driven, and use actual numbers from the data above. Format responses with markdown for readability."""

        data=json.dumps({"model": "claude-haiku-4-5-20251001","max_tokens":2000,"system":system_prompt,"messages":messages}).encode()
        req=urllib.request.Request('https://api.anthropic.com/v1/messages',data=data,headers={'Content-Type':'application/json','x-api-key':ANTHROPIC_KEY,'anthropic-version':'2023-06-01'})
        ctx=ssl.create_default_context()
        with urllib.request.urlopen(req,timeout=30,context=ctx) as r:
            resp=json.loads(r.read())
        reply=resp.get('content',[{}])[0].get('text','No response')
        return{'statusCode':200,'headers':headers,'body':json.dumps({'reply':reply})}
    except urllib.error.HTTPError as e:
        detail = ''
        try: detail = e.read().decode('utf-8', errors='ignore')[:600]
        except Exception: pass
        return {'statusCode':500,'headers':headers,'body':json.dumps({'error':f'HTTP {e.code}: {e.reason}', 'anthropic_body': detail, 'key_prefix': ANTHROPIC_KEY[:12]})}
    except Exception as e:
        return {'statusCode':500,'headers':headers,'body':json.dumps({'error':str(e)})}
