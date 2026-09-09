"""Bounded Alpha Vantage adapter. Function URL configuration owns CORS."""
import json
import re
import urllib.parse
from managed_secret import managed_secret
from public_provider_json import ProviderError, error_metadata, get_json

FUNCTIONS={'GLOBAL_QUOTE','SMA','EMA','RSI','MACD','STOCH','BBANDS','ADX','CCI'}

def response(status, body):
    return {'statusCode':status,'headers':{'Content-Type':'application/json','Cache-Control':'no-store'},
            'body':json.dumps(body,allow_nan=False)}

def lambda_handler(event, context):
    try:
        if not isinstance(event,dict):raise ValueError()
        method=(event.get('requestContext',{}).get('http',{}).get('method') or event.get('httpMethod') or 'POST').upper()
        if method=='OPTIONS':return response(204,{})
        if method not in ('GET','POST'):return response(405,{'error':'method_not_allowed'})
        if event.get('rawPath','/')=='/health':
            return response(200,{'agent':'alphavantage-technical-analysis','status':'HANDLER_READY','provider_data_verified':False})
        if event.get('rawPath','/')!='/':return response(404,{'error':'unknown_route'})
        body=json.loads(event['body']) if event.get('body') else event.get('queryStringParameters') or event
        if not isinstance(body,dict):raise ValueError()
        function=body.get('function','GLOBAL_QUOTE');symbol=body.get('symbol','SPY')
        if not isinstance(function,str) or function not in FUNCTIONS:raise ValueError()
        if not isinstance(symbol,str) or not re.fullmatch(r'[A-Za-z0-9.^_-]{1,24}',symbol):raise ValueError()
        params={'function':function,'symbol':symbol.upper()}
        if function!='GLOBAL_QUOTE':
            interval=body.get('interval','daily');series=body.get('series_type','close');period=body.get('time_period',14)
            if str(period) not in {str(n) for n in range(1,201)}:raise ValueError()
            if interval not in ('1min','5min','15min','30min','60min','daily','weekly','monthly') or series not in ('open','high','low','close'):raise ValueError()
            params.update(interval=interval,time_period=int(period),series_type=series)
    except (ValueError,TypeError,AttributeError):return response(400,{'error':'invalid_request'})
    try:
        params['apikey']=managed_secret(('AV_KEY','ALPHAVANTAGE_KEY','ALPHA_VANTAGE_API_KEY','ALPHAVANTAGE_API_KEY'),('/justhodl/alphavantage/api-key',))
        if not params['apikey']:raise ProviderError('PROVIDER_CREDENTIAL_UNAVAILABLE')
        data=get_json('https://www.alphavantage.co/query?'+urllib.parse.urlencode(params))
        if any(key in data for key in ('Error Message','Note','Information')):
            raise ProviderError('PROVIDER_REJECTED_OR_RATE_LIMITED')
        expected='Global Quote' if function=='GLOBAL_QUOTE' else 'Technical Analysis: '+function
        if not isinstance(data.get(expected),dict) or not data[expected]:raise ProviderError('PROVIDER_SCHEMA_INVALID')
        return response(200,data)
    except ProviderError as exc:return response(503,error_metadata(exc))
    except Exception:return response(503,{'error':'provider_unavailable'})
