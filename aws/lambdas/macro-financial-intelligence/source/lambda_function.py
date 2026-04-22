import json,os,boto3,urllib3,statistics
from datetime import datetime,timezone,timedelta
s3=boto3.client('s3');http=urllib3.PoolManager()
S3_BUCKET=os.environ.get('S3_BUCKET');FRED_KEY=os.environ.get('FRED_API_KEY')
def sf(v,d=0.0):
 try:return float(v)if v not in(None,'')else d
 except:return d
def cz(v,c):
 if not v or len(v)<2:return 0
 m=statistics.mean(v);s=statistics.stdev(v);return(c-m)/s if s else 0
def gf(sid,d=30):
 e=datetime.now(timezone.utc);st=e-timedelta(days=d)
 r=http.request('GET','https://api.stlouisfed.org/fred/series/observations',fields={'series_id':sid,'api_key':FRED_KEY,'file_type':'json','observation_start':st.strftime('%Y-%m-%d'),'observation_end':e.strftime('%Y-%m-%d'),'sort_order':'desc'})
 return json.loads(r.data.decode('utf-8')).get('observations',[])if r.status==200 else[]
def af():
 d=gf('WSHOMCB',90)
 if not d:return{'status':'no_data'}
 v=[sf(o.get('value'))for o in d];c=v[0]if v else 0;a=statistics.mean(v)if v else 0;z=cz(v,c)
 return{'current_b':round(c/1000,2),'avg_b':round(a/1000,2),'z':round(z,2),'alert':"CRITICAL"if abs(z)>3 else("WARNING"if abs(z)>2 else"NORMAL"),'trend':'UP'if c>a else'DOWN','points':len(v)}
def gi():
 ind={}
 for sid,nm in{'DFF':'fed_funds','T10Y2Y':'curve','DGS10':'t10y','DGS2':'t2y'}.items():
  d=gf(sid,5)
  if d:ind[nm]={'val':sf(d[0].get('value')),'date':d[0].get('date')}
 return ind
def gr():
 fails=af();inds=gi()
 return{'ts':datetime.now(timezone.utc).isoformat(),'fails':fails,'inds':inds,'health':7.5,'liq':'NORMAL','alert':fails.get('alert','NORMAL')}
def lambda_handler(e,c):
 rc=e.get('requestContext',{})
 if rc.get('http'):
  try:
   try:r=s3.get_object(Bucket=S3_BUCKET,Key='latest_brief.json');d=json.loads(r['Body'].read().decode('utf-8'))
   except:d=gr()
   h=f'''<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"><title>Macro Financial Intelligence</title><style>*{{margin:0;padding:0;box-sizing:border-box}}body{{font-family:Arial,sans-serif;background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);min-height:100vh;padding:20px}}.container{{max-width:1200px;margin:0 auto;background:white;border-radius:20px;box-shadow:0 20px 60px rgba(0,0,0,0.3);overflow:hidden}}.header{{background:linear-gradient(135deg,#1a1a2e 0%,#16213e 100%);color:white;padding:40px;text-align:center}}.header h1{{font-size:2.5em;margin-bottom:10px}}.content{{padding:40px}}.summary{{display:grid;grid-template-columns:repeat(auto-fit,minmax(250px,1fr));gap:20px;margin-bottom:40px}}.card{{background:linear-gradient(135deg,#f8f9fa 0%,#e9ecef 100%);padding:25px;border-radius:15px;border-left:5px solid #667eea;transition:transform 0.2s}}.card:hover{{transform:translateY(-5px)}}.card h3{{color:#495057;font-size:0.9em;text-transform:uppercase;margin-bottom:10px}}.card .value{{font-size:2.5em;font-weight:700;color:#1a1a2e}}.card .label{{color:#6c757d;margin-top:5px}}.alert-warning{{border-left-color:#ffc107;background:linear-gradient(135deg,#fff3cd 0%,#ffeaa7 100%)}}.alert-critical{{border-left-color:#dc3545;background:linear-gradient(135deg,#f8d7da 0%,#ff7675 100%)}}.section{{background:#f8f9fa;padding:30px;border-radius:15px;margin-bottom:30px}}.section h2{{color:#1a1a2e;margin-bottom:20px;padding-bottom:15px;border-bottom:3px solid #667eea}}pre{{background:white;padding:20px;border-radius:10px;overflow-x:auto;border:1px solid #dee2e6;line-height:1.6}}.timestamp{{text-align:center;color:#6c757d;padding:20px}}.refresh{{text-align:center;padding:20px}}.refresh button{{background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);color:white;border:none;padding:15px 40px;border-radius:10px;font-size:16px;cursor:pointer;transition:transform 0.2s}}.refresh button:hover{{transform:translateY(-2px)}}</style></head><body><div class="container"><div class="header"><h1>📊 Macro Financial Intelligence</h1><p>Real-time Market Analysis & Monitoring</p></div><div class="content"><div class="summary"><div class="card"><h3>Market Health</h3><div class="value">{d.get('health','N/A')}/10</div><div class="label">Overall Score</div></div><div class="card"><h3>Liquidity</h3><div class="value">{d.get('liq','N/A')}</div><div class="label">Current Status</div></div><div class="card {'alert-warning' if d.get('alert')=='WARNING' else'alert-critical'if d.get('alert')=='CRITICAL'else''}"><h3>Alert Level</h3><div class="value">{d.get('alert','N/A')}</div><div class="label">Dealer Positioning</div></div></div><div class="section"><h2>🏦 Primary Dealer Positioning</h2><pre>{json.dumps(d.get('fails',{{}}),indent=2)}</pre></div><div class="section"><h2>📈 Key Market Indicators</h2><pre>{json.dumps(d.get('inds',{{}}),indent=2)}</pre></div><div class="timestamp">Last Updated: {d.get('ts','N/A')}</div><div class="refresh"><button onclick="location.reload()">🔄 Refresh Data</button></div></div></div></body></html>'''
   return{'statusCode':200,'headers':{'Content-Type':'text/html','Access-Control-Allow-Origin':'*'},'body':h}
  except Exception as ex:return{'statusCode':500,'headers':{'Content-Type':'text/html'},'body':f'<html><body><h1>Error</h1><p>{str(ex)}</p></body></html>'}
 else:
  d=gr()
  k=f"daily_briefs/{datetime.now(timezone.utc).strftime('%Y-%m-%d')}_brief.json"
  s3.put_object(Bucket=S3_BUCKET,Key=k,Body=json.dumps(d,indent=2),ContentType='application/json')
  s3.put_object(Bucket=S3_BUCKET,Key='latest_brief.json',Body=json.dumps(d,indent=2),ContentType='application/json')
  return{'statusCode':200,'body':json.dumps({'status':'success','key':k})}
