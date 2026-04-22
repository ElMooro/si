import os
import boto3,json,urllib.request,base64
def lambda_handler(event,context):
    if not event.get('_push_dex_html'):
        return {'statusCode':200,'body':'ok'}
    TOKEN=os.environ.get('TOKEN', '')
    REPO='ElMooro/si'
    HEADERS={'Authorization':'token '+TOKEN,'Accept':'application/vnd.github.v3+json','Content-Type':'application/json'}
    s3=boto3.client('s3',region_name='us-east-1')
    dex_html=s3.get_object(Bucket='justhodl-dashboard-live',Key='dex.html')['Body'].read()
    try:
        req=urllib.request.Request('https://api.github.com/repos/'+REPO+'/contents/dex.html',headers=HEADERS)
        with urllib.request.urlopen(req) as r:
            existing=json.loads(r.read())
        sha=existing['sha']
    except:
        sha=None
    enc=base64.b64encode(dex_html).decode('utf-8')
    body={'message':'Add dex.html to GitHub Pages','content':enc}
    if sha:
        body['sha']=sha
    payload=json.dumps(body).encode('utf-8')
    req2=urllib.request.Request('https://api.github.com/repos/'+REPO+'/contents/dex.html',data=payload,headers=HEADERS,method='PUT')
    with urllib.request.urlopen(req2) as r2:
        res=json.loads(r2.read())
    return {'statusCode':200,'body':json.dumps({'ok':True,'url':res.get('content',{}).get('html_url','')})}
