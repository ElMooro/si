"""Synthetic official response fixtures compiled by the actual archive model."""
import hashlib
from copy import deepcopy
import fred_vintage_model as m
STAMP='2026-09-09T00:00:00+00:00'


def record(sid,endpoint,body,start=m.EARLIEST,end=m.MAX_DATE,offset=0):
    req=m.request(sid,endpoint,start,end,offset);raw=m.encoded(body);sha=hashlib.sha256(raw).hexdigest()
    return {'raw':raw,'request':req,'acquired_at':'2026-09-08T20:00:00+00:00',
        'evidence':{'key':m.PREFIX+'originals/'+m.digest(req)+'/'+sha+'.json.gz',
                    'sha256':sha,'bytes':len(raw),'request_sha256':m.digest(req)}}


def inputs(sid='WALCL',value='10000',units='Millions of U.S. Dollars'):
    meta={'realtime_start':m.EARLIEST,'realtime_end':m.MAX_DATE,'seriess':[
        {'id':sid,'realtime_start':'2026-09-01','realtime_end':m.MAX_DATE,'title':sid,
         'units':units,'frequency_short':'W','seasonal_adjustment':'Not Seasonally Adjusted'}]}
    req=m.request(sid,'series/observations','2026-09-01','2026-09-08')['params']
    body={k:v for k,v in req.items() if k!='series_id'}
    body.update(count=1,observations=[{'date':'2026-09-01','realtime_start':'2026-09-01','realtime_end':'2026-09-08','value':value}])
    return record(sid,'series',meta),record(sid,'series/observations',body,'2026-09-01','2026-09-08')


def seal(doc):
    doc=deepcopy(doc);doc.pop('replay',None)
    doc['replay']={'output_sha256':m.digest(doc),'manifest_key':m.PREFIX+'runs/'+'a'*64+'.json'}
    return doc


def packet(sid='WALCL',value='10000',units='Millions of U.S. Dollars'):
    definition,page=inputs(sid,value,units)
    return seal(m.compile_series(sid,definition,[page],STAMP,'one-collection','2026-09-08','2026-09-08T19:00:00+00:00'))


def liquidity_docs():
    return {'WALCL':packet(),'WTREGEN':packet('WTREGEN','1','Billions of U.S. Dollars'),
            'RRPONTSYD':packet('RRPONTSYD','2','Billions of US Dollars')}
