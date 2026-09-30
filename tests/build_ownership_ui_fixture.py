"""Offline summary overlay on the existing synthetic public holdings fixture."""
import gzip,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'aws/shared'))
import etf_holdings_model as model
f=json.loads((ROOT/'tests/fixtures/etf-holdings-native.json').read_text())
objects=f['artifacts'];extra={};packets={};overrides={}
def read(ref):return json.loads(objects[ref['key']])
def emit(k,v):extra[k]=v.decode()
def snapshot(ref):
    doc=read(ref);doc['rows']=[r for p in doc['parts'] for r in read(p)['rows']];return doc
run=read({'key':f['publications']['holdings']['replay']['manifest_key']});p=read(run['output'])
summary=model.OwnershipSummary(p['generated_at'],len(p['funds']))
for t,fund in sorted(p['funds'].items()):
    a,b=[snapshot(fund[role]['snapshot']) for role in ('current','prior')]
    if t=='SPY':
        a['rows']=[r for r in a['rows'] if r['identity_key'] and not r['field_errors']]
        a['quality'].update(missing_identity_rows=0,duplicate_identity_rows=0,rows_with_field_errors=0)
        overrides[t]=model.retain_snapshot(a,emit);fund['current']=overrides[t]
    comp=read(fund['comparison']);comp['rows']=[r for part in comp['parts'] for r in read(part)['rows']]
    summary.add(t,a,b,comp,fund['current']['snapshot'],fund['prior']['snapshot'],fund['comparison'],fund.get('configured_tag_unverified',{}))
ref=summary.finish(emit)
assert ref['status']=='complete'
for kind in ('holdings','lookthrough'):
    run=read({'key':f['publications'][kind]['replay']['manifest_key']});p=read(run['output']);p['ownership_summary']=ref;p['version']='1.1.0'
    for t,current in overrides.items():p['funds'][t]['current']=current
    base=model.PREFIX if kind=='holdings' else model.LOOK_PREFIX
    raw=model.encoded(p);digest=model.sha(raw);key=base+'outputs/'+digest+'.json';emit(key,raw)
    run['output']={'key':key,'sha256':digest,'bytes':len(raw)};run['output_sha256']=digest
    raw=model.encoded(run);key=base+'runs/'+model.sha(raw)+'.json';emit(key,raw)
    packets[kind]={**p,'replay':{'manifest_key':key,'output_sha256':digest}}
out={'synthetic':True,'artifacts':extra,'packets':packets}
(ROOT/'tests/fixtures/ownership-summary-ui-synthetic.json.gz').write_bytes(gzip.compress(model.encoded(out),mtime=0))
print('Synthetic summary:',ref,'objects',len(extra))
