#!/usr/bin/env python3
"""Build source-bound public output inspection contracts and install the inspector on every public route.
No network or runtime completeness claim. Dynamic/private/unresolved outputs remain explicitly classified.
"""
import argparse,json,os,re,shutil,sys
from collections import defaultdict
from pathlib import Path
from page_sources import scan_pages,pages
ROOT=Path(__file__).resolve().parents[1]
SENSITIVE=re.compile(r'(^|[/_.-])(private|secrets?|credentials?|tokens?|passwords?|userdata|users?|brain|journal|portfolio|orders?|accounts?|subscriptions?|auth)([/_.-]|$)',re.I)
PUBLIC_PREFIXES={'data','screener','etf-flows','macro','sentiment','air','regime','base-rates','divergence','plumbing-composite','risk','calibration'}
def public_key(key):
    return (key.endswith('.json') and '*' not in key and '..' not in key.split('/') and not SENSITIVE.search(key)
            and (key.split('/')[0] in PUBLIC_PREFIXES or key=='liquidity-data.json')
            and not any(part.startswith('_') for part in key.split('/')))
def contract(root):
    doc=json.loads((root/'engine-manifest.json').read_text());engines={e['engine']:e for e in doc['engines']};writers=defaultdict(set)
    for e in engines.values():
        for key in e['keys']:writers[key].add(e['engine'])
    emap={}
    for name,e in engines.items():
        allowed=[{'engine':name,'key':k,'inspection_schema':'json-value.v1','ownership_evidence':e['write_evidence'].get(k,[])} for k in e['keys'] if public_key(k)]
        emap[name]={'outputs':allowed,'restricted_count':sum(not public_key(k) for k in e['keys']),
                    'historical_or_dynamic_family_count':len(e['key_patterns']), 'unresolved_count':len(e['unresolved_writes']),
                    'runtime_coverage':'unverified_until_opened','ownership_basis':'actual source write arguments'}
    pmap={};graphs=scan_pages(root)
    for route,graph in graphs.items():
        producers=set();unresolved=[]
        for w in graph['wires']:
            if w['engine'] not in writers.get(w['feed'],set()):raise ValueError('Invalid page producer declaration: '+route+' '+w['engine']+' '+w['feed'])
            producers.add(w['engine'])
        for key in graph['keys']:
            owners=writers.get(key,set())
            if len(owners)==1:producers.update(owners)
            elif len(owners)>1:unresolved.append({'reason':'multiple source writers','key':key})
            else:unresolved.append({'reason':'no source-bound writer','key':key})
        outputs=[];seen=set()
        for name in sorted(producers):
            for o in emap[name]['outputs']:
                pair=(o['engine'],o['key'])
                if pair not in seen:outputs.append(o);seen.add(pair)
        pmap[route]={'outputs':outputs,'producers':sorted(producers),'restricted_count':sum(emap[e]['restricted_count'] for e in producers),
                     'unresolved_count':len(unresolved)+sum(emap[e]['unresolved_count'] for e in producers),
                     'historical_or_dynamic_family_count':sum(emap[e]['historical_or_dynamic_family_count'] for e in producers),
                     'association_basis':'exact source references or explicit validated page declarations; shared-script conditional use not inferred as primary ownership',
                     'runtime_coverage':'unverified_until_opened','missing_script_count':len(graph['missing_scripts'])}
    return {'schema_version':'page-data-contract.v1','inspection_schema':'json-value.v1','source_manifest_schema':doc['schema_version'],
            'pages':pmap,'engines':emap,'coverage':{'public_routes':len(pmap),'engines':len(emap),'routes_with_source_bound_outputs':sum(bool(x['outputs']) for x in pmap.values()),
            'routes_without_source_bound_outputs':sum(not x['outputs'] for x in pmap.values()),'all_routes_receive_inspector':True,
            'claim':'Every returned JSON path and row is inspectable; availability, source freshness, domain semantics and unresolved ownership are not certified by static generation.'}}
def main():
    ap=argparse.ArgumentParser();ap.add_argument('--site');ap.add_argument('--check',action='store_true');a=ap.parse_args()
    doc=contract(ROOT);path=ROOT/'config/page-data-contracts.json'
    if a.check:
        old=json.loads(path.read_text())
        if old!=doc:raise SystemExit('Page data contracts drift: regenerate before deploy')
        print(json.dumps(doc['coverage']));return
    tmp=path.with_suffix('.json.tmp');tmp.write_text(json.dumps(doc,separators=(',',':')));os.replace(tmp,path)
    if a.site:
        site=Path(a.site);(site/'config').mkdir(exist_ok=True);shutil.copyfile(path,site/'config/page-data-contracts.json');shutil.copyfile(ROOT/'jh-data-inspector.js',site/'jh-data-inspector.js')
        for page in pages(site):
            source=page.read_text(errors='replace')
            if 'src="/jh-data-inspector.js"' not in source:
                tag='<script src="/jh-data-inspector.js" defer data-contract="page-data-contract.v1"></script>'
                source=re.sub(r'</body>',lambda m:tag+'\n'+m[0],source,count=1,flags=re.I) if re.search(r'</body>',source,re.I) else source+tag
                page.write_text(source)
    print(json.dumps(doc['coverage']))
if __name__=='__main__':main()
