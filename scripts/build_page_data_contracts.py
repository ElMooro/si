#!/usr/bin/env python3
"""Build source-bound public output inspection contracts and install the inspector on every public route.
No network or runtime completeness claim. Dynamic/private/unresolved outputs remain explicitly classified.
"""
import argparse,ast,json,os,re,shutil,sys
from collections import defaultdict
from functools import lru_cache
from pathlib import Path
from page_sources import scan_pages,pages
ROOT=Path(__file__).resolve().parents[1]
SENSITIVE=re.compile(r'(^|[/_.-])(private|secrets?|credentials?|tokens?|passwords?|userdata|users?|brain|journal|portfolio|orders?|accounts?|subscriptions?|auth)([/_.-]|$)',re.I)
PUBLIC_PREFIXES={'data','screener','etf-flows','macro','sentiment','air','regime','base-rates','divergence','plumbing-composite','risk','calibration','analytics','backtest','cot','foreign-flows','opportunities','reports','signals'}
PUBLIC_EXACT={'intelligence-report.json','liquidity-data.json','ecb_data.json','edge-data.json','flow-data.json','repo-data.json','treasury_historical_comprehensive.json','valuations-data.json','crypto-intel.json','config/engine-contracts.json',
              'data/proven-portfolio.json','data/proven-portfolio-history.json','data/strategy-portfolio.json','data/simulated-portfolio.json','data/forward-orders.json','predictions.json','portfolio/signal-portfolio-state.json','portfolio/signal-portfolio-history.json','portfolio/sizer-v2.json','data/brain-compiler.json','data/trade-journal.json'}

# Explicit index schemas from the snapshotter's actual rows and bound family writes.
ARCHIVE_INDEXES={
 'calibration/index.json':{'engine':'justhodl-calibration-snapshotter','rows':'versions','key_field':'key','pattern':'calibration/versions/cal-*-*-*.json'},
 'calibration/history-index.json':{'engine':'justhodl-calibration-snapshotter','rows':'snapshots','key_field':'key','patterns':['calibration/history/*.json','calibration/versions/cal-*-*-*.json']},
}

@lru_cache(maxsize=1)
def access_rules():
    """Read only literal policy data, without importing credential/cloud modules."""
    path=ROOT/'aws/shared/private_artifact.py'
    if not path.exists():return {},set(),()
    mirrors={};private=set();prefixes=()
    for node in ast.parse(path.read_text()).body:
        if not isinstance(node,ast.Assign):continue
        names={t.id for t in node.targets if isinstance(t,ast.Name)}
        if 'MIRRORED_ARTIFACTS' in names:mirrors=ast.literal_eval(node.value)
        if 'PRIVATE_PREFIXES' in names:
            if isinstance(node.value,ast.Tuple):prefixes=ast.literal_eval(node.value)
            elif isinstance(node.value,ast.BinOp) and isinstance(node.value.op,ast.Add) and isinstance(node.value.left,ast.Tuple):
                # The policy appends raw-history families for each private key and /data alias.
                keys=set(mirrors)|private
                prefixes=ast.literal_eval(node.value.left)+tuple('history/archive/feed/'+key+'/' for key in keys|{key.removeprefix('data/') for key in keys})
            else:raise ValueError('Unsupported private prefix policy; explicit access review required')
        if 'PRIVATE_KEYS' in names:
            for part in ast.walk(node.value):
                if isinstance(part,ast.Set):private.update(ast.literal_eval(part))
    return mirrors,set(mirrors)|private,tuple(prefixes)

def public_key(key):
    _,private,prefixes=access_rules()
    return (key.endswith('.json') and '*' not in key and '..' not in key.split('/') and key not in private and not key.startswith(prefixes)
            and (key in PUBLIC_EXACT or (not SENSITIVE.search(key) and key.split('/')[0] in PUBLIC_PREFIXES and not any(part.startswith('_') for part in key.split('/')))))
def contract(root):
    mirrors,private_keys,private_prefixes=access_rules()
    role_path=root/'config/page-role-overrides.json'
    roles=json.loads(role_path.read_text())['pages'] if role_path.exists() else {}
    doc=json.loads((root/'engine-manifest.json').read_text());engines={e['engine']:e for e in doc['engines']};writers=defaultdict(set)
    for e in engines.values():
        for key in e['keys']:writers[key].add(e['engine'])
    augmentations={}
    for name,engine in engines.items():
        for role in engine.get('output_roles',[]):
            if role.get('role')=='augmentation' and role.get('cas_write_verified') and role.get('base_producer') in writers.get(role.get('key'),set()):augmentations[(role['key'],name)]=role
    emap={}
    for name,e in engines.items():
        allowed=[{'engine':name,'key':k,'access':'owner_authenticated' if k in mirrors else 'public','private_kind':mirrors.get(k),'required_projection':'brain-compiler' if k=='data/brain-compiler.json' else 'sizing' if k=='data/sizing.json' else None,'inspection_schema':'json-value.v1','ownership_evidence':e['write_evidence'].get(k,[])} for k in e['keys'] if public_key(k) or k in mirrors]
        for output in allowed:
            if (output['key'],name) in augmentations:output['ownership_role']=augmentations[(output['key'],name)]
            index=ARCHIVE_INDEXES.get(output['key'])
            if index and index['engine']==name:
                patterns=index.get('patterns') or [index['pattern']]
                if any(pattern not in e['key_patterns'] for pattern in patterns):raise ValueError('Archive index write family drift: '+name)
                output['archive_index']={**index,'key_regex':'^(?:'+'|'.join(re.escape(pattern).replace(r'\*',r'[^/]+') for pattern in patterns)+')$'}
        emap[name]={'outputs':allowed,'restricted_count':sum(not public_key(k) and k not in mirrors for k in e['keys']),'owner_authenticated_count':sum(k in mirrors for k in e['keys']),
                    'historical_or_dynamic_family_count':len(e['key_patterns']), 'unresolved_count':len(e['unresolved_writes']),
                    'runtime_coverage':'unverified_until_opened','ownership_basis':'actual source write arguments'}
    pmap={};graphs=scan_pages(root);source_usage=defaultdict(int)
    for graph in graphs.values():
        for source in graph['scripts']:source_usage[source]+=1
    for route,graph in graphs.items():
        producers=set();primary=set();unresolved=[];role=roles.get(route,{})
        api_responses=role.get('api_responses',[])
        declared_engines=graph.get('primary_engines',[])+role.get('primary_engines',[])+[r['engine'] for r in api_responses]
        for api in api_responses:
            if api.get('origin','').startswith('https://') is False or not api.get('pathname','').startswith('/') or '*' in api['pathname'] or set(api.get('methods',[])) - {'GET','POST'}:raise ValueError('Unsafe or unsupported API inspection contract: '+route)
        for declared in declared_engines:
            if declared not in engines:raise ValueError('Unknown dedicated engine declaration: '+route+' '+declared)
            producers.add(declared);primary.add(declared)
        primary_keys=set(graph.get('direct_keys',[]))
        for source,keys in graph.get('key_sources',{}).items():
            if source_usage[source]==1:primary_keys.update(keys)
        for w in graph['wires']:
            if w['engine'] not in writers.get(w['feed'],set()):raise ValueError('Invalid page producer declaration: '+route+' '+w['engine']+' '+w['feed'])
            producers.add(w['engine']);primary.add(w['engine'])
        for key in graph['keys']:
            owners=writers.get(key,set())
            bases=owners-{name for name in owners if (key,name) in augmentations}
            pipeline=len(bases)==1 and all(augmentations[(key,name)]['base_producer'] in bases for name in owners-bases)
            if len(owners)==1 or pipeline:
                producers.update(owners)
                if key in primary_keys:primary.update(owners)
            elif len(owners)>1:unresolved.append({'reason':'multiple source writers','key':key,'primary':key in primary_keys,'writers':sorted(owners)})
            else:unresolved.append({'reason':'no source-bound writer','key':key,'primary':key in primary_keys})
        outputs=[];seen=set()
        for name in sorted(producers,key=lambda name:(name not in primary,name)):
            for o in emap[name]['outputs']:
                pair=(o['engine'],o['key'])
                if pair not in seen:outputs.append(o);seen.add(pair)
        static_keys=set()
        for static in role.get('static_outputs',[]):
            from gen_engine_manifest import ast_keys
            source=root/static['source'];key=static['key'];written,_,parsed=ast_keys(source.read_text())
            if not parsed or key not in written or not public_key(key):raise ValueError('Static output ownership or access invalid: '+route+' '+key)
            outputs.append({'engine':static['engine'],'key':key,'access':'public','inspection_schema':'json-value.v1','ownership_evidence':[{'file':static['source'],'basis':'bound write argument'}]});static_keys.add(key)
        unresolved=[row for row in unresolved if row['key'] not in static_keys]
        primary_accessible=sum(o['engine'] in primary for o in outputs)+len(api_responses)+len(static_keys)
        primary_withheld=sum(emap[e]['restricted_count'] for e in primary)
        primary_unresolved=sum(emap[e]['unresolved_count'] for e in primary)
        primary_families=sum(emap[e]['historical_or_dynamic_family_count'] for e in primary)
        indexed={(o['engine'],pattern) for o in outputs if o['engine'] in primary and o.get('archive_index') for pattern in (o['archive_index'].get('patterns') or [o['archive_index']['pattern']])}
        unindexed_families=primary_families-len(indexed)
        unresolved_primary=[row for row in unresolved if row['primary']]
        role_name='NO_ENGINE_EXPECTED' if graph.get('redirect') else role.get('role','ENGINE_PAGE')
        role_reason='Redirect: '+graph['redirect'] if graph.get('redirect') else role.get('reason')
        missing_primary=not primary_accessible or unresolved_primary or graph['script_parse_errors'] or role.get('unresolved_reason')
        primary_status=('NO_PRIMARY_OUTPUT_ACCESS_CONTRACT' if not primary_accessible else
                        'PARTIAL_PRIMARY_OUTPUT_ACCESS' if primary_withheld or primary_unresolved or unindexed_families or missing_primary else 'ACCESSIBLE_BY_CONTRACT')
        coverage_class=('NOT_APPLICABLE' if role_name=='NO_ENGINE_EXPECTED' else 'PRIMARY_VALID_CONTRACT' if primary_status=='ACCESSIBLE_BY_CONTRACT' else 'PRIMARY_PARTIAL' if primary_accessible or primary else 'SUPPORT_ONLY' if producers else 'NO_ASSOCIATION')
        pmap[route]={'page_role':role_name,'role_reason':role_reason,'coverage_class':coverage_class,'api_responses':api_responses,'unresolved_primary_references':unresolved_primary,'dedicated_coverage_note':role.get('unresolved_reason'),'script_parse_errors':graph['script_parse_errors'],'outputs':outputs,'producers':sorted(producers),'primary_producers':sorted(primary),'supplemental_producers':sorted(producers-primary),'primary_output_status':primary_status,'primary_withheld_output_count':primary_withheld,'primary_unresolved_write_count':primary_unresolved,'primary_dynamic_family_count':primary_families,'primary_unindexed_family_count':unindexed_families,'owner_authenticated_count':sum(emap[e]['owner_authenticated_count'] for e in producers),'restricted_count':sum(emap[e]['restricted_count'] for e in producers),
                     'unresolved_count':len(unresolved)+sum(emap[e]['unresolved_count'] for e in producers),'unresolved_references':unresolved,
                     'historical_or_dynamic_family_count':sum(emap[e]['historical_or_dynamic_family_count'] for e in producers),
                     'association_basis':'exact source references or explicit validated page declarations; shared-script conditional use not inferred as primary ownership',
                     'runtime_coverage':'unverified_until_opened','missing_script_count':len(graph['missing_scripts'])}
    return {'schema_version':'page-data-contract.v1','inspection_schema':'json-value.v1','source_manifest_schema':doc['schema_version'],
            'pages':pmap,'engines':emap,'coverage':{'public_routes':len(pmap),'engines':len(emap),'routes_with_source_bound_outputs':sum(bool(x['outputs']) for x in pmap.values()),
            'primary_valid_contract':sum(x['coverage_class']=='PRIMARY_VALID_CONTRACT' for x in pmap.values()),'primary_partial':sum(x['coverage_class']=='PRIMARY_PARTIAL' for x in pmap.values()),'support_only':sum(x['coverage_class']=='SUPPORT_ONLY' for x in pmap.values()),'no_association':sum(x['coverage_class']=='NO_ASSOCIATION' for x in pmap.values()),'not_applicable':sum(x['coverage_class']=='NOT_APPLICABLE' for x in pmap.values()),'routes_with_api_response_contract':sum(bool(x['api_responses']) for x in pmap.values()),
            'routes_without_source_bound_outputs':sum(not x['outputs'] for x in pmap.values()),'routes_with_primary_output_access':sum(x['primary_output_status']!='NO_PRIMARY_OUTPUT_ACCESS_CONTRACT' for x in pmap.values()),'routes_with_partial_primary_output_access':sum(x['primary_output_status']=='PARTIAL_PRIMARY_OUTPUT_ACCESS' for x in pmap.values()),'all_routes_receive_inspector':True,
            'claim':'Every field and row in an opened artifact or observed contracted API response is inspectable. Valid/partial counts are static access contracts, not runtime completeness certification; unindexed families, private paths, unknown writers and availability remain explicit.'}}
def install_html(source,apis):
    # Refresh generated bootstrap even when a build directory is reused.
    source=re.sub(r'<script\b[^>]*\bid=["\']jh-api-data-contract["\'][^>]*>.*?</script>','',source,flags=re.I|re.S)
    source=re.sub(r'<script\b[^>]*\bsrc=["\']/jh-data-inspector\.js["\'][^>]*>\s*</script>','',source,flags=re.I|re.S)
    source=re.sub(r'(<head\b[^>]*>)\s*',r'\1',source,count=1,flags=re.I)
    encoded=json.dumps(apis,separators=(',',':')).replace('<','\\u003c')
    tag='<script id="jh-api-data-contract" type="application/json">'+encoded+'</script><script src="/jh-data-inspector.js" data-contract="page-data-contract.v1"></script>'
    return re.sub(r'<head\b[^>]*>',lambda m:m[0]+tag,source,count=1,flags=re.I) if re.search(r'<head\b',source,re.I) else tag+source

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
            route=str(page.relative_to(site));apis=doc['pages'].get(route,{}).get('api_responses',[])
            page.write_text(install_html(source,apis))
    print(json.dumps(doc['coverage']))
if __name__=='__main__':main()
