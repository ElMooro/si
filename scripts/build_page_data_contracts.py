#!/usr/bin/env python3
"""Build source-bound public output inspection contracts and install the inspector on every public route.
No network or runtime completeness claim. Dynamic/private/unresolved outputs remain explicitly classified.
"""
import argparse,ast,hashlib,json,os,re,shutil,sys
from collections import defaultdict
from functools import lru_cache
from pathlib import Path
from page_sources import scan_pages,pages
ROOT=Path(__file__).resolve().parents[1]
SENSITIVE=re.compile(r'(^|[/_.-])(private|secrets?|credentials?|tokens?|passwords?|userdata|users?|brain|journal|portfolio|orders?|accounts?|subscriptions?|auth)([/_.-]|$)',re.I)
REPOSITORY_ASSETS={
 'assets/vendor/world-atlas-2.0.2-countries-110m.json':'Vendored geographic geometry for the country map',
 'config/home-layout.json':'Reviewed home layout configuration',
 'config/section-registry.json':'Stable navigation section registry',
 'engine-manifest.json':'Build-generated source ownership inventory',
 'nav-manifest.json':'Repository navigation manifest',
}
PUBLIC_PREFIXES={'data','screener','etf-flows','macro','sentiment','air','regime','base-rates','divergence','plumbing-composite','risk','calibration','analytics','backtest','cot','foreign-flows','opportunities','reports','signals'}
PUBLIC_EXACT={'intelligence-report.json','liquidity-data.json','ecb_data.json','edge-data.json','flow-data.json','repo-data.json','treasury_historical_comprehensive.json','valuations-data.json','crypto-intel.json','config/engine-contracts.json',
              'data/proven-portfolio.json','data/proven-portfolio-history.json','data/strategy-portfolio.json','data/simulated-portfolio.json','data/forward-orders.json','predictions.json','portfolio/signal-portfolio-state.json','portfolio/signal-portfolio-history.json','portfolio/sizer-v2.json','data/brain-compiler.json','data/trade-journal.json'}

# Source-reviewed public market histories/caches and government data; privacy policy always wins.
PUBLIC_EXACT.update({
 '_health/fleet.json','data/_fleet-monitor.json','data/_freshness-monitor.json',
 'data/_altseason/global-history.json','data/_backtest/graded.json.gz',
 '13f/clone-holdings-cache.json','13f/clone-price-cache.json','asia/kr-flash-tape.json','asia/tw-orders-levels.json',
 'boom/boom-stage-history.json','chokepoint/fundamentals-ledger.json','credit/credit-before-equity-history.json',
 'data/_cache/chokepoint-irreplaceability.json','data/_ma200/closes.json','data/_ma200/crypto-closes.json',
 'data/ecb-hist/_manifest.json','data/portfolio-analytics.json','data/warm/archived-fred/_index.json',
 'data/warm/tv-bars/_index.json','domain-barometers/history-ledger.json','estimate-revisions/state.json',
 'geo/geopolitical-risk-history.json','investor-debate/_index.json','kcs/flash-cache.json','pboc/afre-flow-cache.json',
 'readthrough/consensus-snapshots.json','sec-filings-cache/company-tickers.json','sec/company-tickers.json',
 'spx-beaters/ai-cache.json','spx-beaters/weekly-closes.json','spx-ma/member-closes.json','state/universe-discovery-snapshot.json',
})

# Explicit index schemas from the snapshotter's actual rows and bound family writes.
ARCHIVE_INDEXES={
 'data/ecb-hist/_manifest.json':{'engine':'justhodl-ecb-history','rows':'series','key_field':'id','key_prefix':'data/ecb-hist/','key_suffix':'.json','family_prefix':'data/ecb-hist/','include_exact_prefix':'data/ecb-hist/'},
 'data/warm/tv-bars/_index.json':{'engine':'justhodl-tv-notes-ingest','rows':'symbols','rows_mode':'object_values','key_field':'key','pattern':'data/warm/tv-bars/*.json'},
 'data/warm/archived-fred/_index.json':{'engine':'justhodl-tv-notes-ingest','rows':'series','rows_mode':'object_keys','key_field':'$value','key_prefix':'data/warm/archived-fred/','key_suffix':'.json','pattern':'data/warm/archived-fred/*.json'},
 'investor-debate/_index.json':{'engine':'justhodl-watchlist-debate','rows':'tickers','key_field':'$value','key_prefix':'investor-debate/','key_suffix':'.json','pattern':'investor-debate/*.json'},
 'data/impact/etf-holdings-index.json':{'engine':'justhodl-flow-lookthrough','rows':'etfs','rows_mode':'object_keys','key_field':'$value','key_prefix':'etf-constituents-v2/','key_suffix':'.json','pattern':'etf-constituents-v2/*.json'},
 'data/snapshots-index.json':{'engine':'justhodl-whats-changed','rows':'snapshots','key_field':'key','family_prefix':'data/snapshots/','required_schema':'daily-snapshot-index.v1','require_complete':True},
 'calibration/index.json':{'engine':'justhodl-calibration-snapshotter','rows':'versions','key_field':'key','pattern':'calibration/versions/cal-*-*-*.json'},
 'calibration/history-index.json':{'engine':'justhodl-calibration-snapshotter','rows':'snapshots','key_field':'key','patterns':['calibration/history/*.json','calibration/versions/cal-*-*-*.json']},
}

@lru_cache(maxsize=1)
def access_rules():
    """Read only literal policy data, without importing credential/cloud modules."""
    path=ROOT/'aws/shared/private_artifact.py'
    if not path.exists():return {},set(),()
    mirrors={};aliases={};private=set();prefixes=()
    for node in ast.parse(path.read_text()).body:
        if not isinstance(node,ast.Assign):continue
        names={t.id for t in node.targets if isinstance(t,ast.Name)}
        if 'MIRRORED_ARTIFACTS' in names:mirrors=ast.literal_eval(node.value)
        if 'PRIVATE_ARTIFACT_ALIASES' in names:aliases=ast.literal_eval(node.value)
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
    for alias,canonical in aliases.items():
        if canonical not in mirrors:raise ValueError('Owner artifact alias has no canonical mirror: '+alias)
        mirrors[alias]=mirrors[canonical]
    return mirrors,set(mirrors)|private,tuple(prefixes)

def public_key(key):
    _,private,prefixes=access_rules()
    return (key.endswith(('.json','.json.gz')) and '*' not in key and '..' not in key.split('/') and key not in private and not key.startswith(prefixes)
            and (key in PUBLIC_EXACT or (not SENSITIVE.search(key) and key.split('/')[0] in PUBLIC_PREFIXES and not any(part.startswith('_') for part in key.split('/')))))

def add_archive_index_relationships(emap,engines,root):
    """Attach reviewed listings to their source engine without relabeling the writer."""
    publisher='justhodl-public-archive-index'
    path=root/'aws/lambdas'/publisher/'source/lambda_function.py'
    if not path.exists():return
    registry=None
    for declaration in ast.parse(path.read_text()).body:
        if isinstance(declaration,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='REGISTRY' for t in declaration.targets):registry=ast.literal_eval(declaration.value)
    if not isinstance(registry,(tuple,list)) or publisher not in engines:raise ValueError('Reviewed archive registry missing its source publisher')
    for name,pattern in registry:
        key='data/archive-indexes/'+name+'.json'
        owners={engine for engine,value in engines.items() if pattern in value['key_patterns']}
        if owners!={name} or not public_key(pattern.replace('*','reviewed-member')):raise ValueError('Archive family ownership or public boundary drift: '+name)
        index=next((output for output in emap[publisher]['outputs'] if output['key']==key),None)
        if index is None:raise ValueError('Archive index has no source-bound concrete writer: '+key)
        emap[name]['outputs'].append({**index,'source_engine':name,
            'association_basis':'source-owned family enumerated by a separate reviewed metadata publisher',
            'archive_family_evidence':engines[name]['write_evidence'].get(pattern,[]),
            'archive_index':{'engine':name,'publisher_engine':publisher,'required_schema':'public-engine-archive-index.v1','require_complete':True,
                'rows':'snapshots','key_field':'key','patterns':[pattern],'key_regex':'^'+re.escape(pattern).replace(r'\*',r'[^/]+')+'$'}})

def internal_output_roles(root,engines):
    path=root/'config/engine-output-roles.json'
    if not path.exists():return {}
    result=defaultdict(dict)
    doc=json.loads(path.read_text())
    required={(row['engine'],row['key']) for row in doc.get('must_remain_required_or_receive_public_review',[])}
    for row in doc.get('roles',[]):
        name,key=row['engine'],row['key'];engine=engines.get(name);evidence=row.get('evidence',{})
        source=evidence.get('source','');approved_roles={'internal_input_cache','internal_operational_storage','private_operational_state','internal_configuration'}
        proofs=engine.get('write_evidence',{}).get(key,[]) if engine else []
        proven_sources={'aws/lambdas/'+name+'/source/'+p['file'] for p in proofs}
        known=set(engine.get('keys',[]))|set(engine.get('key_patterns',[])) if engine else set()
        if (not engine or key not in known or source not in proven_sources or not (root/source).is_file()
                or row.get('role') not in approved_roles or not row.get('purpose') or row.get('public_access_approved') is not False
                or (name,key) in required or key in result[name]):raise ValueError('Unproven or conflicting internal output role: '+name+' '+key)
        if '*' in key:
            # A family exclusion is tied to reviewed function bodies, not its name
            # or a permissive prefix. Changed cache semantics require new review.
            functions=evidence.get('functions',[]);reviewed=[]
            nodes={node.name:node for node in ast.walk(ast.parse((root/source).read_text())) if isinstance(node,(ast.FunctionDef,ast.AsyncFunctionDef))}
            for function in functions:
                node=nodes.get(function.get('name'))
                if node is None or hashlib.sha256(ast.dump(node,include_attributes=False).encode()).hexdigest()!=function.get('ast_sha256'):
                    raise ValueError('Internal family source review drift: '+name+' '+key)
                reviewed.append(node)
            if not reviewed or any(not any(node.lineno<=proof.get('line',0)<=node.end_lineno for node in reviewed) for proof in proofs):
                raise ValueError('Internal family writer not covered by source review: '+name+' '+key)
        result[name][key]=row
    return result

def validated_runtime_outputs(records,engines,graph,root,route):
    for record in records:
        name=record.get('engine');source=record.get('inspection_source');patterns=record.get('patterns',[])
        engine=engines.get(name)
        known=set(engine.get('key_patterns',[]))|{row['key'] for row in engine.get('other_format_outputs',[])} if engine else set()
        if (not engine or not patterns or not set(patterns)<=known or source not in [route,*graph['scripts']]
                or 'JHDataInspector.inspect' not in (root/source).read_text() or not record.get('scope')):raise ValueError('Unproven selected-response contract: '+route+' '+str(name))
    return records

def validated_primary_scopes(role,engines,root,route):
    scopes=role.get('primary_output_keys',{})
    for name,keys in scopes.items():
        evidence=role.get('primary_scope_evidence',{}).get(name,{})
        source=evidence.get('source','');constants={}
        if name not in engines or not keys or not set(keys)<=set(engines[name]['keys']) or not evidence.get('purpose') or not source.startswith('aws/lambdas/'+name+'/source/') or not (root/source).is_file():raise ValueError('Unproven dedicated output scope: '+route)
        for node in ast.parse((root/source).read_text()).body:
            if isinstance(node,ast.Assign) and isinstance(node.value,ast.Constant) and isinstance(node.value.value,str):
                for target in node.targets:
                    if isinstance(target,ast.Name):constants[target.id]=node.value.value
        if set(keys)!={constants.get(name) for name in evidence.get('constants',[])}:raise ValueError('Dedicated output constants drift: '+route)
    return scopes
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
    internal_roles=internal_output_roles(root,engines);emap={}
    for name,e in engines.items():
        allowed=[{'engine':name,'key':k,'access':'owner_authenticated' if k in mirrors else 'public','private_kind':mirrors.get(k),'required_projection':{'data/brain-compiler.json':'brain-compiler','data/sizing.json':'sizing','_health/fleet.json':'fleet-health','data/_fleet-monitor.json':'fleet-errors','data/_freshness-monitor.json':'fleet-freshness','data/source-map.json':'source-map','etf-flows/daily.json':'provider-metrics','macro/regime.json':'provider-metrics'}.get(k),'inspection_schema':'json-value.v1','ownership_evidence':e['write_evidence'].get(k,[])} for k in e['keys'] if k not in internal_roles.get(name,{}) and (public_key(k) or k in mirrors)]
        for output in allowed:
            if (output['key'],name) in augmentations:output['ownership_role']=augmentations[(output['key'],name)]
            index=ARCHIVE_INDEXES.get(output['key'])
            if index and index['engine']==name:
                patterns=[pattern for pattern in e['key_patterns'] if pattern.startswith(index['family_prefix'])] if index.get('family_prefix') else index.get('patterns') or [index['pattern']]
                if not patterns or any(pattern not in e['key_patterns'] for pattern in patterns):raise ValueError('Archive index write family drift: '+name)
                exacts=[key for key in e['keys'] if key!=output['key'] and public_key(key) and key.startswith(index['include_exact_prefix'])] if index.get('include_exact_prefix') else []
                output['archive_index']={**index,'patterns':patterns,'key_regex':'^(?:'+'|'.join(re.escape(pattern).replace(r'\*',r'[^/]+') for pattern in patterns+exacts)+')$'}
        emap[name]={'outputs':allowed,'restricted_count':sum(not public_key(k) and k not in mirrors for k in e['keys']),'owner_authenticated_count':sum(k in mirrors for k in e['keys']),
                    'excluded_internal_outputs':list(internal_roles.get(name,{}).values()),
                    'historical_or_dynamic_family_count':sum(pattern not in internal_roles.get(name,{}) for pattern in e['key_patterns']), 'unresolved_count':len(e['unresolved_writes']),
                    'runtime_coverage':'unverified_until_opened','ownership_basis':'actual source write arguments'}
    add_archive_index_relationships(emap,engines,root)
    pmap={};graphs=scan_pages(root);source_usage=defaultdict(int)
    for graph in graphs.values():
        for source in graph['scripts']:source_usage[source]+=1
    for route,graph in graphs.items():
        producers=set();primary=set();unresolved=[];repository_assets=[];role=roles.get(route,{})
        scopes=validated_primary_scopes(role,engines,root,route)
        api_responses=role.get('api_responses',[])
        runtime_outputs=validated_runtime_outputs(role.get('runtime_outputs',[]),engines,graph,root,route)
        declared_engines=graph.get('primary_engines',[])+role.get('primary_engines',[])+[r['engine'] for r in api_responses+runtime_outputs]
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
            if key in REPOSITORY_ASSETS:
                asset=root/key
                if not asset.is_file():raise ValueError('Reviewed repository asset missing: '+key)
                json.loads(asset.read_text())
                repository_assets.append({'key':key,'role':'repository_configuration_or_asset','purpose':REPOSITORY_ASSETS[key],'sha256':hashlib.sha256(asset.read_bytes()).hexdigest()})
                continue
            owners=writers.get(key,set())
            bases=owners-{name for name in owners if (key,name) in augmentations}
            pipeline=len(bases)==1 and all(augmentations[(key,name)]['base_producer'] in bases for name in owners-bases)
            if len(owners)==1 or pipeline:
                producers.update(owners)
                if key in primary_keys and not role.get('primary_exclusive'):primary.update(owners)
            elif len(owners)>1:unresolved.append({'reason':'multiple source writers','key':key,'primary':key in primary_keys,'writers':sorted(owners)})
            else:unresolved.append({'reason':'no source-bound writer','key':key,'primary':key in primary_keys})
        outputs=[];seen=set()
        for name in sorted(producers,key=lambda name:(name not in primary,name)):
            for o in emap[name]['outputs']:
                if name in scopes and o['key'] not in scopes[name]:continue
                pair=(o['engine'],o['key'])
                if pair not in seen:outputs.append(o);seen.add(pair)
        static_keys=set()
        for static in role.get('static_outputs',[]):
            from gen_engine_manifest import ast_keys
            source=root/static['source'];key=static['key'];written,_,parsed=ast_keys(source.read_text())
            if not parsed or key not in written or not public_key(key):raise ValueError('Static output ownership or access invalid: '+route+' '+key)
            outputs.append({'engine':static['engine'],'key':key,'access':'public','inspection_schema':'json-value.v1','ownership_evidence':[{'file':static['source'],'basis':'bound write argument'}]});static_keys.add(key)
        unresolved=[row for row in unresolved if row['key'] not in static_keys]
        primary_accessible=sum((o.get('source_engine') or o['engine']) in primary for o in outputs)+len(api_responses)+len(runtime_outputs)+len(static_keys)
        primary_withheld=sum(sum(not public_key(k) and k not in mirrors and k not in internal_roles.get(e,{}) for k in scopes.get(e,engines[e]['keys'])) for e in primary)
        internal_inventory=[row for name in sorted(primary) for row in emap[name]['excluded_internal_outputs'] if name not in scopes or row['key'] in scopes[name]]
        primary_unresolved=sum(emap[e]['unresolved_count'] for e in primary if e not in scopes)
        primary_families=sum(emap[e]['historical_or_dynamic_family_count'] for e in primary if e not in scopes)
        indexed={(o.get('source_engine') or o['engine'],pattern) for o in outputs if (o.get('source_engine') or o['engine']) in primary and o.get('archive_index') for pattern in (o['archive_index'].get('patterns') or [o['archive_index']['pattern']])}
        unindexed_families=primary_families-sum(pattern not in internal_roles.get(name,{}) for name,pattern in indexed)
        scoped_keys={key for keys in scopes.values() for key in keys}
        unresolved_primary=[row for row in unresolved if row['primary'] and (not role.get('primary_exclusive') or row['key'] in scoped_keys or set(row.get('writers',[]))&primary)]
        role_name='NO_ENGINE_EXPECTED' if graph.get('redirect') else role.get('role','ENGINE_PAGE')
        role_reason='Redirect: '+graph['redirect'] if graph.get('redirect') else role.get('reason')
        missing_primary=not primary_accessible or unresolved_primary or graph['script_parse_errors'] or role.get('unresolved_reason')
        primary_status=('NO_PRIMARY_OUTPUT_ACCESS_CONTRACT' if not primary_accessible else
                        'PARTIAL_PRIMARY_OUTPUT_ACCESS' if primary_withheld or primary_unresolved or unindexed_families or missing_primary else 'ACCESSIBLE_BY_CONTRACT')
        coverage_class=('NOT_APPLICABLE' if role_name=='NO_ENGINE_EXPECTED' else 'PRIMARY_VALID_CONTRACT' if primary_status=='ACCESSIBLE_BY_CONTRACT' else 'PRIMARY_PARTIAL' if primary_accessible or primary else 'SUPPORT_ONLY' if producers else 'NO_ASSOCIATION')
        pmap[route]={'page_role':role_name,'role_reason':role_reason,'coverage_class':coverage_class,'repository_assets':repository_assets,'api_responses':api_responses,'runtime_outputs':runtime_outputs,'primary_output_scopes':scopes,'primary_scope_evidence':role.get('primary_scope_evidence',{}),'unresolved_primary_references':unresolved_primary,'dedicated_coverage_note':role.get('unresolved_reason'),'script_parse_errors':graph['script_parse_errors'],'outputs':outputs,'producers':sorted(producers),'primary_producers':sorted(primary),'supplemental_producers':sorted(producers-primary),'primary_output_status':primary_status,'primary_withheld_output_count':primary_withheld,'primary_unresolved_write_count':primary_unresolved,'primary_dynamic_family_count':primary_families,'primary_unindexed_family_count':unindexed_families,'excluded_internal_outputs':internal_inventory,'owner_authenticated_count':sum(emap[e]['owner_authenticated_count'] for e in producers),'restricted_count':sum(emap[e]['restricted_count'] for e in producers),
                     'unresolved_count':len(unresolved)+sum(emap[e]['unresolved_count'] for e in producers if e not in scopes),'unresolved_references':unresolved,
                     'historical_or_dynamic_family_count':sum(emap[e]['historical_or_dynamic_family_count'] for e in producers if e not in scopes),
                     'association_basis':'exact source references or explicit validated page declarations; shared-script conditional use not inferred as primary ownership',
                     'runtime_coverage':'unverified_until_opened','missing_script_count':len(graph['missing_scripts'])}
    return {'schema_version':'page-data-contract.v1','inspection_schema':'json-value.v1','source_manifest_schema':doc['schema_version'],
            'pages':pmap,'engines':emap,'coverage':{'public_routes':len(pmap),'engines':len(emap),'routes_with_source_bound_outputs':sum(bool(x['outputs']) for x in pmap.values()),
            'primary_valid_contract':sum(x['coverage_class']=='PRIMARY_VALID_CONTRACT' for x in pmap.values()),'primary_partial':sum(x['coverage_class']=='PRIMARY_PARTIAL' for x in pmap.values()),'support_only':sum(x['coverage_class']=='SUPPORT_ONLY' for x in pmap.values()),'no_association':sum(x['coverage_class']=='NO_ASSOCIATION' for x in pmap.values()),'not_applicable':sum(x['coverage_class']=='NOT_APPLICABLE' for x in pmap.values()),'routes_with_api_response_contract':sum(bool(x['api_responses']) for x in pmap.values()),
            'routes_without_source_bound_outputs':sum(not x['outputs'] for x in pmap.values()),'routes_with_primary_output_access':sum(x['primary_output_status']!='NO_PRIMARY_OUTPUT_ACCESS_CONTRACT' for x in pmap.values()),'routes_with_partial_primary_output_access':sum(x['primary_output_status']=='PARTIAL_PRIMARY_OUTPUT_ACCESS' for x in pmap.values()),'all_routes_receive_inspector':True,
            'claim':'Every field and row in an opened artifact or observed contracted API response is inspectable. Valid/partial counts are static access contracts, not runtime completeness certification; unindexed families, private paths, unknown writers and availability remain explicit.'}}
def install_html(source,apis,page_contract=None,asset_version=None):
    # Refresh generated bootstrap even when a build directory is reused.
    source=re.sub(r'<script\b[^>]*\bid=["\']jh-page-data-contract["\'][^>]*>.*?</script>','',source,flags=re.I|re.S)
    source=re.sub(r'<script\b[^>]*\bid=["\']jh-api-data-contract["\'][^>]*>.*?</script>','',source,flags=re.I|re.S)
    source=re.sub(r'<script\b[^>]*\bsrc=["\']/jh-data-inspector\.js(?:\?[^"\']*)?["\'][^>]*>\s*</script>','',source,flags=re.I|re.S)
    source=re.sub(r'(<head\b[^>]*>)\s*',r'\1',source,count=1,flags=re.I)
    encoded=json.dumps(apis,separators=(',',':')).replace('<','\\u003c')
    page_json=json.dumps(page_contract,separators=(',',':')).replace('<','\\u003c') if page_contract is not None else None
    page_tag='<script id="jh-page-data-contract" type="application/json">'+page_json+'</script>' if page_json is not None else ''
    version=asset_version or hashlib.sha256((ROOT/'jh-data-inspector.js').read_bytes()).hexdigest()[:16]
    if not re.fullmatch(r'[a-f0-9]{16}',version):raise ValueError('Invalid inspector content hash')
    tag=page_tag+'<script id="jh-api-data-contract" type="application/json">'+encoded+'</script><script src="/jh-data-inspector.js?v='+version+'" data-contract="page-data-contract.v1"></script>'
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
            page.write_text(install_html(source,apis,doc['pages'].get(route)))
    print(json.dumps(doc['coverage']))
if __name__=='__main__':main()
