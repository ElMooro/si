#!/usr/bin/env python3
"""Enumerate remaining static page access gaps; does not assert deployed completeness."""
import json
from collections import Counter
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]

def report(root=ROOT):
    contracts=json.loads((root/'config/page-data-contracts.json').read_text())
    engines={e['engine']:e for e in json.loads((root/'engine-manifest.json').read_text())['engines']}
    rows=[];combinations=Counter()
    for route,page in contracts['pages'].items():
        reasons=[];tags=[]
        for key,tag in [('primary_withheld_output_count','withheld'),('primary_unresolved_write_count','unresolved writes'),('primary_unindexed_family_count','unindexed families')]:
            if page.get(key):reasons.append(tag+': '+str(page[key]));tags.append(tag)
        if page.get('unresolved_primary_references'):reasons.append('unresolved primary references: '+str(len(page['unresolved_primary_references'])));tags.append('unresolved references')
        if page.get('dedicated_coverage_note'):reasons.append(page['dedicated_coverage_note']);tags.append('explicit unresolved')
        if page['coverage_class']=='PRIMARY_PARTIAL':combinations[' + '.join(tags) or 'no accessible primary payload']+=1
        if page['coverage_class'] in ['PRIMARY_VALID_CONTRACT','NOT_APPLICABLE']:continue
        if not reasons:reasons=['No source-proven primary engine/API association']
        gaps={}
        for name in page['primary_producers']:
            engine=engines[name];visible={o['key'] for o in contracts['engines'][name]['outputs']}
            internal=contracts['engines'][name].get('excluded_internal_outputs',[]);internal_keys={row['key'] for row in internal}
            scope=page.get('primary_output_scopes',{}).get(name)
            indexed={pattern for o in contracts['engines'][name]['outputs'] if o.get('archive_index') for pattern in o['archive_index'].get('patterns',[])}
            gaps[name]={'withheld_result_keys':[k for k in (scope or engine['keys']) if k not in visible and k not in internal_keys],
                        'excluded_internal_outputs':internal,'unresolved_writes':[] if scope else engine['unresolved_writes'],
                        'unindexed_dynamic_families':[] if scope else [pattern for pattern in engine['key_patterns'] if pattern not in indexed],
                        'other_format_outputs':[] if scope else engine.get('other_format_outputs',[])}
        rows.append({'route':route,'classification':page['coverage_class'],'primary_engines':page['primary_producers'],'reasons':reasons,'unresolved_primary_references':page.get('unresolved_primary_references',[]),'engine_gaps':gaps})
    inventory=[row for engine in contracts['engines'].values() for row in engine.get('excluded_internal_outputs',[])]
    result={'schema_version':'page-coverage-review.v1','generated_from':'Current source contracts; static access classification, not deployed certification','coverage':contracts['coverage'],'source_reviewed_internal_storage':inventory,'partial_reason_combinations':dict(combinations),'remaining_routes':rows}
    dest=root/'docs/audit/2026-09-09';dest.mkdir(parents=True,exist_ok=True)
    (dest/'page-coverage-remaining.json').write_text(json.dumps(result,indent=2)+'\n')
    lines=['# Page coverage follow-up — 9 September 2026','',contracts['coverage']['claim'],'','| Classification | Routes |','|---|---:|']
    for key in ['primary_valid_contract','primary_partial','support_only','no_association','not_applicable']:lines.append('| '+key+' | '+str(contracts['coverage'][key])+' |')
    lines+=['',str(contracts['coverage']['routes_with_api_response_contract'])+' routes have exact reviewed API response contracts, installed before the first application request. Each route embeds its own small access contract; the full registry is fetched only by the generic engine browser. Public JSON/gzip, authenticated owner mirrors and reviewed archive indexes remain separate access mechanisms.',
            '',str(len(inventory))+' exact source-reviewed operational storage keys remain withheld and visible in the inspection inventory. They are excluded from published analytical-result completeness; meaningful health, model grades and historical observations remain required. Dedicated AI-router context scopes are checked against literal writer constants, preserving separate report and history contracts for each context.','','## Partial categories','','| Reason combination | Routes |','|---|---:|']
    lines+=['| '+key+' | '+str(value)+' |' for key,value in combinations.most_common()]
    lines+=['','## Remaining routes','','The JSON companion enumerates source write locations, exact withheld paths, format-only exports and families for every route. A source-analysis gap is not proof of runtime absence. Protected raw storage and model caches are distinguished from published user-facing records in source review; privacy is not weakened to change these counts.','','| Route | Classification | Remaining reason |','|---|---|---|']
    for row in rows:lines.append('| '+row['route']+' | '+row['classification']+' | '+'; '.join(row['reasons']).replace('|','/')+' |')
    (dest/'page-coverage-remaining.md').write_text('\n'.join(lines)+'\n')
    print(json.dumps({'coverage':contracts['coverage'],'partial_reason_combinations':dict(combinations)}))

if __name__=='__main__':report()
