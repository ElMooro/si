"""Complete public observation chronology, without an inferred trading signal."""
from collections import Counter
import hashlib
import genealogy_public_archive as archive
import genealogy_registration_model as registration
import genealogy_capture_timing as timing

CONTRACT = 'signal-genealogy-research.v2'


def compile_frozen(inputs):
    archive.require(type(inputs) is dict and set(inputs)=={'contract','inventories','audit','contexts'},'genealogy_input_shape')
    archive.require(inputs['contract']=='genealogy-research-inputs.v1','genealogy_input_contract')
    inventories=archive.validate_inventory(inputs['inventories']);audit=inputs['audit'];contexts=inputs['contexts']
    cutoff=next(iter(inventories.values()))['cutoff']
    archive.require(audit['cutoff']==cutoff,'audit_cutoff_differs')
    archive.require(audit['inventory_hashes']=={p:i['inventory_sha256'] for p,i in inventories.items()},'audit_inventory_differs')
    archive.require(type(contexts) is list,'capture_contexts_required')
    indexed={c['capture_key']:c for c in contexts}
    listed={r['key']:r for r in inventories[archive.PREFIX+'captures/']['objects']}
    captures={r['key']:r for r in audit['captures']}
    evidence={r['key']:r for r in audit['evidence']}
    archive.require(len(indexed)==len(contexts)==len(captures)==len(listed),'capture_population_differs')
    archive.require(set(indexed)==set(captures)==set(listed),'capture_population_differs')
    archive.require(len(evidence)==len(audit['evidence']),'duplicate_read_evidence')
    for key,context in indexed.items():
        summary=captures[key];proof=evidence[key];meta=listed[key]
        archive.require(context['capture_sha256']==summary['sha256']==proof['sha256'],'capture_projection_hash_differs')
        archive.require(proof['bytes']==meta['bytes'] and archive.clock(proof['last_modified'])==archive.clock(meta['last_modified']),'capture_storage_evidence_differs')
        archive.require(context['generated_at']==summary['generated_at'] and context['started_at']==summary['started_at'],'capture_projection_clock_differs')
        archive.require(context['candidate_scan_complete'] is summary['coverage']['candidate_scan_complete'],'capture_projection_coverage_differs')
        archive.require(len(context['sources'])==summary['sources_with_observations'],'capture_source_population_differs')
    registered=registration.compile_archive(audit)
    selected=timing.compile_contexts(contexts,cutoff)
    # Registration and membership computations must describe the same complete
    # instrument/direction/source population, including every first observation.
    identity=lambda row:(row['instrument_id'],row['direction'],row['source_key'])
    archive.require({identity(r) for r in registered['first_registrations']}=={identity(r) for r in selected['first_observations']},'registration_membership_population_differs')
    archive.require(registered['possible_comparisons']==selected['possible_comparisons'],'comparison_population_differs')
    return {'contract':CONTRACT,'engine':'signal-genealogy','version':'2.0.0','generated_at':cutoff,
        'input_sha256':hashlib.sha256(archive.canonical(inputs)).hexdigest(),
        'archive':audit,'registration':registered,'membership':selected,
        'authority':{'forecast_qualified':False,'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False},
        'independent_evidence_count':None,'original_engine_replay_verified':False,
        'definition':'Complete retained public journal at the declared storage cutoff. Registration order and selected-membership intervals are observations of collection, not model ancestry, predictive leadership, independent evidence or investable performance.'}


def collect(client,inventories,workers=8):
    contexts=[]
    audit=archive.audit(client,inventories,workers=workers,
                        capture_observer=lambda doc,receipt:contexts.append(timing.capture_context(doc,receipt)))
    contexts.sort(key=lambda c:c['capture_key'])
    inputs={'contract':'genealogy-research-inputs.v1','inventories':inventories,'audit':audit,'contexts':contexts}
    return inputs,compile_frozen(inputs)


def summary(output):
    archive.require(output['contract']==CONTRACT,'genealogy_output_contract')
    a=output['archive'];r=output['registration'];m=output['membership']
    return {'contract':'genealogy-research-head.v1','engine':output['engine'],'version':output['version'],
        'generated_at':output['generated_at'],'input_sha256':output['input_sha256'],
        'coverage':{'captures':a['validated_captures'],'complete_capture_scans':a['capture_scans_complete'],
                    'partial_capture_scans':a['validated_captures']-a['capture_scans_complete'],
                    'registered_records':r['archive_records'],'retained_records':r['retained_records'],'excluded_records':r['excluded_records'],
                    'ineligible_source_snapshots':m['ineligible_source_snapshots'],'identity_partial_source_snapshots':m['identity_partial_source_snapshots'],
                    'first_observed_groups':m['first_observed_groups'],'possible_comparisons':m['possible_comparisons']},
        'exclusion_counts':dict(sorted(Counter(reason for row in r['exclusions'] for reason in row['reasons']).items())),
        'comparison_status_counts':m['comparison_status_counts'],'comparisons':m['comparisons'],
        'authority':output['authority'],'independent_evidence_count':None,'original_engine_replay_verified':False,
        'definition':output['definition'],'interval_definition':m['definition']}
