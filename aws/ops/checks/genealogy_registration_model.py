"""Pure complete-population registration chronology; not native or forecast code."""
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta
from itertools import combinations
import hashlib, json, re

CONTRACT = 'genealogy-registration-chronology.v1'


def encoded(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()


def require(condition,reason):
    if not condition:raise ValueError(reason)


def clock(value):
    require(type(value) is str,'typed clock required')
    parsed=datetime.fromisoformat(value.replace('Z','+00:00'))
    require(parsed.tzinfo is not None,'aware clock required')
    return parsed.astimezone(timezone.utc)


def compile_archive(audit):
    require(type(audit) is dict and audit.get('contract')=='genealogy-public-archive-audit.v1','complete archive audit required')
    require(audit.get('record_body_and_storage_checks_complete') is True,'unreconciled archive cannot produce chronology')
    require(all(type(audit.get(k)) is list and not audit[k] for k in ('failures','reference_errors','reference_conflicts')),'archive failure cannot produce chronology')
    require(all(audit.get(k) is False for k in ('forecast_qualified','calls_eligible','sizing_eligible')),'archive does not grant investment authority')
    rows=audit.get('records');require(type(rows) is list,'complete record array required')
    require(type(audit.get('validated_records')) is int and audit['validated_records']==len(rows),'complete population count differs')
    cutoff=clock(audit['cutoff']);seen=set();all_rows=[];kept=[];excluded=[]
    orphan_rows=audit.get('unreferenced_record_ids')
    require(type(orphan_rows) is list and all(type(x) is str for x in orphan_rows),'explicit orphan population required')
    orphan=set(orphan_rows);require(len(orphan)==len(orphan_rows),'duplicate orphan identity')
    for row in rows:
        require(type(row) is dict,'record object required')
        fid=row.get('forecast_id');require(type(fid) is str and re.fullmatch('[0-9a-f]{64}',fid) and fid not in seen,'unique registration identity required');seen.add(fid)
        require(row.get('key')=='data/research-forecasts/records/'+fid+'.json','record reference path differs')
        require(all(type(row.get(k)) is str and re.fullmatch('[0-9a-f]{64}',row[k]) for k in ('sha256','source_bytes_sha256')),'whole record and source hashes required')
        at=clock(row['registered_at']);require(at<=cutoff,'future registration')
        require(type(row.get('source_key')) is str and re.fullmatch(r'data/[A-Za-z0-9_.-]+\.json',row['source_key']),'source path required')
        require(row.get('direction') in ('UP','DOWN'),'explicit direction required')
        require(set(('source_issue','identity_issue'))<=set(row),'explicit source and identity classifications required')
        reasons=[row[k] for k in ('source_issue','identity_issue') if row[k] is not None]
        require(all(type(reason) is str and reason for reason in reasons),'typed exclusion reasons required')
        if fid in orphan:reasons.append('no_completed_capture_reference')
        identity=row['instrument']
        require(type(identity) is dict and identity.get('asset_class')=='equity' and identity.get('currency')=='USD'
                and re.fullmatch(r'equity:US:[A-Z][A-Z.\-]{0,6}',str(identity.get('instrument_id'))),'typed USD equity identity required')
        projected={'forecast_id':fid,'key':row['key'],'sha256':row['sha256'],'instrument_id':identity['instrument_id'],
                   'source_key':row['source_key'],'direction':row['direction'],'registered_at':at.isoformat(),
                   'source_generated_at':row['source_generated_at'],'source_bytes_sha256':row['source_bytes_sha256']}
        all_rows.append(projected)
        if reasons:excluded.append({**projected,'reasons':reasons})
        else:kept.append(projected)
    require(orphan<=seen,'orphan population differs')
    actual=Counter(row['source_key'] for row in all_rows)
    require(type(audit.get('registration_counts_by_source')) is dict and all(type(k) is str and type(v) is int and v>=0 for k,v in audit['registration_counts_by_source'].items()),'typed source totals required')
    require(dict(actual)==audit['registration_counts_by_source'],'source totals differ')
    key=lambda row:(clock(row['registered_at']),row['forecast_id'])
    kept.sort(key=key);excluded.sort(key=key)
    groups=defaultdict(list)
    for row in kept:groups[(row['instrument_id'],row['direction'],row['source_key'])].append(row)
    first=[]
    for (instrument,direction,source),values in sorted(groups.items()):
        at=values[0]['registered_at'];tied=[r['forecast_id'] for r in values if r['registered_at']==at]
        first.append({'instrument_id':instrument,'direction':direction,'source_key':source,'registered_at':at,
                      'first_registration_ids':tied,'all_registration_ids':[r['forecast_id'] for r in values],
                      'registration_count':len(values)})
    shared=defaultdict(list)
    for row in first:shared[(row['instrument_id'],row['direction'])].append(row)
    comparisons=[];pair_counts=defaultdict(Counter);expected_pairs=0
    for (instrument,direction),values in sorted(shared.items()):
        values.sort(key=lambda row:row['source_key']);expected_pairs+=len(values)*(len(values)-1)//2
        for a,b in combinations(values,2):
            ta,tb=clock(a['registered_at']),clock(b['registered_at']);delta=tb-ta
            micros=(delta.days*86400+delta.seconds)*1000000+delta.microseconds
            order='same_timestamp' if micros==0 else 'a_registered_earlier' if micros>0 else 'b_registered_earlier'
            comparisons.append({'instrument_id':instrument,'direction':direction,'source_a':a['source_key'],'source_b':b['source_key'],
                                'a_first_registration_ids':a['first_registration_ids'],'b_first_registration_ids':b['first_registration_ids'],
                                'a_registered_at':a['registered_at'],'b_registered_at':b['registered_at'],
                                'b_minus_a_elapsed_microseconds':micros,'b_minus_a_utc_calendar_days':(tb.date()-ta.date()).days,
                                'registration_order':order})
            pair_counts[(a['source_key'],b['source_key'])][order]+=1
    require(len(comparisons)==expected_pairs,'comparison population incomplete')
    summaries=[]
    for (a,b),counts in sorted(pair_counts.items()):
        summaries.append({'source_a':a,'source_b':b,'shared_instrument_direction_groups':sum(counts.values()),
                          **{name:counts[name] for name in ('a_registered_earlier','b_registered_earlier','same_timestamp')},
                          'independent_evidence_count':None})
    return {'contract':CONTRACT,'cutoff':audit['cutoff'],'input_audit_sha256':hashlib.sha256(encoded(audit)).hexdigest(),
            'archive_records':len(all_rows),'retained_records':len(kept),'excluded_records':len(excluded),
            'records':kept,'exclusions':excluded,'first_registrations':first,'same_instrument_comparisons':comparisons,
            'pair_summaries':summaries,'possible_comparisons':expected_pairs,'compared':len(comparisons),'uncompared':0,
            'source_replay_verified':False,'forecast_qualified':False,'calls_eligible':False,'sizing_eligible':False,
            'definition':'First retained registration for the same instrument, explicit direction and source. Repeated later registrations remain in the full history. Registration timestamp order is collection chronology, not economic onset, causal ancestry, independence or predictive performance.',
            'population_scope':'Complete reconciled archive at cutoff. Capture scans can be incomplete; symbol identities are operational USD equity identifiers, not a historical security master. No omitted group is inferred to be inactive.'}
