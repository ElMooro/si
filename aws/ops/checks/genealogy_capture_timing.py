"""Observed selected-membership intervals, not inferred economic signal onset.

Every source snapshot is a collector projection (possibly a top-list selection).
An absent member means absent from that projection, never absent from the whole
source or market. Missing/ineligible snapshots are gaps, not observed absences.
"""
from collections import Counter, defaultdict
from itertools import combinations
import hashlib,json

from genealogy_public_archive import validate_capture,clock,canonical,public_key,hex64
import re
from research_identity import research_output_source,record_identity_issue

CONTRACT='genealogy-selected-membership-intervals.v1'


def capture_context(document,evidence):
    summary,_=validate_capture(document,evidence)
    sources=[]
    for index,source in enumerate(document['sources']):
        issue=('derived_research_summary_is_not_an_original_forecast' if research_output_source(source['source_key'])
               else record_identity_issue({'source':{'source_key':source['source_key']}}))
        selected=sorted({(r['instrument']['instrument_id'],r['direction']) for r in source['observations'] if r['origin']=='explicit_direction'})
        sources.append({'source_key':source['source_key'],'source_bytes_sha256':source['source_bytes_sha256'],
            'source_generated_at':source['source_generated_at'],'source_received_at':source['source_received_at'],
            'eligible':not source['eligibility_reasons'] and issue is None,
            'unsupported_identity_count':source['unsupported_identity_count'],
            'exclusion_reasons':([issue] if issue else [])+source['eligibility_reasons'],
            'explicit_members':[{'instrument_id':i,'direction':d} for i,d in selected],
            'capture_source_index':index})
    return {'capture_key':evidence['key'],'capture_sha256':evidence['sha256'],
            'generated_at':document['generated_at'],'started_at':document['started_at'],
            'candidate_scan_complete':summary['coverage']['candidate_scan_complete'],'sources':sources}


def compile_contexts(contexts,cutoff):
    if type(contexts) is not list:raise ValueError('complete capture contexts required')
    end=clock(cutoff);seen=set();by_source=defaultdict(list);excluded=Counter();scope_counts=Counter()
    for context in contexts:
        key=context['capture_key']
        if not re.fullmatch(r'data/research-forecasts/captures/[a-f0-9]{64}\.json',str(key)) or not hex64(context['capture_sha256']):raise ValueError('content-addressed capture required')
        if key.rsplit('/',1)[1]!=context['capture_sha256']+'.json':raise ValueError('capture identity differs')
        if key in seen:raise ValueError('duplicate capture context')
        seen.add(key)
        if clock(context['generated_at'])>end:raise ValueError('capture after cutoff')
        if type(context['candidate_scan_complete']) is not bool:raise ValueError('typed capture coverage required')
        scope_counts['complete_capture_scans' if context['candidate_scan_complete'] else 'partial_capture_scans']+=1
        sources_seen=set()
        for source in context['sources']:
            name=source['source_key']
            if not public_key(name) or not hex64(source['source_bytes_sha256']):raise ValueError('public source and whole-byte hash required')
            if name in sources_seen:raise ValueError('duplicate source in capture')
            sources_seen.add(name)
            at=clock(source['source_received_at'])
            if not clock(context['started_at'])<=at<=clock(context['generated_at']):raise ValueError('source receipt outside capture')
            if type(source['eligible']) is not bool or type(source['exclusion_reasons']) is not list:raise ValueError('typed source eligibility required')
            if source['eligible'] is bool(source['exclusion_reasons']):raise ValueError('eligibility and reasons differ')
            forced_issue=('derived_research_summary_is_not_an_original_forecast' if research_output_source(name)
                          else record_identity_issue({'source':{'source_key':name}}))
            if forced_issue and (source['eligible'] or forced_issue not in source['exclusion_reasons']):raise ValueError('excluded research source admitted')
            if source['eligible'] and not 0<=(at-clock(source['source_generated_at'])).total_seconds()<=86400:raise ValueError('eligible source publication clock differs')
            unsupported=source['unsupported_identity_count']
            if type(unsupported) is not int or unsupported<0:raise ValueError('typed unsupported identity count required')
            if type(source['capture_source_index']) is not int or source['capture_source_index']<0:raise ValueError('zero-based source row required')
            members=[]
            for member in source['explicit_members']:
                if type(member) is not dict or set(member)!={'instrument_id','direction'} or member['direction'] not in ('UP','DOWN'):
                    raise ValueError('explicit identified member required')
                if type(member['instrument_id']) is not str or not re.fullmatch(r'equity:US:[A-Z][A-Z.\-]{0,6}',member['instrument_id']):raise ValueError('operational equity identity required')
                members.append((member['instrument_id'],member['direction']))
            if len(set(members))!=len(members):raise ValueError('duplicate projected member')
            by_source[name].append({'at':at,'members':set(members),'eligible':source['eligible'],
                'absence_eligible':unsupported==0,
                'receipt':{'capture_key':key,'capture_sha256':context['capture_sha256'],
                    'capture_source_index':source['capture_source_index'],'source_bytes_sha256':source['source_bytes_sha256'],
                    'source_generated_at':source['source_generated_at'],'source_received_at':source['source_received_at']}})
            if not source['eligible']:
                excluded.update(source['exclusion_reasons'])
    intervals=[];ambiguities=[];first_positive={};ambiguous_times=set()
    valid_count=0;gap_count=0;duplicate_snapshots=0;partial_identity_snapshots=0
    for name,snapshots in sorted(by_source.items()):
        times=defaultdict(list)
        for snapshot in snapshots:times[snapshot['at']].append(snapshot)
        previous=None;known_present=set();gaps=0;source_intervals=Counter()
        for at,batch in sorted(times.items()):
            valid=[s for s in batch if s['eligible']]
            bad=len(batch)-len(valid);gap_count+=bad;gaps+=bad;valid_count+=len(valid)
            partial_identity_snapshots+=sum(not s['absence_eligible'] for s in valid)
            if not valid:continue
            for instrument,direction in sorted(set().union(*(s['members'] for s in valid))):
                first_positive.setdefault((instrument,direction,name),{'at':at,
                    'receipts':sorted([s['receipt'] for s in valid if (instrument,direction) in s['members']],
                                      key=lambda r:(r['capture_key'],r['capture_source_index']))})
            states={tuple(sorted(s['members'])) for s in valid}
            if len(states)!=1:
                ambiguous_times.add((name,at))
                ambiguities.append({'source_key':name,'source_received_at':at.isoformat(),
                                    'capture_keys':sorted(s['receipt']['capture_key'] for s in valid),
                                    'reason':'conflicting_projected_membership_at_same_receipt_timestamp'})
                # There is no defensible final state within this timestamp.
                previous=None;known_present=set()
                continue
            members=valid[0]['members'];duplicate_snapshots+=len(valid)-1
            receipts=sorted([s['receipt'] for s in valid],key=lambda r:(r['capture_key'],r['capture_source_index']))
            added=members-known_present
            for instrument,direction in sorted(added):
                identity=(instrument,direction);source_intervals[identity]+=1
                intervals.append({'source_key':name,'instrument_id':instrument,'direction':direction,
                    'source_occurrence':source_intervals[identity],
                    'lower_exclusive_utc':previous['at'].isoformat() if previous else None,
                    'upper_inclusive_utc':at.isoformat(),
                    'status':'observed_absence_to_presence' if previous else 'no_prior_resolved_source_observation',
                    'previous_absence_receipts':previous['receipts'] if previous else [],'presence_receipts':receipts,
                    'ineligible_snapshots_since_previous_observation':gaps,
                    'onset_qualified':False,'forecast_qualified':False})
            complete=[s for s in valid if s['absence_eligible']]
            if complete:
                previous={'at':at,'members':members,'receipts':sorted([s['receipt'] for s in complete],key=lambda r:(r['capture_key'],r['capture_source_index']))}
                known_present=set(members);gaps=0
            else:
                # Partial identity resolution proves positive members only.
                # It cannot erase a previously observed member or supply a
                # lower bound claiming every other instrument was absent.
                known_present.update(members)
    intervals.sort(key=lambda r:(r['instrument_id'],r['direction'],r['source_key'],clock(r['upper_inclusive_utc'])))
    first={}
    for interval in intervals:first.setdefault((interval['instrument_id'],interval['direction'],interval['source_key']),interval)
    # An ambiguous initial observation remains in the entire comparison
    # population. Never drop it or replace it with a later cleaner episode.
    for (instrument,direction,name),positive in first_positive.items():
        identity=(instrument,direction,name)
        if identity not in first or (name,positive['at']) in ambiguous_times:
            first[identity]={'source_key':name,'instrument_id':instrument,'direction':direction,
                'source_occurrence':None,'lower_exclusive_utc':None,'upper_inclusive_utc':positive['at'].isoformat(),
                'status':'ambiguous_initial_membership','previous_absence_receipts':[],
                'presence_receipts':positive['receipts'],'ineligible_snapshots_since_previous_observation':None,
                'onset_qualified':False,'forecast_qualified':False}
    grouped=defaultdict(list)
    for (instrument,direction,_),interval in first.items():grouped[(instrument,direction)].append(interval)
    comparisons=[]
    for (instrument,direction),values in sorted(grouped.items()):
        values.sort(key=lambda r:r['source_key'])
        for a,b in combinations(values,2):
            al,bl=a['lower_exclusive_utc'],b['lower_exclusive_utc'];au,bu=a['upper_inclusive_utc'],b['upper_inclusive_utc']
            if a['status']=='ambiguous_initial_membership' or b['status']=='ambiguous_initial_membership':order='unresolved_initial_membership_ambiguity'
            elif bl is not None and clock(au)<=clock(bl):order='a_observed_presence_before_b_bounded_entry'
            elif al is not None and clock(bu)<=clock(al):order='b_observed_presence_before_a_bounded_entry'
            elif al is None or bl is None:order='unresolved_prior_observation_missing'
            else:order='overlapping_observation_intervals'
            common=sorted({r['capture_key'] for r in a['presence_receipts']} & {r['capture_key'] for r in b['presence_receipts']})
            comparisons.append({'instrument_id':instrument,'direction':direction,'source_a':a['source_key'],'source_b':b['source_key'],
                'a_lower_exclusive_utc':al,'a_upper_inclusive_utc':au,'b_lower_exclusive_utc':bl,'b_upper_inclusive_utc':bu,
                'interval_order':order,'shared_presence_capture_keys':common,'causal_order_qualified':False,
                'independent_evidence_count':None})
    expected=sum(len(v)*(len(v)-1)//2 for v in grouped.values())
    if len(comparisons)!=expected:raise ValueError('incomplete interval comparisons')
    return {'contract':CONTRACT,'cutoff':cutoff,'input_contexts_sha256':hashlib.sha256(canonical(contexts)).hexdigest(),
        'capture_count':len(contexts),**dict(scope_counts),'source_count':len(by_source),'valid_source_snapshots':valid_count,
        'ineligible_source_snapshots':gap_count,'excluded_reason_counts':dict(sorted(excluded.items())),
        'identity_partial_source_snapshots':partial_identity_snapshots,
        'same_timestamp_duplicate_snapshots':duplicate_snapshots,'same_timestamp_ambiguities':ambiguities,
        'intervals':intervals,'first_observations':[first[k] for k in sorted(first)],
        'first_observed_groups':len(first),'comparisons':comparisons,'possible_comparisons':expected,
        'comparison_status_counts':dict(sorted(Counter(r['interval_order'] for r in comparisons).items())),
        'source_replay_verified':False,'forecast_qualified':False,'calls_eligible':False,'sizing_eligible':False,
        'definition':'Intervals bound changes in explicit-direction membership within the collector selected projection. They do not establish the source first-ever signal, economic onset, causal ancestry, price predictiveness or independent evidence. Missing/ineligible sources are never empty observed selections; partially resolved identities cannot establish absence. First observations with no prior resolved source state remain left-censored. Observed presence can precede another source bounded entry when its upper bound is at or before that entry exclusive lower bound. All pairs use the first observed interval, never a cherry-picked later resolved interval.'}
