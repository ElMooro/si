"""Stream one validated source timeline; emit complete intervals without retaining them.

This is a computation candidate only. Reading, retained shard integrity, complete
inventory reconciliation and publishing remain separate required boundaries.
"""
from collections import Counter
from itertools import groupby
from genealogy_public_archive import clock


def fold_source(name,snapshots,emit_interval,emit_ambiguity):
    previous=None;known_present=set();gaps=0;occurrences=Counter();first={}
    counts=Counter();last=None
    def ordered():
        nonlocal last
        for snapshot in snapshots:
            at=clock(snapshot['receipt']['source_received_at'])
            if last is not None and at<last:raise ValueError('source_receipt_order_requires_rebuild')
            last=at
            yield at,snapshot
    for at,items in groupby(ordered(),key=lambda item:item[0]):
        batch=[item[1] for item in items];valid=[s for s in batch if s['eligible']]
        bad=len(batch)-len(valid);counts['ineligible_source_snapshots']+=bad;gaps+=bad
        counts['valid_source_snapshots']+=len(valid)
        counts['identity_partial_source_snapshots']+=sum(not s['absence_eligible'] for s in valid)
        if not valid:continue
        states={tuple(sorted(s['members'])) for s in valid}
        receipt_sort=lambda r:(r['capture_key'],r['capture_source_index'])
        if len(states)!=1:
            emit_ambiguity({'source_key':name,'source_received_at':at.isoformat(),
                'capture_keys':sorted(s['receipt']['capture_key'] for s in valid),
                'reason':'conflicting_projected_membership_at_same_receipt_timestamp'})
            for identity in sorted(set().union(*(s['members'] for s in valid))):
                if identity in first:continue
                instrument,direction=identity
                receipts=sorted([s['receipt'] for s in valid if identity in s['members']],key=receipt_sort)
                first[identity]={'source_key':name,'instrument_id':instrument,'direction':direction,
                    'source_occurrence':None,'lower_exclusive_utc':None,'upper_inclusive_utc':at.isoformat(),
                    'status':'ambiguous_initial_membership','previous_absence_receipts':[],
                    'presence_receipts':receipts,'ineligible_snapshots_since_previous_observation':None,
                    'onset_qualified':False,'forecast_qualified':False}
            previous=None;known_present=set()
            continue
        members=valid[0]['members'];counts['same_timestamp_duplicate_snapshots']+=len(valid)-1
        receipts=sorted([s['receipt'] for s in valid],key=receipt_sort)
        for identity in sorted(members-known_present):
            instrument,direction=identity;occurrences[identity]+=1
            interval={'source_key':name,'instrument_id':instrument,'direction':direction,
                'source_occurrence':occurrences[identity],
                'lower_exclusive_utc':previous['at'].isoformat() if previous else None,
                'upper_inclusive_utc':at.isoformat(),
                'status':'observed_absence_to_presence' if previous else 'no_prior_resolved_source_observation',
                'previous_absence_receipts':previous['receipts'] if previous else [],'presence_receipts':receipts,
                'ineligible_snapshots_since_previous_observation':gaps,
                'onset_qualified':False,'forecast_qualified':False}
            emit_interval(interval);first.setdefault(identity,interval)
            counts['total_membership_intervals']+=1
        complete=[s for s in valid if s['absence_eligible']]
        if complete:
            previous={'at':at,'receipts':sorted([s['receipt'] for s in complete],key=receipt_sort)}
            known_present=set(members);gaps=0
        else:known_present.update(members)
    return {'first_observations':[first[k] for k in sorted(first)],'counts':dict(counts)}
