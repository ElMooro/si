"""Versioned Credit collection clock; observation age remains a separate limit.

The two verified existing UTC weekday bindings are collection opportunities,
not provider releases or successful executions. Legacy publications keep their
original 36-hour ceiling. New-policy outputs require their own retained replay.
"""
from datetime import datetime,timedelta,timezone
import json,re

POLICY='credit-collection-cadence.v1'
VERSION='2.1.0'
GRACE_SECONDS=300
SLOTS=((20,0),(22,10))


def stamp(value):
    if not isinstance(value,str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})',value):
        raise ValueError('Explicit collection timestamp required')
    result=datetime.fromisoformat(value.replace('Z','+00:00'))
    if result.tzinfo is None:raise ValueError('Collection timezone required')
    return result.astimezone(timezone.utc)


def policy(generated_at):
    generated=stamp(generated_at)
    for offset in range(8):
        day=generated+timedelta(days=offset)
        if day.weekday()>4:continue
        for hour,minute in SLOTS:
            candidate=day.replace(hour=hour,minute=minute,second=0,microsecond=0)
            if candidate<=generated:continue
            return {'contract':POLICY,'timezone':'UTC','weekdays':[0,1,2,3,4],
                'collection_times':['20:00','22:10'],'next_collection_at':candidate.isoformat(),
                'completion_allowance_seconds':GRACE_SECONDS,
                'pipeline_check_due_at':(candidate+timedelta(seconds=GRACE_SECONDS)).isoformat(),
                'basis':'Next existing weekday collection opportunity plus the observed 300-second native timeout. Not a provider release or holiday calendar.',
                'provider_release_calendar_verified':False,'successful_collection_asserted':False}
    raise ValueError('No bounded next collection opportunity')


def collection_current(packet,evaluated_at):
    """Validate policy-derived bounds; legacy packets keep their old 36h cap.

    Unknown or incomplete new-policy claims are rejected, never treated as
    permission to borrow a longer clock. This does not validate measurements.
    """
    generated=stamp(packet['generated_at']);at=stamp(evaluated_at)
    freshness=packet['freshness'];due=stamp(freshness['pipeline_check_due_at'])
    declared=freshness.get('collection_policy')
    if 'collection_policy' in freshness:
        expected=policy(packet['generated_at'])
        same=json.dumps(declared,sort_keys=True,allow_nan=False)==json.dumps(expected,sort_keys=True,allow_nan=False)
        if packet.get('version')!=VERSION or not same or due!=stamp(expected['pipeline_check_due_at']):
            raise ValueError('Reviewed collection policy or derived deadline differs')
    else:
        if packet.get('version')!= '2.0.0':raise ValueError('Unknown legacy collection policy')
        due=min(due,generated+timedelta(hours=36))
    if due<=generated:raise ValueError('Collection deadline must follow generation')
    return generated<=at<due


def candidate(packet):
    """New-policy proposal only; drop the old replay identity rather than forge it.

    All measurements and their clocks remain unchanged. The operation invoking
    this helper must first reproduce the complete predecessor from originals.
    A newly compiled native run would need its own retained inputs/output/run.
    """
    import copy
    if packet.get('version')!='2.0.0' or packet.get('contract')!='credit-native-research.v1':
        raise ValueError('Reviewed native predecessor required')
    result=copy.deepcopy(packet);result.pop('replay',None)
    derived=policy(packet['generated_at']);deadline=stamp(derived['pipeline_check_due_at'])
    source=[stamp(row['source_valid_until']) for row in packet['measurements'].values() if row.get('source_valid_until')]
    result['version']=VERSION
    result['freshness']={**packet['freshness'],'collection_policy':derived,
        'pipeline_check_due_at':deadline.isoformat(),'valid_until':min([deadline,*source]).isoformat()}
    return result
