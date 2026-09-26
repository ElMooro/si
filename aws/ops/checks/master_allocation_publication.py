"""Validate complete research projection and its matching non-actionable SSM view."""
from datetime import datetime
import json
import master_allocation_authority as policy
from public_brain_projection import sanitize_public


def encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()


def clock(value):
    if not isinstance(value,str):raise ValueError('Dated native publication required')
    result=datetime.fromisoformat(value.replace('Z','+00:00'))
    if result.tzinfo is None:raise ValueError('Timezone required')
    return result


def validate(packet,machine,not_before,now):
    if not isinstance(packet,dict) or packet.get('contract')!=policy.CONTRACT:
        raise ValueError('Native research publication required')
    stamp=packet.get('generated_at')
    if not clock(not_before)<=clock(stamp)<=clock(now):raise ValueError('New normal publication clock required')
    original=packet.get('unqualified_projection')
    if not isinstance(original,dict) or original.get('as_of')!=stamp:
        raise ValueError('Whole matching research calculation required')
    expected=sanitize_public(policy.CURRENT,policy.project(original))
    if encoded(packet)!=encoded(expected):raise ValueError('Complete typed public projection differs')
    if encoded(machine)!=encoded(policy.execution_packet(packet)):
        raise ValueError('Machine-readable target or publication binding differs')
    weights=original.get('target_allocation');rows=(original.get('best_asset') or {}).get('ranked')
    if not isinstance(weights,dict) or not isinstance(rows,list):raise ValueError('Complete native research inventory required')
    return {'contract':policy.CONTRACT,'generated_at':stamp,'projection_replayed':True,
        'machine_target_is_null':True,'machine_publication_matches':True,
        'retained_weight_count':len(weights),'retained_momentum_rows':len(rows),
        'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False,
        'original_market_sources_replayed':False,'forecast_qualified':False,'portfolio_mandate_verified':False}
