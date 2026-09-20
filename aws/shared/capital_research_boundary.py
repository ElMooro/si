"""A calculation boundary for the retired CapitalFlow stock-flow score.

Context is retained, not promoted by an eligibility flag or a replacement name.
This boundary does not qualify other direct or indirect signal families.
"""
import hashlib
import json

SOURCE = 'data/capital-flow.json'
BASIS = 'capital-flow-stock-score-retired.v1'


def context(packet):
    packet = packet if isinstance(packet, dict) else {}
    return {'basis': BASIS, 'source_key': SOURCE,
        'status': 'descriptive_research_only' if packet.get('contract') == 'capital-evidence-research.v1'
                  else 'unqualified_legacy_calculation' if packet else 'source_unavailable',
        'source_contract': packet.get('contract'), 'source_generated_at': packet.get('generated_at'),
        'input_json_sha256': hashlib.sha256(json.dumps(packet, sort_keys=True, separators=(',', ':'),
                                ensure_ascii=False, allow_nan=False).encode()).hexdigest(),
        'research_replay': packet.get('replay') if packet.get('contract') == 'capital-evidence-research.v1' else None,
        'excluded_from': ['stock_score', 'distress_flag', 'agreement_count', 'weight_denominator', 'universe', 'trade_narrative'],
        'reason': 'Changes in reported holdings values and unqualified ownership percentages are not stock cash flows. The replacement research keeps holdings bridges, issuer share changes and monthly Treasury transactions distinct.',
        'scope': 'This direct CapitalFlow input only. Other indirect paths and predictive performance remain unqualified.',
        'missing_is_not_zero': True, 'call': None, 'calls_eligible': False, 'sizing_eligible': False,
        'execution_eligible': False, 'additional_independent_votes': 0}


def current_basis(packet):
    value = packet.get('capital_flow_exclusion') if isinstance(packet, dict) else None
    return isinstance(value, dict) and value.get('basis') == BASIS
