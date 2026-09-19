"""13F disclosures are research context, not validated trading-flow evidence.

This boundary does not authenticate an arbitrary upstream eligibility flag.
Neither a legacy dollar delta nor a correctly parsed holdings comparison is
an executed transaction, independent return forecast, or sizing instruction.
"""


def context(packet, source_key='data/13f-positions.json'):
    packet = packet if isinstance(packet, dict) else {}
    return {
        'contract': 'holdings-consumer-boundary.v1',
        'source_key': source_key,
        'source_generated_at': packet.get('generated_at') or packet.get('as_of'),
        'source_present': bool(packet),
        'status': 'research_only' if packet else 'unavailable',
        'vote_eligible': False, 'calls_eligible': False, 'sizing_eligible': False,
        'direction': None, 'score': None, 'independent_votes': 0,
        'legacy_values_used_in_score': False,
        'reason': 'Dated 13F disclosures do not establish purchases, sales, net capital flows or a validated return forecast.',
        'research_url': '/holdings-research.html',
        'qualification_scope': 'Direct 13F contribution only; other and indirect signal families remain subject to separate validation.',
    }
