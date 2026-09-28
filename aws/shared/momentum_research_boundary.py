"""A descriptive price source cannot supply a vote or a legacy confirmation.

Pure adapters only. No IO, native invocation, learning, account or message access.
The marker establishes exclusion of this input, not qualification of other inputs.
"""
from copy import deepcopy

BASIS = 'momentum-price-abstention.v1'
DIRECT = 'data/momentum-breakout.json'
COMPOSITES = ('data/convergence-radar.json', 'data/momentum-leaders.json')


def exclusion():
    return {'source': DIRECT, 'basis': BASIS, 'status': 'research_only_abstain',
            'investment_votes': 0, 'calls_eligible': False, 'forecast_qualified': False,
            'sizing_eligible': False, 'execution_eligible': False, 'call': None,
            'reason': 'Dated price/volume observations and same-date local-price differences have no validated investment direction or independent forecast authority.'}


def current(doc):
    return (isinstance(doc, dict) and doc.get('momentum_research_exclusion') == exclusion())


def guard(key, doc):
    if key == DIRECT or (key in COMPOSITES and not current(doc)):
        return exclusion()
    return doc


def transition_state(state):
    """Retain a complete pre-boundary Velocity snapshot; never promote its confirmations."""
    if not isinstance(state, dict): return {'pending': {}, 'momentum_boundary': BASIS}
    if state.get('momentum_boundary') == BASIS: return state
    return {**state, 'momentum_preboundary_state': deepcopy(state),
            'pending': {}, 'momentum_boundary': BASIS, 'last_trading_date': ''}


def transition_abstention(records):
    for row in records:
        row.update(prior_n_engines=None, is_new_high=False, is_accelerating=False, is_ultra_new=False,
                   momentum_history_comparable=False)
    return {'sent': False, 'is_first_run': False, 'n_ultra_new': 0, 'n_new_high': 0,
            'n_accelerating': 0, 'ultra_new_tickers': [], 'new_high_tickers': [],
            'accelerating_tickers': [], 'reason': 'Pre-boundary counts are retained but incomparable; no transition notification.'}
