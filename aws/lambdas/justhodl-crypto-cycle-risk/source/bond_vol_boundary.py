"""Do not impute a neutral vote when a required source declines authority."""
from copy import deepcopy

def apply_bond_vol_boundary(output,bond):
    if bond.get('calls_eligible') is not False:return output
    out=deepcopy(output)
    out['factors']['macro_regime'].update(risk=None,status='unqualified_bond_vol_vote',
        note='Bond Vol provides descriptive research only; no macro vote is qualified.',source_replay=bond.get('replay'))
    out.update(dump_risk_score=None,risk_level='UNAVAILABLE',action='WAIT — required macro vote is unqualified.',
        top_drivers=[],calls_eligible=False,sizing_eligible=False,execution_eligible=False,
        decision={'verb':'WAIT','meaning':'abstain'},quality={'status':'unqualified_required_input','source':'bond-vol'})
    return out
