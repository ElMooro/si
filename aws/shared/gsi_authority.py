"""Global Stress research cannot promote its own score into votes, forecasts or sizing."""

def qualified_score(packet):
    # No GSI model currently has an independently reviewed, point-in-time,
    # cost-aware out-of-sample scorecard. A producer flag cannot grant authority.
    # Promotion requires a reviewed change to this boundary and its evidence.
    return None


def decision_view(packet):
    return {'global_stress_index':None,'global_stress_level':None,
        'equity_stress':None,'bond_stress':None,'worst_market':None,
        'equities':[],'bonds':[],'flashing_red':[],'hot_signals':[],
        'gsi_by_horizon':{},'qualification':'research_only',
        'reason':'Global Stress measurements have no qualified voting, forecasting or sizing model.',
        'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False}


def filter_history(rows):
    """Exclude unqualified GSI scores before redundancy clusters can affect sizing."""
    return [{**row,'scores':{k:v for k,v in (row.get('scores') or {}).items()
        if k not in ('global_stress','gsi') and not k.startswith('gsi_')}} for row in rows
        if isinstance(row,dict)]


def calibration_status(packet,generated_at):
    p=packet if isinstance(packet,dict) else {}
    return {'contract':'gsi-qualification-status.v1','version':'2.0.0','generated_at':generated_at,
        'status':'research_only','mode':'unqualified','weights':{},'sample_size':0,
        'term_structure':[],'gsi_by_horizon':{},'source_generated_at':p.get('generated_at'),
        'source_contract':p.get('contract'),'source_replay':p.get('replay'),
        'validated_forecast_observations':0,'ssm_weight_writes':0,
        'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False,
        'requirements':[
            'Pre-register target, horizon, instruments, information cutoff and baseline.',
            'Retain point-in-time inputs and independently verified executable total-return outcomes.',
            'Use chronological train/validation/test splits, purging and horizon embargo.',
            'Report costs, turnover, drawdown, calibration, uncertainty and regime sample sizes.',
            'Record all trials and selection decisions; reserve an untouched final evaluation.',
            'Review the immutable scorecard and prospective shadow performance before any portfolio authority.'
        ],
        'methodology':'Full-sample IC, partial forward outcomes, duplicated dates and blended priors do not establish out-of-sample predictive performance.'}
