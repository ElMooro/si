"""Research history and self-reported accuracy do not authorize portfolio or alert decisions."""

UNQUALIFIED_SIGNAL_TYPES=frozenset(('jsi-episode-entry','jsi-flare','jsi-complacency'))


def qualified_percentile(packet):
    # No JSI model has passed independent point-in-time, cost-aware out-of-sample
    # qualification. Promotion must change this reviewed boundary with evidence.
    # A fresh producer flag or a percentile found recursively is not that evidence.
    return None


def alert_view(packet):
    return {'regime':None,'jsi':None,'v2':None,'percentile_since_1990':None,
            'qualification':'research_only','reason':'No qualified JSI transition or forecast model.'}


def calibration_status(packet,generated_at):
    measurements=packet.get('measurements',{}) if isinstance(packet,dict) else {}
    return {'contract':'jsi-qualification-status.v1','version':'2.0.0','generated_at':generated_at,
        'status':'research_only','source_generated_at':packet.get('generated_at') if isinstance(packet,dict) else None,
        'source_contract':packet.get('contract') if isinstance(packet,dict) else None,
        'source_replay':packet.get('replay') if isinstance(packet,dict) else None,
        'spine':{'weights':{},'mode':'unqualified','sample_size':0},
        'overlay':{'weights':{},'mode':'unqualified','sample_size':0},
        'ssm_weight_writes':0,'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False,
        'native_series':len(measurements),'validated_forecast_observations':0,
        'requirements':[
            'Pre-register target, horizon, universe, information cutoff and baseline.',
            'Use retained point-in-time source vintages and executable total-return outcomes.',
            'Use sequential train/validation/test periods with purging and horizon embargo.',
            'Report costs, turnover, drawdown, calibration, uncertainty and regime sample sizes.',
            'Record all candidates and selection trials; hold out final untouched evaluation.',
            'Independent reviewed scorecard identity and monitored shadow results before any sizing permission.'
        ],
        'methodology':{'prior_status':'Full-sample IC weights and a fixed-confidence return atlas do not constitute out-of-sample validation.',
            'current_status':'Native histories describe current vintages. No predictive performance or position size is claimed.'}}
