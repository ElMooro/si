"""Yield threshold observations are context, not an allocation or alert model."""
CURRENT='data/us10y-sentinel.json'
FLAGS=('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified','alert_eligible')
REASON='No independently qualified Sentinel threshold-alert or portfolio-allocation policy is registered.'


def context(packet):
    source=packet if isinstance(packet,dict) else {}
    def text(key):return source.get(key) if isinstance(source.get(key),str) else None
    return {'source_key':CURRENT,'status':'ABSTAIN','reported_tier':text('tier'),
        'reported_generated_at':text('generated_at'),'reported_observation_date':text('fred_date'),
        'independent_investment_votes':0,'original_source_replay_performed_by_consumer':False,
        'reason':REASON,**dict.fromkeys(FLAGS,False)}
