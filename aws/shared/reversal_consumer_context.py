"""Consumers distinguish native descriptive measurements from unvalidated policy claims."""
from datetime import datetime, timezone
import gzip, hashlib, io, json
import canonical_macro_sources
from report_observations import measurement

CONTRACT='liquidity-reversal-research.v1'


def context(packet, at=None):
    packet=packet if isinstance(packet,dict) else {}; at=at or datetime.now(timezone.utc)
    try:
        stamp=datetime.fromisoformat(packet['generated_at'].replace('Z','+00:00'))
        current=stamp.tzinfo is not None and 0<=(at-stamp).total_seconds()<=26*3600
    except (KeyError,TypeError,ValueError): current=False
    recognized=packet.get('contract')==CONTRACT and current
    return {'state':'ABSTAIN','status':'descriptive_research' if recognized else 'unqualified_or_stale',
        'source':'data/liquidity-reversal.json','generated_at':packet.get('generated_at'),
        'source_replay':packet.get('replay') if recognized else None,
        'trend':None,'trend_score':None,'reversal':None,'reversal_score':None,
        'calls_eligible':False,'sizing_eligible':False,
        'reason':'Native descriptive changes do not establish monetary policy, sector catalysts or a calibrated crisis state'}


def native_yield(client,bucket,at=None):
    """Independently verify exact DGS10 original bytes; no Reversal/Black Swan alias fallback."""
    at=at or datetime.now(timezone.utc)
    def read(key):
        if not isinstance(key,str) or not key.startswith('data/') or '..' in key: raise ValueError('public source path')
        stream=client.get_object(Bucket=bucket,Key=key)['Body']
        try: raw=stream.read(32*1024*1024+1)
        finally: stream.close()
        if len(raw)>32*1024*1024: raise ValueError('source byte bound')
        if key.endswith('.gz'):
            with gzip.GzipFile(fileobj=io.BytesIO(raw)) as reader:raw=reader.read(4*1024*1024+1)
            if len(raw)>4*1024*1024: raise ValueError('original byte bound')
        return raw
    try:
        source=json.loads(read('data/report-measurements.json'))
        source_clock=datetime.fromisoformat(source['generated_at'].replace('Z','+00:00'))
        if source_clock.tzinfo is None or source_clock>at: raise ValueError('canonical source clock differs')
        original=canonical_macro_sources.originals(source,read,['DGS10'])['DGS10']
        args=(original['definition'],original['observations'],original['evidence'])
        rebuilt=measurement('DGS10',*args,source['generated_at'],original['acquired_at'])
        if rebuilt!=source['measurements']['DGS10']: raise ValueError('native DGS10 differs')
        current=measurement('DGS10',*args,at.isoformat(),original['acquired_at'])
        if current['unit']!='Percent' or current['frequency']!='D' or current['quality']['status']!='fresh':
            raise ValueError('native DGS10 unit, cadence or freshness unavailable')
        return {'value':current['current'],'value_decimal':current['current_decimal'],'unit':'Percent',
            'date':current['date'],'series_id':'DGS10','status':'verified_native_measurement',
            'acquired_at':current['acquired_at'],'source_replay':source['replay'],
            'evidence':current['evidence'],'row_index':current['current_row_index'],
            'calls_eligible':False,'sizing_eligible':False}
    except Exception:
        return {'value':None,'series_id':'DGS10','status':'native_measurement_unavailable',
            'calls_eligible':False,'sizing_eligible':False}
