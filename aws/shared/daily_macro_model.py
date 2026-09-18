"""Canonical macro projection for the legacy report's compatibility envelope.

The macro projection is sourced and reproducible. Auxiliary market collectors
remain explicitly unverified; their retained inputs are not original-source
evidence. No score, investment action, or portfolio weight is inferred here.
"""
from copy import deepcopy

from research_brief_model import build as brief, clock, encoded, digest

CONTRACT = 'daily-research-report.v1'
REASON = 'Model validation and account-specific portfolio constraints are not established.'
AUXILIARY = ('stocks', 'crypto', 'crypto_global', 'ecb_ciss', 'news', 'ticker_names', 'tenor_research',
             'liquidity_credit_engine', 'global_business_cycle', 'tenor_signals', 'cftc_positioning')
STOCK_AUTHORITY = ('signal', 'score', 'tech_score', 'technical_score', 'recommendation',
                   'risk_reward', 'upside_pct', 'downside_pct', 'ad_signal', 'macd_cross', 'cross', 'grade', 'formation')


def unavailable_score():
    return {'score': None, 'value': None, 'regime': None, 'status': 'unvalidated',
            'signals': [], 'call': None, 'calls_eligible': False, 'sizing_eligible': False,
            'reason': REASON}


def market_context(report):
    """No alternate key/default can resurrect the retired macro authority."""
    net = report.get('net_liquidity') or {}
    return {'status': 'research_only', 'ml_regime': None, 'risk_level': None,
            'risk_score': None, 'liquidity': None, 'carry_risk': None, 'sector_regime': None,
            'us_outlook': None, 'net_liquidity_value': net.get('net'),
            'net_liquidity_unit': net.get('unit'), 'net_liquidity_components': deepcopy(net.get('components')),
            'call': None, 'sizing_eligible': False, 'calls_eligible': False, 'reason': REASON}


def build(source, auxiliary, generated_at):
    macro = brief(source, generated_at)
    now = clock(generated_at)
    if not isinstance(auxiliary, dict) or not isinstance(auxiliary.get('observations'), dict):
        raise ValueError('explicit auxiliary input envelope required')
    acquired = clock(auxiliary['collected_at'])
    if acquired > now:
        raise ValueError('future auxiliary collection')
    observations = auxiliary['observations']
    out = {key: deepcopy(observations.get(key, [] if key == 'news' else {})) for key in AUXILIARY}
    out.update(version='V10', contract=CONTRACT, generated_at=generated_at,
               source_generated_at=source['generated_at'],
               call=None, calls_eligible=False, sizing_eligible=False, regime=None)
    fred = {}
    projected = {row['series_id']: row for row in macro['metrics_table']}
    for sid, catalog in source['catalog'].items():
        row = deepcopy(source.get('measurements', {}).get(sid, {}))
        state = projected[sid]['status']
        row.update(series_id=sid, name=row.get('name') or catalog.get('display_name') or sid,
                   calls_eligible=False, sizing_eligible=False, call=None)
        row['quality'] = {**row.get('quality', {}), 'status': state,
                          'evaluated_at': generated_at, 'basis': 'source acquisition and observation age'}
        row['last_observed_value'] = row.get('current')
        row['last_observed_decimal'] = row.get('current_decimal')
        if state != 'fresh':
            for key in ('current', 'current_decimal', 'prev', 'change', 'pct_change', 'week_pct', 'month_pct', 'quarter_pct', 'year_pct'):
                row[key] = None
        row['category_definition'] = 'Broad-dollar and bilateral FX series; not ICE DXY' if catalog.get('category') == 'dxy' else catalog.get('category')
        fred.setdefault(catalog.get('category', 'other'), {})[sid] = row
    out['fred'] = fred
    out['khalid_index'] = unavailable_score()
    out['ka_index'] = deepcopy(out['khalid_index'])
    out['risk_dashboard'] = {'composite': None, 'composite_score': None, 'overall_risk': None,
                             'regime': None, 'status': 'unvalidated', 'call': None,
                             'sizing_eligible': False, 'calls_eligible': False, 'reason': REASON}
    out['net_liquidity'] = deepcopy(macro['net_liquidity'])
    out['research_brief'] = macro
    out['decision'] = deepcopy(macro['decision'])
    out['ai_analysis'] = {'status': 'research_only', 'method': 'deterministic dated observations',
                          'summary': ' '.join(row['summary'] for row in macro['brief_items']),
                          'observations': deepcopy(macro['brief_items']),
                          'call': None, 'sizing_eligible': False, 'calls_eligible': False,
                          'portfolio': {'status': 'withheld', 'construction': {}, 'moves': [], 'reason': REASON},
                          'actions': [], 'forecast': None}
    # Preserve collected input bytes in the replay store. Active fields cannot
    # advertise a technical heuristic as a qualified investment recommendation.
    for row in out['stocks'].values():
        if not isinstance(row, dict):
            raise ValueError('stock observation must be an object')
        for field in STOCK_AUTHORITY:
            row[field] = None
        row.update(call=None, sizing_eligible=False, calls_eligible=False,
                   quality={'status': 'legacy_unverified', 'original_source_verified': False},
                   source_collected_at=auxiliary['collected_at'])
    out['signals'] = {'buys': [], 'sells': [], 'warnings': [], 'status': 'withheld', 'reason': REASON}
    # Preserve legacy envelope fields and retain their collected inputs in the
    # run snapshot. A secondary composite cannot bypass the same authority gate.
    out['liquidity_credit_engine'] = {'regime': None, 'composite': None, 'series': {}, 'by_category': {},
                                     'interpretation': {}, 'generated_at': None, 'status': 'unvalidated',
                                     'sizing_eligible': False, 'calls_eligible': False}
    out['global_business_cycle'] = {'aggregate': {}, 'interpretation': {}, 'by_country': {},
                                    'generated_at': None, 'status': 'unvalidated',
                                    'sizing_eligible': False, 'calls_eligible': False}
    out['tenor_signals'] = {'composite_score': None, 'any_firing': False, 'any_watch': False,
                            'signals': {}, 'generated_at': out['tenor_research'].get('generated_at'),
                            'status': 'research_only', 'sizing_eligible': False, 'calls_eligible': False}
    out['market_flow'] = {'status': 'unavailable', 'total_buying': None, 'total_selling': None,
                          'sectors_buying': [], 'sectors_selling': [], 'carry_risk': None,
                          'reason': 'Signed price-volume proxies are not measured capital flows.'}
    out['ath_breakouts'] = {'status': 'unavailable', 'total_at_ath': None, 'total_near_ath': None,
                            'ath_coverage': None, 'at_ath': [], 'near_ath': [],
                            'reason': 'A bounded daily-price window does not establish an all-time high.'}
    out['sectors'] = {}
    for symbol, name in {'XLF':'Financials','XLE':'Energy','XLK':'Technology','XLV':'Healthcare',
                         'XLI':'Industrials','XLU':'Utilities','XLP':'Staples','XLY':'Discretionary',
                         'XLB':'Materials','XLC':'Communications','XLRE':'Real Estate'}.items():
        if symbol in out['stocks']:
            row = out['stocks'][symbol]
            out['sectors'][symbol] = {key: row.get(key) for key in ('price','date','day_pct','week_pct','month_pct','quarter_pct','quality')}
            out['sectors'][symbol].update(name=name, calls_eligible=False, sizing_eligible=False)
    for row in out['ecb_ciss'].values():
        if isinstance(row, dict):
            for key in ('change', 'pct_change', 'week_pct', 'month_pct', 'quarter_pct'):
                row[key] = None
            row.update(quality={'status': 'legacy_unverified'}, calls_eligible=False, sizing_eligible=False)
    out['auxiliary_quality'] = {'status': 'legacy_unverified', 'collected_at': auxiliary['collected_at'],
                                'original_source_verified': False, 'fields': list(AUXILIARY),
                                'errors': deepcopy(auxiliary.get('errors', {})),
                                'reason': 'Market collectors are a separate source-evidence migration; collection time does not prove observation freshness.'}
    out['quality'] = {**macro['quality'], 'status': 'degraded', 'macro_status': macro['quality']['status'],
                       'auxiliary_status': 'legacy_unverified'}
    out['stats'] = {'fred': len(source.get('measurements', {})), 'stocks': len(out['stocks']),
                    'crypto': len(out['crypto']), 'ecb_ciss': len(out['ecb_ciss'])}
    out['scope'] = 'Canonical FRED observations and abstention are source-replayable; auxiliary market observations are explicitly unverified. No portfolio authority.'
    out['market_intelligence'] = market_context(out)
    return out
