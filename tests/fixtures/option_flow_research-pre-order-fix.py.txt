"""Reproducible option-chain research from complete retained source pages.

Universe selection is research discovery, not an investment ranking. Captured
contract counts and dated last-bar groups carry no dealer-position inference.
"""
from collections import Counter, defaultdict
from datetime import timedelta
import json, re
import option_snapshot_capture as capture
import option_contract_research as contracts
import option_research_rows as codec

CONTRACT = 'option-flow-original-research.v1'
PREFIX = 'data/option-flow-research/'
CURRENT = 'data/option-flow-research.json'
LEGACY = 'data/polygon-options-flow.json'
PRIVATE = 'audit-private/20260909-originals/options-research/'
DISCOVERY = ('data/theme-cascade.json', 'data/convergence-radar.json', 'data/momentum-leaders.json')
# Continuity of the 40 public Options Flow names and ten public GEX names in
# the retained ops 5987 baseline; their union is 44. No private ticket reads.
CONTINUITY = tuple('NET MPC VLO APA NTRA COIN OXY AMZN NVDA DELL PLTR META AMD GOOG AVGO TSLA SNDK HOOD WDC SKHY TSM MU UNH MSFT INTC SPCX V MA QCOM ORCL MRK LLY AAPL ASML COP JPM ADBE XOM ABBV MCK SPY QQQ IWM GOOGL'.split())
MAX_UNIVERSE = 96
MAX_ARTIFACT = 16 * 1024 * 1024
PERMISSIONS = contracts.PERMISSIONS
encoded, sha, clock = codec.encoded, capture.sha, contracts.clock


def ref(raw, kind):
    if kind not in ('inputs', 'outputs', 'runs', 'rows', 'chains', 'compilers'):
        raise ValueError('Reviewed option artifact kind required')
    digest = sha(raw)
    return {'key': PREFIX + kind + '/' + digest + ('.py' if kind == 'compilers' else '.json'),
            'sha256': digest, 'bytes': len(raw)}


def artifact(doc, kind, emit):
    raw = encoded(doc)
    if not 0 < len(raw) <= MAX_ARTIFACT: raise ValueError('Bounded option research artifact required')
    identity = ref(raw, kind); emit(identity['key'], raw)
    return identity


def checked(identity, read, kind=None):
    if not isinstance(identity, dict) or type(identity.get('bytes')) is not int or not 0 < identity['bytes'] <= MAX_ARTIFACT:
        raise ValueError('Bounded immutable option reference required')
    raw = read(identity['key'])
    if len(raw) != identity['bytes'] or sha(raw) != identity.get('sha256'):
        raise ValueError('Option artifact bytes differ')
    if kind is not None and identity != ref(raw, kind): raise ValueError('Option artifact identity differs')
    return json.loads(raw)


def protected(identity, read):
    normalized = contracts.original_ref(identity)
    if normalized['bytes'] > MAX_ARTIFACT: raise ValueError('Protected option evidence bound')
    raw = read(normalized['key'])
    if len(raw) != normalized['bytes'] or sha(raw) != normalized['sha256']:
        raise ValueError('Protected option evidence bytes differ')
    return raw


def universe(contexts, read):
    if set(contexts) != set(DISCOVERY): raise ValueError('Complete public discovery inventory required')
    names = {}; issues = []; evidence = {}
    def add(value, origin):
        if not isinstance(value, str): issues.append({'origin': origin, 'reason': 'non_text_symbol'}); return
        value = value.upper().strip()
        try: capture.ticker(value)
        except ValueError: issues.append({'origin': origin, 'reason': 'invalid_symbol'}); return
        names.setdefault(value, []).append(origin)
    for i, name in enumerate(CONTINUITY): add(name, {'kind': 'public_baseline_continuity', 'position': i})
    for key, identity in contexts.items():
        if identity is None:
            evidence[key] = {'status': 'unavailable', 'original': None}; continue
        if identity.get('source_key') != key: raise ValueError('Discovery source identity differs')
        doc = json.loads(protected(identity, read))
        if not isinstance(doc, dict): raise ValueError('Structured public discovery packet required')
        evidence[key] = {'status': 'captured_discovery_only', 'original': identity,
            'reported_generated_at': doc.get('generated_at') if isinstance(doc.get('generated_at'), str) else None,
            'confers_investment_authority': False}
        fields = ('alert_tier', 'medium_tier', 'watch_tier', 'laggards_hot_themes') if key == DISCOVERY[0] else (
            ('items', 'tickers') if key == DISCOVERY[1] else ('leaders',))
        for field in fields:
            rows = doc.get(field, [])
            if not isinstance(rows, list): issues.append({'source_key': key, 'field': field, 'reason': 'invalid_list'}); continue
            if len(rows) > 5000: raise ValueError('Discovery source row bound exceeded')
            for i, row in enumerate(rows):
                if key == DISCOVERY[1] and (not isinstance(row, dict) or row.get('tier') not in ('ULTRA', 'HIGH')): continue
                value = (row.get('ticker') or row.get('symbol')) if isinstance(row, dict) else row
                add(value, {'kind': 'public_discovery', 'source_key': key, 'pointer': '/' + field + '/' + str(i)})
    ordered = list(names)
    return {'contract': 'option-research-universe.v1', 'selected': ordered[:MAX_UNIVERSE],
        'deferred': ordered[MAX_UNIVERSE:], 'origins': names, 'source_contexts': evidence, 'issues': issues,
        'selection_limit': MAX_UNIVERSE, 'selection_rule': 'Published continuity universe, then public discovery in declared source order; overflow explicitly deferred.',
        'scope': 'A bounded research watchlist, not all listed underlyings or an investment ranking. Private accounts and tickets are not used.'}


def reconstruct(symbol, chain, read, generated_at):
    """Verify every original, including failed replies; compile only a valid prefix."""
    capture.ticker(symbol); cutoff = clock(generated_at)
    if not isinstance(chain, dict) or chain.get('underlying') != symbol or type(chain.get('pagination_complete')) is not bool:
        raise ValueError('Explicit underlying capture descriptor required')
    start, finish = clock(chain['started_at']), clock(chain['completed_at'])
    if not start <= finish <= cutoff: raise ValueError('Option acquisition interval differs')
    all_pages = chain.get('pages')
    if not isinstance(all_pages, list) or len(all_pages) > capture.MAX_PAGES: raise ValueError('Bounded option page inventory required')
    pages = []; originals = []; stopped = False; last = start
    expected = capture.next_url(capture.initial_url(symbol), symbol); seen = set()
    for index, page in enumerate(all_pages):
        if type(page.get('page')) is not int or page['page'] != index + 1: raise ValueError('Contiguous captured page inventory required')
        received = clock(page['acquired_at'])
        if not last <= received <= finish: raise ValueError('Page clock outside acquisition interval')
        last = received
        if page.get('request_url') != expected or page.get('request_sha256') != sha(expected.encode()) or expected in seen:
            raise ValueError('Captured request graph differs')
        seen.add(expected)
        raw = protected(page['original'], read) if page.get('original') else None
        if raw is not None: originals.append(page['original'])
        if stopped: raise ValueError('Page after failed acquisition')
        if page.get('status') != 'received' or page.get('http_status') != 200:
            stopped = True; continue
        if raw is None: raise ValueError('Successful page original required')
        try:
            doc = capture.decode(raw)
            if not isinstance(doc, dict) or doc.get('status') not in ('OK', 'DELAYED') or not isinstance(doc.get('results'), list):
                raise ValueError('Successful envelope required')
            if doc.get('next_url'): expected = capture.next_url(doc['next_url'], symbol)
        except (ValueError, UnicodeDecodeError): stopped = True; continue
        pages.append({**page, 'raw': raw})
    complete = chain['pagination_complete']
    if complete != (chain.get('stop') == 'complete_returned_pagination') or (complete and (stopped or len(pages) != len(all_pages))):
        raise ValueError('Capture completion claim differs')
    context = {'underlying': symbol, 'started_at': chain['started_at'], 'completed_at': chain['completed_at'],
        'stop': chain.get('stop'), 'captured_pages': len(all_pages), 'compiled_pages': len(pages), 'originals': originals,
        'pagination_complete': complete, 'capture_is_atomic': False, 'exchange_chain_completeness_verified': False}
    if not pages:
        return {'contract': contracts.CONTRACT, 'underlying': symbol, 'rows': [], 'acquisition': context,
            'coverage': {'returned_rows': 0, 'eligible_identity_rows': 0, 'pages': 0, 'pagination_complete': False},
            'research_status': 'source_unavailable', 'call': None, 'score': None, 'portfolio_action': 'WAIT',
            'independent_investment_votes': 0, **PERMISSIONS}
    output = contracts.compile_rows(symbol, pages, complete)
    output.update(acquisition=context, research_status='complete_returned_snapshot' if complete else 'partial_returned_snapshot')
    return output


def build(inputs, read, emit):
    if inputs.get('contract') != 'option-flow-inputs.v1': raise ValueError('Native option inputs required')
    stamp = inputs['generated_at']; clock(stamp)
    selection = universe(inputs['discovery'], read)
    if inputs.get('universe') != selection or set(inputs.get('chains', {})) != set(selection['selected']):
        raise ValueError('Option universe must reproduce from retained sources')
    if type(inputs.get('provider_requests')) is not int or not 0 <= inputs['provider_requests'] <= MAX_UNIVERSE * capture.MAX_PAGES:
        raise ValueError('Bounded option request count required')
    if type(inputs.get('source_bytes')) is not int or not 0 <= inputs['source_bytes'] <= 1024 * 1024 * 1024:
        raise ValueError('Bounded option source bytes required')
    predecessors = inputs['predecessors']
    if set(predecessors) != {LEGACY, CURRENT} or predecessors[LEGACY] is None: raise ValueError('Whole option predecessors required')
    for key, identity in predecessors.items():
        if identity is not None:
            if identity.get('source_key') != key: raise ValueError('Option predecessor identity differs')
            json.loads(protected(identity, read))
    original_bytes = sum(p['original']['bytes'] for c in inputs['chains'].values() for p in c.get('pages', []) if p.get('original'))
    if original_bytes != inputs['source_bytes']: raise ValueError('Captured source byte inventory differs')
    chains = {}; counts = Counter(); total_rows = total_eligible = total_blocks = 0
    for symbol in selection['selected']:
        output = reconstruct(symbol, inputs['chains'][symbol], read, stamp)
        groups = defaultdict(list)
        for row in output['rows']: groups[row['evidence']['page']].append(row)
        blocks = []
        for page, rows in groups.items():
            block = codec.pack(rows); identity = artifact(block, 'rows', emit)
            blocks.append({'artifact': identity, 'source_page': page, 'rows': len(rows), 'expanded_sha256': block['expanded_sha256']})
        summary = {k: v for k, v in output.items() if k != 'rows'}
        summary.update(contract='option-research-chain.v1', record_blocks=blocks)
        identity = artifact(summary, 'chains', emit)
        counts[output['research_status']] += 1
        coverage = output['coverage']; total_rows += coverage['returned_rows']; total_eligible += coverage['eligible_identity_rows']; total_blocks += len(blocks)
        chains[symbol] = {'chain': identity, 'research_status': output['research_status'], 'coverage': coverage,
            'acquisition': {k: v for k, v in output['acquisition'].items() if k != 'originals'},
            'reported_open_interest': output.get('reported_open_interest'), 'dated_bar_groups': len(output.get('daily_bar_update_groups', [])),
            'record_blocks': len(blocks), 'call': None, 'score': None, **PERMISSIONS}
    available = len(chains) - counts['source_unavailable']
    return {'contract': CONTRACT, 'engine': 'justhodl-polygon-options-flow', 'version': '3.0.0', 'generated_at': stamp,
        'clock_role': 'original_capture_completed; not a quote, OI, Greek or session observation time',
        'universe': selection, 'chains': chains,
        'quality': {'status': 'unavailable' if not available else 'partial' if counts['source_unavailable'] or counts['partial_returned_snapshot'] or selection['deferred'] else 'descriptive',
            'capture_status_counts': dict(counts), 'selected_underlyings': len(chains), 'deferred_underlyings': len(selection['deferred']),
            'returned_rows': total_rows, 'identity_eligible_rows': total_eligible, 'record_blocks': total_blocks,
            'independent_oi_greek_clocks_verified': False, 'exchange_chain_completeness_verified': False,
            'source_capture_completed_at': stamp, 'acquisition_review_due_at': (clock(stamp) + timedelta(hours=2)).isoformat(),
            'freshness_rule': 'The two-hour acquisition review clock never qualifies OI/IV/Greek observations or relabels last-bar dates.'},
        'predecessors': predecessors, 'provider_requests': inputs['provider_requests'], 'source_bytes': inputs['source_bytes'],
        'call': None, 'score': None, 'portfolio_action': 'WAIT', 'independent_investment_votes': 0, **PERMISSIONS,
        'meaning': 'Complete returned pagination where verified, with immutable row evidence and field-specific validity. Daily bars are grouped by their own update dates. No dealer inventory, trade initiator, predictive edge or position size is inferred.',
        'dependency_roots': [{'source_family': 'massive_polygon_option_snapshot', 'independent_investment_votes': 0}]}
