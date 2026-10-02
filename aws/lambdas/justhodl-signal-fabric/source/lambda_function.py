"""Descriptive adapter aggregation with explicit research boundaries.

Compound records remain inspectable context and cannot create directional votes.
Other legacy adapters and weights remain research heuristics; grouping names does
not establish source independence, calibrated probabilities or portfolio fit.
Output: data/signal-fabric.json"""
import hashlib, json, math, re, time
from copy import deepcopy
from decimal import Decimal
from context_evidence_store import strict, encode, MAX_BYTES, MAX_TOTAL
from fabric_numeric import CONTRACT, field, symbol, collection, stance, weight, digest, family_projection
from datetime import datetime, timezone

import boto3
from holdings_authority import context as holdings_context
from compound_research_context import BASIS as COMPOUND_CONTEXT_BASIS, read as read_compound_context, pointers as compound_pointers

s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"
OUT = "data/signal-fabric.json"
TICK_RX = re.compile(r"^[A-Z][A-Z0-9.\-]{0,6}$")
DOWN_RX = re.compile(r"DOWN|UNDER|SHORT|SELL|BEAR|AVOID|TOP_FORM",
                     re.I)
UP_RX = re.compile(r"UP|OUT?PERF|LONG|BUY|BULL|BOTTOM_FORM", re.I)
SOURCE_FAMILY = {
    "trend-reversal": "price_reversal",
    "compound-aggregator": "multi_engine_compound",
    "ai-rerating": "fundamental_rerating",
    "magic-formula": "systematic_value",
    "opportunities": "opportunity_composite",
    "insider-clusters": "insider_transactions",
    "congress-direct": "congress_transactions",
    "squeeze-fuel": "short_positioning",
    "short-interest": "short_positioning",
    "13f-flows": "institutional_13f",
}

# ── explicit adapters: (artifact_key, rows_getter, stance_fn) ──
# stance_fn(row) -> (kind, value, direction, confidence) or None


def _g(d, *ks):
    return field(d, *ks)


def _dirn(v):
    sv = str(v or "")
    if DOWN_RX.search(sv):
        return "DOWN"
    if UP_RX.search(sv):
        return "UP"
    return None


def st_best_setups(r0):
    v = _g(r0, "verdict")
    return ("verdict", v, _dirn(v) or "UP",
            (_g(r0, "conviction") or 50) / 100.0)


def st_reversal(r0):
    return stance('trend-reversal', r0)


def st_compound(r0):
    # Neither row count nor score is a signed independent observation. Retain
    # the whole packet through compound_context without creating a vote.
    return None


def st_rerating(r0):
    return stance('ai-rerating', r0)


def st_magic(r0):
    return stance('magic-formula', r0)


def st_opps(r0):
    return stance('opportunities', r0)


def st_insider(r0):
    return stance('insider-clusters', r0)


def st_congress(r0):
    return stance('congress-direct', r0)


def st_squeeze(r0):
    return stance('squeeze-fuel', r0)


def legacy_st_13f(sym, tf):
    x = (tf or {}).get(sym)
    if not isinstance(x, dict):
        return None
    n0 = x.get("n")
    if not n0:
        return None
    return ("13f-flow", "$net %.1fB" % (n0 / 1e9),
            "UP" if n0 > 0 else "DOWN",
            min(1.0, abs(n0) / 5e9))


def st_13f(sym, tf):
    # Values remain in the source packet, but are not a signed flow signal.
    return None


ADAPTERS = [
    # best-setups consumes feature-bus. It is a downstream L3 view and is
    # intentionally excluded here to break the former
    # signal-fabric -> feature-bus -> best-setups -> signal-fabric loop.
    ("data/trend-reversal.json", ("rows",), st_reversal,
     "trend-reversal"),
    ("data/compound-signals.json", ("compound",), st_compound,
     "compound-aggregator"),
    ("data/ai-rerating-radar.json", ("all_ranked", "rows"),
     st_rerating, "ai-rerating"),
    ("data/magic-formula.json",
     ("rows", "top", "ranked", "top_50", "stocks"),
     st_magic, "magic-formula"),
    ("data/opportunities.json",
     ("rows", "opportunities", "ranked", "results"),
     st_opps, "opportunities"),
    ("data/insider-clusters.json", ("clusters", "rows"),
     st_insider, "insider-clusters"),
    ("data/squeeze-fuel.json",
     ("rows", "scored", "ranked", "candidates"),
     st_squeeze, "squeeze-fuel"),
]


def resolve_rows(d, keys):
    return collection(d, keys)[1]


PAGE = {"short-interest": "/short-interest.html",
        "best-setups": "/best-setups.html",
        "trend-reversal": "/trend-reversal.html",
        "compound-aggregator": "/convergence-desk.html",
        "ai-rerating": "/ai-rerating.html",
        "magic-formula": "/magic-formula.html",
        "opportunities": "/opportunities.html",
        "insider-clusters": "/insiders.html",
        "congress-direct": "/political-stocks.html",
        "squeeze-fuel": "/short-interest.html",
        "13f-flows": "/sectors.html"}


def rd(key):
    body = None
    try:
        response = s3.get_object(Bucket=B, Key=key)
        body = response['Body']
        length = response.get('ContentLength')
        if type(length) is not int or not 0 <= length <= MAX_BYTES:
            raise ValueError('Whole bounded Fabric source required')
        chunks = bytearray()
        while True:
            block = body.read(min(65536, MAX_BYTES + 1 - len(chunks)))
            if not isinstance(block, bytes):
                raise ValueError('Byte stream required')
            if not block:
                break
            chunks.extend(block)
            if len(chunks) > MAX_BYTES:
                raise ValueError('Fabric source exceeds bound')
        if len(chunks) != length:
            raise ValueError('Incomplete Fabric source')
        return __import__('sec_ftd_context').guard(key, strict(bytes(chunks), response.get('ContentEncoding', '')))
    except Exception:
        return None
    finally:
        if body is not None:
            try:
                body.close()
            except Exception:
                pass


def lambda_handler(event=None, context=None):
    t0 = time.time()
    generated = datetime.now(timezone.utc).isoformat()
    cache = {}
    total_bytes = 0

    def read_once(key):
        nonlocal total_bytes
        if key not in cache:
            cache[key] = rd(key)
            # No output is published after an incomplete aggregate read budget.
            total_bytes += len(encode(cache[key]))
            if total_bytes > MAX_TOTAL:
                raise ValueError('Complete Fabric context exceeds aggregate bound')
        return cache[key]

    lb = read_once('data/engine-leaderboard.json')
    learned = read_once('data/learned-weights.json')
    cycle = read_once('data/us-cycle.json')
    regime = field(cycle, 'regime', 'phase')
    regime = regime.upper() if isinstance(regime, str) and regime else 'UNKNOWN'
    weights = {}
    source_context = {}
    source_stats = {engine: 0 for _, _, _, engine in ADAPTERS}
    source_stats.update({'congress-direct': 0, 'short-interest': 0, '13f-flows': 0})
    fab = {}

    def add_rows(packet, keys, engine, source, prefix=''):
        key, rows, status = collection(packet, keys)
        context_key = engine + prefix
        evidence = {'source': source, 'collection': key, 'status': status,
                    'packet': deepcopy(packet), 'occurrences': [],
                    'source_freshness_qualified': False, 'current_vote_eligible': False,
                    'original_bytes_retained': False,
                    'observation_note': 'Received publication clocks are context, not observation freshness.'}
        source_context[context_key] = evidence
        if engine not in weights:
            weights[engine] = weight(engine, lb, learned, regime)
        for i, row in enumerate(rows):
            pointer = prefix + '/' + key + '/' + str(i)
            sym = symbol(row)
            st = stance(engine, row)
            occ = {'pointer': pointer, 'record': deepcopy(row), 'symbol': sym,
                   'status': 'withheld_invalid_identity' if sym is None else 'not_selected_or_invalid_adapter',
                   'current_vote_eligible': False}
            evidence['occurrences'].append(occ)
            if sym is None or st is None:
                continue
            kind, value, direction, strength = st
            evidence_id = 'sf2:' + digest([source, pointer, row])[:24]
            occ.update(status='descriptive_candidate', evidence_id=evidence_id)
            w = weights[engine]
            envelope = {
                'evidence_id': evidence_id, 'engine': engine, 'kind': kind,
                'value': value, 'direction': direction,
                'confidence': round(strength, 2), 'heuristic_strength': strength,
                'confidence_semantics': 'uncalibrated_heuristic_strength_not_probability',
                'weight': w['value'], 'weight_basis': w['basis'],
                'weight_context': deepcopy(w),
                'source_family': SOURCE_FAMILY.get(engine, engine),
                'evidence_level': 'L2', 'role': 'derived_heuristic',
                'independence_eligible': False, 'ancestry': [], 'ancestry_status': 'not_traced',
                'forecast_qualified': False, 'sizing_eligible': False,
                'calls_eligible': False, 'current_vote_eligible': False,
                'provenance': {'producer': engine, 'source': source, 'pointer': pointer,
                               'record': deepcopy(row), 'parsed_record_sha256': digest(row)},
                'link': 'https://justhodl.ai' + PAGE.get(engine, '/engine-leaderboard.html')}
            fab.setdefault(sym, []).append(envelope)
        evidence['selected_count'] = len(rows)
        evidence['candidate_count'] = sum(r['status'] == 'descriptive_candidate' for r in evidence['occurrences'])

    compound_context = read_compound_context(s3, B)
    total_bytes += len(encode(compound_context))
    if total_bytes > MAX_TOTAL:
        raise ValueError('Complete Fabric context exceeds aggregate bound')
    for key, keys, _, engine in ADAPTERS:
        if engine != 'compound-aggregator':
            add_rows(read_once(key), keys, engine, key)
    congress = read_once('data/congress-direct.json')
    for chamber in ('senate', 'house'):
        add_rows(congress.get(chamber) if isinstance(congress, dict) else None,
                 ('rows', 'transactions', 'filings'), 'congress-direct',
                 'data/congress-direct.json', '/' + chamber)

    # The original short-interest and holdings packets remain context only.
    # No fallback through squeeze-fuel or a legacy 13F net value can grant a vote.
    short_interest = read_once('data/short-interest.json')
    holdings_packet = read_once('data/13f-flows-by-ticker.json')
    holdings_qualification = holdings_context(holdings_packet, 'data/13f-flows-by-ticker.json')
    tickers = []
    conflicts = []
    for sym, occurrences in sorted(fab.items()):
        envs, families = family_projection(occurrences)
        ups = [e for e in envs if e['direction'] == 'UP']
        downs = [e for e in envs if e['direction'] == 'DOWN']
        # Decimal accumulation makes an exact equal-weight tie independent of
        # source order; published precision also controls the displayed direction.
        score = sum((Decimal(str(e['weight'])) * Decimal(str(e['heuristic_strength']))
                     * (1 if e['direction'] == 'UP' else -1) for e in envs), Decimal(0))
        score = round(float(score), 2) if envs else None
        row = {'ticker': sym, 'n_engines': len(envs), 'fabric_score': score,
               'net_direction': None if score is None or score == 0 else 'UP' if score > 0 else 'DOWN',
               'agreement_pct': round(100 * max(len(ups), len(downs)) / len(envs)) if envs else None,
               'engines': sorted(envs, key=lambda e: e['engine']),
               'all_occurrences': sorted(occurrences, key=lambda e: e['evidence_id']),
               'family_projection': families,
               'calls_eligible': False, 'sizing_eligible': False, 'forecast_qualified': False,
               'current_vote_eligible': False, 'n_independent_roots': None,
               'direction_semantics': 'signed_descriptive_balance_not_a_current_trade'}
        tickers.append(row)
        for e in envs:
            source_stats[e['engine']] += 1
        if ups and downs:
            conflicts.append({'ticker': sym, 'n_engines': len(envs),
                              'up': [e['engine'] for e in ups], 'down': [e['engine'] for e in downs],
                              'note': 'Opposing descriptive heuristics; independence and forward edge unqualified.'})

    # Use the same received rerating snapshot; duplicate identities cannot pick
    # the final peer group merely by arriving last.
    rerating = read_once('data/ai-rerating-radar.json')
    _, peer_rows, _ = collection(rerating, ('all_ranked', 'rows'))
    peer_candidates = {}
    for row in peer_rows:
        sym = symbol(row)
        if sym:
            peer_candidates.setdefault(sym, []).append(field(row, 'peer_group'))
    peer_groups = {sym: vals[0] for sym, vals in peer_candidates.items()
                   if len(vals) == 1 and isinstance(vals[0], str) and vals[0].strip()}
    for row in tickers:
        group = peer_groups.get(row['ticker'])
        peers = [r for r in tickers if group and peer_groups.get(r['ticker']) == group
                 and r['ticker'] != row['ticker'] and r['fabric_score'] is not None]
        if len(peers) >= 2 and row['fabric_score'] is not None:
            row['peer_group'] = group
            row['peer_fabric_score'] = round(math.fsum(r['fabric_score'] for r in peers) / len(peers), 2)
            row['peer_calculation'] = {'other_members': [{'ticker': r['ticker'], 'score': r['fabric_score']} for r in peers],
                                       'formula': 'mean of received other-member descriptive balances',
                                       'complete_market_peer_universe': False}
    tickers.sort(key=lambda r: (r['fabric_score'] is None, -abs(r['fabric_score'] or 0), r['ticker']))
    news_packet = read_once('data/tiingo-news.json')
    news = news_packet.get('by_ticker') if isinstance(news_packet, dict) else None
    news = news if isinstance(news, dict) else {}
    bus = {}
    for row in tickers:
        env = {e['engine']: e for e in row['engines']}
        value = lambda engine, key: env.get(engine, {}).get(key)
        bus[row['ticker']] = {
            'fabric_score': row['fabric_score'], 'net_direction': row['net_direction'],
            'agreement_pct': row['agreement_pct'], 'n_engines': row['n_engines'],
            'conflict': any(c['ticker'] == row['ticker'] for c in conflicts),
            'reversal': value('trend-reversal', 'value'), 'congress': value('congress-direct', 'direction'),
            'flow_13f': None, 'squeeze': value('squeeze-fuel', 'value'), 'insider': value('insider-clusters', 'value'),
            'compound': None, 'compound_context_pointers': compound_pointers(compound_context, row['ticker']),
            'setups': None, 'peer_group': row.get('peer_group'), 'peer_fabric_score': row.get('peer_fabric_score'),
            'calls_eligible': False, 'sizing_eligible': False, 'forecast_qualified': False,
            'ranking_eligible': False, 'learning_weight_eligible': False, 'current_vote_eligible': False,
            'research_evidence': row}
        nw = news.get(row['ticker'])
        if isinstance(nw, dict):
            bus[row['ticker']].update(news_24h=nw.get('n_24h'), news_7d=nw.get('n_7d'), news_burst=nw.get('burst'))
    previous = read_once('data/feature-bus.json')
    boundary = {'measurement_contract': CONTRACT, 'generated_at': generated,
                'compound_research_boundary': COMPOUND_CONTEXT_BASIS,
                'calls_eligible': False, 'sizing_eligible': False, 'forecast_qualified': False,
                'ranking_eligible': False, 'current_vote_eligible': False,
                'source_freshness_qualified': False, 'source_replay_performed': False,
                'publication_atomic': False,
                'population_note': 'Complete received declared collections within bounds; upstream universe completeness unqualified.'}
    trace = {'source_context': source_context, 'weight_context': weights,
             'weight_inputs': {'leaderboard': lb, 'learned': learned, 'cycle': cycle},
             'short_interest_context': {'packet': short_interest, 'current_vote_eligible': False},
             'news_context': news_packet, 'congress_context': congress,
             'holdings_context': holdings_qualification,
             'compound_context': compound_context}
    out = {**boundary, **trace, 'engine': 'justhodl-signal-fabric', 'version': '2.2',
           'elapsed_s': round(time.time() - t0, 1),
           'architecture': 'Typed descriptive adapter research; no calibrated confidence, independent roots, current votes or portfolio authority.',
           'source_stats': source_stats, 'source_stats_semantics': 'included descriptive family occurrences after validation',
           'n_tickers': len(tickers), 'n_conflicts': len(conflicts), 'tickers': tickers,
           'conflicts': conflicts, 'by_ticker': {r['ticker']: r['engines'] for r in tickers}}
    bus_packet = {**boundary, **trace, 'n_tickers': len(bus), 'tickers': bus,
                  'integration': {'note': 'Research context only. Do not use for rank multipliers, learned skill or portfolio sizing.'}}
    events_packet = {**boundary, 'n': 0, 'events': [], 'prior_calculation_comparable': False,
                     'prior_contract': field(previous, 'measurement_contract'),
                     'comparison_note': 'Observation-vintage compatibility is unqualified. Publication changes cannot establish market events.'}
    # Validate every complete projection before the first write. These legacy
    # heads are sequential, not an atomic multi-object transaction.
    planned = [('data/feature-bus.json', bus_packet),
               ('data/archive/feature-bus/' + generated[:10].replace('-', '') + '.json', bus_packet),
               ('data/fabric-events.json', events_packet), (OUT, out)]
    encoded = [(key, encode(value)) for key, value in planned]
    if any(len(raw) > MAX_BYTES for _, raw in encoded):
        raise ValueError('Complete Fabric output exceeds bound; no partial population published')
    prepared = dict(encoded)
    # Literal public destinations keep the static source-to-page contract visible.
    s3.put_object(Bucket=B, Key='data/feature-bus.json', Body=prepared['data/feature-bus.json'],
                  ContentType='application/json', CacheControl='no-cache')
    s3.put_object(Bucket=B, Key='data/archive/feature-bus/' + generated[:10].replace('-', '') + '.json',
                  Body=prepared[planned[1][0]], ContentType='application/json', CacheControl='no-cache')
    s3.put_object(Bucket=B, Key='data/fabric-events.json', Body=prepared['data/fabric-events.json'],
                  ContentType='application/json', CacheControl='no-cache')
    s3.put_object(Bucket=B, Key=OUT, Body=prepared[OUT], ContentType='application/json', CacheControl='no-cache')
    print(json.dumps({'ok': True, 'bus': len(bus), 'events': 0, 'tickers': len(tickers),
                      'conflicts': len(conflicts), 'sources': source_stats}))
    return {'ok': True}
