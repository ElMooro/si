"""Deterministic, source-anchored AAII observations; no forecast authority.

Only reviewed survey containers are parsed. Marketing examples, performance
tables and unlabelled percentages cannot become a current survey observation.
The publisher's explicit main-page year anchors the yearless recent-results
table. No machine-clock year is substituted for a missing source date.
"""
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from html.parser import HTMLParser
from zoneinfo import ZoneInfo
import hashlib
import json
import re

CONTRACT = 'aaii-native-research.v1'
ET = ZoneInfo('America/New_York')
URLS = {'main': 'https://www.aaii.com/sentimentsurvey',
        'results': 'https://www.aaii.com/sentimentsurvey/sent_results'}
NAMES = ('bullish', 'neutral', 'bearish')
VOID = {'area', 'base', 'br', 'col', 'embed', 'hr', 'img', 'input', 'link', 'meta', 'param', 'source', 'track', 'wbr'}


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def sha(raw): return hashlib.sha256(raw).hexdigest()


def stamp(value):
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None: raise ValueError('explicit timezone required')
    return result.astimezone(timezone.utc)


class Node:
    def __init__(self, tag, attrs=(), line=0):
        self.tag, self.attrs, self.line, self.children = tag, dict(attrs), line, []

    def nodes(self, tag=None, cls=None):
        result = []
        for child in self.children:
            if not isinstance(child, Node): continue
            if (tag is None or child.tag == tag) and (cls is None or cls in child.attrs.get('class', '').split()):
                result.append(child)
            result.extend(child.nodes(tag, cls))
        return result

    def text(self):
        return ' '.join(' '.join(c.text() if isinstance(c, Node) else c for c in self.children).split())


class Document(HTMLParser):
    def __init__(self, raw):
        super().__init__(convert_charrefs=True)
        if not isinstance(raw, bytes) or len(raw) > 2*1024*1024: raise ValueError('bounded original HTML required')
        self.root = Node('root'); self.stack = [self.root]; self.count = 0
        self.feed(raw.decode('utf-8-sig', errors='strict')); self.close()

    def handle_starttag(self, tag, attrs):
        self.count += 1
        if self.count > 30000 or len(self.stack) > 160: raise ValueError('HTML complexity bound')
        node = Node(tag, attrs, self.getpos()[0]); self.stack[-1].children.append(node)
        if tag not in VOID: self.stack.append(node)

    def handle_startendtag(self, tag, attrs):
        self.handle_starttag(tag, attrs)
        if tag not in VOID: self.handle_endtag(tag)

    def handle_endtag(self, tag):
        for i in range(len(self.stack)-1, 0, -1):
            if self.stack[i].tag == tag:
                del self.stack[i:]; return

    def handle_data(self, data): self.stack[-1].children.append(data)


def only(nodes):
    if len(nodes) != 1: raise ValueError('unique reviewed survey element required')
    return nodes[0]


def percentage(text, prefix=''):
    match = re.fullmatch(re.escape(prefix)+r'\s*(\d{1,3}(?:\.\d{1,4})?)\s*%', text.strip())
    if not match: raise ValueError('explicit survey percentage required')
    value = Decimal(match[1])
    if not 0 <= value <= 100: raise ValueError('percentage outside 0..100')
    return value


def observation(day, values, evidence):
    if day.weekday() != 2: raise ValueError('reviewed current survey week must end Wednesday')
    if len(values) != 3: raise ValueError('three labelled categories required')
    # Printed decimal precision supplies a rounding interval, never a renormalization.
    tolerance = sum(Decimal(10)**v.as_tuple().exponent / 2 for v in values)
    if abs(sum(values)-100) > tolerance: raise ValueError('survey percentages do not reconcile within printed rounding')
    result = {'week_ending': day.isoformat(), **{key+'_pct': float(v) for key, v in zip(NAMES, values)},
        'bull_bear_spread_pp': float(values[0]-values[2]), 'printed_sum_pct': float(sum(values)),
        'rounding_tolerance_pp': float(tolerance), 'source_evidence': evidence}
    return result


def values(row): return tuple(Decimal(str(row[k+'_pct'])) for k in NAMES)


def parse_main(raw):
    root = Document(raw).root
    anchor = only(root.nodes(cls='ssv2-gauge-week'))
    match = re.fullmatch(r'Week ending ([A-Za-z]+ \d{1,2}, \d{4})', anchor.text(), re.I)
    if not match: raise ValueError('explicit current survey year required')
    day = datetime.strptime(match[1], '%B %d, %Y').date()
    bars = only(root.nodes(cls='ssv2-gauge-bars')).nodes(cls='ssv2-sbar')
    if len(bars) != 3: raise ValueError('three current gauge bars required')
    by_name = {}; averages = {}
    for bar in bars:
        name = only(bar.nodes(cls='ssv2-slabel')).text().lower()
        if name not in NAMES or name in by_name: raise ValueError('duplicate or unknown category label')
        by_name[name] = percentage(only(bar.nodes(cls='ssv2-snum')).text())
        averages[name+'_pct'] = float(percentage(only(bar.nodes(cls='ssv2-savg')).text(), 'Avg'))
    current = observation(day, [by_name[k] for k in NAMES],
        [{'source': 'main', 'selector': '.ssv2-gauge-week', 'value_selector': '.ssv2-gauge-bars .ssv2-snum', 'anchor_line': anchor.line}])
    spread_node = only(root.nodes(cls='ssv2-spread-pill'))
    spread_match = re.search(r'Bull[–−-]Bear Spread:\s*([+-]?\d+(?:\.\d+)?)\s*pp', spread_node.text())
    if not spread_match or Decimal(spread_match[1]) != by_name['bullish']-by_name['bearish']:
        raise ValueError('publisher spread does not reconcile')
    recent_card = only([card for card in root.nodes(cls='ssv2-card')
                        if [h.text() for h in card.nodes(tag='h3')] == ['Recent weekly results']])
    rows = []
    for row in recent_card.nodes(cls='datebars'):
        day_node = only(row.nodes(cls='date'))
        row_day = datetime.strptime(day_node.text(), '%m/%d/%Y').date()
        container = only(row.nodes(cls='bars'))
        vals = [percentage(only(container.nodes(cls=k)).text()) for k in NAMES]
        rows.append(observation(row_day, vals, [{'source': 'main', 'selector': '.ssv2-card .datebars', 'container_heading': 'Recent weekly results', 'line': row.line}]))
    dates = [r['week_ending'] for r in rows]
    if len(rows) < 2 or dates != sorted(set(dates), reverse=True) or dates[0] != current['week_ending']:
        raise ValueError('dated recent survey section must reconcile to current anchor')
    if values(rows[0]) != values(current): raise ValueError('current gauge and dated row disagree')
    if any((day-date.fromisoformat(d)).days not in range(0, 371, 7) for d in dates):
        raise ValueError('recent survey dates outside weekly window')
    return current, rows, {'values': averages, 'source': 'main', 'selector': '.ssv2-gauge-bars .ssv2-savg',
        'method': 'Publisher-displayed gauge averages; not independently recomputed from full 1987 history.'}


def parse_results(raw, anchor):
    root = Document(raw).root
    tables = []
    for table in root.nodes(tag='table'):
        trs = table.nodes(tag='tr')
        if not trs: continue
        cells = [c.text() for c in trs[0].children if isinstance(c, Node) and c.tag in ('td', 'th')]
        if cells == ['Reported Date', 'Bullish', 'Neutral', 'Bearish']: tables.append(trs)
    trs = only(tables)
    if not 2 <= len(trs) <= 54: raise ValueError('reviewed recent table bound exceeded')
    rows = []; upper = anchor
    for index, tr in enumerate(trs[1:], 1):
        cells = [c.text() for c in tr.children if isinstance(c, Node) and c.tag in ('td', 'th')]
        if len(cells) != 4: raise ValueError('survey table row must match four labelled columns')
        parsed = None
        for fmt in ('%B %d, %Y', '%b %d, %Y', '%m/%d/%Y'):
            try: parsed = datetime.strptime(cells[0], fmt).date(); break
            except ValueError: pass
        if parsed is None:
            # Bound a yearless month/day using the explicit publisher anchor and
            # strict descending chronology. No access to today's date here.
            if not re.fullmatch(r'[A-Za-z]+\s+\d{1,2}', cells[0]): raise ValueError('reviewed result date required')
            for year in (upper.year, upper.year-1):
                for fmt in ('%B %d %Y', '%b %d %Y'):
                    try: candidate = datetime.strptime(cells[0]+' '+str(year), fmt).date()
                    except ValueError: continue
                    if candidate <= upper: parsed = candidate; break
                if parsed is not None: break
        if parsed is None or parsed > upper or not 0 <= (anchor-parsed).days <= 364:
            raise ValueError('ambiguous or out-of-window result year')
        row = observation(parsed, [percentage(t) for t in cells[1:]],
            [{'source': 'results', 'selector': 'table', 'exact_header': ['Reported Date', 'Bullish', 'Neutral', 'Bearish'], 'row': index, 'line': tr.line,
              'year_basis': 'explicit publisher main-page anchor and descending recent rows'}])
        rows.append(row); upper = parsed-timedelta(days=1)
    if rows[0]['week_ending'] != anchor.isoformat(): raise ValueError('results and main release dates disagree')
    return rows


def deadlines(day, at):
    # The publisher states Thursday morning, not an exact release timestamp.
    # 19:00 UTC is our monitoring deadline, not a claimed publication instant.
    next_due = datetime.combine(day+timedelta(days=8), time(19), timezone.utc)
    return {'source_due_at': next_due.isoformat(), 'pipeline_check_due_at': (at+timedelta(hours=36)).isoformat(),
        'valid_until': min(next_due, at+timedelta(hours=36)).isoformat(),
        'policy': 'Weekly Thursday 19:00 UTC monitoring deadline; source publication instant unknown. Check source at least every 36 hours.'}


def legacy_row(row):
    if not row: return {**{k: None for k in NAMES}, 'week_ending': None, 'bull_bear_spread': None}
    return {'week_ending': row['week_ending'], **{k: float(Decimal(str(row[k+'_pct']))/100) for k in NAMES},
        'bull_bear_spread': float(Decimal(str(row['bull_bear_spread_pp']))/100)}


def compute(sources, generated_at):
    at = stamp(generated_at); errors = []; current = None; history = []; averages = None; reconciliation = []
    try:
        current, recent, averages = parse_main(sources['main']['raw'])
        day = date.fromisoformat(current['week_ending'])
        if at.astimezone(ET) < datetime.combine(day+timedelta(days=1), time(), ET):
            raise ValueError('survey period has not closed')
        history = parse_results(sources['results']['raw'], day)
        by_day = {r['week_ending']: r for r in history}
        for row in recent:
            other = by_day.get(row['week_ending'])
            if other is None or values(other) != values(row): raise ValueError('published recent history disagrees')
            other['source_evidence'] += row['source_evidence']
            reconciliation.append(row['week_ending'])
        current['source_evidence'] += history[0]['source_evidence']
    except (KeyError, ValueError, TypeError, UnicodeError, OverflowError):
        # Originals retain the diagnostic details without publishing arbitrary HTML.
        errors.append('publisher_acquisition_structure_date_or_reconciliation_failed')
        current = None; history = []; averages = None
    dates = sorted(r['week_ending'] for r in history)
    as_of = current['week_ending'] if current else None
    clocks = deadlines(date.fromisoformat(as_of), at) if as_of else None
    quality = 'unavailable' if errors else 'fresh' if at < stamp(clocks['source_due_at']) else 'stale'
    gaps = []
    if dates:
        d = date.fromisoformat(dates[0]); end = date.fromisoformat(dates[-1])
        while d <= end:
            if d.isoformat() not in dates: gaps.append(d.isoformat())
            d += timedelta(days=7)
    packet = {'contract': CONTRACT, 'version': '2.0.0', 'generated_at': generated_at, 'as_of': as_of,
        'quality': {'status': quality, 'errors': errors, 'freshness': clocks, 'cross_checked_weeks': reconciliation,
                    'history_missing_weeks': gaps, 'status_means': 'Source date, labelled columns and publisher-page reconciliation; not predictive qualification.'},
        'observation': current, 'history': sorted(history, key=lambda r: r['week_ending']),
        'units': {'survey_categories': 'percent_of_respondents', 'bull_bear_spread': 'percentage_points',
                  'legacy_latest_and_history_26w': 'fractions; spread is difference of fractions'},
        'survey': {'provider': 'AAII', 'question_count': 1, 'outlook_months': 6,
            'population': 'Participating AAII members; self-selected online responses, not all investors or portfolio positions.',
            'period': 'Thursday 00:01 through Wednesday 23:59 America/New_York',
            'publication': 'Thursday morning per publisher; exact publication timestamp unavailable.',
            'respondent_count': None, 'representative_population_claim': False, 'confidence_interval': None},
        'publisher_gauge_averages': averages,
        'source_evidence': {key: {k: v for k, v in sources.get(key, {}).items() if k != 'raw'} for key in URLS},
        'history_scope': {'rows': len(history), 'first_week': dates[0] if dates else None, 'last_week': as_of,
            'current_vintage': True, 'historical_availability_known': False, 'full_1987_history': False,
            'note': 'All rows in the reviewed public recent-results table. No paid/member history is fetched; prior snapshots remain archived.'},
        'latest': legacy_row(current), 'history_26w': [legacy_row(r) for r in sorted(history, key=lambda r: r['week_ending'])[-26:]],
        'historical_avg': {k: None for k in (*NAMES, 'spread')},
        'z_scores': {k: None for k in ('bullish', 'bearish', 'spread')},
        'extremes': {'is_bullish_extreme': None, 'is_bearish_extreme': None},
        'interpretation': 'Weekly survey observations only. A contrarian return forecast has not been qualified on point-in-time data after costs.',
        'source': 'aaii_labelled_public_sections', 'backfilled_rows': len(history),
        'call': None, 'decision': {'verb': 'WAIT', 'abstain': True, 'reason': 'descriptive_survey_without_qualified_forecast'},
        'calls_eligible': False, 'forecast_qualified': False, 'sizing_eligible': False, 'execution_eligible': False,
        'portfolio_consequences': {'automatic_position_change': False, 'target_weights': None, 'forced_liquidation': False,
            'scenario_page': '/position-sizer.html', 'reason': 'Survey opinions alone do not determine exposure. Portfolio scenarios need explicit holdings and shock assumptions.'}}
    return packet
