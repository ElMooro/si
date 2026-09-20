"""Synthetic publisher layouts: arithmetic, scope, clocks and disagreement faults."""
from datetime import date, timedelta
from pathlib import Path
import copy
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'source'))
import aaii_research_model as m

AT = '2026-09-20T14:00:00+00:00'


def fixtures(anchor=date(2026, 9, 16), count=22):
    dates = [anchor-timedelta(days=7*i) for i in range(count)]
    readings = [('28.8', '17.9', '53.3'), ('38.0', '22.7', '39.3'), ('39.7', '22.7', '37.6'), ('32.9', '22.6', '44.4')]
    triples = [readings[i % 4] for i in range(count)]
    gauge = '<span class="ssv2-gauge-week">Week ending '+anchor.strftime('%B %d, %Y')+'</span><div class="ssv2-gauge-bars">'
    for name, value, avg in zip(m.NAMES, triples[0], ('37.5', '31.0', '31.5')):
        gauge += f'<div class="ssv2-sbar"><span class="ssv2-slabel">{name.title()}</span><span class="ssv2-snum">{value}%</span><span class="ssv2-savg">Avg {avg}%</span></div>'
    gauge += '</div><div class="ssv2-spread-pill">▼ Bull–Bear Spread: -24.5 pp</div>'
    recent = '<div class="ssv2-card"><h3>Recent weekly results</h3>'
    for day, triple in zip(dates[:4], triples[:4]):
        recent += '<div class="datebars"><div class="date">'+day.strftime('%m/%d/%Y')+'</div><div class="bars">'
        recent += ''.join(f'<div class="bar {k}">{v}%</div>' for k, v in zip(m.NAMES, triple))+'</div></div>'
    recent += '</div>'
    # This misleading, valid-summing historical example must never become current.
    marketing = '<div class="ssv2-card"><h3>Historical view</h3><div class="datebars"><div class="date">Historical Averages</div><div class="bars"><div class="bullish">100.0%</div><div class="neutral">0.0%</div><div class="bearish">0.0%</div></div></div></div><table><tr><td>September 17, 2026</td><td>100%</td><td>0%</td><td>0%</td></tr></table>'
    table = '<table><tr><td>Reported Date</td><td>Bullish</td><td>Neutral</td><td>Bearish</td></tr>'
    for day, triple in zip(dates, triples):
        table += '<tr><td>'+day.strftime('%B %d')+'</td>'+''.join('<td>'+v+'%</td>' for v in triple)+'</tr>'
    table += '</table>'
    return {'main': {'raw': ('<!DOCTYPE html><html><body>'+gauge+recent+marketing+'</body></html>').encode(), 'acquired_at': AT},
            'results': {'raw': ('<!DOCTYPE html><html><body>'+table+'</body></html>').encode(), 'acquired_at': AT}}


class NativeTests(unittest.TestCase):
    def test_realistic_layout_and_legacy_units_reconcile(self):
        packet = m.compute(fixtures(), AT)
        self.assertEqual(packet['quality']['status'], 'fresh')
        self.assertEqual(packet['observation']['bull_bear_spread_pp'], -24.5)
        self.assertEqual(packet['latest']['bull_bear_spread'], -.245)
        self.assertEqual(packet['latest']['bullish'], .288)
        self.assertEqual(packet['history_scope']['rows'], 22)
        self.assertEqual(packet['history'][0]['week_ending'], '2026-04-22')
        self.assertEqual(len(packet['quality']['cross_checked_weeks']), 4)
        self.assertEqual(packet['history'][-4]['printed_sum_pct'], 99.9)

    def test_no_source_arithmetic_becomes_forecast_authority(self):
        packet = m.compute(fixtures(), AT)
        for key in ('calls_eligible', 'forecast_qualified', 'sizing_eligible', 'execution_eligible'):
            self.assertIs(packet[key], False)
        self.assertIsNone(packet['call']); self.assertTrue(packet['decision']['abstain'])
        self.assertTrue(all(v is None for v in packet['z_scores'].values()))
        self.assertTrue(all(v is None for v in packet['extremes'].values()))
        self.assertIsNone(packet['survey']['respondent_count'])

    def test_missing_year_never_uses_machine_date(self):
        sources = fixtures(); sources['main']['raw'] = sources['main']['raw'].replace(b'Week ending September 16, 2026', b'Week ending September 16')
        packet = m.compute(sources, AT)
        self.assertEqual(packet['quality']['status'], 'unavailable'); self.assertIsNone(packet['observation'])

    def test_year_rollover_uses_publisher_anchor(self):
        sources = fixtures(date(2026, 1, 7))
        current, _, _ = m.parse_main(sources['main']['raw'])
        rows = m.parse_results(sources['results']['raw'], date.fromisoformat(current['week_ending']))
        self.assertEqual(rows[1]['week_ending'], '2025-12-31')
        self.assertEqual(rows[-1]['week_ending'], '2025-08-13')

    def test_marketing_and_performance_tables_are_not_survey_rows(self):
        packet = m.compute(fixtures(), AT)
        self.assertEqual(packet['as_of'], '2026-09-16')
        self.assertEqual(packet['observation']['bullish_pct'], 28.8)
        self.assertNotIn('2026-09-17', [r['week_ending'] for r in packet['history']])

    def test_cross_page_disagreement_abstains(self):
        sources = fixtures(); sources['results']['raw'] = sources['results']['raw'].replace(b'28.8%', b'29.8%', 1).replace(b'53.3%', b'52.3%', 1)
        self.assertEqual(m.compute(sources, AT)['quality']['status'], 'unavailable')

    def test_main_gauge_and_dated_table_disagreement_abstains(self):
        sources = fixtures(); raw = sources['main']['raw']; i = raw.index(b'<h3>Recent weekly results')
        sources['main']['raw'] = raw[:i]+raw[i:].replace(b'28.8%', b'29.8%', 1).replace(b'53.3%', b'52.3%', 1)
        self.assertEqual(m.compute(sources, AT)['quality']['status'], 'unavailable')

    def test_wrong_column_order_or_duplicate_survey_table_fails(self):
        for transform in (lambda raw: raw.replace(b'<td>Bullish</td><td>Neutral</td>', b'<td>Neutral</td><td>Bullish</td>'), lambda raw: raw+raw):
            sources = fixtures(); sources['results']['raw'] = transform(sources['results']['raw'])
            self.assertEqual(m.compute(sources, AT)['quality']['status'], 'unavailable')

    def test_duplicate_current_anchor_or_changed_label_fails(self):
        for transform in (lambda raw: raw.replace(b'</body>', b'<span class="ssv2-gauge-week">Week ending September 16, 2026</span></body>'),
                          lambda raw: raw.replace(b'>Bullish</span>', b'>Prices</span>')):
            sources = fixtures(); sources['main']['raw'] = transform(sources['main']['raw'])
            self.assertEqual(m.compute(sources, AT)['quality']['status'], 'unavailable')

    def test_rounding_preserved_without_renormalizing(self):
        row = m.observation(date(2026, 9, 16), [m.Decimal(v) for v in ('32.9', '22.6', '44.4')], [])
        self.assertEqual(row['printed_sum_pct'], 99.9)
        self.assertEqual(row['bull_bear_spread_pp'], -11.5)
        with self.assertRaises(ValueError): m.observation(date(2026, 9, 16), [m.Decimal(v) for v in ('32.9', '22.6', '44.3')], [])

    def test_mathematically_valid_endpoints_not_arbitrarily_excluded(self):
        row = m.observation(date(2026, 9, 16), [m.percentage(v) for v in ('100.0%', '0.0%', '0.0%')], [])
        self.assertEqual(row['bullish_pct'], 100)
        for text in ('NaN%', 'Infinity%', '100.1%', '-0.1%', '28.8', '28.8% later 40%'):
            with self.assertRaises(ValueError): m.percentage(text)

    def test_source_deadline_is_not_generation_freshness(self):
        packet = m.compute(fixtures(), '2026-09-24T19:00:00+00:00')
        self.assertEqual(packet['quality']['status'], 'stale')
        self.assertEqual(packet['as_of'], '2026-09-16')
        self.assertEqual(packet['quality']['freshness']['source_due_at'], '2026-09-24T19:00:00+00:00')

    def test_open_survey_and_unzoned_generation_rejected(self):
        self.assertEqual(m.compute(fixtures(), '2026-09-16T22:00:00+00:00')['quality']['status'], 'unavailable')
        with self.assertRaises(ValueError): m.compute(fixtures(), '2026-09-20T14:00:00')

    def test_missing_history_week_is_exposed(self):
        sources = fixtures(); raw = sources['results']['raw']
        start = raw.index(b'<tr><td>August 12'); end = raw.index(b'</tr>', start)+5
        sources['results']['raw'] = raw[:start]+raw[end:]
        packet = m.compute(sources, AT)
        self.assertEqual(packet['quality']['history_missing_weeks'], ['2026-08-12'])
        self.assertEqual(packet['history_scope']['rows'], 21)

    def test_acquisition_failure_stays_unavailable_and_does_not_mutate_inputs(self):
        sources = fixtures(); sources['results'] = {'error': 'provider_http_403'}; before = copy.deepcopy(sources)
        packet = m.compute(sources, AT)
        self.assertEqual(sources, before); self.assertEqual(packet['quality']['status'], 'unavailable')
        self.assertTrue(all(v is None for v in packet['latest'].values()))

    def test_original_input_size_and_utf8_bounds(self):
        for raw in (b'x'*(2*1024*1024+1), b'\xff\xfe'):
            with self.assertRaises((ValueError, UnicodeError)): m.Document(raw)


if __name__ == '__main__': unittest.main()
