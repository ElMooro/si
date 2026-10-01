"""Actual alert boundary, entirely offline: no SDK import or message transport."""
import ast
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / 'aws/shared'))
from capital_contract import authority_view, finite, timestamp, publication_summary, CRITICAL_SLAS

NOW = datetime(2026, 10, 1, 15, tzinfo=timezone.utc)

class Clock(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW


def packet():
    stamp = (NOW - timedelta(minutes=20)).isoformat()
    expiry = (NOW + timedelta(hours=1)).isoformat()
    raw = dict(engine='justhodl-khalid-risk', schema_version='1.0.0', status='OK',
               generated_at=stamp, expires_at=expiry, exposure_cap_pct=100,
               policy=dict(mode='SELECTIVE_RISK_ON', allows_new_entries=True, exposure_cap_pct=100),
               hard_vetoes=[], critical_failures=[], source_health=[
                   dict(name=k, critical=True, status='FRESH', as_of=stamp, max_age_h=v)
                   for k, v in CRITICAL_SLAS.items()])
    return dict(engine='justhodl-katlin', schema='1.1', generated_at=stamp,
                expires_at=expiry, research_generated_at=stamp, research_status='FRESH', session='2026-10-01',
                war_room=dict(entries_allowed=True, exposure_cap_pct=100, posture='FULL_RISK', thermometer=25,
                              hold_reasons=[], vetoes=[], expires_at=expiry, authority=authority_view(raw, NOW)),
                basket=dict(core=[dict(ticker='AAA', weight_pct=10)], barbell=[]),
                changes=dict(new_prime=['AAA']), picks=[dict(ticker='AAA', tier='KATLIN_PRIME',
                name='Example', sniper=dict(state='SNIPE_NOW'), composite=90)])


def actual(d):
    tree = ast.parse((ROOT / 'aws/lambdas/justhodl-alert-router/source/lambda_function.py').read_text(encoding='utf-8'))
    nodes = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in ('katlin_alert_permission', 'check_katlin')]
    ns = dict(datetime=Clock, timezone=timezone, timedelta=timedelta, authority_view=authority_view,
              finite=finite, timestamp=timestamp, publication_summary=publication_summary, load_json=lambda _: d, track_errors=lambda fn: fn)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), 'actual-katlin-alerts', 'exec'), ns)
    result = []
    ns['check_katlin'](result)
    return result


class KatlinPermission(unittest.TestCase):
    def test_valid_original_alerts_preserved(self):
        alerts = actual(packet())
        self.assertEqual([a['id'] for a in alerts], ['katlin_prime_AAA_2026-10-01', 'katlin_snipe_AAA_2026-10-01', 'katlin_posture_FULL_RISK_2026-10-01'])
        self.assertEqual([a['severity'] for a in alerts], ['HIGH', 'MEDIUM', 'LOW'])
        self.assertIn('plan: stop', alerts[0]['detail'])
        for posture in ('SELECTIVE', 'DEFENSIVE'):
            p = packet(); p['war_room']['posture'] = posture
            self.assertEqual(len(actual(p)), 2)

    def test_missing_invalid_and_blocked(self):
        for value in (None, [], 12, {}, {'war_room': []}):
            with self.subTest(value=value): self.assertEqual(actual(value), [])
        for field, value in [('entries_allowed', False), ('entries_allowed', 'true'), ('entries_allowed', 1),
                             ('exposure_cap_pct', 0), ('exposure_cap_pct', False), ('exposure_cap_pct', '100'),
                             ('exposure_cap_pct', float('nan')), ('exposure_cap_pct', 101), ('exposure_cap_pct', -1),
                             ('posture', 'DATA_HOLD'), ('posture', 'UNKNOWN'), ('hold_reasons', ['funding unavailable']),
                             ('vetoes', ['raw gate veto']), ('authority', []), ('authority', {})]:
            p = packet(); p['war_room'][field] = value
            with self.subTest(field=field, value=value): self.assertEqual(actual(p), [])
        for field in ('engine', 'schema', 'generated_at', 'expires_at', 'research_generated_at', 'research_status', 'session', 'basket'):
            p = packet(); del p[field]
            with self.subTest(missing=field): self.assertEqual(actual(p), [])

    def test_clock_failures_and_expiry_boundaries(self):
        for field, value in [('generated_at', (NOW + timedelta(seconds=1)).isoformat()),
                             ('generated_at', '2026-10-01T15:00:00'), ('generated_at', 'bad'),
                             ('expires_at', NOW.isoformat()), ('expires_at', (NOW - timedelta(seconds=1)).isoformat()),
                             ('expires_at', (NOW + timedelta(days=2)).isoformat()),
                             ('research_generated_at', (NOW - timedelta(hours=37)).isoformat()),
                             ('research_generated_at', (NOW + timedelta(seconds=1)).isoformat()),
                             ('session', '2026-09-26'), ('session', '2026-10-02'), ('research_status', 'STALE')]:
            p = packet(); p[field] = value
            with self.subTest(field=field, value=value): self.assertEqual(actual(p), [])
        p = packet(); p['war_room']['expires_at'] = NOW.isoformat()
        self.assertEqual(actual(p), [])
        p = packet(); p['permission_refreshed_at'] = p['generated_at']; p['research_generated_at'] = (NOW - timedelta(hours=37)).isoformat()
        self.assertEqual(actual(p), [])
        p = packet(); p['expires_at'] = p['war_room']['expires_at'] = (NOW + timedelta(seconds=1)).isoformat()
        self.assertEqual(len(actual(p)), 3)

    def test_authority_revalidated_not_cached_fresh(self):
        mutations = [('expires_at', NOW.isoformat()), ('generated_at', (NOW + timedelta(seconds=1)).isoformat()),
                     ('generated_at', (NOW - timedelta(hours=25)).isoformat()), ('source', 'other'),
                     ('schema_version', '0'), ('status', 'STALE'), ('engine_status', 'DATA_HOLD'),
                     ('allows_new_entries', False), ('exposure_cap_pct', 65), ('mode', {}),
                     ('hard_vetoes', ['veto']), ('critical_failures', ['source']), ('source_health', [])]
        for field, value in mutations:
            p = packet(); p['war_room']['authority'][field] = value
            with self.subTest(field=field): self.assertEqual(actual(p), [])
        for value in (None, 'bad', (NOW + timedelta(seconds=1)).isoformat(), (NOW - timedelta(days=10)).isoformat()):
            p = packet(); p['war_room']['authority']['source_health'][0]['as_of'] = value
            with self.subTest(source_clock=value): self.assertEqual(actual(p), [])

    def test_basket_constraints(self):
        for value in (True, '10', float('inf'), -1, 11):
            p = packet(); p['basket']['core'][0]['weight_pct'] = value
            with self.subTest(weight=value): self.assertEqual(actual(p), [])
        p = packet(); p['war_room']['exposure_cap_pct'] = 5
        self.assertEqual(actual(p), [])

    def test_cash_diagnostic_is_explicit_and_never_actionable(self):
        p = packet(); p['war_room'].update(posture='CASH_OR_TBILLS', entries_allowed=False, exposure_cap_pct=0)
        alerts = actual(p)
        self.assertEqual(len(alerts), 1)
        self.assertIn('Research diagnostic only; no entry permission.', alerts[0]['detail'])
        self.assertIn(p['generated_at'], alerts[0]['detail'])
        p['generated_at'] = (NOW - timedelta(hours=25)).isoformat()
        self.assertEqual(actual(p), [])

    def test_input_not_mutated_and_no_transport(self):
        p = packet(); before = deepcopy(p); actual(p)
        self.assertEqual(p, before)

if __name__ == '__main__': unittest.main(verbosity=2)
