"""Isolated whole-source/native-entry/calendar/replay and publication regressions."""
from pathlib import Path
from copy import deepcopy
from io import BytesIO
from types import ModuleType
from unittest.mock import patch
from xml.sax.saxutils import escape
import ast, importlib.util, json, sys, unittest, urllib.error, urllib.parse, zipfile

ROOT = Path(__file__).resolve().parents[4]
SOURCE = Path(__file__).resolve().parents[1] / 'source'
sys.path[:0] = [str(SOURCE), str(ROOT / 'aws/shared')]
import trade_store as store
import trade_measurements as m
AT = '2026-09-27T12:50:00+00:00'
REPORT = 'https://www.cpb.nl/wereldhandelsmonitor/cpb-wereldhandelsmonitor-juli-2026'
WORKBOOK = 'https://www.cpb.nl/system/files/cpbmedia/CPB-world-trade-monitor-july-2026.xlsx'
SITEMAP = ('<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"><url><loc>' + REPORT + '</loc></url>'
           '<url><loc>https://www.cpb.nl/agenda/publicatie-wereldhandelsmonitor-augustus-2026</loc></url></urlset>').encode()
HTML = ('<html><head><meta name="contenttype" content="world_trade_monitor"><meta name="publicationdatetime" content="2026-09-25T10:00:00+00:00">'
        '<meta property="og:url" content="' + REPORT + '"><meta property="og:description" content="Synthetic source description; no numeric parsing.">'
        '<script>window.fakeTradeChange=9999</script></head><body><article class="node node--type-world-trade-monitor">'
        '<a href="' + WORKBOOK + '">Original workbook</a></article></body></html>').encode()


def col(n):
    result = ''
    while n:
        n, r = divmod(n - 1, 26); result = chr(65 + r) + result
    return result


def workbook(mutate=None):
    sheets = {}
    def put(rows, row, column, value, kind='inlineStr', formula=None):
        rows.setdefault(row, {})[column] = {'value': str(value) if value is not None else None, 'kind': kind, 'formula': formula}
    for name in ('trade_out', 'inpro_out'):
        rows = {}; sheets[name] = rows
        put(rows, 1, 'B', 'CPB WORLD TRADE MONITOR')
        put(rows, 2, 'B', 'Merchandise world trade, fixed base 2021=100' if name == 'trade_out' else 'Industrial production volume excluding construction, fixed base 2021=100')
        put(rows, 4, 'B', '21 September 2026  10:00:42')
        for i in range(319):
            put(rows, 4, col(i + 6), m.shift('2000-01', i).replace('-', 'm'))
        if name == 'trade_out':
            sections = {6: 'Volumes, seasonally adjusted', 40: 'Prices / unit values in usd', 74: 'Prices / unit values in usd'}
            indices = {8: 'tgz_w1_qnmi_sn', 42: 'tgz_w1_pdmi_sn', 76: 'hfl_w1_pdmi_nn', 77: 'hpr_w1_pdmi_nn'}
            for base, flow, measure in ((10, 'mgz', 'qnmi_sn'), (25, 'xgz', 'qnmi_sn'), (44, 'mgz', 'pdmi_sn'), (59, 'xgz', 'pdmi_sn')):
                indices.update({base + i: f'{flow}_{region}_{measure}' for i, region in enumerate(m.REGIONS)})
        else:
            sections = {6: 'Import weighted, seasonally adjusted', 24: 'Production weighted, seasonally adjusted'}
            indices = {8: 'ipz_w1_qnmi_sm', 26: 'ipz_w1_qnmi_sp'}
            for base, weight in ((10, 'sm'), (28, 'sp')):
                indices.update({base + i: f'ipz_{region}_qnmi_{weight}' for i, region in enumerate(m.REGIONS[1:])})
        for r, label in sections.items():
            put(rows, r, 'B', label)
        for r, sid in indices.items():
            put(rows, r, 'B', 'Synthetic ' + sid); put(rows, r, 'C', sid); put(rows, r, 'D', 1000, 'n')
            for i in range(319):
                put(rows, r, col(i + 6), str(100 + i / 100 + r / 1000), 'n')
    if mutate:
        mutate(sheets)
    stream = BytesIO()
    with zipfile.ZipFile(stream, 'w', zipfile.ZIP_DEFLATED) as z:
        z.writestr('xl/workbook.xml', f'<workbook xmlns="{m.NS}" xmlns:r="{m.RNS}"><sheets><sheet name="trade_out" r:id="rId1"/><sheet name="inpro_out" r:id="rId2"/></sheets></workbook>')
        z.writestr('xl/_rels/workbook.xml.rels', f'<Relationships xmlns="{m.PNS}">' + ''.join(f'<Relationship Id="rId{i}" Target="worksheets/sheet{i}.xml" Type="{m.RNS}/worksheet"/>' for i in (1, 2)) + '</Relationships>')
        for i, rows in enumerate(sheets.values(), 1):
            body = []
            for r, cells in sorted(rows.items()):
                rendered = []
                for c, item in sorted(cells.items(), key=lambda p: m.col_number(p[0])):
                    value = item['value']; kind = item['kind']; formula = item['formula']
                    content = '<is><t>' + escape(value or '') + '</t></is>' if kind == 'inlineStr' else ('<v>' + escape(value) + '</v>' if value is not None else '')
                    if formula is not None:
                        content += '<f>' + escape(formula) + '</f>'
                    rendered.append(f'<c r="{c}{r}" t="{kind}">{content}</c>')
                body.append(f'<row r="{r}">' + ''.join(rendered) + '</row>')
            # Actual CPB workbook has an incorrect A1 dimension: never use it to truncate.
            z.writestr(f'xl/worksheets/sheet{i}.xml', f'<worksheet xmlns="{m.NS}"><dimension ref="A1"/><sheetData>' + ''.join(body) + '</sheetData></worksheet>')
    return stream.getvalue()


class Error(Exception):
    def __init__(self, code):
        self.response = {'Error': {'Code': code}}


class Memory:
    def __init__(self):
        self.data = {store.HEAD: store.encode({'generated_at': '2026-09-26T12:50:00Z', 'version': '1.0.0',
            'series': {'ocean_ppi': {'q_ann_pct': 1234, 'old_nested': {'zero': 0, 'null': None}}},
            'bdi': {'level': 2222}, 'cpb_wtm': {'period': '2026-8'}, 'rate_pressure': 44,
            'unknown_original': [0, False, None, {'keep': 'all'}]})}
        self.reads = []; self.writes = []; self.denied = set(); self.truncated = set(); self.race = None
    def get_object(self, **kw):
        key = kw['Key']; self.reads.append(key)
        if key in self.denied:
            raise Error('AccessDenied')
        if key not in self.data:
            raise Error('NoSuchKey')
        raw = self.data[key]
        return {'Body': BytesIO(raw), 'ContentLength': len(raw) + (key in self.truncated), 'ETag': store.sha(raw)}
    def put_object(self, **kw):
        key = kw['Key']; raw = kw['Body']
        if self.race:
            self.race(key)
        if key in self.denied:
            raise Error('AccessDenied')
        old = self.data.get(key)
        if kw.get('IfNoneMatch') == '*' and old is not None:
            raise Error('PreconditionFailed')
        if 'IfMatch' in kw and (old is None or kw['IfMatch'] != store.sha(old)):
            raise Error('PreconditionFailed')
        self.data[key] = raw; self.writes.append(key)


class Response(BytesIO):
    def __init__(self, raw, url, status=200, length=None):
        super().__init__(raw); self.url = url; self.status = status
        self.headers = {'Content-Length': str(len(raw) if length is None else length), 'Content-Type': 'application/octet-stream'}
    def getcode(self):
        return self.status
    def geturl(self):
        return self.url


def fred_packets(sid):
    key, unit, title = m.PROFILES[sid]
    meta = {'seriess': [{'id': sid, 'title': title, 'units': unit, 'frequency_short': 'M', 'seasonal_adjustment_short': 'NSA', 'observation_start': '1985-01-01', 'observation_end': '2026-08-01'}]}
    rows = [{'date': m.shift('1985-01', i) + '-01', 'value': str(100 + i / 10), 'realtime_start': AT[:10], 'realtime_end': AT[:10]} for i in range(500)]
    return meta, {'count': len(rows), 'offset': 0, 'units': 'lin', 'output_type': 1, 'observations': rows}


class Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.raw_workbook = workbook()
    def source(self, req, timeout):
        self.calls.append(req.full_url); p = urllib.parse.urlsplit(req.full_url); q = dict(urllib.parse.parse_qsl(p.query))
        if p.netloc == 'api.stlouisfed.org':
            meta, obs = fred_packets(q['series_id']); raw = store.encode(meta if p.path.endswith('/series') else obs)
        elif req.full_url == store.CPB:
            raw = SITEMAP
        elif req.full_url == REPORT:
            raw = HTML
        elif req.full_url == WORKBOOK:
            raw = self.raw_workbook
        elif req.full_url == store.BALTIC:
            raw = b'<html><span id="p">2222</span><script>{"last":2222}</script>Baltic 2222 points</html>'
        else:
            raise AssertionError('Unexpected provider URL')
        return Response(raw, req.full_url)
    def execute(self, mem=None, opener=None):
        mem = mem or Memory(); self.calls = []
        result = store.run(mem, 'b', 'fixture-secret-not-real', at=AT, opener=opener or self.source)
        return mem, store.strict(mem.data[store.HEAD]), result
    def test_full_native_population_originals_and_replay(self):
        mem = Memory(); original = mem.data[store.HEAD]; mem, packet, result = self.execute(mem)
        self.assertEqual(len(self.calls), 12); self.assertEqual(result['cpb_series'], 88)
        self.assertGreater(len(mem.data[store.HEAD]), 40000); self.assertEqual(packet['measurement_review']['cpb']['monthly_positions'], 28072)
        self.assertEqual(sum(r['returned_rows'] for r in packet['measurement_review']['series'].values()), 2000)
        self.assertIn(original, mem.data.values()); self.assertIn(self.raw_workbook, mem.data.values())
        self.assertEqual(packet['unknown_original'], [0, False, None, {'keep': 'all'}])
        self.assertEqual(packet['legacy_pre_research_fields']['rate_pressure'], 44); self.assertIsNone(packet['rate_pressure'])
        self.assertEqual(packet['series']['ocean_ppi']['old_nested'], {'zero': 0, 'null': None})
        self.assertEqual(packet['cpb_wtm']['period'], '2026-07'); self.assertEqual(packet['portfolio_action'], 'WAIT')
        self.assertEqual(packet['bdi']['independent_sources'], 1); self.assertIsNone(packet['bdi']['level']); self.assertEqual(len(packet['bdi']['candidates']), 3)
        self.assertNotIn(b'fixture-secret-not-real', b''.join(mem.data.values()))
        before = list(mem.writes)
        with patch.object(urllib.request, 'urlopen', side_effect=AssertionError('No HTTP during replay')):
            replay = store.replay(mem, 'b', packet)
        self.assertEqual(replay['cpb_monthly_positions'], 28072); self.assertEqual(mem.writes, before)
    def test_real_entry_and_complete_predecessor_functions_preserved(self):
        old = (ROOT / 'tests/fixtures/pre-freight-research-trade-nowcast.py.txt').read_bytes()
        self.assertEqual(store.sha(old), 'ffca69fa5e6ec7d009dab8a1ef0125ae59d8e97734ed6c567f250364a5b45a49')
        originals = {n.name: n for n in ast.parse(old).body if isinstance(n, ast.FunctionDef)}
        current = {n.name: n for n in ast.parse((SOURCE / 'lambda_function.py').read_bytes()).body if isinstance(n, ast.FunctionDef)}
        for name, node in originals.items():
            target = '_legacy_lambda_handler' if name == 'lambda_handler' else name
            other = deepcopy(current[target]); other.name = name
            self.assertEqual(ast.dump(node), ast.dump(other))
        mem = Memory(); boto = ModuleType('boto3'); boto.client = lambda *a, **kw: mem
        secret = ModuleType('managed_secret'); secret.managed_secret = lambda *a, **kw: 'fixture-key'
        spec = importlib.util.spec_from_file_location('trade_native', SOURCE / 'lambda_function.py'); native = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {'boto3': boto, 'managed_secret': secret}):
            spec.loader.exec_module(native)
        with patch.object(store, 'run', return_value={'new_entry': True}) as call:
            self.assertEqual(native.lambda_handler({}, None), {'new_entry': True}); self.assertIs(call.call_args.args[0], mem)
    def test_calendar_not_row_positions_missing_and_zero_latest(self):
        sid = 'IR'; meta, obs = fred_packets(sid); obs['observations'] = obs['observations'][-18:]
        obs['observations'].pop(-13); obs['count'] = len(obs['observations'])
        v = m.fred(sid, meta, obs, AT); self.assertEqual(v['yoy']['status'], 'prior_month_missing')
        self.assertIsNone(v['annualized_three_month_percent']); self.assertEqual(v['latest_month'], '2026-08')
        obs['observations'][-1]['value'] = '.'; v = m.fred(sid, meta, obs, AT)
        self.assertEqual(v['status'], 'latest_missing'); self.assertEqual(v['latest_month'], '2026-08')
        obs['observations'][-1]['value'] = '0'; v = m.fred(sid, meta, obs, AT)
        self.assertEqual(v['level'], 0); self.assertEqual(v['mom']['percent'], -100)
        obs['observations'][-2]['value'] = '0'; self.assertEqual(m.fred(sid, meta, obs, AT)['mom']['status'], 'zero_denominator')
    def test_definition_count_transformation_duplicate_and_invalid_numbers(self):
        sid = 'IQ'
        for kind, expected in [('unit', 'metadata_definition_changed'), ('count', 'incomplete_or_transformed_response'), ('duplicate', 'ambiguous_or_invalid_observations'), ('number', 'ambiguous_or_invalid_observations'), ('future', 'ambiguous_or_invalid_observations')]:
            meta, obs = fred_packets(sid)
            if kind == 'unit': meta['seriess'][0]['units'] = 'Dollars'
            if kind == 'count': obs['count'] += 1
            if kind == 'duplicate': obs['observations'][-1]['date'] = obs['observations'][-2]['date']
            if kind == 'number': obs['observations'][-1]['value'] = '1e-999'
            if kind == 'future': obs['observations'][-1]['date'] = '2027-01-01'
            self.assertEqual(m.fred(sid, meta, obs, AT)['status'], expected)
    def test_actual_report_not_future_agenda_and_no_script_number_parsing(self):
        d = m.cpb_candidates(SITEMAP, AT); self.assertEqual(d['selected_url'], REPORT); self.assertEqual(len(d['candidates']), 2)
        self.assertEqual(d['candidates'][1]['status'], 'agenda_or_unreviewed_route_excluded')
        p = m.cpb_report(HTML, d, AT); self.assertFalse(p['description_used_for_numbers']); self.assertEqual(p['workbook_url'], WORKBOOK)
        for raw in (HTML.replace(b'world_trade_monitor', b'agenda'), HTML.replace(b'2026-09-25T10', b'2026-09-28T10'), HTML.replace(b'july-2026.xlsx', b'august-2026.xlsx')):
            with self.assertRaises(ValueError): m.cpb_report(raw, d, AT)
        with self.assertRaises(ValueError): m.cpb_candidates(b'<!DOCTYPE x><urlset/>', AT)
    def measure(self, raw):
        p = m.cpb_report(HTML, m.cpb_candidates(SITEMAP, AT), AT)
        return m.cpb_workbook(raw, p, AT)
    def test_complete_cpb_population_bad_dimension_and_separate_weight_cells(self):
        v = self.measure(self.raw_workbook)
        self.assertEqual(v['series_count'], 88); self.assertEqual(v['monthly_positions'], 28072)
        self.assertEqual(len(v['calendar']), 319); self.assertEqual(v['world_trade']['latest_month'], '2026-07')
        self.assertLess(v['world_trade']['level'], 200); self.assertEqual(v['world_trade']['base_year_weight_or_value_cell']['value'], '1000')
        self.assertEqual(v['series']['ipz_w1_qnmi_sm']['section'], 'Import weighted, seasonally adjusted')
        self.assertEqual(v['series']['ipz_w1_qnmi_sp']['section'], 'Production weighted, seasonally adjusted')
    def test_workbook_scope_formula_unknown_id_duplicate_and_calendar_drift(self):
        mutations = [lambda s: s['trade_out'][2]['B'].update(value='Trade value in dollars'),
                     lambda s: s['trade_out'][8]['LL'].update(formula='LK8+1'),
                     lambda s: s['trade_out'][8]['C'].update(value='unknown_series'),
                     lambda s: s['trade_out'][10]['C'].update(value='tgz_w1_qnmi_sn'),
                     lambda s: s['trade_out'][4]['LL'].update(value='2026m06'),
                     lambda s: s['inpro_out'][24]['B'].update(value='Import weighted, seasonally adjusted'),
                     lambda s: s['trade_out'][8]['LL'].update(value='NaN')]
        for mutate in mutations:
            with self.subTest(mutate=mutate), self.assertRaises(ValueError): self.measure(workbook(mutate))
    def test_cpb_missing_latest_and_year_month_dont_shift(self):
        def missing(s): s['trade_out'][8]['LL']['value'] = None
        row = self.measure(workbook(missing))['world_trade']; self.assertEqual(row['latest_month'], '2026-07'); self.assertIsNone(row['level'])
        def zero(s): s['trade_out'][8]['LL']['value'] = '0'; s['trade_out'][8]['LK']['value'] = '0'
        row = self.measure(workbook(zero))['world_trade']; self.assertEqual(row['level'], 0); self.assertEqual(row['mom']['status'], 'zero_denominator')
    def test_missing_cpb_headline_is_partial_and_repeat_keeps_original_chain(self):
        raw = workbook(lambda s: s['trade_out'][8]['LL'].update(value=None))
        def source(req, timeout):
            if req.full_url == WORKBOOK:
                self.calls.append(req.full_url); return Response(raw, req.full_url)
            return self.source(req, timeout)
        mem, packet, _ = self.execute(opener=source)
        self.assertFalse(packet['ok']); self.assertEqual(packet['quality']['status'], 'partial_measurements')
        self.assertIsNone(packet['cpb_wtm']['trade_volume_chg_pct'])
        old = mem.data[store.HEAD]; old_legacy = packet['legacy_pre_research_fields']; self.calls = []
        store.run(mem, 'b', 'fixture-key', at='2026-09-28T12:50:00+00:00', opener=self.source)
        updated = store.strict(mem.data[store.HEAD]); self.assertIn(old, mem.data.values())
        self.assertEqual(updated['legacy_pre_research_fields'], old_legacy)
        self.assertEqual(store.replay(mem, 'b', updated)['cpb_series'], 88)
    def test_predecessor_absent_denied_truncated_future_or_invalid_never_bootstraps(self):
        for kind in ('absent', 'denied', 'truncated', 'future', 'malformed'):
            mem = Memory()
            if kind == 'absent': del mem.data[store.HEAD]
            if kind == 'denied': mem.denied.add(store.HEAD)
            if kind == 'truncated': mem.truncated.add(store.HEAD)
            if kind == 'future': mem.data[store.HEAD] = store.encode({'generated_at': '2027-01-01T00:00:00Z'})
            if kind == 'malformed': mem.data[store.HEAD] = b'{"zero":0,"zero":1}'
            before = mem.data.get(store.HEAD)
            with self.assertRaises((ValueError, Error)): self.execute(mem)
            self.assertEqual(mem.data.get(store.HEAD), before); self.assertEqual(self.calls, [])
    def test_retention_failure_never_publishes(self):
        mem = Memory(); before = mem.data[store.HEAD]; original_put = mem.put_object
        def deny(**kw):
            if kw['Key'].startswith(store.PRIVATE): raise Error('AccessDenied')
            return original_put(**kw)
        mem.put_object = deny
        with self.assertRaises(Error): self.execute(mem)
        self.assertEqual(mem.data[store.HEAD], before); self.assertEqual(self.calls, [])
    def test_truncated_redirected_invalid_core_responses_preserve_old_head(self):
        for kind in ('length', 'redirect', 'duplicate', 'workbook'):
            mem = Memory(); before = mem.data[store.HEAD]
            def source(req, timeout):
                response = self.source(req, timeout)
                if kind == 'length': response.headers['Content-Length'] = '999999'
                if kind == 'redirect': response.url = 'https://evil.invalid/'
                if kind == 'duplicate': return Response(b'{"seriess":[],"seriess":[]}', req.full_url)
                if kind == 'workbook' and req.full_url == WORKBOOK: return Response(b'not a workbook', req.full_url)
                return response
            with self.assertRaises(ValueError): self.execute(mem, source)
            self.assertEqual(mem.data[store.HEAD], before)
    def test_429_is_retained_and_never_retried(self):
        mem = Memory(); before = mem.data[store.HEAD]
        def core(req, timeout):
            self.calls.append(req.full_url)
            return Response(b'{"error":"rate limited"}', req.full_url, 429)
        with self.assertRaises(ValueError): self.execute(mem, core)
        self.assertEqual(len(self.calls), 1); self.assertEqual(mem.data[store.HEAD], before)
        def optional(req, timeout):
            if req.full_url == store.BALTIC:
                self.calls.append(req.full_url); return Response(b'Rate limited', req.full_url, 429)
            return self.source(req, timeout)
        mem, packet, _ = self.execute(opener=optional)
        self.assertEqual(len(self.calls), 12); self.assertEqual(packet['bdi']['status'], 'source_unavailable')
        self.assertEqual(store.replay(mem, 'b', packet)['provider_attempts'], 12)
    def test_partial_optional_response_or_retention_is_not_transport_fallback(self):
        mem = Memory(); before = mem.data[store.HEAD]
        class Broken(Response):
            def read(self, *args): raise OSError('broken stream')
        def source(req, timeout):
            if req.full_url == store.BALTIC: return Broken(b'x', req.full_url)
            return self.source(req, timeout)
        with self.assertRaises(ValueError): self.execute(mem, source)
        self.assertEqual(mem.data[store.HEAD], before)
    def test_one_conditional_publication_preserves_foreign_writers(self):
        mem = Memory(); foreign = b'{"foreign":"do not overwrite"}'
        def race(key):
            if key == store.HEAD: mem.data[key] = foreign
        mem.race = race
        with self.assertRaises(Error): self.execute(mem)
        self.assertEqual(mem.data[store.HEAD], foreign)
    def test_whole_replay_detects_field_original_and_compiler_tampering(self):
        mem, packet, _ = self.execute()
        bad = deepcopy(packet); bad['series']['import_prices']['level'] += 1
        with self.assertRaises(ValueError): store.replay(mem, 'b', bad)
        bad = deepcopy(packet); bad['sizing_eligible'] = True
        with self.assertRaises(ValueError): store.replay(mem, 'b', bad)
        ref = packet['publication_context']['manifest']; old = mem.data[ref['key']]; mem.data[ref['key']] += b' '
        with self.assertRaises(ValueError): store.replay(mem, 'b', packet)
        mem.data[ref['key']] = old
        with patch.object(store, 'compiler_hashes', return_value={'changed': 'x'}), self.assertRaises(ValueError): store.replay(mem, 'b', packet)
    def test_http_generation_and_expired_runtime_do_no_reads_or_writes(self):
        mem = Memory(); result = store.run(mem, 'b', '', {'httpMethod': 'GET'})
        self.assertEqual(result['statusCode'], 409); self.assertEqual(mem.reads, []); self.assertEqual(mem.writes, [])
        class Context:
            def get_remaining_time_in_millis(self): return 1000
        with self.assertRaises(ValueError): store.run(mem, 'b', 'fixture', context=Context(), at=AT, opener=self.source)
        self.assertEqual(mem.reads, [])
    def test_credential_and_provider_request_boundaries(self):
        for url in ('http://www.cpb.nl/sitemap.xml', 'https://evil.invalid/x', store.CPB + '?key=secret', 'https://api.stlouisfed.org/fred/series?series_id=IR&file_type=json&api_key=x'):
            with self.assertRaises(ValueError): store.identity(url)
        url = 'https://api.stlouisfed.org/fred/series?' + urllib.parse.urlencode({'series_id': 'IR', 'file_type': 'json', 'api_key': 'never-store-this', 'realtime_start': AT[:10], 'realtime_end': AT[:10]})
        self.assertNotIn('never-store-this', store.encode(store.identity(url)).decode())
    def test_numeric_grammar_and_overflow_comparisons_remain_unavailable(self):
        for value in ('1_000', 'NaN', 'Infinity', True, '1e-999', '-1'):
            with self.assertRaises(ValueError): m.number(value)
        result = m.change(m.number('1e15'), m.number('1e-300'), '2026-07', '2025-07')
        self.assertEqual(result['status'], 'outside_numeric_range'); self.assertIsNone(result['percent'])
        self.assertEqual(m.number('0e-999'), 0)


if __name__ == '__main__':
    unittest.main()
