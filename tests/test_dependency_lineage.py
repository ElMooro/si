from pathlib import Path
import hashlib
import json
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
import build_dependency_map as graph
import page_sources


def engine(reads=(), writes=()):
    return {'reads': list(reads), 'writes': list(writes)}


class Tests(unittest.TestCase):
    def test_shared_ancestors_unknowns_and_conflicting_writers_remain_distinct(self):
        engines = {'root': engine(('provider/unresolved.json',), ('root.json',)),
                   'a': engine(('root.json',), ('a.json',)), 'b': engine(('root.json',), ('b.json',)),
                   'other': engine((), ('root.json',))}
        writers = {'root.json': ['root', 'other'], 'a.json': ['a'], 'b.json': ['b']}
        result = graph.lineage(engines, {'desk.html': {'keys': ['a.json', 'b.json']}}, writers)
        page = result['page_lineage']['desk.html']
        self.assertEqual(page['upstream_engines'], ['a', 'b', 'other', 'root'])
        self.assertEqual(page['unresolved_keys'], ['provider/unresolved.json'])
        self.assertEqual(page['ambiguous_writer_keys'], ['root.json'])
        self.assertIsNone(page['independent_evidence_count'])
        self.assertIs(page['calls_eligible'], False)

    def test_long_cycles_and_self_history_are_bounded_without_recursion(self):
        engines = {str(i): engine((str((i+1) % 1500) + '.json',), (str(i)+'.json',)) for i in range(1500)}
        writers = {str(i)+'.json': [str(i)] for i in range(1500)}
        engines['0']['reads'].append('0.json')
        result = graph.lineage(engines, {'desk': {'keys': ['0.json']}}, writers)
        self.assertEqual(len(result['cyclic_engine_components']), 1)
        self.assertEqual(len(result['cyclic_engine_components'][0]), 1500)
        self.assertEqual(len(result['page_lineage']['desk']['upstream_engines']), 1500)
        self.assertEqual(result['self_read_keys'], {'0': ['0.json']})
        simple = graph.lineage({'own': engine(('self.json',), ('self.json',))}, {}, {'self.json': ['own']})
        self.assertEqual(simple['cyclic_engine_components'], [])

    def test_missing_or_duplicate_inventory_is_not_an_empty_fleet(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaises(FileNotFoundError): graph.engine_manifest(root)
            for value in ({'engines': []}, {'engines': [{'engine': 'a'}, {'engine': 'a'}]}):
                (root/'engine-manifest.json').write_text(json.dumps(value), encoding='utf-8')
                with self.assertRaises(ValueError): graph.engine_manifest(root)

    def test_every_rendered_population_survives_old_display_cutoffs(self):
        names = [f'data/{i:03}.json' for i in range(180)]
        model = {'generated_at': '2026-09-28T00:00:00Z', 'note': 'test', 'counts': {'engines_without_consumer': 450, 'engines_without_schedule': 450, 'two_cycles': 80},
                 'input_identity': {'sha256': 'a'*64}, 'lineage': {'scope': 'static', 'cyclic_engine_components': [['x','y','z']]},
                 'orphan_page_refs': names, 'orphan_engine_refs': names, 'page_readers': {k: [f'page{i}' for i in range(9)] for k in names},
                 'engine_readers': {k: [f'engine{i}' for i in range(9)] for k in names}, 'duplicate_writers': {k: ['a','b'] for k in names},
                 'engines_without_consumer': [f'unused{i}' for i in range(450)], 'engines_without_schedule': [f'unscheduled{i}' for i in range(450)],
                 'two_cycles': [(f'a{i}',f'b{i}') for i in range(80)]}
        text = graph.render_md(model)
        self.assertEqual(text, graph.render_md(json.loads(json.dumps(model, sort_keys=True))))
        for value in ('data/179.json','page8','engine8','unused449','unscheduled449','a79 <-> b79','x, y, z'):
            self.assertIn(value, text)

    def test_utf8_is_explicit_and_invalid_bytes_do_not_disappear(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); source = root/'aws/lambdas/demo/source'; source.mkdir(parents=True)
            code = source/'lambda_function.py'; code.write_bytes(b'# invalid \xff')
            with self.assertRaises(UnicodeDecodeError): graph.scan_engines(root, {'demo': {'keys': [], 'reads': []}})
            page = root/'page.html'; page.write_bytes(b'<html>\xff</html>')
            with self.assertRaises(UnicodeDecodeError): page_sources.scan_pages(root)
            code.write_text('# 金融\nx = 1\n', encoding='utf-8')
            got = graph.scan_engines(root, {'demo': {'keys': [], 'reads': ['data/bound-helper.json']}})
            self.assertIn('data/bound-helper.json', got['demo']['reads'])

    def test_same_process_asset_revision_invalidates_parse_cache(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); page = root/'page.html'; asset = root/'main.js'
            page.write_text('<script src="main.js"></script>', encoding='utf-8')
            asset.write_text('old', encoding='utf-8')
            def parse(code): return {'keys': [code+'.json'] if code else [], 'imports': []}
            with patch.object(page_sources, 'prime_source_analysis'), patch.object(page_sources, 'source_analysis', side_effect=parse):
                self.assertIn('old.json', page_sources.scan_pages(root)['page.html']['keys'])
                asset.write_text('new', encoding='utf-8')
                self.assertIn('new.json', page_sources.scan_pages(root)['page.html']['keys'])

    def test_original_helpers_preserved_exactly(self):
        hashes = {'build_dependency_map.py':'36ff7a34d447b15a2ad5d6d7663b5257a04c13f7775ae60aa960ac7b0bdb55cc',
                  'page_sources.py':'ab7b2467b1b78150a50f13ae931476bfc4a4c24c8309d98e775469addd65102c'}
        for name, expected in hashes.items():
            self.assertEqual(hashlib.sha256((ROOT/'tests/fixtures'/('pre-dependency-lineage-'+name+'.txt')).read_bytes()).hexdigest(), expected)

    def test_complete_source_inventory_binds_repeated_builds_and_shared_edits(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            files = ['engine-manifest.json','scripts/build_dependency_map.py','scripts/page_sources.py',
                     'scripts/gen_engine_manifest.py','scripts/js_source_refs.cjs',
                     'aws/shared/helper.py','aws/lambdas/a/source/lambda_function.py',
                     'aws/lambdas/a/config.json','desk.html','asset.js']
            for name in files:
                path = root/name; path.parent.mkdir(parents=True,exist_ok=True); path.write_text('original',encoding='utf-8')
            pages = {'desk.html': {'scripts': ['asset.js']}}
            first = graph.source_identity(root,pages)
            self.assertEqual(first,graph.source_identity(root,pages))
            self.assertEqual([v['path'] for v in first['files']],sorted(files))
            (root/'aws/shared/helper.py').write_text('revised',encoding='utf-8')
            self.assertNotEqual(first['sha256'],graph.source_identity(root,pages)['sha256'])


if __name__ == '__main__': unittest.main()
