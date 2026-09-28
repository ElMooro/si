"""Pure source labels cannot add network reads to the existing page."""
from pathlib import Path
import sys,unittest
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'scripts'))
from page_sources import page_graph,source_analysis


class Tests(unittest.TestCase):
    def test_renderer_is_display_only_without_network_storage_or_dynamic_calls(self):
        source=(ROOT/'jh-theme-cascade-evidence.js').read_text(encoding='utf-8')
        self.assertIsNone(source_analysis(source).get('error'));self.assertEqual(source_analysis(source)['imports'],[])
        self.assertNotRegex(source,r'\b(fetch|XMLHttpRequest|WebSocket|EventSource|importScripts|sendBeacon|localStorage|sessionStorage|indexedDB|eval|Function)\b')
    def test_primary_source_and_existing_access_graph_remain(self):
        graph=page_graph(ROOT,ROOT/'pre-pump-radar.html')
        self.assertIn('data/theme-cascade.json',graph['keys']);self.assertIn('jh-theme-cascade-evidence.js',graph['scripts'])
        self.assertNotIn('macro/regime.json',graph['keys']);self.assertNotIn('etf-flows/stock-exposure-lookup.json',graph['keys'])
        self.assertEqual(graph['script_parse_errors'],[])


if __name__=='__main__':unittest.main(verbosity=2)
