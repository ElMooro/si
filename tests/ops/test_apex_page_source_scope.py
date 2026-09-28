"""Display-only provenance labels cannot expand a page's source-access contract."""
from pathlib import Path
import sys,re,unittest
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
from page_sources import page_graph,source_analysis


class Tests(unittest.TestCase):
    def test_pure_presenter_has_no_network_storage_or_dynamic_loader_calls(self):
        source=(ROOT/'jh-apex-evidence.js').read_text(encoding='utf-8')
        self.assertIsNone(source_analysis(source).get('error'))
        self.assertEqual(source_analysis(source)['imports'],[])
        self.assertNotRegex(source,r'\b(fetch|XMLHttpRequest|WebSocket|EventSource|importScripts|sendBeacon|localStorage|sessionStorage|indexedDB|eval|Function)\b')
    def test_real_apex_fetch_remains_and_displayed_source_keys_are_not_new_consumers(self):
        graph=page_graph(ROOT,ROOT/'apex.html')
        self.assertIn('data/apex-fusion.json',graph['direct_keys'])
        self.assertIn('jh-apex-evidence.js',graph['scripts'])
        for key in ('data/signal-scorecard.json','data/cascade-validation-log.json','data/pump-positioning.json','data/report.json'):
            self.assertNotIn(key,graph['keys'])
        self.assertEqual(graph['script_parse_errors'],[])


if __name__=='__main__':unittest.main(verbosity=2)
