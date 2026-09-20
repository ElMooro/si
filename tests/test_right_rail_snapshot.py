from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
import json,re,sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'scripts'))
import bake_right_rail as rail


class RightRailSnapshot(unittest.TestCase):
    def test_offline_build_keeps_source_links_without_fetching_research_or_claiming_freshness(self):
        with TemporaryDirectory() as directory:
            root=Path(directory)
            (root/'nav-manifest.json').write_text(json.dumps({'categories':[{'pages':[{'href':'/defcon.html','title':'Crisis &amp; Research'}]}]}))
            source='<html><head><meta name="description" content="An &lt;/script&gt; source-description injection &amp; other text."></head><body>'+' '*2100+'<a href="/data/crisis-composite.json">Crisis</a></body></html>'
            (root/'defcon.html').write_text(source)
            with patch('urllib.request.urlopen',side_effect=AssertionError('No network in offline build')):
                self.assertEqual(rail.main(str(root),live=False),1)
            text=(root/'defcon.html').read_text();payload=re.search(r'window\.__jhRail=(.*?);</script>',text).group(1)
            data=json.loads(payload)
            self.assertEqual(data['title'],'Crisis & Research');self.assertIsNotNone(data['snapshot_at'])
            self.assertEqual(data['feeds'][0]['href'],'/data/crisis-composite.json');self.assertIsNone(data['feeds'][0]['modified_at'])
            self.assertEqual(data['research'],{'href':'/panels.html','kind':'reference_only'})
            self.assertNotIn('</script>',payload);self.assertNotIn('pressure',payload)
            self.assertEqual(rail.main(str(root),live=False),0)

    def test_missing_or_future_file_header_is_not_freshness(self):
        class Response:
            def __init__(self,header):self.headers={'Last-Modified':header}
            def __enter__(self):return self
            def __exit__(self,*args):return False
        for header in ('bad','Sun, 20 Sep 2099 11:00:00 GMT',None):
            with patch('urllib.request.urlopen',return_value=Response(header)) as opened:
                self.assertIsNone(rail.fetch_metadata('data/crisis-composite.json',True))
                self.assertEqual(opened.call_args.args[0].method,'HEAD')


if __name__=='__main__':unittest.main()
