"""Whole actual asset-stamper regression; static sources and invented revisions only."""
from pathlib import Path
import hashlib
import json
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
import stamp_assets


def main():
    manifest = json.loads((ROOT / 'tests/fixtures/website-research-status/imports-preservation.json').read_bytes())
    widget = (ROOT / 'jh-ai-insights.js').read_bytes()
    with tempfile.TemporaryDirectory(prefix='website-status-imports-') as temporary:
        site = Path(temporary)
        (site / 'jh-ai-insights.js').write_bytes(widget)
        for row in manifest['pages']:
            (site / row['page']).write_bytes((ROOT / row['predecessor']).read_bytes())
        versions, mode = stamp_assets.compute_versions(site)
        assert mode == 'topo'
        stamp_assets.rewrite_html(site, versions)
        for row in manifest['pages']:
            assert manifest['old'] in (site / row['page']).read_text(encoding='utf-8')
            (site / row['page']).write_bytes((ROOT / row['page']).read_bytes())
        prior_versions = set()
        for suffix in (b'', b'\n/* invented offline content revision */\n'):
            (site / 'jh-ai-insights.js').write_bytes(widget + suffix)
            versions, mode = stamp_assets.compute_versions(site)
            assert mode == 'topo'
            version = versions['/jh-ai-insights.js']
            assert version == hashlib.md5((site / 'jh-ai-insights.js').read_bytes()).hexdigest()[:8]
            assert version not in prior_versions
            prior_versions.add(version)
            stamp_assets.rewrite_html(site, versions)
            for row in manifest['pages']:
                page = (site / row['page']).read_text(encoding='utf-8')
                assert page.count('src="/jh-ai-insights.js?v=' + version + '"') == 1, row['page']
                assert manifest['old'] not in page
            before = {p.name: p.read_bytes() for p in site.iterdir()}
            repeated, repeated_mode = stamp_assets.compute_versions(site)
            stamp_assets.rewrite_html(site, repeated)
            assert repeated == versions and repeated_mode == mode
            assert before == {p.name: p.read_bytes() for p in site.iterdir()}
    print('8 legacy references reproduced; 8 current references follow both asset revisions; idempotent')


if __name__ == '__main__':
    main()
