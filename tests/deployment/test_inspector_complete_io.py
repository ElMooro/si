"""The shared complete reader must be present before any consumer can execute."""
from pathlib import Path
import hashlib,re,sys
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
from build_page_data_contracts import install_html

def test_inspector_installs_exact_reader_before_inspection_without_duplicate_execution():
    version=hashlib.sha256((ROOT/'jh-evidence-io.js').read_bytes()).hexdigest()[:16]
    original='<html><head><script src="/jh-evidence-io.js?v=old" defer></script><script src="/jh-portfolio-scenario-io.js" defer></script></head><body>Full source</body></html>'
    built=install_html(original,[],{'outputs':[]})
    assert built.count('src="/jh-evidence-io.js?')==1
    assert '/jh-evidence-io.js?v='+version in built
    assert built.index('/jh-evidence-io.js')<built.index('/jh-data-inspector.js')<built.index('/jh-portfolio-scenario-io.js')
    assert install_html(built,[],{'outputs':[]})==built

def test_raw_portfolio_pages_load_reader_before_scenario_adapter_with_matching_execution_order():
    for page in ('portfolio/index.html','position-sizer.html'):
        text=(ROOT/page).read_text(encoding='utf-8')
        reader=re.search(r'<script[^>]+src="/jh-evidence-io\.js[^>]+>',text)
        adapter=re.search(r'<script[^>]+src="/jh-portfolio-scenario-io\.js[^>]+>',text)
        assert reader and adapter and reader.start()<adapter.start()
        assert (' defer' in reader[0])==(' defer' in adapter[0])
        assert ' async' not in reader[0] and ' async' not in adapter[0]
